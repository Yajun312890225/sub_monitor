package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

// ===== 配置 =====

type Config struct {
	ListenAddr    string        `yaml:"listen_addr"`
	CheckInterval Duration      `yaml:"check_interval"`
	Cron          string        `yaml:"cron"`         // 可选：标准 5 段 cron 表达式；留空则用 @every check_interval
	Threshold     float64       `yaml:"threshold"`    // 0~1，达到该使用率即关闭调度
	UsageSource   string        `yaml:"usage_source"` // active(实时拉上游) | passive(读快照)
	DryRun        bool          `yaml:"dry_run"`      // 只记录将要做的调度变更，不真正调用
	StorePath     string        `yaml:"store_path"`   // 用户存储文件路径，默认 data.json
	// TestModels 覆盖各厂商的连通性测试模型（platform -> model_id），留空则用内置默认。
	TestModels map[string]string `yaml:"test_models"`
	Sub2API    Sub2APIConfig     `yaml:"sub2api"`
	Admin      AdminConfig       `yaml:"admin"`
}

// ScheduleSpec 返回传给 robfig/cron 的调度表达式。
// 优先使用显式配置的 cron 表达式；否则用 check_interval 转成 @every 形式。
func (c Config) ScheduleSpec() string {
	if spec := strings.TrimSpace(c.Cron); spec != "" {
		return spec
	}
	return "@every " + c.CheckInterval.Duration.String()
}

type Sub2APIConfig struct {
	BaseURL     string `yaml:"base_url"`
	AdminAPIKey string `yaml:"admin_api_key"`
}

type AdminConfig struct {
	APIKey    string `yaml:"api_key"`    // 本监控服务的管理员 key
	EntryCode string `yaml:"entry_code"` // 暗门固定串：在首页输入即跳转 /admin，留空则关闭暗门
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", raw, err)
	}
	d.Duration = parsed
	return nil
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	cfg := Config{}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}

	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.CheckInterval.Duration == 0 {
		cfg.CheckInterval.Duration = time.Minute
	}
	if cfg.Threshold == 0 {
		cfg.Threshold = 0.9
	}
	if cfg.UsageSource == "" {
		cfg.UsageSource = "active"
	}
	if cfg.StorePath == "" {
		cfg.StorePath = "data.json"
	}

	if cfg.Sub2API.BaseURL == "" {
		return Config{}, errors.New("sub2api.base_url is required")
	}
	if cfg.Sub2API.AdminAPIKey == "" {
		return Config{}, errors.New("sub2api.admin_api_key is required")
	}
	if cfg.Admin.APIKey == "" {
		return Config{}, errors.New("admin.api_key is required")
	}
	if cfg.UsageSource != "active" && cfg.UsageSource != "passive" {
		return Config{}, fmt.Errorf("usage_source must be active or passive, got %q", cfg.UsageSource)
	}

	return cfg, nil
}

// ===== 对外（本监控服务）模型 =====

// AccountQuota 是本监控服务对外暴露的账号额度视图。
// utilization 字段为百分比（0~100，可能超过 100）。
type AccountQuota struct {
	ID                    string     `json:"id"`
	Name                  string     `json:"name"`
	Platform              string     `json:"platform"`
	Status                string     `json:"status"`
	Schedulable           bool       `json:"schedulable"`
	AutoSchedule          bool       `json:"auto_schedule"`       // 是否参与本服务的自动调度（默认 true）
	Concurrency           int        `json:"concurrency"`         // 配置的最大并发
	CurrentConcurrency    int        `json:"current_concurrency"` // 当前实时并发
	FiveHourPercent       float64    `json:"five_hour_percent"`
	SevenDayPercent       float64    `json:"seven_day_percent"`
	SevenDaySonnetPercent float64    `json:"seven_day_sonnet_percent"`
	FiveHourResetsAt      *time.Time `json:"five_hour_resets_at,omitempty"`
	SevenDayResetsAt      *time.Time `json:"seven_day_resets_at,omitempty"`
	UsageError            string     `json:"usage_error,omitempty"`
}

// OverThreshold 判断任一窗口是否达到阈值。threshold 为 0~1 的比例。
func (a AccountQuota) OverThreshold(threshold float64) bool {
	limit := threshold * 100
	return a.FiveHourPercent >= limit ||
		a.SevenDayPercent >= limit ||
		a.SevenDaySonnetPercent >= limit
}

type quotaResponse struct {
	Accounts []AccountQuota `json:"accounts"`
}

// ===== sub2api 真实接口模型 =====

const (
	pathListAccounts = "/api/v1/admin/accounts"
	pathUsageFmt     = "/api/v1/admin/accounts/%s/usage"
	pathSchedulable  = "/api/v1/admin/accounts/%s/schedulable"
	pathTestFmt      = "/api/v1/admin/accounts/%s/test"
	listPageSize     = 200
)

// envelope 是 sub2api 统一响应封装：{"code":0,"message":"success","data":...}
type envelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// rawAccount 仅取本服务关心的账号字段。
type rawAccount struct {
	ID                 json.Number `json:"id"`
	Name               string      `json:"name"`
	Platform           string      `json:"platform"`
	Status             string      `json:"status"`
	Schedulable        bool        `json:"schedulable"`
	Concurrency        int         `json:"concurrency"`         // 配置的最大并发
	CurrentConcurrency int         `json:"current_concurrency"` // 当前实时并发
}

type accountListData struct {
	Items []rawAccount `json:"items"`
	Total int64        `json:"total"`
}

// usageWindow 对应 sub2api 的 UsageProgress（utilization 为百分比 0~100）。
type usageWindow struct {
	Utilization float64    `json:"utilization"`
	ResetsAt    *time.Time `json:"resets_at"`
}

type usageData struct {
	FiveHour       *usageWindow `json:"five_hour"`
	SevenDay       *usageWindow `json:"seven_day"`
	SevenDaySonnet *usageWindow `json:"seven_day_sonnet"`
	Error          string       `json:"error"`
}

type Sub2APIClient struct {
	baseURL    string
	apiKey     string
	source     string
	httpClient *http.Client
}

func NewSub2APIClient(cfg Config) *Sub2APIClient {
	return &Sub2APIClient{
		baseURL:    strings.TrimRight(cfg.Sub2API.BaseURL, "/"),
		apiKey:     cfg.Sub2API.AdminAPIKey,
		source:     cfg.UsageSource,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// ListAccounts 列出全部账号（不限平台，含 anthropic / openai，自动翻页）。
func (c *Sub2APIClient) ListAccounts(ctx context.Context) ([]rawAccount, error) {
	var all []rawAccount
	page := 1
	for {
		q := url.Values{}
		q.Set("page", fmt.Sprint(page))
		q.Set("page_size", fmt.Sprint(listPageSize))

		var data accountListData
		if err := c.getJSON(ctx, pathListAccounts+"?"+q.Encode(), &data); err != nil {
			return nil, err
		}
		all = append(all, data.Items...)

		if len(data.Items) == 0 || int64(len(all)) >= data.Total {
			break
		}
		page++
	}
	return all, nil
}

// GetUsage 获取单账号 5h/7d 窗口使用率。
//
// 注意：部分平台（如 anthropic）在 source=active 下只返回 5h 窗口，7d 数据仅在 passive 快照里。
// 因此当主源缺少 seven_day 时，再拉一次 passive 补齐 7d（及 7d_sonnet），
// 既保证 5h 用实时数据，又能完整展示 7d。openai 的 active 本身含 7d，不会触发这次补拉。
func (c *Sub2APIClient) GetUsage(ctx context.Context, id string) (usageData, error) {
	data, err := c.getUsage(ctx, id, c.source)
	if err != nil {
		return usageData{}, err
	}
	if c.source == "active" && data.SevenDay == nil {
		if passive, perr := c.getUsage(ctx, id, "passive"); perr == nil {
			data.SevenDay = passive.SevenDay
			if data.SevenDaySonnet == nil {
				data.SevenDaySonnet = passive.SevenDaySonnet
			}
		}
	}
	return data, nil
}

// getUsage 按指定 source 拉取单账号用量。
func (c *Sub2APIClient) getUsage(ctx context.Context, id, source string) (usageData, error) {
	q := url.Values{}
	q.Set("source", source)
	var data usageData
	if err := c.getJSON(ctx, fmt.Sprintf(pathUsageFmt, url.PathEscape(id))+"?"+q.Encode(), &data); err != nil {
		return usageData{}, err
	}
	return data, nil
}

// SetSchedulable 开启/关闭账号调度。
func (c *Sub2APIClient) SetSchedulable(ctx context.Context, id string, schedulable bool) error {
	body, _ := json.Marshal(map[string]bool{"schedulable": schedulable})
	var sink json.RawMessage
	return c.do(ctx, http.MethodPost, fmt.Sprintf(pathSchedulable, url.PathEscape(id)), body, &sink)
}

// TestResult 是账号连通性测试的聚合结果（由 sub2api 的 SSE 事件流归并而来）。
type TestResult struct {
	Success bool   `json:"success"`         // 是否收到 test_complete 且成功
	Model   string `json:"model"`           // 实际测试所用模型
	Reply   string `json:"reply"`           // 归并后的模型回复文本
	Error   string `json:"error,omitempty"` // 测试失败时的错误信息
}

// TestAccount 调用 sub2api 的账号测试接口，消费其 SSE 事件流并归并成一次性结果。
//
// 该接口返回 text/event-stream（每行形如 `data: {"type":...}`），事件类型有：
// test_start{model} / content{text} / test_complete{success} / error{error}，
// 不是标准的 {"code":0,...} 封装，因此单独实现、不走 do()。
func (c *Sub2APIClient) TestAccount(ctx context.Context, id, modelID string) (TestResult, error) {
	body, _ := json.Marshal(map[string]string{"model_id": modelID, "prompt": ""})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+fmt.Sprintf(pathTestFmt, url.PathEscape(id)), bytes.NewReader(body))
	if err != nil {
		return TestResult{}, err
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return TestResult{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		return TestResult{}, fmt.Errorf("sub2api POST test status=%d body=%s",
			resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	res := TestResult{Model: modelID}
	sc := bufio.NewScanner(io.LimitReader(resp.Body, 10<<20))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		var ev struct {
			Type    string `json:"type"`
			Model   string `json:"model"`
			Text    string `json:"text"`
			Success bool   `json:"success"`
			Error   string `json:"error"`
		}
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue // 跳过非 JSON 的心跳/注释行
		}
		switch ev.Type {
		case "test_start":
			if ev.Model != "" {
				res.Model = ev.Model
			}
		case "content":
			if len(res.Reply) < 4096 { // 回复文本封顶，避免异常超长响应
				res.Reply += ev.Text
			}
		case "test_complete":
			res.Success = ev.Success
		case "error":
			res.Error = ev.Error
		}
	}
	if err := sc.Err(); err != nil {
		return res, fmt.Errorf("read test stream: %w", err)
	}
	if res.Error != "" { // 出现 error 事件即视为失败
		res.Success = false
	}
	return res, nil
}

func (c *Sub2APIClient) getJSON(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

// do 发请求、校验 envelope.code==0、把 data 解码进 out。
func (c *Sub2APIClient) do(ctx context.Context, method, path string, body []byte, out any) error {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("x-api-key", c.apiKey)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("sub2api %s %s status=%d body=%s", method, path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode sub2api envelope (%s %s): %w", method, path, err)
	}
	if env.Code != 0 {
		return fmt.Errorf("sub2api %s %s code=%d message=%s", method, path, env.Code, env.Message)
	}
	if out == nil || len(env.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		return fmt.Errorf("decode sub2api data (%s %s): %w", method, path, err)
	}
	return nil
}

// ===== 组装额度视图 =====

func quotaFromUsage(acc rawAccount, usage usageData) AccountQuota {
	q := AccountQuota{
		ID:                 acc.ID.String(),
		Name:               acc.Name,
		Platform:           acc.Platform,
		Status:             acc.Status,
		Schedulable:        acc.Schedulable,
		Concurrency:        acc.Concurrency,
		CurrentConcurrency: acc.CurrentConcurrency,
		UsageError:         usage.Error,
	}
	if usage.FiveHour != nil {
		q.FiveHourPercent = usage.FiveHour.Utilization
		q.FiveHourResetsAt = usage.FiveHour.ResetsAt
	}
	if usage.SevenDay != nil {
		q.SevenDayPercent = usage.SevenDay.Utilization
		q.SevenDayResetsAt = usage.SevenDay.ResetsAt
	}
	if usage.SevenDaySonnet != nil {
		q.SevenDaySonnetPercent = usage.SevenDaySonnet.Utilization
	}
	return q
}

// CollectQuotas 列账号 + 并发拉取每个账号的 usage。
func (s *Server) CollectQuotas(ctx context.Context) ([]AccountQuota, error) {
	accounts, err := s.client.ListAccounts(ctx)
	if err != nil {
		return nil, err
	}

	quotas := make([]AccountQuota, len(accounts))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)
	for i, acc := range accounts {
		i, acc := i, acc
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			usage, err := s.client.GetUsage(ctx, acc.ID.String())
			if err != nil {
				quotas[i] = quotaFromUsage(acc, usageData{Error: err.Error()})
				return
			}
			quotas[i] = quotaFromUsage(acc, usage)
		}()
	}
	wg.Wait()
	for i := range quotas {
		quotas[i].AutoSchedule = s.store.AutoScheduleEnabled(quotas[i].ID)
	}
	return quotas, nil
}

// QuotaByID 查询单个账号。
func (s *Server) QuotaByID(ctx context.Context, id string) (AccountQuota, error) {
	var acc rawAccount
	if err := s.client.getJSON(ctx, pathListAccounts+"/"+url.PathEscape(id), &acc); err != nil {
		return AccountQuota{}, err
	}
	var q AccountQuota
	if usage, err := s.client.GetUsage(ctx, id); err != nil {
		q = quotaFromUsage(acc, usageData{Error: err.Error()})
	} else {
		q = quotaFromUsage(acc, usage)
	}
	q.AutoSchedule = s.store.AutoScheduleEnabled(id)
	return q, nil
}

// defaultTestModels 是各厂商的默认连通性测试模型（可被 config.test_models 覆盖）。
var defaultTestModels = map[string]string{
	"anthropic": "claude-sonnet-4-6",
	"openai":    "gpt-5.5",
}

// testModelFor 按账号厂商决定用于连通性测试的模型 ID：
// 优先取 config.test_models 的覆盖值，否则回退到内置默认；未知厂商返回 false。
func (s *Server) testModelFor(platform string) (string, bool) {
	if m, ok := s.cfg.TestModels[platform]; ok && strings.TrimSpace(m) != "" {
		return strings.TrimSpace(m), true
	}
	if m, ok := defaultTestModels[platform]; ok {
		return m, true
	}
	return "", false
}

// runAccountTest 判定测试模型并对账号执行一次连通性测试（供管理员/用户两个入口复用）。
// platformHint 为调用方已知的厂商（可空，空则回查账号详情）；modelOverride 显式指定模型（可空）。
// 返回测试结果、建议的 HTTP 状态码与错误。
func (s *Server) runAccountTest(ctx context.Context, accountID, platformHint, modelOverride string) (TestResult, int, error) {
	modelID := strings.TrimSpace(modelOverride)
	if modelID == "" {
		platform := strings.TrimSpace(platformHint)
		if platform == "" {
			var acc rawAccount
			if err := s.client.getJSON(ctx, pathListAccounts+"/"+url.PathEscape(accountID), &acc); err != nil {
				return TestResult{}, http.StatusBadGateway, err
			}
			platform = acc.Platform
		}
		m, ok := s.testModelFor(platform)
		if !ok {
			return TestResult{}, http.StatusBadRequest,
				fmt.Errorf("厂商 %q 未配置测试模型，请在 config.yaml 的 test_models 中指定", platform)
		}
		modelID = m
	}
	res, err := s.client.TestAccount(ctx, accountID, modelID)
	if err != nil {
		return TestResult{}, http.StatusBadGateway, err
	}
	return res, http.StatusOK, nil
}

// ===== HTTP 服务 =====

type Server struct {
	cfg          Config
	client       *Sub2APIClient
	store        *UserStore
	sessionToken string // 暗门通过后写入浏览器的会话令牌（由 admin key 派生，不可反解）
	mux          *http.ServeMux
}

// 会话 cookie 名：管理员（暗门通过）与普通用户各一个。
const (
	sessionCookieName = "mon_session" // 管理员会话（值为 admin key 派生哈希）
	userCookieName    = "mon_user"    // 用户会话（值为用户 key 的哈希，绑定某账号）
)

type APIKeyInfo struct {
	Admin     bool
	AccountID string
}

func NewServer(cfg Config, store *UserStore) *Server {
	s := &Server{
		cfg:    cfg,
		client: NewSub2APIClient(cfg),
		store:  store,
		// 会话令牌由 admin key 派生：稳定（重启不失效）、不可反解出 admin key。
		sessionToken: hashKey("mon-session:" + cfg.Admin.APIKey),
		mux:          http.NewServeMux(),
	}
	// 兼容旧的 JSON 接口
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /quotas", s.handleQuotas)
	s.mux.HandleFunc("GET /quota/{id}", s.handleQuotaByID)
	// 网页与 Web API（页面、用户查询、管理接口），实现见 web.go
	s.registerWebRoutes()
	return s
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleQuotas(w http.ResponseWriter, r *http.Request) {
	key, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid api key")
		return
	}
	if !key.Admin {
		writeError(w, http.StatusForbidden, "user key can only query its bound account")
		return
	}

	quotas, err := s.CollectQuotas(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, quotaResponse{Accounts: quotas})
}

func (s *Server) handleQuotaByID(w http.ResponseWriter, r *http.Request) {
	key, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "invalid api key")
		return
	}

	accountID := r.PathValue("id")
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "account id is required")
		return
	}
	if !key.Admin && key.AccountID != accountID {
		writeError(w, http.StatusForbidden, "user key cannot query this account")
		return
	}

	quota, err := s.QuotaByID(r.Context(), accountID)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, quota)
}

func (s *Server) authenticate(r *http.Request) (APIKeyInfo, bool) {
	// 暗门会话 cookie 视为管理员（通过暗门后无需再输 admin key）。
	if c, err := r.Cookie(sessionCookieName); err == nil && s.sessionToken != "" &&
		constantTimeEqual(c.Value, s.sessionToken) {
		return APIKeyInfo{Admin: true}, true
	}

	apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
	if apiKey == "" {
		apiKey = strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	}
	if apiKey == "" {
		return APIKeyInfo{}, false
	}

	if constantTimeEqual(apiKey, s.cfg.Admin.APIKey) {
		return APIKeyInfo{Admin: true}, true
	}
	if accountID, ok := s.store.FindByKey(apiKey); ok {
		return APIKeyInfo{AccountID: accountID}, true
	}
	return APIKeyInfo{}, false
}

func constantTimeEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var result byte
	for i := range a {
		result |= a[i] ^ b[i]
	}
	return result == 0
}

// ===== 后台监控 =====

// checkStats 汇总单次检查的处理结果，用于日志输出。
type checkStats struct {
	accounts int
	enabled  int64 // 成功开启调度（dry-run 时为将要开启）
	disabled int64 // 成功关闭调度（dry-run 时为将要关闭）
	skipped  int   // 因 usage 错误或状态非 active 而跳过
	failed   int64 // 调用 sub2api 失败
}

func (s *Server) StartQuotaMonitor(ctx context.Context) {
	spec := s.cfg.ScheduleSpec()

	// 启动时立即执行一次，方便确认任务确实在跑。
	s.runCheck(ctx, "startup")

	// SkipIfStillRunning：上一次尚未跑完时跳过本次触发（并打印日志），
	// 对齐原 ticker 逐次串行执行、不重叠的行为。
	c := cron.New(cron.WithChain(
		cron.SkipIfStillRunning(cron.PrintfLogger(log.Default())),
	))
	id, err := c.AddFunc(spec, func() {
		s.runCheck(ctx, "cron")
	})
	if err != nil {
		log.Printf("quota monitor: invalid schedule spec=%q: %v", spec, err)
		return
	}

	c.Start()
	log.Printf("quota monitor scheduled: spec=%q next=%s",
		spec, c.Entry(id).Next.Format("2006-01-02 15:04:05"))

	<-ctx.Done()
	// 等待正在执行的检查任务收尾后再返回。
	<-c.Stop().Done()
}

// runCheck 执行一次检查并打印开始/结束日志，让每次触发都可见。
func (s *Server) runCheck(ctx context.Context, trigger string) {
	start := time.Now()
	log.Printf("[check] start trigger=%s", trigger)
	stats := s.checkOnce(ctx)
	log.Printf("[check] done trigger=%s accounts=%d enabled=%d disabled=%d skipped=%d failed=%d took=%s",
		trigger, stats.accounts, stats.enabled, stats.disabled, stats.skipped, stats.failed,
		time.Since(start).Round(time.Millisecond))
}

func (s *Server) checkOnce(ctx context.Context) checkStats {
	quotas, err := s.CollectQuotas(ctx)
	if err != nil {
		log.Printf("quota check failed: %v", err)
		return checkStats{}
	}

	stats := checkStats{accounts: len(quotas)}

	var wg sync.WaitGroup
	sem := make(chan struct{}, 5)
	for _, q := range quotas {
		q := q
		// 已关闭「自动调度」的账号，本服务不做任何调度变更。
		if !q.AutoSchedule {
			stats.skipped++
			continue
		}
		// usage 拉取失败的账号不做调度变更，避免误判。
		if q.UsageError != "" {
			log.Printf("skip account=%s name=%q: usage error: %s", q.ID, q.Name, q.UsageError)
			stats.skipped++
			continue
		}

		shouldEnable := !q.OverThreshold(s.cfg.Threshold)
		if q.Schedulable == shouldEnable {
			continue
		}
		// 只对健康(active)账号执行自动「开启调度」，避免把因 error/disabled/expired
		// 等原因停用的账号强行恢复，跟 sub2api 自身的状态管理打架。
		// 「关闭调度」(达到额度阈值) 不受此限制，照常执行。
		if shouldEnable && q.Status != "active" {
			stats.skipped++
			continue
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			action := "disable"
			counter := &stats.disabled
			if shouldEnable {
				action = "enable"
				counter = &stats.enabled
			}
			if s.cfg.DryRun {
				atomic.AddInt64(counter, 1)
				log.Printf("[dry-run] would %s account=%s name=%q 5h=%.1f%% 7d=%.1f%% 7d_sonnet=%.1f%%",
					action, q.ID, q.Name, q.FiveHourPercent, q.SevenDayPercent, q.SevenDaySonnetPercent)
				return
			}
			if err := s.client.SetSchedulable(ctx, q.ID, shouldEnable); err != nil {
				atomic.AddInt64(&stats.failed, 1)
				log.Printf("%s schedule account=%s failed: %v", action, q.ID, err)
				return
			}
			atomic.AddInt64(counter, 1)
			log.Printf("%s schedule account=%s name=%q 5h=%.1f%% 7d=%.1f%% 7d_sonnet=%.1f%%",
				action, q.ID, q.Name, q.FiveHourPercent, q.SevenDayPercent, q.SevenDaySonnetPercent)
		}()
	}
	wg.Wait()
	return stats
}

// ===== 工具 =====

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func main() {
	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store, err := LoadUserStore(cfg.StorePath)
	if err != nil {
		log.Fatalf("load user store: %v", err)
	}

	server := NewServer(cfg, store)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go server.StartQuotaMonitor(ctx)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           server.mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()

	log.Printf("monitor listening on %s (threshold=%.0f%% interval=%s source=%s dry_run=%v)",
		cfg.ListenAddr, cfg.Threshold*100, cfg.CheckInterval.Duration, cfg.UsageSource, cfg.DryRun)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("http server: %v", err)
	}
}
