package main

import (
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
	Sub2API       Sub2APIConfig `yaml:"sub2api"`
	Admin         AdminConfig   `yaml:"admin"`
	Users         []UserConfig  `yaml:"users"`
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
	APIKey string `yaml:"api_key"`
}

type UserConfig struct {
	ID     string `yaml:"id"` // sub2api 账号 ID（数字字符串，例如 "5"）
	APIKey string `yaml:"api_key"`
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
	ID          json.Number `json:"id"`
	Name        string      `json:"name"`
	Platform    string      `json:"platform"`
	Status      string      `json:"status"`
	Schedulable bool        `json:"schedulable"`
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

// ListAnthropicAccounts 列出全部 anthropic 平台账号（自动翻页）。
func (c *Sub2APIClient) ListAnthropicAccounts(ctx context.Context) ([]rawAccount, error) {
	var all []rawAccount
	page := 1
	for {
		q := url.Values{}
		q.Set("platform", "anthropic")
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
func (c *Sub2APIClient) GetUsage(ctx context.Context, id string) (usageData, error) {
	q := url.Values{}
	q.Set("source", c.source)
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
		ID:          acc.ID.String(),
		Name:        acc.Name,
		Platform:    acc.Platform,
		Status:      acc.Status,
		Schedulable: acc.Schedulable,
		UsageError:  usage.Error,
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
	accounts, err := s.client.ListAnthropicAccounts(ctx)
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
	return quotas, nil
}

// QuotaByID 查询单个账号。
func (s *Server) QuotaByID(ctx context.Context, id string) (AccountQuota, error) {
	var acc rawAccount
	if err := s.client.getJSON(ctx, pathListAccounts+"/"+url.PathEscape(id), &acc); err != nil {
		return AccountQuota{}, err
	}
	usage, err := s.client.GetUsage(ctx, id)
	if err != nil {
		return quotaFromUsage(acc, usageData{Error: err.Error()}), nil
	}
	return quotaFromUsage(acc, usage), nil
}

// ===== HTTP 服务 =====

type Server struct {
	cfg    Config
	client *Sub2APIClient
	mux    *http.ServeMux
}

type APIKeyInfo struct {
	Admin     bool
	AccountID string
}

func NewServer(cfg Config) *Server {
	s := &Server{
		cfg:    cfg,
		client: NewSub2APIClient(cfg),
		mux:    http.NewServeMux(),
	}
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /quotas", s.handleQuotas)
	s.mux.HandleFunc("GET /quota/{id}", s.handleQuotaByID)
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
	for _, user := range s.cfg.Users {
		if constantTimeEqual(apiKey, user.APIKey) {
			return APIKeyInfo{AccountID: user.ID}, true
		}
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

	server := NewServer(cfg)

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
