package main

import (
	"embed"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// ===== 内嵌前端静态资源 =====

//go:embed web/index.html web/admin.html
var webFS embed.FS

// registerWebRoutes 注册页面与 Web API 路由（普通用户查询 + 管理接口）。
func (s *Server) registerWebRoutes() {
	// 页面
	s.mux.HandleFunc("GET /", s.handleIndexPage)
	s.mux.HandleFunc("GET /admin", s.handleAdminPage)

	// 普通用户：输入 key 查询自己的额度；命中暗门则返回跳转。
	s.mux.HandleFunc("POST /api/query", s.handleQuery)
	// 凭用户会话 cookie 查询自己的额度（登录一次后免再输）。
	s.mux.HandleFunc("GET /api/me/quota", s.handleMeQuota)
	// 凭用户会话 cookie 对自己绑定的账号发起连通性测试。
	s.mux.HandleFunc("POST /api/me/test", s.handleMeTest)
	// 退出：清除会话 cookie（管理员/用户通用）。
	s.mux.HandleFunc("POST /api/logout", s.handleLogout)

	// 管理接口（均需 admin key）
	s.mux.HandleFunc("GET /api/admin/quotas", s.handleAdminQuotas)
	s.mux.HandleFunc("GET /api/admin/users", s.handleAdminListUsers)
	s.mux.HandleFunc("POST /api/admin/users", s.handleAdminCreateUser)
	s.mux.HandleFunc("POST /api/admin/users/{id}/reset", s.handleAdminResetUser)
	s.mux.HandleFunc("PATCH /api/admin/users/{id}", s.handleAdminUpdateUser)
	s.mux.HandleFunc("DELETE /api/admin/users/{id}", s.handleAdminDeleteUser)
	s.mux.HandleFunc("POST /api/admin/accounts/{id}/schedulable", s.handleAdminSetSchedulable)
	s.mux.HandleFunc("POST /api/admin/accounts/{id}/auto-schedule", s.handleAdminSetAutoSchedule)
	s.mux.HandleFunc("POST /api/admin/accounts/{id}/test", s.handleAdminTestAccount)
}

// ===== 页面 =====

func (s *Server) handleIndexPage(w http.ResponseWriter, r *http.Request) {
	// GET / 是精确根路径；ServeMux 的 "GET /" 会兜底所有未匹配路径，这里显式挡掉非根路径返回 404。
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	s.servePage(w, "web/index.html")
}

func (s *Server) handleAdminPage(w http.ResponseWriter, r *http.Request) {
	s.servePage(w, "web/admin.html")
}

func (s *Server) servePage(w http.ResponseWriter, name string) {
	data, err := webFS.ReadFile(name)
	if err != nil {
		http.Error(w, "page not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

// ===== 普通用户查询 =====

type queryRequest struct {
	Key string `json:"key"`
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req queryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	key := strings.TrimSpace(req.Key)
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	// 暗门：输入固定串即写入会话 cookie 并跳转 admin 页面（entry_code 为空则不启用）。
	// 之后访问 /admin 凭该 cookie 自动鉴权，无需再输入管理员密钥。
	if s.cfg.Admin.EntryCode != "" && constantTimeEqual(key, s.cfg.Admin.EntryCode) {
		s.setCookie(w, sessionCookieName, s.sessionToken)
		writeJSON(w, http.StatusOK, map[string]string{"redirect": "/admin"})
		return
	}

	accountID, ok := s.store.FindByKey(key)
	if !ok {
		writeError(w, http.StatusUnauthorized, "无效的密钥")
		return
	}

	quota, err := s.QuotaByID(r.Context(), accountID)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	// 写入用户会话 cookie（值为 key 的哈希，不含明文），下次免再输。
	s.setCookie(w, userCookieName, hashKey(key))
	writeJSON(w, http.StatusOK, quota)
}

// handleMeQuota 凭用户会话 cookie 返回该用户绑定账号的额度。
func (s *Server) handleMeQuota(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(userCookieName)
	if err != nil || c.Value == "" {
		writeError(w, http.StatusUnauthorized, "未登录")
		return
	}
	accountID, ok := s.store.FindByHash(c.Value)
	if !ok {
		// key 被重置/删除后，旧会话自动失效。
		writeError(w, http.StatusUnauthorized, "会话已失效，请重新输入密钥")
		return
	}
	quota, err := s.QuotaByID(r.Context(), accountID)
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, quota)
}

// handleLogout 清除会话 cookie（管理员与用户会话一并清除）。
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	for _, name := range []string{sessionCookieName, userCookieName} {
		http.SetCookie(w, &http.Cookie{
			Name:     name,
			Value:    "",
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
			MaxAge:   -1, // 立即过期
		})
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ===== 管理接口 =====

// requireAdmin 校验请求携带的是 admin key，否则写 401 并返回 false。
func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	key, ok := s.authenticate(r)
	if !ok || !key.Admin {
		writeError(w, http.StatusUnauthorized, "invalid admin key")
		return false
	}
	return true
}

func (s *Server) handleAdminQuotas(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	quotas, err := s.CollectQuotas(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, quotaResponse{Accounts: quotas})
}

func (s *Server) handleAdminListUsers(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": s.store.List()})
}

type createUserRequest struct {
	AccountID string `json:"account_id"`
	Label     string `json:"label"`
}

func (s *Server) handleAdminCreateUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	var req createUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if strings.TrimSpace(req.AccountID) == "" {
		writeError(w, http.StatusBadRequest, "account_id is required")
		return
	}

	key, err := s.store.Create(req.AccountID, req.Label)
	if err != nil {
		if errors.Is(err, ErrUserExists) {
			writeError(w, http.StatusConflict, "该账号已存在用户，若要换 key 请使用重置")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	// 明文 key 仅此一次返回。
	writeJSON(w, http.StatusOK, map[string]string{"key": key})
}

func (s *Server) handleAdminResetUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	accountID := r.PathValue("id")
	key, err := s.store.Reset(accountID)
	if err != nil {
		if errors.Is(err, ErrUserNotFound) {
			writeError(w, http.StatusNotFound, "user not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key})
}

type updateUserRequest struct {
	Label string `json:"label"`
}

func (s *Server) handleAdminUpdateUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	accountID := r.PathValue("id")
	var req updateUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.store.Update(accountID, req.Label); err != nil {
		if errors.Is(err, ErrUserNotFound) {
			writeError(w, http.StatusNotFound, "user not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleAdminDeleteUser(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	accountID := r.PathValue("id")
	if err := s.store.Delete(accountID); err != nil {
		if errors.Is(err, ErrUserNotFound) {
			writeError(w, http.StatusNotFound, "user not found")
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type setSchedulableRequest struct {
	Schedulable bool `json:"schedulable"`
}

func (s *Server) handleAdminSetSchedulable(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	accountID := r.PathValue("id")
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "account id is required")
		return
	}
	var req setSchedulableRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.client.SetSchedulable(r.Context(), accountID, req.Schedulable); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedulable": req.Schedulable})
}

type setAutoScheduleRequest struct {
	Enabled bool `json:"enabled"`
}

// handleAdminSetAutoSchedule 开关某账号是否参与本服务的自动调度。
func (s *Server) handleAdminSetAutoSchedule(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	accountID := r.PathValue("id")
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "account id is required")
		return
	}
	var req setAutoScheduleRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := s.store.SetAutoSchedule(accountID, req.Enabled); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"auto_schedule": req.Enabled})
}

type testAccountRequest struct {
	ModelID  string `json:"model_id"` // 可选：显式指定测试模型；留空则按账号厂商自动选择
	Platform string `json:"platform"` // 可选：账号厂商（前端已知则直接传，省一次上游查询）
}

// handleAdminTestAccount 对某账号发起一次连通性测试。
// 按账号「厂商」自动选择测试模型（anthropic→claude、openai→gpt，可用 test_models 覆盖），
// 调用 sub2api 的测试接口并把其 SSE 流归并成一次性结果返回。
func (s *Server) handleAdminTestAccount(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	accountID := r.PathValue("id")
	if accountID == "" {
		writeError(w, http.StatusBadRequest, "account id is required")
		return
	}

	var req testAccountRequest
	if err := decodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	res, status, err := s.runAccountTest(r.Context(), accountID, req.Platform, req.ModelID)
	if err != nil {
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, status, res)
}

// handleMeTest 凭用户会话 cookie 对该用户绑定的账号发起一次连通性测试。
// 与管理员入口共用测试逻辑，但只测自己绑定的账号、按厂商自动选模型（不接受自定义模型）。
func (s *Server) handleMeTest(w http.ResponseWriter, r *http.Request) {
	c, err := r.Cookie(userCookieName)
	if err != nil || c.Value == "" {
		writeError(w, http.StatusUnauthorized, "未登录")
		return
	}
	accountID, ok := s.store.FindByHash(c.Value)
	if !ok {
		writeError(w, http.StatusUnauthorized, "会话已失效，请重新输入密钥")
		return
	}

	var req testAccountRequest
	if err := decodeJSON(r, &req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	res, status, err := s.runAccountTest(r.Context(), accountID, req.Platform, "")
	if err != nil {
		writeError(w, status, err.Error())
		return
	}
	writeJSON(w, status, res)
}

// ===== 工具 =====

// setCookie 写入一个 7 天有效的会话 cookie（HttpOnly + SameSite=Lax）。
func (s *Server) setCookie(w http.ResponseWriter, name, value string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   7 * 24 * 3600, // 7 天
	})
}

// decodeJSON 解析请求体 JSON（限制 1MB），禁止未知字段。
func decodeJSON(r *http.Request, out any) error {
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}
