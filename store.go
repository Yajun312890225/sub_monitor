package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ===== 用户存储 =====
//
// 用户 key 由服务端随机生成，仅保存其 SHA-256 哈希（KeyHash）与前缀（KeyPrefix，用于展示）。
// 服务端无法从存储反解出明文 key；明文只在创建 / 重置时返回一次。
// 存储介质为一个 JSON 文件，进程内用 RWMutex 保护，落盘采用「临时文件 + Rename」原子写。

var (
	// ErrUserExists 账号已存在（同一 sub2api 账号 ID 只允许绑定一个用户 key）。
	ErrUserExists = errors.New("user already exists for this account id")
	// ErrUserNotFound 账号不存在。
	ErrUserNotFound = errors.New("user not found")
)

// StoredUser 是落盘的用户记录。KeyHash 为 sha256(key) 的 hex，不可反解。
type StoredUser struct {
	AccountID string    `json:"account_id"` // sub2api 账号 ID（数字字符串，例如 "5"）
	Label     string    `json:"label"`      // 备注名
	KeyHash   string    `json:"key_hash"`   // sha256(key) 的 hex
	KeyPrefix string    `json:"key_prefix"` // key 前缀（sk-xxxxxxxx），仅用于展示
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// PublicUser 是对外（管理页）展示的用户视图，绝不包含 KeyHash。
type PublicUser struct {
	AccountID string    `json:"account_id"`
	Label     string    `json:"label"`
	KeyPrefix string    `json:"key_prefix"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// storeFile 是磁盘上的 JSON 结构。
type storeFile struct {
	Users []*StoredUser `json:"users"`
	// 自动调度默认开启，这里只记录被关闭自动调度的账号 ID（例外列表）。
	AutoScheduleOff []string `json:"auto_schedule_off,omitempty"`
}

type UserStore struct {
	path    string
	mu      sync.RWMutex
	byID    map[string]*StoredUser // AccountID -> user
	byHash  map[string]string      // KeyHash   -> AccountID
	autoOff map[string]bool        // AccountID -> 是否关闭自动调度（不在表中=默认开启）
}

// LoadUserStore 从 path 加载用户存储；文件不存在时初始化为空存储（不落盘，首次写入时才创建文件）。
func LoadUserStore(path string) (*UserStore, error) {
	s := &UserStore{
		path:    path,
		byID:    make(map[string]*StoredUser),
		byHash:  make(map[string]string),
		autoOff: make(map[string]bool),
	}

	// 确保存储目录存在（便于挂载数据卷，如 store_path=data/data.json）。
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return s, nil
	}

	var file storeFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("decode user store %q: %w", path, err)
	}
	for _, u := range file.Users {
		s.byID[u.AccountID] = u
		s.byHash[u.KeyHash] = u.AccountID
	}
	for _, id := range file.AutoScheduleOff {
		s.autoOff[id] = true
	}
	return s, nil
}

// AutoScheduleEnabled 返回该账号是否参与本服务定时任务的自动调度（默认开启）。
func (s *UserStore) AutoScheduleEnabled(accountID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return !s.autoOff[accountID]
}

// SetAutoSchedule 设置账号是否参与自动调度；关闭后定时任务不再自动开关该账号。
func (s *UserStore) SetAutoSchedule(accountID string, enabled bool) error {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return errors.New("account id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	currentlyOff := s.autoOff[accountID]
	if enabled == !currentlyOff {
		return nil // 状态未变
	}
	if enabled {
		delete(s.autoOff, accountID)
	} else {
		s.autoOff[accountID] = true
	}
	if err := s.persistLocked(); err != nil {
		// 回滚
		if enabled {
			s.autoOff[accountID] = true
		} else {
			delete(s.autoOff, accountID)
		}
		return err
	}
	return nil
}

// List 返回按账号 ID 排序的用户视图（不含哈希）。
func (s *UserStore) List() []PublicUser {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]PublicUser, 0, len(s.byID))
	for _, u := range s.byID {
		out = append(out, PublicUser{
			AccountID: u.AccountID,
			Label:     u.Label,
			KeyPrefix: u.KeyPrefix,
			CreatedAt: u.CreatedAt,
			UpdatedAt: u.UpdatedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AccountID < out[j].AccountID })
	return out
}

// FindByKey 用明文 key 反查绑定的账号 ID：对 key 取 sha256 再查表。
func (s *UserStore) FindByKey(key string) (accountID string, ok bool) {
	return s.FindByHash(hashKey(key))
}

// FindByHash 用 key 的哈希直接反查账号 ID（用于会话 cookie：cookie 存哈希，不存明文）。
func (s *UserStore) FindByHash(hash string) (accountID string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byHash[hash]
	return id, ok
}

// Create 为指定账号新建用户并生成 key，返回明文 key（仅此一次）。账号已存在则返回 ErrUserExists。
func (s *UserStore) Create(accountID, label string) (plainKey string, err error) {
	accountID = strings.TrimSpace(accountID)
	if accountID == "" {
		return "", errors.New("account id is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.byID[accountID]; exists {
		return "", ErrUserExists
	}

	plainKey, hash, prefix, err := newKey()
	if err != nil {
		return "", err
	}
	now := time.Now()
	u := &StoredUser{
		AccountID: accountID,
		Label:     strings.TrimSpace(label),
		KeyHash:   hash,
		KeyPrefix: prefix,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.byID[accountID] = u
	s.byHash[hash] = accountID

	if err := s.persistLocked(); err != nil {
		// 回滚内存，保持内存与磁盘一致。
		delete(s.byID, accountID)
		delete(s.byHash, hash)
		return "", err
	}
	return plainKey, nil
}

// Reset 为已存在的账号重新生成 key，返回新的明文 key（仅此一次）。
func (s *UserStore) Reset(accountID string) (plainKey string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.byID[accountID]
	if !ok {
		return "", ErrUserNotFound
	}

	plainKey, hash, prefix, err := newKey()
	if err != nil {
		return "", err
	}
	oldHash, oldPrefix, oldUpdated := u.KeyHash, u.KeyPrefix, u.UpdatedAt

	delete(s.byHash, oldHash)
	u.KeyHash = hash
	u.KeyPrefix = prefix
	u.UpdatedAt = time.Now()
	s.byHash[hash] = accountID

	if err := s.persistLocked(); err != nil {
		// 回滚。
		delete(s.byHash, hash)
		u.KeyHash = oldHash
		u.KeyPrefix = oldPrefix
		u.UpdatedAt = oldUpdated
		s.byHash[oldHash] = accountID
		return "", err
	}
	return plainKey, nil
}

// Update 修改用户备注。
func (s *UserStore) Update(accountID, label string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.byID[accountID]
	if !ok {
		return ErrUserNotFound
	}
	oldLabel, oldUpdated := u.Label, u.UpdatedAt
	u.Label = strings.TrimSpace(label)
	u.UpdatedAt = time.Now()

	if err := s.persistLocked(); err != nil {
		u.Label = oldLabel
		u.UpdatedAt = oldUpdated
		return err
	}
	return nil
}

// Delete 删除用户及其 key 绑定。
func (s *UserStore) Delete(accountID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	u, ok := s.byID[accountID]
	if !ok {
		return ErrUserNotFound
	}
	delete(s.byID, accountID)
	delete(s.byHash, u.KeyHash)

	if err := s.persistLocked(); err != nil {
		// 回滚。
		s.byID[accountID] = u
		s.byHash[u.KeyHash] = accountID
		return err
	}
	return nil
}

// persistLocked 把当前内存状态原子写入磁盘。调用方必须已持有写锁。
func (s *UserStore) persistLocked() error {
	users := make([]*StoredUser, 0, len(s.byID))
	for _, u := range s.byID {
		users = append(users, u)
	}
	sort.Slice(users, func(i, j int) bool { return users[i].AccountID < users[j].AccountID })

	offIDs := make([]string, 0, len(s.autoOff))
	for id := range s.autoOff {
		offIDs = append(offIDs, id)
	}
	sort.Strings(offIDs)

	data, err := json.MarshalIndent(storeFile{Users: users, AutoScheduleOff: offIDs}, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".store-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // Rename 成功后此文件已不存在，忽略错误即可。

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

// ===== key 生成与哈希 =====

// newKey 生成一个新的用户 key，返回明文、哈希(hex)、前缀。
func newKey() (plain, hash, prefix string, err error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", "", "", err
	}
	plain = "sk-" + hex.EncodeToString(buf)
	hash = hashKey(plain)
	// 前缀 = sk- + 明文前 8 位 hex，用于列表展示，不泄露完整 key。
	prefix = plain[:11]
	return plain, hash, prefix, nil
}

// hashKey 返回 sha256(key) 的十六进制字符串。
func hashKey(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}
