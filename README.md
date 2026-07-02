# sub2api 额度监控

一个简单的 Go 监控服务 + 网页，对接 [sub2api](https://github.com/Wei-Shaw/sub2api) 的管理后台 API：

## 网页

- **首页 `/`**：普通用户输入自己的 key，查询其绑定账号的 5 小时 / 7 天（含 Sonnet）额度。
- **暗门跳转**：在首页输入配置的 `admin.entry_code` 固定串，直接跳转到 `/admin`。
- **管理后台 `/admin`**：输入 `admin.api_key` 登录后，可查看**所有账号**额度、**管理用户**（新建 / 重置 / 删除 / 改备注）、**手动开关某账号调度**。

### 用户 key 的生成与存储

- 用户 key **不再写在配置里**，由管理员在 `/admin` 页面「新建用户」时**服务端随机生成**（`sk-` 开头）。
- 存储只保留 `SHA-256` 哈希与前缀（见 `store_path` 指向的 JSON 文件，默认 `data.json`），**服务端无法反解出明文**。
- 明文 key **只在创建 / 重置时显示一次**，请立即复制保存；管理员可随时重置。

## 后台监控

- 后台基于 [robfig/cron](https://github.com/robfig/cron) 定时检查所有账号的 5 小时 / 7 天窗口使用率：默认每 `check_interval`（以 `@every` 形式）执行一次，也可用 `cron` 配置标准 cron 表达式。每次执行都会打印 `[check] start` / `[check] done`（含账号数、开启/关闭/跳过/失败计数、耗时），便于确认任务确实在跑。
- 任一窗口使用率达到 `threshold`（默认 90%）时，关闭该账号调度（schedulable=false）。
- 已关闭调度的账号，在窗口刷新、使用率回落到阈值以下后自动重新开启调度。
- 注意：管理页的「手动开关调度」可能在下个检查周期被自动调度覆盖。

## 兼容的 JSON 接口

- `GET /quotas`：管理员 key 查询所有 anthropic 账号额度。
- `GET /quota/{id}`：管理员 key 可查任意账号；用户 key 只能查自己绑定的账号。

## 对接的 sub2api 接口

| 用途 | 方法与路径 |
|---|---|
| 列出账号 | `GET /api/v1/admin/accounts?platform=anthropic&page=&page_size=` |
| 查账号详情 | `GET /api/v1/admin/accounts/{id}` |
| 查账号用量 | `GET /api/v1/admin/accounts/{id}/usage?source=active` |
| 开关调度 | `POST /api/v1/admin/accounts/{id}/schedulable` body `{"schedulable":true}` |

鉴权使用 sub2api 的 **Admin API Key**（后台系统设置里生成，以 `admin-` 开头），通过 `x-api-key` 请求头发送。

用量接口返回的是各窗口的 `utilization`（百分比 0~100），本服务直接以此对比阈值，不需要再配置字段路径。

## 使用

先复制配置：

```bash
cp config.example.yaml config.yaml
```

修改 `config.yaml`：

- `sub2api.base_url`：你的 sub2api 地址（不带末尾斜杠）。
- `sub2api.admin_api_key`：sub2api 的 Admin API Key（`admin-` 开头）。
- `admin.api_key`：本监控服务的管理员 key（在 `/admin` 页面登录用）。
- `admin.entry_code`：暗门固定串，在首页输入即跳转 `/admin`（留空则关闭暗门）。
- `store_path`：用户存储文件路径，默认 `data.json`（存放自动生成的用户 key 哈希，请勿提交到版本库）。
- `threshold` / `check_interval` / `usage_source` / `dry_run`：按需调整。

> 用户 key 不在配置里配置，由管理员登录 `/admin` 后在页面上新建。

生成随机 key 可用：

```bash
openssl rand -hex 32
```

启动：

```bash
go mod tidy
go run . config.yaml
```

浏览器打开 `http://127.0.0.1:8080/` 即可使用。

> 建议首次运行先把 `dry_run` 设为 `true`，确认日志里的调度判断符合预期后再改回 `false` 真正生效。

## Docker 部署

仓库内含 `Dockerfile`（多阶段构建静态二进制）与 `docker-compose.yml`：

```bash
cp config.example.yaml config.yaml   # 按需修改
docker compose up -d --build
```

- 容器内固定监听 `8080`，对外发布端口在 `docker-compose.yml` 里配置（默认 `8090:8080`，按需改）。
- `config.yaml` 以只读方式挂载；用户存储持久化到宿主机 `./data/`（对应 `store_path: data/data.json`）。
- 常用命令：`docker compose logs -f`（看日志）、`docker compose up -d --build`（更新重建）、`docker compose down`（停止）。

若前面有反向代理（如 Caddy），可加一段把域名反代到发布端口，示例：

```
monitor.example.com {
    reverse_proxy 127.0.0.1:8090
}
```

## 查询（JSON 接口）

管理员查询所有：

```bash
curl -H "X-API-Key: monitor-admin-key" http://127.0.0.1:8080/quotas
```

用户查询自己绑定的账号（key 为管理页生成的用户 key）：

```bash
curl -H "X-API-Key: sk-xxxxxxxx..." http://127.0.0.1:8080/quota/5
```

也支持 Bearer：

```bash
curl -H "Authorization: Bearer monitor-admin-key" http://127.0.0.1:8080/quotas
```

返回示例（单账号）：

```json
{
  "id": "5",
  "name": "acc-5",
  "platform": "anthropic",
  "status": "active",
  "schedulable": true,
  "five_hour_percent": 12.3,
  "seven_day_percent": 45.6,
  "seven_day_sonnet_percent": 0,
  "five_hour_resets_at": "2026-07-01T12:00:00Z"
}
```

## 配置说明

- `usage_source`：
  - `active`（默认）：实时向上游拉取用量，sub2api 服务端缓存约 10 分钟，因此每分钟检查也不会频繁打上游。
  - `passive`：只读 sub2api 已保存的快照，不触发上游请求，但数据可能较旧。
- `threshold`：0~1 的比例，例如 `0.9` 表示 90%。
- `check_interval` / `cron`：调度频率。默认按 `check_interval`（如 `1m`）以 `@every` 形式运行；若配置了 `cron`（标准 5 段表达式，如 `*/5 * * * *`）则优先生效。上一次检查未跑完时会自动跳过本次触发并打印跳过日志，避免重叠执行。
- `dry_run`：为 `true` 时后台只打印将要执行的开/关调度动作，不真正调用 sub2api。
- `admin.entry_code`：首页暗门固定串，输入后跳转 `/admin`；留空关闭暗门。
- `store_path`：用户存储 JSON 文件路径（默认 `data.json`）。仅保存用户 key 的 SHA-256 哈希与前缀，服务端无法反解明文。

## 管理页操作

登录 `/admin`（输入 `admin.api_key`）后：

- **新建用户**：填 sub2api 账号 ID（数字）+ 可选备注 → 生成一个 `sk-` 开头的用户 key（弹层显示，仅一次）。
- **重置 key**：为账号重新生成 key，旧 key 立即失效。
- **改备注 / 删除**：管理用户绑定。
- **开/关调度**：手动切换某账号的 `schedulable`（注意会被后台自动调度覆盖）。
