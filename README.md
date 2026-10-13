# TokenArena 用量上报

将 AstrBot 内部 Agent 的 LLM token 用量自动上报到 [TokenArena](https://github.com/GuJi08233/TokenArena)，在 [TokenArena 仪表盘](https://token.guji.uno/zh/usage) 查看设备、模型和提供商维度的统计。

插件版本 **1.2.0**，最低要求 **AstrBot 4.22.3**（支持范围 `>=4.22.3,<5`）。本次适配 AstrBot **4.28.2 / 4.29.0-beta.2** 和 TokenArena **0.20.2**，使用 TokenArena ingest **schemaVersion 2**。

## 安装与配置

在 AstrBot WebUI 的插件管理页通过仓库地址安装，或将本仓库放入 `data/plugins/astrbot_plugin_tokenarena/` 后重载插件。在 [TokenArena → Settings → CLI Keys](https://token.guji.uno/zh/settings/cli-keys) 创建 API Key，并填入插件配置。默认每 30 分钟自动同步，插件热加载后也会启动后台同步。

| 配置项 | 说明 | 默认值 |
| --- | --- | --- |
| `base_url` | TokenArena 服务地址，无需带 `/api/usage/ingest` 后缀 | `https://token.guji.uno` |
| `api_key` | TokenArena API Key，用于 Bearer 认证 | 空 |
| `proxy` | 本插件专用代理，支持 HTTP、HTTPS、SOCKS5、SOCKS5H；填写后优先于环境代理 | 空 |
| `use_env_proxy` | `proxy` 留空时，是否使用 AstrBot / 系统环境代理；关闭则直连 | `true` |
| `sync_interval_minutes` | 自动同步间隔（分钟），建议不小于 5 分钟 | `30` |
| `lookback_days` | 每次重新统计最近 N 天，窗口起点向下对齐至完整 UTC 半小时桶 | `2` |
| `source_name` | 仪表盘来源标识，最多 64 个 UTF-8 字节；升级时保持原值 | `astrbot` |
| `device_id` | 可选稳定设备 ID，最多 128 个 UTF-8 字节；留空时使用持久化 ID | 空 |
| `enable_auto_sync` | 是否启用后台自动同步，关闭后可手动同步 | `true` |

公网实例建议使用 HTTPS，避免以明文传输 API Key。自托管实例可配置自己的服务地址。

## 代理

代理只影响本插件的上报请求，按以下优先级选择：

1. `proxy` 非空：使用填写的专用代理，优先于环境代理和 `NO_PROXY`。
2. `proxy` 为空且 `use_env_proxy=true`：由 httpx 使用 `HTTP_PROXY`、`HTTPS_PROXY`、`ALL_PROXY` 等环境变量，并尊重 `NO_PROXY`。
3. `proxy` 为空且 `use_env_proxy=false`：直连。

AstrBot 全局 `http_proxy` 设置写入环境变量后，可通过第二种方式继承。专用代理示例：

| 类型 | 示例 |
| --- | --- |
| HTTP | `http://127.0.0.1:7890` |
| HTTPS | `https://proxy.example.com:8443` |
| SOCKS5 | `socks5://127.0.0.1:1080` |
| SOCKS5H | `socks5h://127.0.0.1:1080` |

代理账号密码填写在 URL 中时，状态与错误信息不会回显凭据。`/tokenarena status` 可查看代理模式；环境模式的具体路由由环境变量和 `NO_PROXY` 决定。

## 工作原理与统计口径

AstrBot 在一次内部 Agent 运行结束后，将各轮 LLM 调用的累计用量写入 `provider_stats`。插件只查询 `agent_type=internal` 的记录，流式读取时间、提供商、模型和三类 token 字段，按 **提供商 + 模型 + UTC 半小时** 聚合。有实际消耗的失败或中止运行也计入用量；外部 Agent 或插件直接调用提供商的用量不保证写入此表。

回溯窗口的起点向下对齐到半小时边界，保证最早的桶也包含完整用量。上传按每批最多 **100 个桶** 分批完成，仅最后一批触发 TokenArena 成就同步。网络传输异常以及 HTTP `429 / 500 / 502 / 503 / 504` 最多尝试 3 次；认证和其他请求错误不会自动重试。自动同步与手动同步串行执行；卸载时停止后台任务，并等待正在进行的手动上传完成后再允许热重载，避免旧实例继续重试、覆盖新快照。

TokenArena 按 **账户 + 设备 + 来源 + 模型 + 项目 + 桶起点** 幂等覆盖，同键上传替换完整计数。重新统计回溯窗口可以补齐遗漏；改变设备、来源或项目身份会生成新的统计记录，不能用于修正已有历史。

| AstrBot `provider_stats` | TokenArena bucket |
| --- | --- |
| `token_input_other` | `inputTokens`：普通输入 |
| `token_input_cached` | `cachedTokens`：缓存读取 |
| `token_output` | `outputTokens`：完整输出 |
| 未单独持久化 | `reasoningTokens=0` |
| 未单独持久化 | `cacheCreationTokens=0` |
| `provider_model` | `model`，缺失时为 `unknown` |
| `provider_id` | 原值作为 `projectKey` / `projectLabel`，缺失时为 `unknown` |

`totalTokens` 为上述五类计数之和。AstrBot 当前未单独持久化推理和缓存写入计数，插件不根据模型名或文本推测它们；输出中的推理仍保留在完整输出总量中。

插件保持原始 `provider_id` 项目维度，**不读取 TokenArena CLI 项目 hash 设置**，因此账户的 `hashed / disabled` 项目偏好不会改变这些上传字段。请避免在提供商 ID 中填写敏感信息。上传包含设备 ID、机器名、来源、提供商 ID、模型和用量时间；不上传用户 ID、群号、提示词或会话内容，`sessions` 始终为空。

## 指令

以下指令仅 AstrBot 管理员可用：

| 指令 | 说明 |
| --- | --- |
| `/tokenarena sync`（别名 `/ta sync`） | 立即手动同步一次 |
| `/tokenarena status`（别名 `/ta status`） | 查看服务、代理模式、设备与上次同步状态 |

## 从 1.1.0 升级

- 新安装的默认服务地址改为 `https://token.guji.uno`。已有 `base_url` 配置不会自动替换；原来使用 `https://tokenarena.app` 的用户请确认服务迁移后在插件配置中修改地址，自托管用户保持自己的地址。
- 更新插件依赖到 `httpx[socks]>=0.28.1,<1` 并重载，以支持当前代理 API 和 SOCKS5H。新增 `use_env_proxy` 默认开启，保留原有环境代理行为。
- 继续使用已有 `source_name`、配置设备 ID 和持久化设备 ID，保留提供商项目身份。不要删除插件数据目录中的 `device_id` 文件；新实例才生成并保存独立 UUID。
- 少于 8 字符的自定义设备 ID 继续使用原有 SHA-256 前 32 位规则；超过 128 UTF-8 字节的 ID 会报配置错误。
- 修复大窗口超过服务端 100 桶上限、回溯起点截断首桶和插件热加载未启动自动同步的问题。手动同步及状态查看现仅向管理员开放。

## 依赖与本地验证

运行依赖为 `httpx[socks]>=0.28.1,<1`，其中 `socks` 附加项提供 SOCKS 代理支持；数据库访问使用 AstrBot 已有的 SQLAlchemy 环境。在已安装 AstrBot 依赖、pytest、pytest-asyncio 和 Ruff 的 Python 环境中，将 `ASTRBOT_SOURCE` 指向 AstrBot 源码目录后运行：

```powershell
$env:ASTRBOT_SOURCE = "C:\path\to\AstrBot"
python -m pytest -q
ruff format .
ruff check .
```

测试在临时目录创建 AstrBot 配置和 SQLite 数据库，使用模拟 HTTP 请求，不访问正在运行的实例或真实 TokenArena 服务。

部署后，先由管理员执行 `/tokenarena status` 检查配置，再执行 `/tokenarena sync`，在 TokenArena 按对应设备、来源、提供商和模型核对统计。手动同步会向配置的服务实际上传用量。
