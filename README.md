# astrbot_plugin_tokenarena

将 AstrBot 的 LLM token 用量自动上报到 [TokenArena](https://github.com/) 的开放 API。

只需配置 **服务地址**、**API Key**、**同步间隔**，插件即可在后台自动统计并上传 AstrBot 内部 Agent 的 token 消耗数据，无需任何手动操作。

## 工作原理

AstrBot 在每次内部 LLM 调用结束后，会把本次用量写入数据库的 `provider_stats` 表（输入/缓存/输出 token、provider、模型、时间等）。本插件：

1. 定时（默认每 30 分钟）从 `provider_stats` 表读取最近 N 天的记录；
2. 按 **provider + 模型 + 30 分钟时间桶** 聚合 token；
3. 转换成 TokenArena 的 ingest 格式，通过 `POST /api/usage/ingest`（Bearer 认证）上传。

TokenArena 的 ingest 接口使用唯一键做 **幂等 upsert**（同一时间桶覆盖而非累加），因此插件每次重新统计整个回溯窗口并上传，既能补齐遗漏，也不会重复计数。

## 配置项

| 配置项 | 说明 | 默认值 |
| --- | --- | --- |
| `base_url` | TokenArena 服务地址，无需带 `/api/usage/ingest` 后缀 | `https://tokenarena.app` |
| `api_key` | TokenArena API Key（在 Settings → CLI Keys 中创建） | 空 |
| `sync_interval_minutes` | 自动同步间隔（分钟） | `30` |
| `lookback_days` | 每次同步回溯统计的天数 | `2` |
| `source_name` | 上报来源标识，用于在仪表盘区分数据来源 | `astrbot` |
| `device_id` | 设备 ID，留空则按机器名自动生成稳定 ID | 空 |
| `enable_auto_sync` | 是否启用后台自动同步 | `true` |

## 数据映射

| AstrBot `provider_stats` | TokenArena bucket |
| --- | --- |
| `token_input_other` | `inputTokens` |
| `token_input_cached` | `cachedTokens` |
| `token_output` | `outputTokens` |
| （无） | `reasoningTokens`（恒为 0） |
| `provider_model` | `model` |
| `provider_id` | `projectKey` / `projectLabel` |

> 说明：插件使用 `provider_id` 作为 TokenArena 的项目维度，不上报任何用户 ID、群号或会话内容，避免隐私泄露。

## 指令

| 指令 | 说明 |
| --- | --- |
| `/tokenarena sync`（别名 `/ta sync`） | 立即手动同步一次 |
| `/tokenarena status`（别名 `/ta status`） | 查看配置与上次同步状态 |

## 安装

将本目录放入 AstrBot 的 `data/plugins/` 下，在 WebUI 插件管理页重载插件，然后在插件配置中填入 `api_key` 即可。

## 依赖

- `httpx`（异步 HTTP 客户端）
