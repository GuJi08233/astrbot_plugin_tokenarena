import asyncio
import hashlib
import os
import socket
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from sqlalchemy import select

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.db.po import ProviderStat

# TokenArena ingest 接口固定使用 schemaVersion 2。
SCHEMA_VERSION = 2
# bucket 聚合粒度（秒）。TokenArena 客户端约定按 30 分钟分桶。
BUCKET_SECONDS = 30 * 60


class TokenArenaPlugin(Star):
    """将 AstrBot 的 token 用量自动上报到 TokenArena。"""

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.data_dir: Path = StarTools.get_data_dir()
        self._sync_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._device_id: str = self._resolve_device_id()
        self._hostname: str = socket.gethostname() or "astrbot"
        self._last_sync_at: datetime | None = None
        self._last_result: str = "尚未同步"

    # ====
    # 生命周期
    # ====

    @filter.on_astrbot_loaded()
    async def on_loaded(self):
        """AstrBot 启动完成后拉起后台同步循环。"""
        if self.config.get("enable_auto_sync", True):
            self._sync_task = asyncio.create_task(self._sync_loop())
            logger.info("[TokenArena] 自动同步已启动。")
        else:
            logger.info("[TokenArena] 自动同步已关闭，仅支持手动同步。")

    async def terminate(self):
        """插件卸载或禁用时停止后台任务。"""
        self._stop_event.set()
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("[TokenArena] 已停止。")

    # ====
    # 指令
    # ====

    @filter.command_group("tokenarena", alias={"ta"})
    def tokenarena(self):
        pass

    @tokenarena.command("sync")
    async def cmd_sync(self, event: AstrMessageEvent):
        """立即手动同步一次 token 用量到 TokenArena。"""
        yield event.plain_result("[TokenArena] 开始同步...")
        try:
            result = await self.sync_once()
            yield event.plain_result(f"[TokenArena] {result}")
        except Exception as e:
            logger.error(f"[TokenArena] 手动同步失败: {e}", exc_info=True)
            yield event.plain_result(f"[TokenArena] 同步失败: {e}")

    @tokenarena.command("status")
    async def cmd_status(self, event: AstrMessageEvent):
        """查看 TokenArena 上报插件的当前状态。"""
        base_url = (self.config.get("base_url") or "").strip()
        configured = "已配置" if self.config.get("api_key") else "未配置 API Key"
        auto = "开启" if self.config.get("enable_auto_sync", True) else "关闭"
        interval = self.config.get("sync_interval_minutes", 30)
        last = (
            self._last_sync_at.astimezone().strftime("%Y-%m-%d %H:%M:%S")
            if self._last_sync_at
            else "从未"
        )
        lines = [
            "[TokenArena] 状态",
            f"- 服务地址: {base_url or '(未配置)'}",
            f"- API Key: {configured}",
            f"- 代理: {self._describe_proxy()}",
            f"- 自动同步: {auto}（间隔 {interval} 分钟）",
            f"- 设备 ID: {self._device_id}",
            f"- 上次同步: {last}",
            f"- 上次结果: {self._last_result}",
        ]
        yield event.plain_result("\n".join(lines))

    # ====
    # 后台同步循环
    # ====

    async def _sync_loop(self):
        # 启动后稍作延迟，避开启动期的资源争用。
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=15)
            return
        except asyncio.TimeoutError:
            pass

        while not self._stop_event.is_set():
            try:
                result = await self.sync_once()
                logger.info(f"[TokenArena] 自动同步: {result}")
            except Exception as e:
                logger.error(f"[TokenArena] 自动同步出错: {e}", exc_info=True)

            interval = max(1, int(self.config.get("sync_interval_minutes", 30) or 30))
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=interval * 60
                )
                return
            except asyncio.TimeoutError:
                continue

    # ====
    # 同步实现
    # ====

    async def sync_once(self) -> str:
        """统计最近窗口内的用量并幂等上传到 TokenArena。返回结果描述。"""
        async with self._lock:
            base_url = (self.config.get("base_url") or "").strip().rstrip("/")
            api_key = (self.config.get("api_key") or "").strip()
            if not base_url:
                self._last_result = "未配置服务地址"
                return self._last_result
            if not api_key:
                self._last_result = "未配置 API Key"
                return self._last_result

            lookback_days = max(1, int(self.config.get("lookback_days", 2) or 2))
            source = (self.config.get("source_name") or "astrbot").strip() or "astrbot"

            buckets = await self._collect_buckets(lookback_days, source)
            if not buckets:
                self._last_sync_at = datetime.now(timezone.utc)
                self._last_result = "窗口内无新用量数据"
                return self._last_result

            payload = {
                "schemaVersion": SCHEMA_VERSION,
                "device": {
                    "deviceId": self._device_id,
                    "hostname": self._hostname,
                },
                "buckets": buckets,
                "sessions": [],
            }

            url = f"{base_url}/api/usage/ingest"
            # proxy 为 None 时 httpx 走 trust_env，即跟随 AstrBot 的全局代理。
            proxy = (self.config.get("proxy") or "").strip() or None
            if proxy:
                logger.debug(f"[TokenArena] 使用代理: {proxy}")
            async with httpx.AsyncClient(timeout=30, proxy=proxy) as client:
                resp = await client.post(
                    url,
                    json=payload,
                    headers={"Authorization": f"Bearer {api_key}"},
                )

            if resp.status_code == 200:
                self._last_sync_at = datetime.now(timezone.utc)
                self._last_result = f"成功上传 {len(buckets)} 个用量分桶"
                return self._last_result

            detail = resp.text[:200]
            self._last_result = f"服务端返回 {resp.status_code}: {detail}"
            raise RuntimeError(self._last_result)

    async def _collect_buckets(self, lookback_days: int, source: str) -> list[dict]:
        """从 provider_stats 表聚合出 TokenArena 的上传分桶。"""
        window_start = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        db = self.context.get_db()

        async with db.get_db() as session:
            result = await session.execute(
                select(ProviderStat).where(
                    ProviderStat.created_at >= window_start,
                    # 只统计内部 agent 的调用，与 AstrBot 官方仪表盘口径一致。
                    ProviderStat.agent_type == "internal",
                )
            )
            records = result.scalars().all()

        # key -> 累加的 token 计数
        agg: dict[tuple, dict] = defaultdict(
            lambda: {"input": 0, "cached": 0, "output": 0}
        )

        for record in records:
            created = self._ensure_utc(record.created_at)
            bucket_start = self._floor_bucket(created)
            model = record.provider_model or "unknown"
            project_key = record.provider_id or "unknown"
            key = (model, project_key, self._iso_z(bucket_start))

            agg[key]["input"] += int(record.token_input_other or 0)
            agg[key]["cached"] += int(record.token_input_cached or 0)
            agg[key]["output"] += int(record.token_output or 0)

        buckets: list[dict] = []
        for (model, project_key, bucket_iso), counts in agg.items():
            input_tokens = counts["input"]
            cached_tokens = counts["cached"]
            output_tokens = counts["output"]
            total = input_tokens + output_tokens + cached_tokens
            if total <= 0:
                continue
            buckets.append(
                {
                    "source": source,
                    "model": model,
                    "projectKey": project_key,
                    "projectLabel": project_key,
                    "bucketStart": bucket_iso,
                    "inputTokens": input_tokens,
                    "outputTokens": output_tokens,
                    "reasoningTokens": 0,
                    "cachedTokens": cached_tokens,
                    "totalTokens": total,
                }
            )

        return buckets

    # ====
    # 工具方法
    # ====

    def _describe_proxy(self) -> str:
        """描述当前生效的代理，供 status 展示。"""
        configured = (self.config.get("proxy") or "").strip()
        if configured:
            return self._mask_proxy(configured)
        # AstrBot 的全局代理是写进环境变量的，httpx 默认会跟随。
        inherited = os.environ.get("https_proxy") or os.environ.get("http_proxy")
        if inherited:
            return f"跟随全局设置（{self._mask_proxy(inherited)}）"
        return "未使用"

    @staticmethod
    def _mask_proxy(proxy: str) -> str:
        """隐藏代理地址里的账号密码，避免 status 在群聊中泄露凭据。"""
        if "@" not in proxy:
            return proxy
        scheme, sep, rest = proxy.rpartition("://")
        host = rest.rpartition("@")[2]
        return f"{scheme}{sep}***@{host}"

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _floor_bucket(value: datetime) -> datetime:
        """将时间向下取整到 30 分钟边界（UTC）。"""
        epoch = int(value.timestamp())
        floored = epoch - (epoch % BUCKET_SECONDS)
        return datetime.fromtimestamp(floored, tz=timezone.utc)

    @staticmethod
    def _iso_z(value: datetime) -> str:
        """生成 TokenArena 要求的 ISO8601 UTC 字符串（以 Z 结尾）。

        TokenArena 用 Zod 的 z.string().datetime() 校验，仅接受 Z 结尾的格式，
        不接受 +00:00 偏移。
        """
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _resolve_device_id(self) -> str:
        """返回稳定的设备 ID。优先用配置项，否则基于机器名生成并持久化。"""
        configured = (self.config.get("device_id") or "").strip()
        if configured:
            # TokenArena 要求 deviceId 至少 8 个字符。
            if len(configured) >= 8:
                return configured
            return hashlib.sha256(configured.encode()).hexdigest()[:32]

        id_file = self.data_dir / "device_id"
        try:
            if id_file.exists():
                saved = id_file.read_text(encoding="utf-8").strip()
                if len(saved) >= 8:
                    return saved
        except Exception as e:
            logger.warning(f"[TokenArena] 读取设备 ID 失败: {e}")

        seed = f"{socket.gethostname()}-{uuid.getnode()}"
        device_id = hashlib.sha256(seed.encode()).hexdigest()[:32]
        try:
            id_file.write_text(device_id, encoding="utf-8")
        except Exception as e:
            logger.warning(f"[TokenArena] 持久化设备 ID 失败: {e}")
        return device_id
