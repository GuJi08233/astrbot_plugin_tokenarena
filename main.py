import asyncio
import hashlib
import socket
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core.db.po import ProviderStat
from sqlalchemy import select

SCHEMA_VERSION = 2
BUCKET_SECONDS = 30 * 60
MAX_BUCKETS = 100
MAX_TOKEN_COUNT = 1_000_000_000_000
MAX_ATTEMPTS = 3


class TokenArenaPlugin(Star):
    """Upload internal AstrBot agent usage to TokenArena."""

    def __init__(self, context: Context, config: AstrBotConfig):
        """Initialize isolated synchronization state and a persistent identity.

        Args:
            context: AstrBot plugin context providing database access.
            config: Plugin configuration loaded by AstrBot.
        """
        super().__init__(context)
        self.config = config
        self.data_dir: Path = StarTools.get_data_dir()
        self._sync_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._device_id = self._resolve_device_id()
        self._hostname = (
            socket.gethostname().encode("utf-8")[:255].decode("utf-8", errors="ignore")
            or "astrbot"
        )
        self._last_sync_at: datetime | None = None
        self._last_result = "尚未同步"

    async def initialize(self):
        """Start one background task on activation, including hot reloads."""
        self._stop_event.clear()
        if not self.config.get("enable_auto_sync", True):
            logger.info("[TokenArena] Automatic sync is disabled.")
            return
        if self._sync_task and not self._sync_task.done():
            return
        self._sync_task = asyncio.create_task(self._sync_loop(), name="tokenarena-sync")
        logger.info("[TokenArena] Automatic sync started.")

    async def terminate(self):
        """Stop the background task and drain manual uploads before unloading."""
        self._stop_event.set()
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass
        self._sync_task = None
        # A reloaded instance has its own lock; finish older manual snapshots first.
        async with self._lock:
            pass
        logger.info("[TokenArena] Stopped.")

    @filter.command_group("tokenarena", alias={"ta"})
    def tokenarena(self):
        """Register the TokenArena command group."""

    @tokenarena.command("sync")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_sync(self, event: AstrMessageEvent):
        """Manually sync usage as an AstrBot administrator.

        Args:
            event: Incoming administrator command.
        """
        yield event.plain_result("[TokenArena] 开始同步...")
        try:
            result = await self.sync_once()
            yield event.plain_result(f"[TokenArena] {result}")
        except Exception:
            logger.error("[TokenArena] Manual sync failed.")
            yield event.plain_result(f"[TokenArena] {self._last_result}")

    @tokenarena.command("status")
    @filter.permission_type(filter.PermissionType.ADMIN)
    async def cmd_status(self, event: AstrMessageEvent):
        """Show sanitized configuration and the last sync result.

        Args:
            event: Incoming administrator command.
        """
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
            f"- 服务地址: {self._mask_proxy(base_url) if base_url else '(未配置)'}",
            f"- API Key: {configured}",
            f"- 代理: {self._describe_proxy()}",
            f"- 自动同步: {auto}（间隔 {interval} 分钟）",
            f"- 设备 ID: {self._device_id}",
            f"- 上次成功同步: {last}",
            f"- 上次结果: {self._last_result}",
        ]
        yield event.plain_result("\n".join(lines))

    async def _sync_loop(self):
        """Sync periodically after a short startup delay."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=15)
            return
        except asyncio.TimeoutError:
            pass

        while not self._stop_event.is_set():
            try:
                await self.sync_once()
                logger.info("[TokenArena] Automatic sync completed.")
            except Exception:
                logger.error("[TokenArena] Automatic sync failed.")

            try:
                interval = max(
                    1, int(self.config.get("sync_interval_minutes", 30) or 30)
                )
            except (ValueError, TypeError, OverflowError):
                interval = 30
                logger.warning("[TokenArena] Invalid sync interval; using 30 minutes.")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=interval * 60)
                return
            except asyncio.TimeoutError:
                continue

    async def sync_once(self) -> str:
        """Upload complete buckets serially with bounded transient retries.

        Returns:
            A sanitized description of the completed sync or missing configuration.

        Raises:
            RuntimeError: If collecting or uploading usage fails.
        """
        async with self._lock:
            if self._stop_event.is_set():
                return "插件已停止"
            uploaded = 0
            buckets = []
            failure_detail = ""
            try:
                base_url = (self.config.get("base_url") or "").strip().rstrip("/")
                api_key = (self.config.get("api_key") or "").strip()
                if not base_url or not api_key:
                    self._last_result = (
                        "未配置服务地址" if not base_url else "未配置 API Key"
                    )
                    return self._last_result

                failure_detail = (
                    "服务地址无效，请填写不含账号密码、查询参数的 HTTP(S) 地址"
                )
                endpoint = httpx.URL(base_url)
                if (
                    endpoint.scheme not in {"http", "https"}
                    or not endpoint.host
                    or endpoint.userinfo
                    or endpoint.query
                    or endpoint.fragment
                ):
                    raise ValueError

                failure_detail = "回溯天数必须是有效的整数"
                lookback_days = max(1, int(self.config.get("lookback_days", 2) or 2))
                source = (
                    self.config.get("source_name") or "astrbot"
                ).strip() or "astrbot"
                failure_detail = "来源标识不能超过 64 个 UTF-8 字节"
                if len(source.encode("utf-8")) > 64:
                    raise ValueError

                failure_detail = (
                    "读取用量统计失败，请检查 AstrBot 版本、数据库和 Token 计数"
                )
                buckets = await self._collect_buckets(lookback_days, source)
                if not buckets:
                    self._last_sync_at = datetime.now(timezone.utc)
                    self._last_result = "窗口内无用量数据"
                    return self._last_result

                failure_detail = "代理配置无效，请检查地址格式和 SOCKS 依赖"
                proxy = (self.config.get("proxy") or "").strip() or None
                if proxy:
                    proxy_url = httpx.URL(proxy)
                    if (
                        proxy_url.scheme not in {"http", "https", "socks5", "socks5h"}
                        or not proxy_url.host
                        or proxy_url.query
                        or proxy_url.fragment
                    ):
                        raise ValueError
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(30, connect=10),
                    proxy=proxy,
                    trust_env=bool(self.config.get("use_env_proxy", True)),
                    follow_redirects=False,
                ) as client:
                    for offset in range(0, len(buckets), MAX_BUCKETS):
                        batch = buckets[offset : offset + MAX_BUCKETS]
                        payload = {
                            "schemaVersion": SCHEMA_VERSION,
                            "device": {
                                "deviceId": self._device_id,
                                "hostname": self._hostname,
                            },
                            "buckets": batch,
                            "sessions": [],
                            "syncAchievements": offset + len(batch) == len(buckets),
                        }
                        for attempt in range(MAX_ATTEMPTS):
                            delay = 2**attempt
                            try:
                                response = await client.post(
                                    f"{base_url}/api/usage/ingest",
                                    json=payload,
                                    headers={"Authorization": f"Bearer {api_key}"},
                                )
                            except httpx.TransportError as exc:
                                failure_detail = f"网络请求失败（{type(exc).__name__}）"
                                logger.warning(
                                    "[TokenArena] Transport failed (%s).",
                                    type(exc).__name__,
                                )
                                if attempt + 1 == MAX_ATTEMPTS:
                                    raise RuntimeError from None
                            else:
                                if response.status_code == 200:
                                    failure_detail = (
                                        "服务端未确认上传，请检查 TokenArena 服务地址"
                                    )
                                    confirmation = response.json()
                                    if (
                                        not isinstance(confirmation, dict)
                                        or confirmation.get("ok") is not True
                                        or confirmation.get("bucketCount") != len(batch)
                                    ):
                                        raise RuntimeError
                                    uploaded += len(batch)
                                    break

                                # Never expose response bodies or exception URLs with credentials.
                                failure_detail = (
                                    f"服务端返回 HTTP {response.status_code}"
                                )
                                logger.warning(
                                    "[TokenArena] Ingest returned HTTP %s.",
                                    response.status_code,
                                )
                                if response.status_code == 401:
                                    failure_detail += "，请检查 API Key 是否有效"
                                if (
                                    response.status_code
                                    not in {429, 500, 502, 503, 504}
                                    or attempt + 1 == MAX_ATTEMPTS
                                ):
                                    raise RuntimeError
                                retry_after = response.headers.get("Retry-After")
                                if retry_after:
                                    try:
                                        delay = float(retry_after)
                                    except ValueError:
                                        try:
                                            delay = (
                                                self._ensure_utc(
                                                    parsedate_to_datetime(retry_after)
                                                )
                                                - datetime.now(timezone.utc)
                                            ).total_seconds()
                                        except (ValueError, TypeError, OverflowError):
                                            pass
                            logger.warning(
                                "[TokenArena] Retrying upload (attempt %s/%s).",
                                attempt + 2,
                                MAX_ATTEMPTS,
                            )
                            await asyncio.sleep(max(0, min(delay, 30)))

                self._last_sync_at = datetime.now(timezone.utc)
                self._last_result = f"成功上传 {uploaded} 个用量分桶"
                return self._last_result
            except Exception as exc:
                self._last_result = (
                    f"同步失败：{failure_detail or type(exc).__name__}"
                    f"；已上传 {uploaded}/{len(buckets)} 个用量分桶"
                )
                logger.error(
                    "[TokenArena] Sync failed (%s); uploaded %s/%s buckets.",
                    type(exc).__name__,
                    uploaded,
                    len(buckets),
                )
                raise RuntimeError(self._last_result) from None

    async def _collect_buckets(self, lookback_days: int, source: str) -> list[dict]:
        """Stream only usage fields and aggregate complete UTC half-hour buckets.

        Args:
            lookback_days: Number of days to look back, rounded to a bucket boundary.
            source: Stable TokenArena source identifier.

        Returns:
            Deterministically ordered usage buckets with mutually exclusive counts.

        Raises:
            ValueError: If a bucket contains counts outside TokenArena's limits.
        """
        window_end = datetime.now(timezone.utc)
        # Upserts replace whole buckets; querying a partial first bucket loses history.
        window_start = self._floor_bucket(window_end - timedelta(days=lookback_days))
        db = self.context.get_db()
        agg: dict[tuple, dict] = defaultdict(
            lambda: {"input": 0, "cached": 0, "output": 0}
        )
        async with db.get_db() as session:
            records = await session.stream(
                select(
                    ProviderStat.created_at,
                    ProviderStat.provider_model,
                    ProviderStat.provider_id,
                    ProviderStat.token_input_other,
                    ProviderStat.token_input_cached,
                    ProviderStat.token_output,
                )
                .where(
                    ProviderStat.created_at >= window_start,
                    ProviderStat.created_at <= window_end,
                    ProviderStat.agent_type == "internal",
                )
                .execution_options(yield_per=1000)
            )
            async for record in records:
                created = self._ensure_utc(record.created_at)
                bucket_start = self._floor_bucket(created)
                # Match server truncation before grouping to prevent key collisions.
                model = (
                    (record.provider_model or "unknown")
                    .encode("utf-8")[:256]
                    .decode("utf-8", errors="ignore")
                )
                project_key = (
                    (record.provider_id or "unknown")
                    .encode("utf-8")[:1024]
                    .decode("utf-8", errors="ignore")
                )
                key = (model, project_key, self._iso_z(bucket_start))
                counts = (
                    int(record.token_input_other or 0),
                    int(record.token_input_cached or 0),
                    int(record.token_output or 0),
                )
                if min(counts) < 0:
                    raise ValueError("Negative token count in provider stats.")
                agg[key]["input"] += counts[0]
                agg[key]["cached"] += counts[1]
                agg[key]["output"] += counts[2]

        buckets: list[dict] = []
        for (model, project_key, bucket_iso), counts in sorted(agg.items()):
            input_tokens = counts["input"]
            cached_tokens = counts["cached"]
            output_tokens = counts["output"]
            total = input_tokens + output_tokens + cached_tokens
            if total > MAX_TOKEN_COUNT:
                raise ValueError("Token count exceeds the TokenArena bucket limit.")
            if total == 0:
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
                    "cacheCreationTokens": 0,
                    "totalTokens": total,
                }
            )
        return buckets

    def _describe_proxy(self) -> str:
        """Describe proxy policy without claiming a NO_PROXY route is proxied.

        Returns:
            Sanitized explicit proxy address or environment/direct policy.
        """
        configured = (self.config.get("proxy") or "").strip()
        if configured:
            return f"独立代理（{self._mask_proxy(configured)}）"
        if self.config.get("use_env_proxy", True):
            return "跟随 AstrBot/环境设置（遵循 NO_PROXY）"
        return "直连（不读取环境代理）"

    @staticmethod
    def _mask_proxy(proxy: str) -> str:
        """Display only an endpoint, redacting userinfo and omitting URL extras.

        Args:
            proxy: Proxy or service URL to display.

        Returns:
            The sanitized endpoint, or a generic invalid-address description.
        """
        try:
            url = httpx.URL(proxy)
            if not url.host or not url.scheme:
                return "地址格式无效"
            host = f"[{url.host}]" if ":" in url.host else url.host
            port = f":{url.port}" if url.port else ""
            auth = "***@" if url.userinfo else ""
            return f"{url.scheme}://{auth}{host}{port}"
        except (httpx.InvalidURL, ValueError):
            return "地址格式无效"

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        """Interpret AstrBot's naive database timestamps as UTC.

        Args:
            value: Naive UTC or timezone-aware timestamp.

        Returns:
            A timezone-aware UTC timestamp.
        """
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _floor_bucket(value: datetime) -> datetime:
        """Round a UTC timestamp down to a half-hour boundary.

        Args:
            value: Timezone-aware timestamp.

        Returns:
            The bucket start as a UTC timestamp.
        """
        epoch = int(value.timestamp())
        return datetime.fromtimestamp(epoch - epoch % BUCKET_SECONDS, tz=timezone.utc)

    @staticmethod
    def _iso_z(value: datetime) -> str:
        """Format a timestamp using TokenArena's UTC Z convention.

        Args:
            value: Timezone-aware timestamp.

        Returns:
            An ISO 8601 timestamp ending in Z.
        """
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _resolve_device_id(self) -> str:
        """Reuse an existing identity or persist a distinct ID for a new instance.

        Returns:
            A stable TokenArena device ID.

        Raises:
            ValueError: If a configured or persisted device ID is invalid.
            OSError: If a device ID cannot be read or persisted safely.
        """
        configured = (self.config.get("device_id") or "").strip()
        if configured:
            if len(configured.encode("utf-8")) > 128:
                raise ValueError("Configured device ID exceeds 128 UTF-8 bytes.")
            if len(configured) >= 8:
                return configured
            return hashlib.sha256(configured.encode()).hexdigest()[:32]

        id_file = self.data_dir / "device_id"
        if id_file.exists():
            saved = id_file.read_text(encoding="utf-8").strip()
            if len(saved) >= 8 and len(saved.encode("utf-8")) <= 128:
                return saved
            raise ValueError("Saved device ID is invalid; configure a valid device ID.")

        device_id = uuid.uuid4().hex
        self.data_dir.mkdir(parents=True, exist_ok=True)
        id_file.write_text(device_id, encoding="utf-8")
        return device_id
