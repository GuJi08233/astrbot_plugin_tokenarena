"""Offline behavior tests using AstrBot's ProviderStat table and HTTP mocks."""

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from astrbot.core.star.filter.permission import PermissionType, PermissionTypeFilter
from conftest import ProviderStat, plugin_module


async def test_window_keeps_complete_oldest_bucket_and_excludes_cache_duplicates(
    database, plugin_factory, monkeypatch
):
    now = datetime(2026, 10, 13, 12, 17, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(plugin_module, "datetime", FixedDateTime)
    async with database.get_db() as session:
        session.add_all(
            [
                ProviderStat(
                    umo="private-platform-user-do-not-upload",
                    conversation_id="private-conversation-do-not-upload",
                    provider_id="provider-a",
                    provider_model="model-a",
                    created_at=now - timedelta(days=2, minutes=12),
                    token_input_other=100,
                    token_input_cached=25,
                    token_output=40,
                ),
                ProviderStat(
                    umo="private-platform-user-do-not-upload",
                    provider_id="provider-a",
                    provider_model="model-a",
                    created_at=now - timedelta(days=2) + timedelta(minutes=8),
                    token_input_other=10,
                    token_input_cached=5,
                    token_output=2,
                ),
                ProviderStat(
                    umo="ignored-old-record",
                    provider_id="provider-a",
                    provider_model="model-a",
                    created_at=now - timedelta(days=2, minutes=18),
                    token_input_other=9999,
                ),
                ProviderStat(
                    umo="ignored-future-record",
                    provider_id="provider-future",
                    provider_model="model-a",
                    created_at=now + timedelta(days=2),
                    token_input_other=9999,
                ),
                ProviderStat(
                    umo="ignored-external-agent",
                    provider_id="provider-a",
                    provider_model="model-a",
                    created_at=now - timedelta(days=2, minutes=5),
                    agent_type="external",
                    token_input_other=9999,
                ),
            ]
        )
        await session.commit()

    plugin = plugin_factory()
    first = await plugin._collect_buckets(2, "astrbot")
    second = await plugin._collect_buckets(2, "astrbot")

    assert first == second
    assert len(first) == 1
    bucket = first[0]
    assert bucket["bucketStart"] == "2026-10-11T12:00:00.000Z"
    assert bucket["inputTokens"] == 110
    assert bucket["cachedTokens"] == 30
    assert bucket["outputTokens"] == 42
    assert bucket["reasoningTokens"] == 0
    assert bucket.get("cacheCreationTokens", 0) == 0
    assert bucket["totalTokens"] == 182
    assert bucket["projectKey"] == "provider-a"
    assert "private" not in json.dumps(first)


async def test_upload_batches_complete_buckets_and_defers_achievements(
    database, plugin_factory, mock_http
):
    now = datetime.now(timezone.utc) - timedelta(minutes=1)
    async with database.get_db() as session:
        for number in range(205):
            session.add(
                ProviderStat(
                    umo="private-user",
                    provider_id=f"provider-{number:03d}",
                    provider_model="model-a",
                    created_at=now,
                    token_input_other=2,
                    token_input_cached=3,
                    token_output=5,
                )
            )
        await session.commit()

    uploads = []

    def receive(request):
        payload = json.loads(request.content)
        uploads.append(payload)
        assert request.url.path == "/api/usage/ingest"
        assert (
            request.headers["authorization"] == "Bearer test-api-key-not-a-real-secret"
        )
        assert "private-user" not in request.content.decode()
        assert payload["schemaVersion"] == 2
        assert payload["sessions"] == []
        return httpx.Response(
            200,
            json={
                "ok": True,
                "bucketCount": len(payload["buckets"]),
                "sessionCount": 0,
                "deviceId": payload["device"]["deviceId"],
            },
        )

    mock_http(receive)
    plugin = plugin_factory()
    await plugin.sync_once()
    assert [len(payload["buckets"]) for payload in uploads] == [100, 100, 5]
    assert [payload["syncAchievements"] for payload in uploads] == [False, False, True]
    buckets = [bucket for payload in uploads for bucket in payload["buckets"]]
    assert len({bucket["projectKey"] for bucket in buckets}) == 205
    assert all(bucket["totalTokens"] == 10 for bucket in buckets)
    assert plugin._last_sync_at is not None


async def test_initialize_starts_once_and_terminate_cancels(
    plugin_factory, monkeypatch
):
    plugin = plugin_factory({"enable_auto_sync": True})
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def loop():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    monkeypatch.setattr(plugin, "_sync_loop", loop)
    await plugin.initialize()
    await asyncio.wait_for(started.wait(), timeout=1)
    first_task = plugin._sync_task
    await plugin.initialize()
    assert plugin._sync_task is first_task
    await asyncio.wait_for(plugin.terminate(), timeout=1)
    assert first_task.done()
    assert stopped.is_set()
    await plugin.terminate()


async def test_initialize_respects_manual_mode(plugin_factory):
    plugin = plugin_factory({"enable_auto_sync": False})
    await plugin.initialize()
    assert plugin._sync_task is None
    await plugin.terminate()


@pytest.mark.parametrize(
    "proxy",
    [
        "http://alice:proxy-secret@localhost:7890",
        "socks5://localhost:1080",
        "socks5h://localhost:1080",
    ],
)
async def test_explicit_proxy_is_used_and_credentials_are_hidden(
    proxy, plugin_factory, mock_http, monkeypatch, caplog, sample_bucket
):
    plugin = plugin_factory({"proxy": proxy})
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    options = mock_http(
        lambda request: httpx.Response(
            200, json={"ok": True, "bucketCount": 1, "sessionCount": 0}
        )
    )
    caplog.set_level("DEBUG")
    await plugin.sync_once()
    assert options[0]["proxy"] == proxy
    assert "proxy-secret" not in plugin._describe_proxy()
    assert "alice" not in plugin._describe_proxy()
    assert "proxy-secret" not in caplog.text


@pytest.mark.parametrize("use_env_proxy", [True, False])
async def test_empty_proxy_respects_environment_policy(
    use_env_proxy, plugin_factory, mock_http, monkeypatch, sample_bucket
):
    monkeypatch.setenv("HTTPS_PROXY", "http://user:environment-secret@localhost:7890")
    monkeypatch.setenv("NO_PROXY", "tokenarena.example.test")
    plugin = plugin_factory({"use_env_proxy": use_env_proxy})
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    options = mock_http(
        lambda request: httpx.Response(
            200, json={"ok": True, "bucketCount": 1, "sessionCount": 0}
        )
    )
    await plugin.sync_once()
    assert options[0].get("proxy") is None
    assert options[0].get("trust_env", True) is use_env_proxy
    assert "environment-secret" not in plugin._describe_proxy()


@pytest.mark.parametrize(
    ("use_env_proxy", "no_proxy", "expected_pool"),
    [
        (True, "", "AsyncHTTPProxy"),
        (True, "tokenarena.example.test", "AsyncConnectionPool"),
        (False, "", "AsyncConnectionPool"),
    ],
)
async def test_environment_proxy_routes_and_no_proxy_are_preserved(
    use_env_proxy, no_proxy, expected_pool, plugin_factory, monkeypatch, sample_bucket
):
    monkeypatch.setenv("HTTPS_PROXY", "http://localhost:7890")
    monkeypatch.setenv("NO_PROXY", no_proxy)
    plugin = plugin_factory({"use_env_proxy": use_env_proxy})
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    pools = []
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, json={"ok": True, "bucketCount": 1, "sessionCount": 0}
        )
    )

    async def route_to_mock(real_transport, request):
        pools.append(type(real_transport._pool).__name__)
        return await transport.handle_async_request(request)

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", route_to_mock)
    await plugin.sync_once()
    assert pools == [expected_pool]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
async def test_transient_http_failures_are_retried_with_unchanged_payload(
    status, plugin_factory, mock_http, monkeypatch, sample_bucket
):
    plugin = plugin_factory()
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    sleeper = AsyncMock()
    monkeypatch.setattr(plugin_module.asyncio, "sleep", sleeper)
    requests = []

    def receive(request):
        requests.append(request.content)
        if len(requests) < 3:
            return httpx.Response(status, json={"error": "TEMPORARY_FAILURE"})
        return httpx.Response(200, json={"ok": True, "bucketCount": 1})

    mock_http(receive)
    await plugin.sync_once()
    assert len(requests) == 3
    assert len(set(requests)) == 1
    assert sleeper.await_count == 2
    assert plugin._last_sync_at is not None


async def test_transport_errors_have_bounded_retry_and_safe_state(
    plugin_factory, mock_http, monkeypatch, sample_bucket, caplog
):
    plugin = plugin_factory()
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    sleeper = AsyncMock()
    monkeypatch.setattr(plugin_module.asyncio, "sleep", sleeper)
    requests = []
    sensitive = "proxy-password-and-api-key-do-not-echo"

    def receive(request):
        requests.append(request)
        raise httpx.ConnectError(sensitive, request=request)

    mock_http(receive)
    caplog.set_level("DEBUG")
    with pytest.raises(RuntimeError) as failure:
        await plugin.sync_once()
    assert len(requests) == 3
    assert sleeper.await_count == 2
    assert "ConnectError" in plugin._last_result
    assert sensitive not in str(failure.value)
    assert sensitive not in plugin._last_result
    assert sensitive not in caplog.text
    assert plugin._last_sync_at is None


@pytest.mark.parametrize("status", [400, 401, 403, 404, 413])
async def test_permanent_http_errors_fail_once_without_exposing_server_response(
    status, plugin_factory, mock_http, monkeypatch, sample_bucket, caplog
):
    plugin = plugin_factory()
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    sleeper = AsyncMock()
    monkeypatch.setattr(plugin_module.asyncio, "sleep", sleeper)
    sensitive = "server-echoed-secret-do-not-show"
    calls = []

    def receive(request):
        calls.append(request)
        return httpx.Response(status, text=sensitive)

    mock_http(receive)
    caplog.set_level("DEBUG")
    with pytest.raises(RuntimeError) as failure:
        await plugin.sync_once()
    assert len(calls) == 1
    sleeper.assert_not_awaited()
    assert str(status) in plugin._last_result
    assert sensitive not in str(failure.value)
    assert sensitive not in plugin._last_result
    assert sensitive not in caplog.text


@pytest.mark.parametrize(
    "acknowledgment",
    [
        {"ok": False, "bucketCount": 1},
        {"ok": True, "bucketCount": 0},
        {"bucketCount": 1},
        [],
    ],
)
async def test_success_requires_matching_server_acknowledgment(
    acknowledgment, plugin_factory, mock_http, monkeypatch, sample_bucket
):
    plugin = plugin_factory()
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    mock_http(lambda request: httpx.Response(200, json=acknowledgment))
    with pytest.raises(RuntimeError):
        await plugin.sync_once()
    assert plugin._last_sync_at is None


def test_device_identity_is_stable_and_new_instances_are_distinct(
    plugin_factory, tmp_path
):
    first = plugin_factory(data_dir=tmp_path / "instance-a")
    reloaded = plugin_factory(data_dir=tmp_path / "instance-a")
    independent = plugin_factory(data_dir=tmp_path / "instance-b")
    assert first._device_id == reloaded._device_id
    assert first._device_id != independent._device_id
    assert len(first._device_id) >= 8


def test_saved_device_identity_is_preserved(plugin_factory, tmp_path):
    data_dir = tmp_path / "existing-instance"
    data_dir.mkdir()
    existing = "existing-stable-device-id"
    (data_dir / "device_id").write_text(existing, encoding="utf-8")
    plugin = plugin_factory(data_dir=data_dir)
    assert plugin._device_id == existing


def test_configured_device_identity_is_preserved(plugin_factory):
    plugin = plugin_factory({"device_id": "configured-stable-device-id"})
    assert plugin._device_id == "configured-stable-device-id"


@pytest.mark.parametrize("source", ["x" * 65, "来" * 22])
async def test_source_utf8_limit_is_checked_before_reading_or_uploading(
    source, plugin_factory, monkeypatch
):
    plugin = plugin_factory({"source_name": source})
    collect = AsyncMock()
    monkeypatch.setattr(plugin, "_collect_buckets", collect)
    with pytest.raises(RuntimeError) as failure:
        await plugin.sync_once()
    assert "64" in plugin._last_result
    assert source not in str(failure.value)
    collect.assert_not_awaited()


@pytest.mark.parametrize("device_id", ["x" * 129, "设" * 43])
def test_device_id_utf8_limit_rejects_unsafe_configuration(plugin_factory, device_id):
    with pytest.raises(ValueError) as failure:
        plugin_factory({"device_id": device_id})
    assert "128" in str(failure.value)
    assert device_id not in str(failure.value)


async def test_truncated_model_and_provider_keys_merge_before_upload(
    database, plugin_factory
):
    model_prefix = "模" * 85
    provider_prefix = "项" * 341
    now = datetime.now(timezone.utc) - timedelta(minutes=1)
    async with database.get_db() as session:
        session.add_all(
            [
                ProviderStat(
                    umo="private-user",
                    provider_id=provider_prefix + "目A",
                    provider_model=model_prefix + "型A",
                    created_at=now,
                    token_input_other=10,
                ),
                ProviderStat(
                    umo="private-user",
                    provider_id=provider_prefix + "目B",
                    provider_model=model_prefix + "型B",
                    created_at=now,
                    token_input_other=20,
                ),
            ]
        )
        await session.commit()
    buckets = await plugin_factory()._collect_buckets(2, "astrbot")
    assert len(buckets) == 1
    assert buckets[0]["model"] == model_prefix
    assert buckets[0]["projectKey"] == provider_prefix
    assert buckets[0]["totalTokens"] == 30


@pytest.mark.parametrize("command_name", ["sync", "status"])
def test_commands_reject_members_and_allow_administrators(command_name):
    group = plugin_module.TokenArenaPlugin.tokenarena.parent_group
    command = next(
        item for item in group.sub_command_filters if item.command_name == command_name
    )
    filters = (
        group.custom_filter_list
        + command.custom_filter_list
        + command.get_handler_md().event_filters
    )
    permissions = [
        item
        for item in filters
        if isinstance(item, PermissionTypeFilter)
        and item.permission_type == PermissionType.ADMIN
    ]
    assert permissions
    member = SimpleNamespace(is_admin=lambda: False)
    administrator = SimpleNamespace(is_admin=lambda: True)
    assert all(not permission.filter(member, {}) for permission in permissions)
    assert all(permission.filter(administrator, {}) for permission in permissions)


async def test_partial_upload_failure_can_be_safely_resynced(
    plugin_factory, mock_http, monkeypatch, sample_bucket
):
    plugin = plugin_factory()
    previous_success = datetime.now(timezone.utc) - timedelta(days=1)
    plugin._last_sync_at = previous_success
    buckets = [
        sample_bucket | {"projectKey": f"provider-{number}"} for number in range(205)
    ]
    monkeypatch.setattr(plugin, "_collect_buckets", AsyncMock(return_value=buckets))
    uploaded_keys = set()
    calls = []

    def receive(request):
        payload = json.loads(request.content)
        calls.append(payload)
        if len(calls) == 2:
            return httpx.Response(401, json={"error": "UNAUTHORIZED"})
        for bucket in payload["buckets"]:
            uploaded_keys.add(
                (
                    bucket["source"],
                    bucket["model"],
                    bucket["projectKey"],
                    bucket["bucketStart"],
                )
            )
        return httpx.Response(
            200, json={"ok": True, "bucketCount": len(payload["buckets"])}
        )

    mock_http(receive)
    with pytest.raises(RuntimeError):
        await plugin.sync_once()
    assert len(uploaded_keys) == 100
    assert "100/205" in plugin._last_result
    assert plugin._last_sync_at == previous_success
    await plugin.sync_once()
    assert len(uploaded_keys) == 205
    assert [len(payload["buckets"]) for payload in calls] == [100, 100, 100, 100, 5]
    assert plugin._last_sync_at > previous_success


@pytest.mark.parametrize(
    ("retry_after", "expected_delay"),
    [
        ("120", 30),
        ("-5", 0),
        ("invalid", 1),
        ("Tue, 13 Oct 2026 12:00:15 GMT", 15),
    ],
)
async def test_retry_after_is_bounded(
    retry_after, expected_delay, plugin_factory, mock_http, monkeypatch, sample_bucket
):
    now = datetime(2026, 10, 13, 12, 0, tzinfo=timezone.utc)

    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return now if tz else now.replace(tzinfo=None)

    monkeypatch.setattr(plugin_module, "datetime", FixedDateTime)
    plugin = plugin_factory()
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    sleeper = AsyncMock()
    monkeypatch.setattr(plugin_module.asyncio, "sleep", sleeper)
    calls = []

    def receive(request):
        calls.append(request)
        if len(calls) == 1:
            return httpx.Response(429, headers={"Retry-After": retry_after})
        return httpx.Response(200, json={"ok": True, "bucketCount": 1})

    mock_http(receive)
    await plugin.sync_once()
    sleeper.assert_awaited_once_with(expected_delay)


async def test_terminate_cancels_pending_retry_without_more_requests(
    plugin_factory, mock_http, monkeypatch, sample_bucket
):
    plugin = plugin_factory({"enable_auto_sync": True})
    monkeypatch.setattr(
        plugin, "_collect_buckets", AsyncMock(return_value=[sample_bucket])
    )
    retry_started = asyncio.Event()
    never_release = asyncio.Event()
    calls = []

    async def sleep_until_cancelled(delay):
        retry_started.set()
        await never_release.wait()

    def receive(request):
        calls.append(request)
        return httpx.Response(503)

    mock_http(receive)
    monkeypatch.setattr(plugin_module.asyncio, "sleep", sleep_until_cancelled)
    # Exercise managed-task cancellation during sync rather than the startup delay.
    monkeypatch.setattr(plugin, "_sync_loop", plugin.sync_once)
    await plugin.initialize()
    task = plugin._sync_task
    await asyncio.wait_for(retry_started.wait(), timeout=1)
    await asyncio.wait_for(plugin.terminate(), timeout=1)
    assert task.cancelled()
    assert len(calls) == 1
    assert plugin._last_sync_at is None


async def test_terminate_waits_for_manual_ack_and_blocks_sync_until_reinitialized(
    plugin_factory, mock_http, monkeypatch, sample_bucket
):
    plugin = plugin_factory({"enable_auto_sync": False})
    collect = AsyncMock(return_value=[sample_bucket])
    monkeypatch.setattr(plugin, "_collect_buckets", collect)
    request_started = asyncio.Event()
    allow_acknowledgment = asyncio.Event()
    calls = []

    async def receive(request):
        calls.append(request)
        request_started.set()
        await allow_acknowledgment.wait()
        return httpx.Response(200, json={"ok": True, "bucketCount": 1})

    mock_http(receive)
    await plugin.initialize()
    manual_sync = asyncio.create_task(plugin.sync_once())
    await asyncio.wait_for(request_started.wait(), timeout=1)
    termination = asyncio.create_task(plugin.terminate())
    await asyncio.sleep(0)
    assert not termination.done()

    allow_acknowledgment.set()
    await asyncio.wait_for(manual_sync, timeout=1)
    await asyncio.wait_for(termination, timeout=1)
    assert len(calls) == 1
    assert "停止" in await plugin.sync_once()
    assert len(calls) == 1
    assert collect.await_count == 1

    await plugin.initialize()
    await plugin.sync_once()
    assert len(calls) == 2
    assert collect.await_count == 2
