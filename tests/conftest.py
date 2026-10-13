"""Bootstrap real AstrBot imports against disposable data and SQLite files."""

import importlib
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

# AstrBot creates its global config and data paths during module imports.
# Override the root before importing any AstrBot module, including the plugin.
IMPORT_DATA_ROOT = tempfile.TemporaryDirectory(prefix="tokenarena-tests-")
os.environ["ASTRBOT_ROOT"] = IMPORT_DATA_ROOT.name
os.environ["ASTRBOT_TEST_MODE"] = "true"
if source_path := os.environ.get("ASTRBOT_SOURCE"):
    sys.path.insert(0, str(Path(source_path).resolve()))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

plugin_module = importlib.import_module("main")
ProviderStat = plugin_module.ProviderStat


def pytest_sessionfinish(session, exitstatus):
    """Clean up only the import-time root created by this test session.

    Args:
        session: Pytest session invoking the cleanup hook.
        exitstatus: Test session exit code supplied by pytest.
    """
    root_path = Path(IMPORT_DATA_ROOT.name).resolve()
    if root_path.parent != Path(tempfile.gettempdir()).resolve():
        raise AssertionError("The disposable AstrBot root escaped the temp directory")
    IMPORT_DATA_ROOT.cleanup()


@pytest.fixture(autouse=True)
def isolate_network(monkeypatch):
    """Reject real HTTP traffic and remove ambient proxy configuration.

    Args:
        monkeypatch: Pytest patch fixture that restores transports and environment.
    """
    for name in (
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
    ):
        monkeypatch.delenv(name, raising=False)

    async def reject_async_request(*args, **kwargs):
        raise AssertionError("Tests must use httpx.MockTransport")

    def reject_request(*args, **kwargs):
        raise AssertionError("Tests must use httpx.MockTransport")

    monkeypatch.setattr(
        httpx.AsyncHTTPTransport, "handle_async_request", reject_async_request
    )
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", reject_request)


@pytest_asyncio.fixture
async def database(tmp_path):
    """Provide actual ProviderStat rows in a disposable SQLite database.

    Args:
        tmp_path: Disposable directory for the SQLite file.

    Yields:
        A database object exposing AstrBot-compatible asynchronous sessions.
    """
    db_path = tmp_path / "usage.sqlite3"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path.as_posix()}")
    async with engine.begin() as connection:
        await connection.run_sync(ProviderStat.__table__.create)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    yield SimpleNamespace(get_db=sessions)
    await engine.dispose()


@pytest.fixture
def plugin_factory(database, tmp_path, monkeypatch):
    """Create plugin instances with real host imports and temporary identity files.

    Args:
        database: Disposable SQLite database with the real ProviderStat table.
        tmp_path: Disposable directory for per-instance identity files.
        monkeypatch: Pytest fixture used to redirect the plugin's data directory.

    Returns:
        A factory accepting optional configuration and an instance data directory.
    """
    config_defaults = {
        "base_url": "https://tokenarena.example.test",
        "api_key": "test-api-key-not-a-real-secret",
        "enable_auto_sync": False,
        "sync_interval_minutes": 30,
        "lookback_days": 2,
        "source_name": "astrbot",
        "proxy": "",
        "use_env_proxy": True,
    }

    def create_plugin(config=None, data_dir=None):
        plugin_data = data_dir or tmp_path / "plugin-data"
        plugin_data.mkdir(parents=True, exist_ok=True)
        monkeypatch.setattr(
            plugin_module.StarTools, "get_data_dir", lambda: plugin_data
        )
        context = SimpleNamespace(get_db=lambda: database)
        return plugin_module.TokenArenaPlugin(context, config_defaults | (config or {}))

    return create_plugin


@pytest.fixture
def mock_http(monkeypatch):
    """Install a MockTransport while recording the real client's proxy arguments.

    Args:
        monkeypatch: Pytest fixture used to replace the HTTP client constructor.

    Returns:
        An installer accepting a request handler and returning recorded options.
    """
    original_client = httpx.AsyncClient

    def install(handler):
        client_options = []

        def create_client(*args, **kwargs):
            client_options.append(dict(kwargs))
            kwargs.pop("proxy", None)
            kwargs["trust_env"] = False
            kwargs["transport"] = httpx.MockTransport(handler)
            return original_client(*args, **kwargs)

        monkeypatch.setattr(plugin_module.httpx, "AsyncClient", create_client)
        return client_options

    return install


@pytest.fixture
def sample_bucket():
    """Return a valid, content-free bucket for isolated transport tests.

    Returns:
        One valid TokenArena usage bucket containing only synthetic metadata.
    """
    return {
        "source": "astrbot",
        "model": "model-a",
        "projectKey": "provider-a",
        "projectLabel": "provider-a",
        "bucketStart": "2026-10-13T00:00:00.000Z",
        "inputTokens": 1,
        "cachedTokens": 0,
        "outputTokens": 0,
        "reasoningTokens": 0,
        "cacheCreationTokens": 0,
        "totalTokens": 1,
    }
