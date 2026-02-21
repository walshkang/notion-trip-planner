import importlib.util
import sys
import types
from pathlib import Path


def _ensure_requests_available_for_import() -> None:
    try:
        import requests  # type: ignore # noqa: F401
        return
    except ModuleNotFoundError:
        pass

    requests_stub = types.ModuleType("requests")

    class RequestException(Exception):
        pass

    class Session:  # pragma: no cover - only used in offline test bootstrap
        def __init__(self) -> None:
            self.headers = {}

        def request(self, *_args, **_kwargs):
            raise RuntimeError("requests stub cannot perform network requests")

    requests_stub.Session = Session
    requests_stub.exceptions = types.SimpleNamespace(RequestException=RequestException)
    sys.modules["requests"] = requests_stub


def load_sync_module():
    _ensure_requests_available_for_import()
    module_path = Path(__file__).resolve().parents[1] / "notion_trip_sync.py"
    spec = importlib.util.spec_from_file_location("notion_trip_sync", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    sys.modules["notion_trip_sync"] = module
    spec.loader.exec_module(module)
    return module
