import threading


class KVStore:
    """
    Thread-safe in-memory key-value store.

    FastAPI (via Uvicorn) can handle concurrent requests using threads
    for sync endpoints, so even a single node needs a lock around
    mutations to avoid race conditions on concurrent PUT/DELETE calls
    to the same key.
    """

    def __init__(self):
        self._data: dict[str, str] = {}
        self._lock = threading.Lock()

    def put(self, key: str, value: str) -> None:
        with self._lock:
            self._data[key] = value

    def get(self, key: str) -> str | None:
        # Reads don't strictly need the lock for a dict in CPython
        # (GIL makes single dict.get atomic), but we keep it for
        # consistency and because this will get replaced with real
        # logic later (versioning, tombstones, etc.)
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str) -> bool:
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def keys(self) -> list[str]:
        with self._lock:
            return list(self._data.keys())