import os
import time
import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from app.store import KVStore
from app.hash_ring import HashRing
from app.metrics import metrics

app = FastAPI(title="Distributed KV Store - Node")

store = KVStore()

NODE_ID = os.environ.get("NODE_ID", "node-unknown")
ALL_NODES = os.environ.get("ALL_NODES", "").split(",")
NODE_URLS = {n: f"http://{n}:8000" for n in ALL_NODES}
REPLICATION_FACTOR = int(os.environ.get("REPLICATION_FACTOR", "3"))
WRITE_QUORUM = int(os.environ.get("WRITE_QUORUM", str(REPLICATION_FACTOR - 1)))

ring = HashRing(nodes=ALL_NODES, virtual_nodes=150)
http_client = httpx.Client(timeout=1.5)


class PutRequest(BaseModel):
    value: str


def _local_put(k: str, value: str):
    store.put(k, value)


def _local_get(k: str):
    return store.get(k)


def _local_delete(k: str):
    return store.delete(k)


def _write_to_node(node: str, method: str, k: str, value: str | None = None) -> tuple[str, bool, str | None]:
    try:
        if node == NODE_ID:
            if method == "PUT":
                _local_put(k, value)
            elif method == "DELETE":
                _local_delete(k)
            return (node, True, None)

        url = f"{NODE_URLS[node]}/internal/key/{k}"
        if method == "PUT":
            resp = http_client.put(url, json={"value": value})
        elif method == "DELETE":
            resp = http_client.delete(url)
        if resp.status_code not in (200, 404):
            resp.raise_for_status()
        return (node, True, None)
    except httpx.HTTPError as e:
        return (node, False, str(e))


@app.put("/key/{k}")
def put_key(k: str, body: PutRequest):
    request_start = time.monotonic()
    preference_list = ring.get_preference_list(k, REPLICATION_FACTOR)

    repl_start = time.monotonic()
    results = [_write_to_node(node, "PUT", k, body.value) for node in preference_list]
    replication_ms = (time.monotonic() - repl_start) * 1000

    succeeded = [n for n, ok, _ in results if ok]
    failed = [n for n, ok, _ in results if not ok]

    total_latency_ms = (time.monotonic() - request_start) * 1000

    if len(succeeded) < WRITE_QUORUM:
        metrics.record_request(
            method="PUT", path=f"/key/{k}", key=k, node_id=NODE_ID, role="coordinator",
            status_code=503, latency_ms=total_latency_ms, replication_ms=replication_ms,
            preference_list=preference_list, acked_by=succeeded, failed_nodes=failed,
        )
        raise HTTPException(
            status_code=503,
            detail=f"write quorum not met: {len(succeeded)}/{WRITE_QUORUM} required. "
                   f"Succeeded: {succeeded}, Failed: {failed}"
        )

    metrics.record_request(
        method="PUT", path=f"/key/{k}", key=k, node_id=NODE_ID, role="coordinator",
        status_code=200, latency_ms=total_latency_ms, replication_ms=replication_ms,
        preference_list=preference_list, acked_by=succeeded, failed_nodes=failed,
    )

    return {
        "status": "ok",
        "key": k,
        "preference_list": preference_list,
        "acked_by": succeeded,
        "failed_nodes": failed,
        "coordinator": NODE_ID,
        "replication_ms": round(replication_ms, 2),
    }


@app.get("/key/{k}")
def get_key(k: str):
    request_start = time.monotonic()
    preference_list = ring.get_preference_list(k, REPLICATION_FACTOR)

    last_error = None
    for i, node in enumerate(preference_list):
        try:
            if node == NODE_ID:
                value = _local_get(k)
                if value is None:
                    latency_ms = (time.monotonic() - request_start) * 1000
                    metrics.record_request(
                        method="GET", path=f"/key/{k}", key=k, node_id=NODE_ID,
                        role="coordinator", status_code=404, latency_ms=latency_ms,
                    )
                    raise HTTPException(status_code=404, detail=f"key '{k}' not found")
                latency_ms = (time.monotonic() - request_start) * 1000
                metrics.record_request(
                    method="GET", path=f"/key/{k}", key=k, node_id=NODE_ID,
                    role="coordinator", status_code=200, latency_ms=latency_ms,
                )
                return {"key": k, "value": value, "handled_by": NODE_ID, "attempt": i + 1}
            else:
                url = f"{NODE_URLS[node]}/internal/key/{k}"
                resp = http_client.get(url)
                if resp.status_code == 404:
                    latency_ms = (time.monotonic() - request_start) * 1000
                    metrics.record_request(
                        method="GET", path=f"/key/{k}", key=k, node_id=NODE_ID,
                        role="coordinator", status_code=404, latency_ms=latency_ms,
                    )
                    raise HTTPException(status_code=404, detail=f"key '{k}' not found")
                resp.raise_for_status()
                data = resp.json()
                data["coordinator"] = NODE_ID
                data["attempt"] = i + 1
                latency_ms = (time.monotonic() - request_start) * 1000
                metrics.record_request(
                    method="GET", path=f"/key/{k}", key=k, node_id=NODE_ID,
                    role="coordinator", status_code=200, latency_ms=latency_ms,
                )
                return data
        except HTTPException:
            raise
        except httpx.HTTPError as e:
            last_error = e
            continue

    latency_ms = (time.monotonic() - request_start) * 1000
    metrics.record_request(
        method="GET", path=f"/key/{k}", key=k, node_id=NODE_ID,
        role="coordinator", status_code=503, latency_ms=latency_ms,
    )
    raise HTTPException(
        status_code=503,
        detail=f"all nodes in preference list unreachable for key '{k}': {last_error}"
    )


@app.delete("/key/{k}")
def delete_key(k: str):
    request_start = time.monotonic()
    preference_list = ring.get_preference_list(k, REPLICATION_FACTOR)

    repl_start = time.monotonic()
    results = [_write_to_node(node, "DELETE", k) for node in preference_list]
    replication_ms = (time.monotonic() - repl_start) * 1000

    succeeded = [n for n, ok, _ in results if ok]
    failed = [n for n, ok, _ in results if not ok]
    total_latency_ms = (time.monotonic() - request_start) * 1000

    if len(succeeded) < WRITE_QUORUM:
        metrics.record_request(
            method="DELETE", path=f"/key/{k}", key=k, node_id=NODE_ID, role="coordinator",
            status_code=503, latency_ms=total_latency_ms, replication_ms=replication_ms,
            preference_list=preference_list, acked_by=succeeded, failed_nodes=failed,
        )
        raise HTTPException(
            status_code=503,
            detail=f"write quorum not met: {len(succeeded)}/{WRITE_QUORUM} required."
        )

    metrics.record_request(
        method="DELETE", path=f"/key/{k}", key=k, node_id=NODE_ID, role="coordinator",
        status_code=200, latency_ms=total_latency_ms, replication_ms=replication_ms,
        preference_list=preference_list, acked_by=succeeded, failed_nodes=failed,
    )

    return {
        "status": "ok",
        "key": k,
        "preference_list": preference_list,
        "acked_by": succeeded,
        "failed_nodes": failed,
        "coordinator": NODE_ID,
        "replication_ms": round(replication_ms, 2),
    }


# --- Internal node-to-node endpoints (unchanged) ---

@app.put("/internal/key/{k}")
def internal_put(k: str, body: PutRequest):
    _local_put(k, body.value)
    return {"status": "ok", "handled_by": NODE_ID, "key": k}


@app.get("/internal/key/{k}")
def internal_get(k: str):
    value = _local_get(k)
    if value is None:
        raise HTTPException(status_code=404, detail=f"key '{k}' not found")
    return {"key": k, "value": value, "handled_by": NODE_ID}


@app.delete("/internal/key/{k}")
def internal_delete(k: str):
    deleted = _local_delete(k)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"key '{k}' not found")
    return {"status": "ok", "handled_by": NODE_ID, "key": k}


@app.get("/health")
def health():
    return {"status": "healthy", "node": NODE_ID}


@app.get("/debug/keys")
def debug_keys():
    return {"node": NODE_ID, "keys": store.keys()}