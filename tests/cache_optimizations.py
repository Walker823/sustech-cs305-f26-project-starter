"""Check coalesced fills against real Origin audit and metadata byte budgets."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
import io
import threading

from extended_tests import laboratory, require
from framework.common import get_bytes, sha256
from framework.data import make_file
from student.cache import ByteLRUCache, resident_bytes
from student.cdn import CDN
from tools.public_tests import events


def burst(url, count=12):
    barrier = threading.Barrier(count)

    def request(index):
        barrier.wait()
        return get_bytes(url, "/chunk/demo/0", headers={"X-Request-ID": "burst-" + str(index)})

    with ThreadPoolExecutor(max_workers=count) as pool:
        return list(pool.map(request, range(count)))


def main():
    sample = {"chunks": [{"id": n, "size": 10, "sha256": "a" * 64} for n in range(8)]}
    cost = resident_bytes(("sample", sample))
    cache = ByteLRUCache(cost * 2 + 100, max_items=64)
    cache.put("sample", sample)
    cache.put("other", sample)
    cache.get("sample")
    cache.put("third", sample)
    require(cache.get("other") is None and cache.get("sample") is sample, "metadata byte LRU")
    require(cache.bytes_used <= cache.capacity, "metadata byte budget")
    cache.put("oversized", {"payload": b"x" * (cache.capacity + 1)})
    require(cache.get("oversized") is None and cache.bytes_used <= cache.capacity,
            "oversized metadata retained")

    with laboratory() as lab:
        proxy, _ = lab.proxy("coalesce-origin", lab.origin_url, {"latency_ms": 120})
        _, _, cdn = lab.service("student.cdn", "coalesced-cdn", {
            "cdn_id": "merged", "origin_url": proxy, "origin_chunk_token": lab.token,
            "cache_capacity_chunks": 2, "max_active_requests": 16,
        })
        replies = burst(cdn)
        require(all(reply.status == 200 for reply in replies), "concurrent coalesced service")
        require(sum(reply.headers["x-cache"] == "MISS" for reply in replies) == 1, "one auditable MISS")
        require(all(sha256(reply.body) == lab.manifests["demo"]["chunks"][0]["sha256"]
                    for reply in replies), "coalesced bytes corrupted")
        audit = events(lab.root / "origin-audit.jsonl")
        fetched = [row for row in audit if row["event"] == "origin_chunk_request"]
        require(len(fetched) == 1, "duplicate origin fetch despite coalescing")
        miss = next(reply for reply in replies if reply.headers["x-cache"] == "MISS")
        require(fetched[0]["request_id"] == miss.headers["x-request-id"], "MISS/audit request mismatch")

        bad, _ = lab.proxy("coalesce-failure", lab.origin_url, {"latency_ms": 120, "rules": [
            {"action": "status", "status": 503, "count": 1},
        ]})
        _, _, failing = lab.service("student.cdn", "coalesced-failure-cdn", {
            "cdn_id": "failure", "origin_url": bad, "origin_chunk_token": lab.token,
            "cache_capacity_chunks": 2, "max_active_requests": 16,
        })
        require(all(reply.status == 502 for reply in burst(failing)), "shared failure was not broadcast")
        recovered = get_bytes(failing, "/chunk/demo/0")
        require(recovered.status == 200 and recovered.headers["x-cache"] == "MISS", "error was cached")

        # Exercise the actual CDN metadata cache across many different files.
        with redirect_stdout(io.StringIO()):
            node = CDN({"cdn_id": "metadata", "origin_url": lab.origin_url,
                        "origin_chunk_token": lab.token, "cache_capacity_chunks": 2,
                        "manifest_cache_bytes": 1024})
            try:
                for index in range(20):
                    file_id = "meta" + str(index)
                    make_file(lab.root / "data", file_id, 32, 32)
                    node.get_chunk(file_id, 0, "meta-request")
                    require(node._manifests.bytes_used <= 1024, "actual CDN metadata memory budget")
                require(len(node._manifests) < 20, "metadata was not evicted")
            finally:
                node.events.close()

        # A cached verified chunk must remain usable without cached metadata.
        origin, _, offline_origin = lab.service("framework.origin", "offline-origin", {
            "host": "127.0.0.1", "port": 0, "data_root": str(lab.root / "data"),
            "chunk_access_token": lab.token, "audit_path": str(lab.root / "offline-origin-audit.jsonl"),
        })
        with redirect_stdout(io.StringIO()):
            node = CDN({"cdn_id": "offline", "origin_url": offline_origin,
                        "origin_chunk_token": lab.token, "cache_capacity_chunks": 2,
                        "manifest_cache_bytes": 1})
            try:
                first, status, digest = node.get_chunk("demo", 0, "before-offline")
                require(status == "MISS" and len(node._manifests) == 0, "uncached metadata setup")
                origin.terminate()
                origin.wait(timeout=3)
                cached, status, cached_digest = node.get_chunk("demo", 0, "after-offline")
                require(status == "HIT" and cached == first and cached_digest == digest,
                        "cached chunk incorrectly depended on Origin/Manifest availability")
            finally:
                node.events.close()
    print("PASS cache_optimizations: one origin MISS/12 requests; matching audit; shared error/recovery; byte LRU; offline HIT after metadata eviction")


if __name__ == "__main__":
    main()
