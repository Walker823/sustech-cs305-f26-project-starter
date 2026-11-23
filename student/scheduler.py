"""Source selection, called only by the file-task coordinator."""
import random
import time
import math
import statistics


def select_chunk_source(chunk, candidates, inflight, measurements):
    now = time.monotonic()
    usable = [node for node in candidates
              if not node.quarantined and not node.trial_inflight
              and (not node.recovering or inflight.get(node.node_id, 0) == 0)
              and now >= max(node.cooldown_until, node.retry_after_until)
              and inflight.get(node.node_id, 0) < node.limit]
    if not usable:
        return None
    policy = measurements.get("__policy__", {})
    rows = {node.node_id: measurements.get(node.node_id, {}) for node in usable}
    stale_after = policy.get("stale_after_s", 4.0)
    headroom = [node for node in usable if not (
        rows[node.node_id].get("status_updated_at") is not None
        and now - rows[node.node_id]["status_updated_at"] <= stale_after
        and rows[node.node_id].get("active_requests") is not None
        and rows[node.node_id]["active_requests"] == rows[node.node_id].get("max_active_requests"))]
    if headroom:
        # Scheduling preference, not a health-state transition: only real chunk
        # failures can cause cooldown/quarantine. Probe/Status keep running.
        usable = headroom

    def least_used(nodes):
        count = min(node.recent_assignments for node in nodes)
        return random.choice([node for node in nodes if node.recent_assignments == count])

    # Bootstrap unknown nodes with a real chunk, rather than treating missing
    # measurements as zero. Do not fill all their slots before one finishes.
    unknown = [node for node in usable if rows[node.node_id].get("goodput_mbps") is None
               and inflight.get(node.node_id, 0) == 0]
    if unknown:
        return least_used(unknown)
    interval = policy.get("exploration_every", 8)
    if (policy.get("learning_window_open", True)
            and policy.get("assignments", 0) % interval == interval - 1):
        return least_used(usable)

    known_rates = [row["goodput_mbps"] for row in rows.values() if row.get("goodput_mbps")]
    known_latency = [row["probe_latency_ms"] for row in rows.values()
                     if row.get("probe_latency_ms") is not None]
    fallback_rate = statistics.median(known_rates) if known_rates else 1.0
    fallback_latency = statistics.median(known_latency) if known_latency else 50.0

    def cost(node):
        row = rows[node.node_id]
        rate = row.get("goodput_mbps") or fallback_rate
        latency = row.get("probe_latency_ms")
        latency = fallback_latency if latency is None else latency
        updated = row.get("chunk_updated_at")
        if updated is not None and now - updated > stale_after:
            # Old rate estimates are progressively less optimistic. Periodic
            # real-chunk exploration is what discovers improvements.
            rate /= min(2.0, 1 + (now - updated) / stale_after * 0.1)
        load = row.get("load_ratio")
        status_at = row.get("status_updated_at")
        if load is None or status_at is None or now - status_at > stale_after:
            load = 0.0
        transfer = chunk.size * 8 / (max(rate, 1e-9) * 1_000_000)
        sharing = 1 + 0.6 * inflight.get(node.node_id, 0)
        return (latency / 1000 + transfer * sharing) * (1 + min(load, 2.0))

    scores = [(cost(node), node) for node in usable]
    best = min(score for score, _ in scores)
    return random.choice([node for score, node in scores
                          if math.isclose(score, best, rel_tol=1e-9, abs_tol=1e-12)])
