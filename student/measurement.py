from framework.common import ProjectError


class Measurements:
    def observe_probe(self, node_id, elapsed_ms):
        # TODO M1：记录 Probe 请求的 latency（毫秒），宜用 EWMA 平滑；未知值为 None。
        raise ProjectError("NOT_IMPLEMENTED", "TODO M1: probe latency", 501)

    def observe_chunk(self, node_id, byte_count, elapsed_ms):
        # TODO M2：只用通过 hash 校验的 chunk 更新 goodput（Mbps）。
        raise ProjectError("NOT_IMPLEMENTED", "TODO M2: goodput", 501)

    def observe_status(self, node_id, status):
        # TODO M3：根据 /status 的 active_requests 和 max_active_requests 计算 load。
        # Probe 和 Status 请求的频率与并发有上限，见接口说明第 5 节。
        raise ProjectError("NOT_IMPLEMENTED", "TODO M3: load", 501)

    def snapshot(self):
        # 返回各 CDN 当前的 latency、goodput 和 load 估计值，未知的为 None。
        raise ProjectError("NOT_IMPLEMENTED", "TODO M1-M3: measurement state", 501)
