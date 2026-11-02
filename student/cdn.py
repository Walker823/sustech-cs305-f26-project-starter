import threading
from framework.common import Events, ProjectError, base_url, integer, valid_id
from framework.cdn_runtime import main
from student.cache import LRUCache


class CDN:
    def __init__(self, config):
        self.config = config
        self.node_id = valid_id(config["cdn_id"])
        self.origin_url = base_url(config["origin_url"])
        self.token = config["origin_chunk_token"]
        self.cache = LRUCache(config["cache_capacity_chunks"])
        self.limit = integer(config.get("max_active_requests", 8), "max_active_requests", 1, 64)
        self.slots = threading.BoundedSemaphore(self.limit)
        self.lock = threading.Lock()
        self.active = 0
        self.events = Events("cdn", self.node_id)

    def get_chunk(self, file_id, chunk_id, request_id):
        # TODO C3：从 Origin 获取 Manifest，先查 cache；miss 时向 Origin 发起 origin fetch
        # （带 X-Origin-Token header），按 Manifest 校验长度和 hash，通过后才写入 cache。
        # 返回 (数据, "HIT" 或 "MISS", Manifest 中该 chunk 的 SHA-256)；
        # 文件或 chunk 不存在、origin fetch 失败时抛出带 HTTP 状态码的 ProjectError。
        # 见接口说明第 1.2、2.3、3 节。
        raise ProjectError("NOT_IMPLEMENTED", "TODO C3: CDN chunk service", 501)


if __name__ == "__main__":
    raise SystemExit(main(CDN))
