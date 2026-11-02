from framework.common import ProjectError, integer


class LRUCache:
    def __init__(self, capacity):
        self.capacity = integer(capacity, "cache_capacity_chunks", 1, 100000)

    def get(self, key):
        # TODO C1：查找 key；cache hit 时更新 LRU 的最近使用顺序并返回数据，miss 时返回 None。
        # 该方法会被多个线程同时调用，读写应加锁。见接口说明第 3 节。
        raise ProjectError("NOT_IMPLEMENTED", "TODO C1: cache lookup", 501)

    def put(self, key, data):
        # TODO C2：写入数据；超过容量（按 chunk 数计）时按 LRU 做 eviction，
        # 返回被淘汰的 key，没有淘汰时返回 None。
        raise ProjectError("NOT_IMPLEMENTED", "TODO C2: LRU eviction", 501)

    def __len__(self):
        raise ProjectError("NOT_IMPLEMENTED", "TODO C2: cache size", 501)
