from framework.common import ProjectError


def select_chunk_source(chunk, candidates, inflight, measurements):
    # TODO S1：根据测量结果，从可用的 CDN 中为这个 chunk 选择一个；
    #          跳过处于 cooldown、quarantine 或 in-flight 名额已满的 CDN。
    # TODO S2：CDN 的状态变化后调整选择，可偶尔探索分配较少的 CDN；
    #          不得依赖 CDN 的 ID 或列表顺序。见接口说明第 6 节。
    raise ProjectError("NOT_IMPLEMENTED", "TODO S1-S2: adaptive scheduling", 501)
