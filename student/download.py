from framework.common import ProjectError


def run_download(client, command):
    # TODO D1：从 Origin 获取并校验 Manifest；每个 chunk 按 Manifest 校验长度和 hash，
    #          写到临时文件的正确偏移；全部完成后校验整个文件。
    # TODO D2：经 CDN 下载 chunk，同时遵守全局和单 CDN 的 in-flight 上限，
    #          同一个 chunk 同时最多一个请求；每次请求使用新的 attempt_id。
    # TODO D3：retry、backoff、Retry-After、cooldown、quarantine 和总 deadline。
    # TODO D4：成功时以 atomic replace 写入目标文件；失败时删除临时文件，保留原有的目标文件。
    # 应输出 download_started，以及 download_complete 或 download_failed 事件；
    # 只有 atomic replace 成功后才返回 True。见接口说明第 3、5、6、7 节。
    raise ProjectError("NOT_IMPLEMENTED", "TODO D1-D4: download and recovery", 501)
