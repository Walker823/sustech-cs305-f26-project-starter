# CS305 本地优化与验收记录

## v2 基线

本地提交 `5461fa3` 导入了队友附件中 `submission-v2.zip` 的 10 个 Python 模块，并保留了实验脚本。该 ZIP 的 SHA-256 为 `cccd2d856eb89fec1089d2fb7f5984ebb348cc9b9eadd0a71253ddae0521b74c`。课程原版框架下，五项公开样例和八组扩展回归都通过，两种初始快慢顺序的双向切换各通过 5/5。

独立复查发现两个反例。CDN 在 256 MiB 容器中以容量 28 缓存 8 MiB 分块，串行回源到第 29 个不同文件时触发 OOM。客户端在未尝试过的备选节点进行恢复试探时，会提前重试刚失败的节点。

## v3 改动

- CDN 对缓存、回源和正在发送的分块使用同一个 160 MiB 预算。达到预算时按 LRU 提前回收，配置的分块数仍是上限。大分块直接读入匿名映射，经完整性校验后以只读缓冲区发送。淘汰只能移除缓存引用，在途响应继续占用预算。持有在途响应导致预算耗尽时，最多等待 250 ms 后返回 503。
- CDN 校验 Manifest 后仅保留文件大小、分块大小与打包后的 SHA-256 摘要。小分块继续使用常规字节缓冲区，回源合并、成功条件和 `HIT/MISS` 与 Origin 审计记录的对应关系保持一致。
- 客户端先确定重试优先级，再检查恢复试探和普通请求的槽位。已过冷却的未尝试节点暂时满载时，等待期间不消耗重试次数。隔离、冷却、`Retry-After` 和迟到成功请求的处理沿用原实现。

## v3 验收结果

| 检查 | 结果 |
| --- | --- |
| 官方公开样例与扩展回归 | 5/5 项公开样例、8/8 组扩展回归通过 |
| 备选节点的恢复槽位被占用 | 等待试探请求完成，随后改派到备选节点，目标分块共尝试两次 |
| CDN 大分块与慢读压力 | 40 次 8 MiB 串行请求、8 路并发回源通过。慢连接在分块被淘汰后仍收到完整数据。容器峰值 **199.4 MiB** |
| 课程容器资源限制 | 检查 12 个容器。八节点用例达到了 32 路分块并发和 8 路测量并发，Client 内存峰值约 55.0 MiB，成功替换 8 MiB 旧文件且 SHA-256 一致。损坏及超时用例保留原输出 |
| 双向切换 | 两种初始快慢顺序各通过 5/5。每次切换后，新快节点获得该阶段的 80.6%–86.3% 分块 |
| 连接超时 | 在独立网络命名空间丢弃 3 个 SYN，连接超时、总截止时间和取消分别在约 150 ms、120 ms、40 ms 生效 |
| 长驻客户端 | 连续完成 30 次 2 MiB 下载，全部通过 SHA-256 校验。每轮任务后抽样仍为 2 个线程和 3 个文件描述符，总日志 124,509 字节 |
| 提交包 | 11 个条目、20,971 字节。格式、大小、CRC 和标准库导入检查通过，源码哈希与回归测试和 Docker 镜像一致 |

三种基准场景各执行四种策略，每种重复三次。v3 的 36 次下载全部通过输出哈希校验。表中耗时为中位数，v2 数值来自附件中的实验记录。

| 场景 | v2 自适应 | v3 自适应 | v3 轮询 | v3 失败请求数，自适应 / 轮询 |
| --- | ---: | ---: | ---: | --- |
| 独立链路 | 2.64 s | 2.76 s | 5.21 s | 0、0、0 / 0、0、0 |
| 共享瓶颈 | 3.24 s | 3.34 s | 3.34 s | 0、0、0 / 0、0、0 |
| 外部负载 | 3.24 s | 3.27 s | 3.37 s | 1、0、0 / 3、3、3 |

v3 在独立链路中每次将 32 个分块中的 28 个分给快节点。共享瓶颈限制了两种策略的吞吐；外部负载下，自适应减少了失败请求。v2 与 v3 的耗时来自各自的本地实验环境，小幅差异无法归因于代码改动。

## 复跑命令

在 WSL2 中从项目根目录执行。公开测试、扩展回归和基准在 Python 3.13 容器内运行。资源测试由 WSL 启动，为各 CDN 和 Client 容器分别设置资源限制。

```bash
docker build --pull=false -t cs305-optimized .
mkdir -p runtime

validate() {
  docker run --rm --network none --read-only \
    --tmpfs /tmp:rw,nosuid,nodev,mode=1777 --user 10001:10001 \
    --cap-drop ALL --security-opt no-new-privileges \
    --mount "type=bind,src=$PWD,dst=/work,readonly" \
    --mount "type=bind,src=$PWD/runtime,dst=/work/runtime" \
    -w /work -e PYTHONDONTWRITEBYTECODE=1 python:3.13-slim-bookworm "$@"
}

validate python -u tests/run_validation.py
validate python -u tests/retry_slots.py
validate python -u tests/adaptive_experiment.py --repeats 5 --output runtime/adaptive-v3-results.json
validate python -u tests/benchmark.py --repeats 3 --output runtime/benchmark-v3-results.json
validate python -u tests/stress_checks.py --tasks 30 --output runtime/stress-v3-results.json
python3 -B tests/cache_pressure.py
python3 -B tests/resource_checks.py

python3 -B tools/package_submission.py --output submission-v3.zip
python3 -B tests/verify_submission.py --archive submission-v3.zip --image cs305-optimized
```

测试报告和原始日志写入本地 `runtime/`。重新打包时使用一个尚不存在的 ZIP 文件名。

连接超时测试可在 WSL 中用 `sudo unshare --net -- python3 -B tests/connect_timeout.py` 复跑。脚本在独立命名空间中应用规则。
