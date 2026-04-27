#!/usr/bin/env python3
"""
HTTP 接口性能压测脚本

用途：
1. 演示分布式系统在并发压力下的吞吐 / 延迟特性
2. 对比 "Kafka 启用 vs 未启用" 时上传接口的差异（前者写日志走异步队列）

测试场景：
- 上传接口（含 HDFS 写 + HBase 写 + EventBus 日志），写密集型
- 下载接口，读密集型
- 列表接口，scan 密集型

运行方式：
    # 1. 先用 --seed 把后端起起来：python run.py --seed
    # 2. 然后跑压测：
    python3 scripts/benchmark.py --base-url http://localhost:5000 \\
                                  --user alice --password 123456 \\
                                  --concurrency 10 --total 200

    # 对比 Kafka 启用前后：
    python3 scripts/benchmark.py --label "no-kafka"  > before.txt
    # 启用 Kafka 后再跑一次：
    KAFKA_ENABLED=1 python3 scripts/benchmark.py --label "with-kafka"  > after.txt

输出：
    - p50 / p95 / p99 延迟、QPS、错误率
    - 一行 CSV 摘要，便于多次运行后画图
"""
import argparse
import io
import json
import os
import sys
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


def login(base_url, username, password):
    rv = requests.post(f"{base_url}/api/auth/login",
                       json={"username": username, "password": password},
                       timeout=10)
    rv.raise_for_status()
    return rv.json()["token"]


def task_upload(base_url, headers, payload_bytes, idx):
    """单次上传任务，返回 (latency_ms, ok)"""
    start = time.perf_counter()
    try:
        resp = requests.post(
            f"{base_url}/api/files/upload",
            headers=headers,
            files={"file": (f"bench-{idx}.txt", io.BytesIO(payload_bytes), "text/plain")},
            timeout=30,
        )
        ok = resp.status_code == 201
    except Exception:
        ok = False
    return (time.perf_counter() - start) * 1000.0, ok


def task_list(base_url, headers, _payload, _idx):
    start = time.perf_counter()
    try:
        resp = requests.get(f"{base_url}/api/files/list", headers=headers, timeout=10)
        ok = resp.status_code == 200
    except Exception:
        ok = False
    return (time.perf_counter() - start) * 1000.0, ok


SCENARIOS = {"upload": task_upload, "list": task_list}


def percentile(values, pct):
    if not values:
        return 0.0
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(round(pct / 100.0 * (len(s) - 1)))))
    return s[k]


def run_scenario(name, fn, base_url, headers, total, concurrency, payload_size):
    payload = b"X" * payload_size
    latencies = []
    ok_count = 0

    print(f"\n=== 场景: {name}  并发={concurrency}  总请求={total}  payload={payload_size}B ===")
    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futures = [ex.submit(fn, base_url, headers, payload, i) for i in range(total)]
        for f in as_completed(futures):
            lat, ok = f.result()
            latencies.append(lat)
            if ok:
                ok_count += 1
    wall = time.perf_counter() - wall_start

    qps = total / wall if wall > 0 else 0
    err_rate = (total - ok_count) / total * 100
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)
    mean = statistics.mean(latencies) if latencies else 0

    print(f"  耗时:    {wall:.2f} s")
    print(f"  成功率:  {(100 - err_rate):.1f}%   错误数: {total - ok_count}")
    print(f"  QPS:     {qps:.1f}")
    print(f"  延迟ms:  mean={mean:.1f}  p50={p50:.1f}  p95={p95:.1f}  p99={p99:.1f}")

    return {
        "scenario": name,
        "total": total,
        "concurrency": concurrency,
        "payload_size": payload_size,
        "wall_seconds": round(wall, 3),
        "qps": round(qps, 2),
        "error_rate_pct": round(err_rate, 2),
        "mean_ms": round(mean, 2),
        "p50_ms": round(p50, 2),
        "p95_ms": round(p95, 2),
        "p99_ms": round(p99, 2),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", default="http://localhost:5000")
    p.add_argument("--user", default="alice")
    p.add_argument("--password", default="123456")
    p.add_argument("--concurrency", type=int, default=10)
    p.add_argument("--total", type=int, default=200)
    p.add_argument("--payload-size", type=int, default=1024,
                   help="上传文件大小（字节），默认 1KB")
    p.add_argument("--scenarios", default="upload,list",
                   help="逗号分隔的场景列表：upload, list")
    p.add_argument("--label", default="default",
                   help="本次运行的标签（输出 CSV 时区分多次运行）")
    p.add_argument("--csv-out", default=None,
                   help="若指定路径，则把 CSV 摘要追加写入该文件")
    args = p.parse_args()

    print(f"压测目标: {args.base_url}   用户: {args.user}   标签: {args.label}")
    print("登录获取 token...")
    try:
        token = login(args.base_url, args.user, args.password)
    except Exception as e:
        print(f"[错误] 登录失败: {e}\n请先 python run.py --seed 启动后端，并确认账号存在。")
        sys.exit(1)
    headers = {"Authorization": f"Bearer {token}"}

    # 预热一次：建立连接、AI 摘要懒加载等
    print("预热请求...")
    requests.get(f"{args.base_url}/api/files/list", headers=headers, timeout=10)

    results = []
    for scen in args.scenarios.split(","):
        scen = scen.strip()
        if scen not in SCENARIOS:
            print(f"[警告] 未知场景: {scen}，跳过")
            continue
        results.append(run_scenario(
            scen, SCENARIOS[scen], args.base_url, headers,
            total=args.total, concurrency=args.concurrency,
            payload_size=args.payload_size,
        ))

    # CSV 摘要
    print("\n=== CSV 摘要 ===")
    cols = ["label", "scenario", "concurrency", "total", "qps",
            "mean_ms", "p50_ms", "p95_ms", "p99_ms", "error_rate_pct"]
    print(",".join(cols))
    for r in results:
        row = [args.label] + [str(r[c]) if c in r else "" for c in cols[1:]]
        print(",".join(row))

    if args.csv_out:
        write_header = not os.path.exists(args.csv_out) or os.path.getsize(args.csv_out) == 0
        with open(args.csv_out, "a", encoding="utf-8") as f:
            if write_header:
                f.write(",".join(cols) + "\n")
            for r in results:
                row = [args.label] + [str(r[c]) for c in cols[1:]]
                f.write(",".join(row) + "\n")
        print(f"\n已追加写入 {args.csv_out}")

    # 也以 JSON 输出一份完整数据到 stdout 末尾，便于程序化处理
    print("\n=== JSON 全量结果 ===")
    print(json.dumps({"label": args.label, "results": results}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
