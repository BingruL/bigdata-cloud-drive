# 多用户大文件并发上传 / 下载压测

`scripts/large_file_concurrency_benchmark.py` 用于验证网盘服务层的大文件并发能力。它走真实 Web API，不绕过 Flask 上传接口：

1. 登录或注册多个测试用户。
2. 生成或复用一个指定大小的本地 payload。
3. 多用户并发上传大文件。
4. 上传成功后并发下载这些文件，并校验下载字节数。
5. 输出上传/下载耗时、吞吐、错误率、p50/p95/p99。

## 预检

先用 100MB 验证链路：

```bash
python scripts/large_file_concurrency_benchmark.py \
  --users benchu1,benchu2 \
  --file-size 100MB \
  --files-per-user 1 \
  --concurrency 2 \
  --csv-out reports/large-file-concurrency.csv
```

也可以运行默认 Makefile 入口：

```bash
make large-file-benchmark
```

## GB 级并发测试

4 个用户同时上传并下载 1GB 文件：

```bash
python scripts/large_file_concurrency_benchmark.py \
  --users benchu1,benchu2,benchu3,benchu4 \
  --file-size 1GB \
  --files-per-user 1 \
  --concurrency 4 \
  --csv-out reports/large-file-concurrency.csv \
  --json-out reports/large-file-concurrency.json
```

如需复用已有 ISO / checkpoint 文件，避免重新生成 payload：

```bash
python scripts/large_file_concurrency_benchmark.py \
  --payload-path /path/to/big.iso \
  --file-size 1GB \
  --users benchu1,benchu2 \
  --concurrency 2
```

`--file-size` 必须与 `--payload-path` 文件大小一致；否则脚本会重写 payload 路径。

## 口径说明

这个实验验证的是 Web 服务层能力，包括浏览器式 multipart 上传、Flask 接口、临时文件、HDFS 写入、HBase 元数据、下载接口和权限校验。它和 `scale_benchmark.py` 的数据处理规模实验是互补关系，不应混为一谈。

答辩时建议这样表述：

> 除了 HBase/HDFS/Spark 的规模数据实验，我还单独做了 Web 服务层的大文件并发压测。该压测通过真实 HTTP 上传和下载接口，记录多用户并发上传/下载 GB 文件时的耗时、吞吐和错误率。
