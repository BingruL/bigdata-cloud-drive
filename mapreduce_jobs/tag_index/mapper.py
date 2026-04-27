#!/usr/bin/env python3
"""
Tag 倒排索引 — Mapper
对应课程 MapReduce 章节：经典的"倒排索引"题型

输入：stdin 每行一条文件记录，TSV 格式
    file_id \t filename \t tags
其中 tags 是逗号分隔的多个 tag，例如 "Hadoop,大数据,文档"

输出：stdout 每行一条 (key, value) 对，TSV 格式
    tag \t file_id|filename
对每条记录拆出多个 tag，发射多次。reducer 将按 key 聚合。

执行方式（Hadoop Streaming 会自动调用 stdin/stdout）：
    cat files.tsv | python3 mapper.py
"""
import sys


def emit(tag, file_id, filename):
    """发射一条 (tag, file_id|filename) 记录到 stdout"""
    if not tag:
        return
    # Hadoop Streaming 用 \t 分割 key/value，因此 value 内不能有 \t
    safe_filename = filename.replace("\t", " ").replace("\n", " ")
    print(f"{tag}\t{file_id}|{safe_filename}")


def main():
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            # 容错：缺字段的行直接跳过（生产环境应记入 counter）
            continue
        file_id, filename, tags_field = parts[0], parts[1], parts[2]
        if not file_id or not tags_field:
            continue
        for tag in tags_field.split(","):
            tag = tag.strip()
            if tag:
                emit(tag, file_id, filename)


if __name__ == "__main__":
    main()
