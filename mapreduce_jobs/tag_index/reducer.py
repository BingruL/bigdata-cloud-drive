#!/usr/bin/env python3
"""
Tag 倒排索引 — Reducer
对应课程 MapReduce 章节：reduce 阶段聚合相同 key 的 values

Hadoop Streaming 在 reduce 前会按 key 排序并将相同 key 的行连续送入同一 reducer。
所以 reducer 只需顺序扫描，遇到 key 变化就输出一条聚合结果。

输入：stdin 每行 TSV，按 key 已排序
    tag \t file_id|filename

输出：stdout 每行 TSV，每个 tag 一行
    tag \t {"count": N, "files": [{"file_id": ..., "filename": ...}, ...]}
JSON 是为了后续 load_index_to_hbase.py 直接整列写入 HBase。

执行方式：
    sort | python3 reducer.py
"""
import sys
import json


def flush(current_tag, files):
    if current_tag is None:
        return
    # 去重：同一 (file_id, filename) 可能在 mapper 阶段被多次 emit（极少见，但稳妥）
    seen = set()
    deduped = []
    for f in files:
        if f["file_id"] in seen:
            continue
        seen.add(f["file_id"])
        deduped.append(f)
    payload = {"count": len(deduped), "files": deduped}
    print(f"{current_tag}\t{json.dumps(payload, ensure_ascii=False)}")


def main():
    current_tag = None
    bucket = []
    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        if "\t" not in line:
            continue
        tag, value = line.split("\t", 1)
        if "|" in value:
            file_id, filename = value.split("|", 1)
        else:
            file_id, filename = value, ""
        if tag != current_tag:
            flush(current_tag, bucket)
            current_tag = tag
            bucket = []
        bucket.append({"file_id": file_id, "filename": filename})
    flush(current_tag, bucket)


if __name__ == "__main__":
    main()
