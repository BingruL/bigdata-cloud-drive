#!/usr/bin/env python3
"""
导出 HBase cloud_drive_files 的 (file_id, filename, tags) 到 HDFS TSV 文件
作为 Hadoop Streaming MR 作业的输入。

输出路径：/cloud-drive/mr_input/files.tsv
格式：每行 file_id \t filename \t tags

用法：
    python3 mapreduce_jobs/tag_index/export_files_to_hdfs.py
"""
import os
import sys
import io

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.config import get_config
from backend.services.hbase_service import HBaseService
from hdfs import InsecureClient


def main():
    config = get_config()

    print(f"[1/3] 连接 HBase {config.HBASE_HOST}:{config.HBASE_PORT}...")
    hbase = HBaseService(config.HBASE_HOST, config.HBASE_PORT)

    print("[2/3] 扫描 cloud_drive_files 表...")
    files = hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=False)
    print(f"  共扫描到 {len(files)} 条文件记录")

    # 拼成 TSV
    buffer = io.StringIO()
    skipped = 0
    for f in files:
        file_id = f.get("file_id") or f.get("rowkey") or ""
        filename = f.get("filename", "")
        tags = f.get("tags", "")
        if not file_id or not tags:
            skipped += 1
            continue
        # 字段内不能含 \t / \n
        filename = filename.replace("\t", " ").replace("\n", " ")
        tags = tags.replace("\t", " ").replace("\n", " ")
        buffer.write(f"{file_id}\t{filename}\t{tags}\n")
    payload = buffer.getvalue().encode("utf-8")
    print(f"  有效记录 {len(files) - skipped} 条，跳过 {skipped} 条（无 tags）")

    print(f"[3/3] 上传到 HDFS {config.HDFS_URL}...")
    client = InsecureClient(config.HDFS_URL, user=config.HDFS_USER)
    hdfs_dir = f"{config.HDFS_ROOT_DIR}/mr_input"
    hdfs_path = f"{hdfs_dir}/files.tsv"
    client.makedirs(hdfs_dir)
    with client.write(hdfs_path, overwrite=True, encoding=None) as writer:
        writer.write(payload)
    print(f"  已写入 {hdfs_path}（{len(payload)} bytes）")
    print("\n完成。可以执行 hadoop streaming 作业了。")


if __name__ == "__main__":
    main()
