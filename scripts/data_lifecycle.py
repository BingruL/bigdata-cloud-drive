#!/usr/bin/env python3
"""
数据生命周期治理脚本

功能：
1. **冷热分层**：把 90 天前未被下载的文件元数据打上 tier=cold 标记，
   并把 HDFS 数据移动到 /cloud-drive/cold/ 路径下（保留 hot 路径下软链或元数据指向）。
   课程语境：演示对"冷热数据分级 + 异构存储"的认知，
   生产环境通常对应 HDFS Storage Policy（HOT / WARM / COLD / FROZEN）。

2. **日志 TTL**：对 cloud_drive_logs 表设置列族 TTL = 30 天。
   到期数据由 HBase Major Compaction 自动清理，无需业务层删除。
   课程语境：演示对 HBase TTL / TimeToLive 机制的运用。

3. **回收站清理**：对在回收站（deleted=1）超过 30 天的文件直接彻底删除。

用法：
    python3 scripts/data_lifecycle.py --dry-run        # 只统计不执行
    python3 scripts/data_lifecycle.py --apply          # 真正执行
    python3 scripts/data_lifecycle.py --apply --set-log-ttl

cron 示例（每天凌晨 3 点跑一次）：
    0 3 * * * cd /path/to/project && python3 scripts/data_lifecycle.py --apply >> /var/log/lifecycle.log 2>&1
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.config import get_config
from backend.services.hbase_service import HBaseService
from backend.services.hdfs_service import HDFSService

DAY_MS = 86400 * 1000

# 阈值（可通过命令行覆盖）
DEFAULT_COLD_DAYS = 90
DEFAULT_TRASH_PURGE_DAYS = 30
DEFAULT_LOG_TTL_DAYS = 30


def now_ms():
    return int(time.time() * 1000)


# ===== 1. 冷热分层 =====

def classify_files(hbase, config, cold_days):
    """扫描所有未删除文件，根据 created_at 划分 hot / cold"""
    cutoff = now_ms() - cold_days * DAY_MS
    hot, cold = [], []
    for f in hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=False):
        try:
            created = int(f.get("created_at", "0") or 0)
        except ValueError:
            created = 0
        already_cold = f.get("tier") == "cold"
        if created and created < cutoff:
            cold.append(f)
        elif already_cold:
            # 之前打过 cold，现在又"年轻"了？保守保持 cold
            cold.append(f)
        else:
            hot.append(f)
    return hot, cold


def move_to_cold(hbase, hdfs, config, cold_files, apply):
    """对 cold 文件：① HBase 加 tier=cold 列；② HDFS 移到 cold 子目录"""
    moved = 0
    for f in cold_files:
        if f.get("tier") == "cold":
            continue  # 已经是冷的
        old_path = f.get("hdfs_path", "")
        if not old_path or "/cold/" in old_path:
            continue
        new_path = old_path.replace("/cloud-drive/files/", "/cloud-drive/cold/files/", 1)

        print(f"  [cold] {f.get('filename', '?')} ({f['file_id'][:8]}) "
              f"{old_path} -> {new_path}")
        if apply:
            try:
                # 确保目标父目录存在，否则 WebHDFS rename 会失败
                parent = new_path.rsplit("/", 1)[0]
                if parent:
                    hdfs.client.makedirs(parent)
                # WebHDFS 没有 rename API 直接暴露，分两步：read+write+delete
                # 简化为：用 hdfs.client.rename
                hdfs.client.rename(old_path, new_path)
                # 更新 HBase 元数据
                with hbase._get_connection() as conn:
                    conn.table(config.HBASE_TABLE_FILES).put(f["file_id"].encode(), {
                        b"meta:tier": b"cold",
                        b"meta:hdfs_path": new_path.encode(),
                        b"meta:tiered_at": str(now_ms()).encode(),
                    })
                moved += 1
            except Exception as e:
                print(f"    ! 失败: {e}")
    return moved


# ===== 2. 回收站超期清理 =====

def purge_old_trash(hbase, hdfs, config, days, apply):
    cutoff = now_ms() - days * DAY_MS
    purged = 0
    for f in hbase.get_all_files_raw(config.HBASE_TABLE_FILES, include_deleted=True):
        if f.get("deleted") != "1":
            continue
        try:
            del_ts = int(f.get("deleted_at", "0") or 0)
        except ValueError:
            del_ts = 0
        if not del_ts or del_ts > cutoff:
            continue
        print(f"  [purge] {f.get('filename', '?')} ({f['file_id'][:8]}) "
              f"在回收站已 {(now_ms() - del_ts) // DAY_MS} 天")
        if apply:
            try:
                if f.get("hdfs_path"):
                    hdfs.delete_file(f["hdfs_path"])
                hbase.delete_file_meta(config.HBASE_TABLE_FILES, f["file_id"])
                purged += 1
            except Exception as e:
                print(f"    ! 失败: {e}")
    return purged


# ===== 3. 日志表 TTL =====

def set_log_ttl(config, ttl_days, apply):
    """对 cloud_drive_logs 列族 log 设置 TTL（秒）。
    happybase 底层用 Thrift，没有直接 alter API；最干净的做法是输出 hbase shell 命令。
    """
    ttl_seconds = ttl_days * 86400
    cmd = (f"alter '{config.HBASE_TABLE_LOGS}', "
           f"{{NAME => 'log', TTL => '{ttl_seconds}'}}")
    print("  [log-ttl] 请在 hbase shell 中执行下面命令（happybase 不支持 alter）：")
    print(f"    {cmd}")
    if apply:
        # 尝试用 happybase 的 admin API（部分版本支持）
        try:
            import happybase
            conn = happybase.Connection(config.HBASE_HOST, config.HBASE_PORT)
            try:
                conn.disable_table(config.HBASE_TABLE_LOGS)
                conn.alter(
                    config.HBASE_TABLE_LOGS,
                    families={"log": {"time_to_live": ttl_seconds}},
                )
                conn.enable_table(config.HBASE_TABLE_LOGS)
                print(f"    ✓ 已通过 Thrift 设置 TTL = {ttl_seconds} 秒")
                return True
            except Exception as e:
                print(f"    ! Thrift 设置失败（可能版本不支持），请手动执行 hbase shell: {e}")
                try:
                    conn.enable_table(config.HBASE_TABLE_LOGS)
                except Exception:
                    pass
            finally:
                conn.close()
        except Exception as e:
            print(f"    ! 跳过自动设置: {e}")
    return False


# ===== 主流程 =====

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true",
                   help="只打印不执行（默认模式）")
    p.add_argument("--apply", action="store_true",
                   help="真正执行迁移和清理")
    p.add_argument("--cold-days", type=int, default=DEFAULT_COLD_DAYS)
    p.add_argument("--trash-days", type=int, default=DEFAULT_TRASH_PURGE_DAYS)
    p.add_argument("--set-log-ttl", action="store_true",
                   help="同时设置 cloud_drive_logs 的 TTL")
    p.add_argument("--log-ttl-days", type=int, default=DEFAULT_LOG_TTL_DAYS)
    args = p.parse_args()

    apply = args.apply and not args.dry_run
    mode_label = "APPLY（真实执行）" if apply else "DRY-RUN（仅打印）"

    config = get_config()
    print("=" * 60)
    print(f"  数据生命周期治理  模式: {mode_label}")
    print(f"  冷数据阈值: {args.cold_days} 天   "
          f"回收站清理阈值: {args.trash_days} 天")
    print("=" * 60)

    hbase = HBaseService(config.HBASE_HOST, config.HBASE_PORT)
    hdfs = HDFSService(config.HDFS_URL, config.HDFS_USER, config.HDFS_ROOT_DIR)

    print("\n[1/3] 冷热分层扫描...")
    hot, cold = classify_files(hbase, config, args.cold_days)
    print(f"  hot: {len(hot)} 个文件   cold: {len(cold)} 个文件")
    moved = move_to_cold(hbase, hdfs, config, cold, apply)
    print(f"  本次迁移: {moved} 个")

    print("\n[2/3] 回收站超期清理...")
    purged = purge_old_trash(hbase, hdfs, config, args.trash_days, apply)
    print(f"  本次彻底删除: {purged} 个")

    if args.set_log_ttl:
        print(f"\n[3/3] 设置 cloud_drive_logs TTL = {args.log_ttl_days} 天...")
        set_log_ttl(config, args.log_ttl_days, apply)
    else:
        print("\n[3/3] 跳过 TTL 设置（添加 --set-log-ttl 启用）")

    print("\n完成。")


if __name__ == "__main__":
    main()
