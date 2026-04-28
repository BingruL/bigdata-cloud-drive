"""
统计分析服务
提供实时统计计算（轻量级，直接从 HBase 读取数据计算）
重度分析交给 Spark 作业（见 spark_jobs/ 目录）
对应课程第 4 章 MapReduce、第 8 章 Spark
"""
import time
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class StatsService:
    """统计分析服务"""

    def __init__(self, hbase_service, config):
        self.hbase = hbase_service
        self.files_table = config.HBASE_TABLE_FILES
        self.logs_table = config.HBASE_TABLE_LOGS
        self.stats_table = config.HBASE_TABLE_STATS

    def _get_all_files(self):
        return self.hbase.get_all_files_raw(self.files_table)

    def _get_all_logs(self):
        return self.hbase.get_logs(self.logs_table, limit=10000)

    def get_user_file_counts(self, username=None):
        """各用户上传文件数统计

        username=None 时（仅供 admin 调用）返回全站各 owner 的统计；
        传入 username 时只返回该用户自己的一行，避免泄露其他用户名。
        """
        files = self._get_all_files()
        counter = Counter()
        for f in files:
            owner = f.get("owner", "unknown")
            if username and owner != username:
                continue
            counter[owner] += 1
        return [{"username": k, "count": v} for k, v in counter.most_common()]

    def get_file_type_distribution(self, username=None):
        """文件类型分布统计"""
        files = self._get_all_files()
        counter = Counter()
        for f in files:
            if username and f.get("owner") != username:
                continue
            ftype = f.get("type", "other").lower()
            counter[ftype] += 1
        return [{"type": k, "count": v} for k, v in counter.most_common()]

    def get_daily_upload_trend(self, days=7, username=None):
        """最近 N 天上传量趋势"""
        files = self._get_all_files()
        now = datetime.now()
        daily = defaultdict(int)

        # 初始化日期
        for i in range(days):
            date = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            daily[date] = 0

        for f in files:
            if username and f.get("owner") != username:
                continue
            ts = f.get("created_at", "0")
            try:
                ts_int = int(ts)
                dt = datetime.fromtimestamp(ts_int / 1000)
                date_str = dt.strftime("%Y-%m-%d")
                if date_str in daily:
                    daily[date_str] += 1
            except (ValueError, OSError):
                continue

        result = [{"date": k, "count": v} for k, v in sorted(daily.items())]
        return result

    def get_storage_stats(self, username=None):
        """存储空间统计。username 非 None 时只统计该用户的文件。"""
        files = self._get_all_files()
        total_size = 0
        total_count = 0
        user_sizes = defaultdict(int)

        for f in files:
            owner = f.get("owner", "unknown")
            if username and owner != username:
                continue
            size = int(f.get("size", 0))
            total_size += size
            total_count += 1
            user_sizes[owner] += size

        return {
            "total_size": total_size,
            "total_size_readable": self._format_size(total_size),
            "total_files": total_count,
            "user_storage": [
                {"username": k, "size": v, "size_readable": self._format_size(v)}
                for k, v in sorted(user_sizes.items(), key=lambda x: x[1], reverse=True)
            ],
        }

    def get_hot_files(self, top_n=10, username=None):
        """热门文件排行（按下载次数，排除回收站文件）

        username 非 None 时只返回该用户拥有的文件，避免泄露其他用户的文件名/下载热度。
        """
        files = [f for f in self._get_all_files() if f.get("deleted") != "1"]
        if username:
            files = [f for f in files if f.get("owner") == username]
        for f in files:
            f["downloads_int"] = int(f.get("downloads", 0))
        files.sort(key=lambda x: x["downloads_int"], reverse=True)
        return files[:top_n]

    def get_recent_activity(self, limit=20, username=None):
        """最近操作动态。username 非 None 时只返回该用户自己的操作。"""
        logs = self._get_all_logs()
        if username:
            logs = [l for l in logs if l.get("username") == username]
        result = []
        for log in logs[:limit]:
            ts = log.get("timestamp", "0")
            try:
                dt = datetime.fromtimestamp(int(ts) / 1000)
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, OSError):
                time_str = "未知"
            result.append({
                "username": log.get("username", ""),
                "action": log.get("action", ""),
                "detail": log.get("detail", ""),
                "time": time_str,
            })
        return result

    def get_dashboard_summary(self, username=None):
        """Dashboard 汇总数据。username 非 None 时只统计该用户视角。"""
        files = self._get_all_files()
        logs = self._get_all_logs()

        if username:
            files = [f for f in files if f.get("owner") == username]
            logs = [l for l in logs if l.get("username") == username]

        total_files = len(files)
        total_size = sum(int(f.get("size", 0)) for f in files)
        total_downloads = sum(int(f.get("downloads", 0)) for f in files)
        total_users = 1 if username else len(set(f.get("owner", "") for f in files))

        return {
            "total_files": total_files,
            "total_size": total_size,
            "total_size_readable": self._format_size(total_size),
            "total_downloads": total_downloads,
            "total_users": total_users,
            "total_logs": len(logs),
        }

    def get_hourly_activity(self, username=None):
        """24 小时活跃度分布。username 非 None 时只统计该用户的操作。"""
        logs = self._get_all_logs()
        hourly = defaultdict(int)
        for i in range(24):
            hourly[i] = 0

        for log in logs:
            if username and log.get("username") != username:
                continue
            ts = log.get("timestamp", "0")
            try:
                dt = datetime.fromtimestamp(int(ts) / 1000)
                hourly[dt.hour] += 1
            except (ValueError, OSError):
                continue

        return [{"hour": h, "count": c} for h, c in sorted(hourly.items())]

    def get_activity_heatmap(self, days=365, username=None):
        """按日聚合操作次数，用于活跃热力图"""
        logs = self._get_all_logs()
        now = datetime.now()
        daily = defaultdict(int)

        # 初始化日期范围
        for i in range(days):
            date = (now - timedelta(days=i)).strftime("%Y-%m-%d")
            daily[date] = 0

        for log in logs:
            if username and log.get("username") != username:
                continue
            ts = log.get("timestamp", "0")
            try:
                dt = datetime.fromtimestamp(int(ts) / 1000)
                date_str = dt.strftime("%Y-%m-%d")
                if date_str in daily:
                    daily[date_str] += 1
            except (ValueError, OSError):
                continue

        return [{"date": k, "count": v} for k, v in sorted(daily.items())]

    @staticmethod
    def _format_size(size_bytes):
        """将字节转为可读格式"""
        size_bytes = int(size_bytes)
        if size_bytes < 1024:
            return f"{size_bytes} B"
        elif size_bytes < 1024 * 1024:
            return f"{size_bytes / 1024:.1f} KB"
        elif size_bytes < 1024 * 1024 * 1024:
            return f"{size_bytes / (1024 * 1024):.1f} MB"
        else:
            return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"
