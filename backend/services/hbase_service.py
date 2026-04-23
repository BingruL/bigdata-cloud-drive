"""
HBase 服务层
负责与 HBase 交互，管理用户、文件元数据、操作日志等
使用 happybase 库连接 HBase Thrift Server
"""
import happybase
import json
import time
import uuid
import logging
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class HBaseService:
    """HBase 数据访问服务"""

    def __init__(self, host="localhost", port=9090):
        self.host = host
        self.port = port
        self._pool = happybase.ConnectionPool(
            size=10, host=self.host, port=self.port, timeout=10000
        )

    @contextmanager
    def _get_connection(self):
        """从连接池获取连接"""
        with self._pool.connection() as conn:
            yield conn

    # ========== 表管理 ==========

    def init_tables(self, table_config):
        """
        初始化 HBase 表
        table_config: dict, 如 {"table_name": {"cf1": dict(), "cf2": dict()}}
        """
        with self._get_connection() as conn:
            existing = [t.decode() for t in conn.tables()]
            for table_name, families in table_config.items():
                if table_name not in existing:
                    conn.create_table(table_name, families)
                    logger.info(f"创建 HBase 表: {table_name}")
                else:
                    logger.info(f"HBase 表已存在: {table_name}")

    # ========== 用户操作 ==========

    def create_user(self, table_name, username, password_hash, role="user"):
        """
        创建用户
        RowKey: username
        列族 info: password, role, created_at, status
        """
        with self._get_connection() as conn:
            table = conn.table(table_name)
            # 检查用户是否已存在
            existing = table.row(username.encode())
            if existing:
                return None

            now = str(int(time.time() * 1000))
            table.put(username.encode(), {
                b"info:password": password_hash.encode(),
                b"info:role": role.encode(),
                b"info:created_at": now.encode(),
                b"info:status": b"active",
            })
            return {
                "username": username,
                "role": role,
                "created_at": now,
            }

    def get_user(self, table_name, username):
        """获取用户信息"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(username.encode())
            if not row:
                return None
            return {
                "username": username,
                "password": row.get(b"info:password", b"").decode(),
                "role": row.get(b"info:role", b"user").decode(),
                "created_at": row.get(b"info:created_at", b"").decode(),
                "status": row.get(b"info:status", b"active").decode(),
            }

    def list_users(self, table_name):
        """获取所有用户列表"""
        users = []
        with self._get_connection() as conn:
            table = conn.table(table_name)
            for key, data in table.scan():
                users.append({
                    "username": key.decode(),
                    "role": data.get(b"info:role", b"user").decode(),
                    "created_at": data.get(b"info:created_at", b"").decode(),
                    "status": data.get(b"info:status", b"active").decode(),
                })
        return users

    # ========== 文件元数据操作 ==========

    def save_file_meta(self, table_name, file_id, meta):
        """
        保存文件元数据
        RowKey: file_id (UUID)
        列族 meta: filename, size, type, owner, hdfs_path, created_at, downloads, summary, tags
        """
        with self._get_connection() as conn:
            table = conn.table(table_name)
            data = {}
            for k, v in meta.items():
                data[f"meta:{k}".encode()] = str(v).encode()
            table.put(file_id.encode(), data)
            return file_id

    def get_file_meta(self, table_name, file_id):
        """获取文件元数据"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(file_id.encode())
            if not row:
                return None
            result = {"file_id": file_id}
            for k, v in row.items():
                col = k.decode().split(":", 1)[1]
                result[col] = v.decode()
            return result

    def delete_file_meta(self, table_name, file_id):
        """删除文件元数据"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(file_id.encode())
            if not row:
                return False
            table.delete(file_id.encode())
            return True

    def list_files(self, table_name, owner=None, file_type=None,
                   keyword=None, page=1, page_size=20,
                   include_deleted=False, only_deleted=False):
        """
        列出文件（支持筛选）
        owner: 按所属用户筛选
        file_type: 按文件类型筛选
        keyword: 按文件名关键字搜索
        include_deleted: 是否包含已软删除的文件（默认 False）
        only_deleted: 只返回软删除的文件（回收站视图）
        """
        files = []
        with self._get_connection() as conn:
            table = conn.table(table_name)
            for key, data in table.scan():
                file_info = {"file_id": key.decode()}
                for k, v in data.items():
                    col = k.decode().split(":", 1)[1]
                    file_info[col] = v.decode()

                is_deleted = file_info.get("deleted") == "1"
                if only_deleted:
                    if not is_deleted:
                        continue
                elif not include_deleted and is_deleted:
                    continue

                # 应用过滤条件
                if owner and file_info.get("owner") != owner:
                    continue
                if file_type and file_info.get("type", "").lower() != file_type.lower():
                    continue
                if keyword and keyword.lower() not in file_info.get("filename", "").lower():
                    continue

                files.append(file_info)

        # 回收站按删除时间倒序，其它按创建时间倒序
        if only_deleted:
            files.sort(key=lambda x: x.get("deleted_at", "0"), reverse=True)
        else:
            files.sort(key=lambda x: x.get("created_at", "0"), reverse=True)

        # 分页
        total = len(files)
        start = (page - 1) * page_size
        end = start + page_size
        return {
            "files": files[start:end],
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }

    def increment_downloads(self, table_name, file_id):
        """增加文件下载次数"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(file_id.encode())
            if not row:
                return 0
            current = int(row.get(b"meta:downloads", b"0").decode())
            new_count = current + 1
            table.put(file_id.encode(), {
                b"meta:downloads": str(new_count).encode()
            })
            return new_count

    def soft_delete_file(self, table_name, file_id):
        """软删除：打上 deleted 标记（保留 HDFS 文件，可恢复）"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(file_id.encode())
            if not row:
                return False
            table.put(file_id.encode(), {
                b"meta:deleted": b"1",
                b"meta:deleted_at": str(int(time.time() * 1000)).encode(),
            })
            return True

    def restore_file(self, table_name, file_id):
        """从回收站恢复文件"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(file_id.encode())
            if not row:
                return False
            table.delete(file_id.encode(), columns=[b"meta:deleted", b"meta:deleted_at"])
            return True

    def update_file_ai(self, table_name, file_id, summary=None, tags=None):
        """更新文件的 AI 摘要和标签"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            data = {}
            if summary is not None:
                data[b"meta:summary"] = summary.encode()
            if tags is not None:
                data[b"meta:tags"] = tags.encode()
            if data:
                table.put(file_id.encode(), data)

    # ========== 操作日志 ==========

    def add_log(self, table_name, username, action, detail=""):
        """
        添加操作日志
        RowKey: timestamp_uuid (保证唯一且按时间排序)
        列族 log: username, action, detail, timestamp
        """
        ts = int(time.time() * 1000)
        row_key = f"{ts}_{uuid.uuid4().hex[:8]}"
        with self._get_connection() as conn:
            table = conn.table(table_name)
            table.put(row_key.encode(), {
                b"log:username": username.encode(),
                b"log:action": action.encode(),
                b"log:detail": str(detail).encode(),
                b"log:timestamp": str(ts).encode(),
            })
        return row_key

    def get_logs(self, table_name, username=None, action=None,
                 limit=100):
        """获取操作日志"""
        logs = []
        with self._get_connection() as conn:
            table = conn.table(table_name)
            for key, data in table.scan(limit=limit * 5, reverse=True):
                log_entry = {"log_id": key.decode()}
                for k, v in data.items():
                    col = k.decode().split(":", 1)[1]
                    log_entry[col] = v.decode()

                if username and log_entry.get("username") != username:
                    continue
                if action and log_entry.get("action") != action:
                    continue

                logs.append(log_entry)
                if len(logs) >= limit:
                    break
        return logs

    # ========== 统计数据缓存 ==========

    def save_stats(self, table_name, stat_key, data):
        """保存统计结果（供 Spark 作业写入）"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            table.put(stat_key.encode(), {
                b"data:value": json.dumps(data, ensure_ascii=False).encode(),
                b"data:updated_at": str(int(time.time() * 1000)).encode(),
            })

    def get_stats(self, table_name, stat_key):
        """读取统计结果"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(stat_key.encode())
            if not row:
                return None
            value = row.get(b"data:value", b"{}").decode()
            updated = row.get(b"data:updated_at", b"0").decode()
            return {
                "key": stat_key,
                "value": json.loads(value),
                "updated_at": updated,
            }

    def get_all_files_raw(self, table_name, include_deleted=True):
        """获取所有文件元数据（供统计分析使用）

        include_deleted=True 时返回全部（含回收站中的文件，用于总存储统计）；
        False 时排除软删除的文件（用于推荐、热门等面向用户的场景）。
        """
        files = []
        with self._get_connection() as conn:
            table = conn.table(table_name)
            for key, data in table.scan():
                file_info = {"file_id": key.decode()}
                for k, v in data.items():
                    col = k.decode().split(":", 1)[1]
                    file_info[col] = v.decode()
                if not include_deleted and file_info.get("deleted") == "1":
                    continue
                files.append(file_info)
        return files
