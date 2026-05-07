"""
HBase 服务层
负责与 HBase 交互，管理用户、文件元数据、操作日志等
使用 happybase 库连接 HBase Thrift Server
"""
import functools
import happybase
import json
import time
import uuid
import logging
from contextlib import contextmanager

from thriftpy2.transport import TTransportException

logger = logging.getLogger(__name__)


def _retry_on_stale(fn):
    """连接池里的 Thrift 连接被对端关掉时，第一次请求会拿到一条 stale 连接并抛
    TTransportException("TSocket read 0 bytes")。happybase 会替换该连接，但当前
    调用本身不会自动重试。这里捕获一次并重发，避免把错误暴露给上层。"""
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except TTransportException as e:
            logger.warning(f"HBase Thrift 连接失活，自动重试一次: {e}")
            return fn(self, *args, **kwargs)
    return wrapper


def _wrap_public_methods_with_retry(cls):
    for name, attr in list(vars(cls).items()):
        if name.startswith("_") or not callable(attr):
            continue
        setattr(cls, name, _retry_on_stale(attr))
    return cls


@_wrap_public_methods_with_retry
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

    def ping(self):
        """探活：拉一次表列表，失败抛异常。供 /api/health 调用。"""
        with self._get_connection() as conn:
            conn.tables()

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
                   include_deleted=False, only_deleted=False,
                   start_date=None, end_date=None):
        """
        列出文件（支持筛选）
        owner: 按所属用户筛选
        file_type: 按文件类型筛选
        keyword: 按文件名关键字搜索
        include_deleted: 是否包含已软删除的文件（默认 False）
        only_deleted: 只返回软删除的文件（回收站视图）
        start_date / end_date: created_at 时间窗口（毫秒），在分页前过滤
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
                searchable_name = " ".join([
                    file_info.get("display_name", ""),
                    file_info.get("filename", ""),
                ])
                if keyword and keyword.lower() not in searchable_name.lower():
                    continue
                if start_date is not None or end_date is not None:
                    try:
                        ts = int(file_info.get("created_at", 0) or 0)
                    except (TypeError, ValueError):
                        ts = 0
                    if start_date is not None and ts < start_date:
                        continue
                    if end_date is not None and ts > end_date:
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
            table.delete(file_id.encode(), columns=[
                b"meta:deleted", b"meta:deleted_at", b"meta:deleted_by_folder",
            ])
            return True

    def update_file_meta_fields(self, table_name, file_id, fields):
        """批量更新文件元数据字段。"""
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(file_id.encode())
            if not row:
                return False
            table.put(file_id.encode(), {
                f"meta:{k}".encode(): str(v).encode() for k, v in fields.items()
            })
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

    def create_folder(self, table_name, folder_id, meta):
        """创建文件夹元数据。"""
        with self._get_connection() as conn:
            conn.table(table_name).put(folder_id.encode(), {
                f"meta:{k}".encode(): str(v).encode() for k, v in meta.items()
            })
        return {"folder_id": folder_id, **meta}

    def list_child_folders(self, table_name, owner, parent_id, include_deleted=False, only_deleted=False):
        """列出指定父目录下的文件夹。"""
        rows = []
        with self._get_connection() as conn:
            table = conn.table(table_name)
            for key, data in table.scan():
                row = {"folder_id": key.decode()}
                for k, v in data.items():
                    col = k.decode().split(":", 1)[1]
                    row[col] = v.decode()
                if row.get("owner") != owner:
                    continue
                if row.get("parent_id", "root") != parent_id:
                    continue
                deleted = row.get("deleted") == "1"
                if only_deleted and not deleted:
                    continue
                if not include_deleted and not only_deleted and deleted:
                    continue
                rows.append({**row, "item_type": "folder"})
        rows.sort(key=lambda r: r.get("name", ""))
        return rows

    def get_folder(self, table_name, folder_id):
        """获取文件夹元数据；root 是不落表的伪目录。"""
        if folder_id == "root":
            return {"folder_id": "root", "name": "全部文件", "parent_id": "", "owner": ""}
        with self._get_connection() as conn:
            row = conn.table(table_name).row(folder_id.encode())
            if not row:
                return None
            result = {"folder_id": folder_id}
            for k, v in row.items():
                col = k.decode().split(":", 1)[1]
                result[col] = v.decode()
            return result

    def update_folder_fields(self, table_name, folder_id, fields):
        """批量更新文件夹元数据字段。"""
        if folder_id == "root":
            return False
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(folder_id.encode())
            if not row:
                return False
            table.put(folder_id.encode(), {
                f"meta:{k}".encode(): str(v).encode() for k, v in fields.items()
            })
            return True

    def _scan_folders_raw(self, table_name):
        folders = []
        with self._get_connection() as conn:
            table = conn.table(table_name)
            for key, data in table.scan():
                folder = {"folder_id": key.decode()}
                for k, v in data.items():
                    col = k.decode().split(":", 1)[1]
                    folder[col] = v.decode()
                folders.append(folder)
        return folders

    def collect_folder_subtree(self, folders_table, files_table, folder_id):
        """返回 folder_id 及所有后代文件夹、文件元数据。"""
        folders = self._scan_folders_raw(folders_table)
        by_parent = {}
        for folder in folders:
            by_parent.setdefault(folder.get("parent_id", "root"), []).append(folder)

        subtree_folders = []
        stack = [folder_id]
        folder_ids = set()
        while stack:
            current_id = stack.pop()
            if current_id in folder_ids:
                continue
            folder_ids.add(current_id)
            if current_id != folder_id:
                folder = next((f for f in folders if f.get("folder_id") == current_id), None)
                if folder:
                    subtree_folders.append(folder)
            for child in by_parent.get(current_id, []):
                stack.append(child["folder_id"])

        root_folder = next((f for f in folders if f.get("folder_id") == folder_id), None)
        if root_folder:
            subtree_folders.insert(0, root_folder)

        subtree_files = [
            f for f in self.get_all_files_raw(files_table, include_deleted=True)
            if f.get("parent_id", "root") in folder_ids
        ]
        return {"folders": subtree_folders, "files": subtree_files}

    def is_descendant_folder(self, table_name, ancestor_folder_id, candidate_folder_id):
        """candidate_folder_id 是否是 ancestor_folder_id 的后代目录。"""
        if candidate_folder_id == "root":
            return False
        folders = {f["folder_id"]: f for f in self._scan_folders_raw(table_name)}
        current = folders.get(candidate_folder_id)
        while current:
            parent_id = current.get("parent_id", "root")
            if parent_id == ancestor_folder_id:
                return True
            if parent_id == "root":
                return False
            current = folders.get(parent_id)
        return False

    def soft_delete_folder_tree(self, folders_table, files_table, folder_id):
        """软删除文件夹子树及其所有文件。"""
        now = str(int(time.time() * 1000))
        subtree = self.collect_folder_subtree(folders_table, files_table, folder_id)
        deleted_folder_ids = {
            folder["folder_id"]
            for folder in subtree["folders"]
            if folder.get("deleted") == "1"
        }

        def has_deleted_ancestor(item_parent_id):
            current_id = item_parent_id or "root"
            while current_id != "root" and current_id != folder_id:
                if current_id in deleted_folder_ids:
                    return True
                parent = next(
                    (f for f in subtree["folders"] if f.get("folder_id") == current_id),
                    None,
                )
                if not parent:
                    return False
                current_id = parent.get("parent_id", "root") or "root"
            return False

        with self._get_connection() as conn:
            folders = conn.table(folders_table)
            files = conn.table(files_table)
            for folder in subtree["folders"]:
                if folder.get("deleted") == "1":
                    continue
                folders.put(folder["folder_id"].encode(), {
                    b"meta:deleted": b"1",
                    b"meta:deleted_at": now.encode(),
                    b"meta:deleted_by_folder": folder_id.encode(),
                    b"meta:updated_at": now.encode(),
                })
            for file_info in subtree["files"]:
                if file_info.get("deleted") == "1":
                    continue
                if has_deleted_ancestor(file_info.get("parent_id", "root")):
                    continue
                files.put(file_info["file_id"].encode(), {
                    b"meta:deleted": b"1",
                    b"meta:deleted_at": now.encode(),
                    b"meta:deleted_by_folder": folder_id.encode(),
                    b"meta:updated_at": now.encode(),
                })
        return subtree

    def restore_folder_tree(self, folders_table, files_table, folder_id):
        """恢复文件夹子树及其所有文件。"""
        now = str(int(time.time() * 1000))
        subtree = self.collect_folder_subtree(folders_table, files_table, folder_id)
        root_folder = next(
            (folder for folder in subtree["folders"] if folder.get("folder_id") == folder_id),
            {},
        )
        marker_aware_restore = root_folder.get("deleted_by_folder") == folder_id
        with self._get_connection() as conn:
            folders = conn.table(folders_table)
            files = conn.table(files_table)
            for folder in subtree["folders"]:
                if marker_aware_restore and folder.get("deleted_by_folder") != folder_id:
                    continue
                folders.delete(folder["folder_id"].encode(), columns=[
                    b"meta:deleted", b"meta:deleted_at", b"meta:deleted_by_folder",
                ])
                folders.put(folder["folder_id"].encode(), {b"meta:updated_at": now.encode()})
            for file_info in subtree["files"]:
                if marker_aware_restore and file_info.get("deleted_by_folder") != folder_id:
                    continue
                files.delete(file_info["file_id"].encode(), columns=[
                    b"meta:deleted", b"meta:deleted_at", b"meta:deleted_by_folder",
                ])
                files.put(file_info["file_id"].encode(), {b"meta:updated_at": now.encode()})
        return subtree

    def purge_folder_tree(self, folders_table, files_table, folder_id, hdfs=None):
        """永久删除文件夹子树、文件元数据，并清理 HDFS 文件。"""
        subtree = self.collect_folder_subtree(folders_table, files_table, folder_id)
        if hdfs:
            for file_info in subtree["files"]:
                hdfs_path = file_info.get("hdfs_path")
                if hdfs_path:
                    try:
                        hdfs.delete_file(hdfs_path)
                    except Exception as e:
                        logger.warning(f"HDFS 文件删除失败（继续清理元数据）: {e}")
        with self._get_connection() as conn:
            folders = conn.table(folders_table)
            files = conn.table(files_table)
            for file_info in subtree["files"]:
                files.delete(file_info["file_id"].encode())
            for folder in subtree["folders"]:
                folders.delete(folder["folder_id"].encode())
        return subtree

    def resolve_available_name(self, files_table, folders_table, owner, parent_id, desired_name,
                               exclude_file_id=None, exclude_folder_id=None):
        """在同一目录下避开文件和文件夹重名，生成可用名称。"""
        base, dot, ext = desired_name.rpartition(".")
        if not dot:
            base, ext = desired_name, ""
        else:
            ext = "." + ext
        used = set()
        for f in self.get_all_files_raw(files_table, include_deleted=False):
            if f.get("owner") == owner and f.get("parent_id", "root") == parent_id and f.get("file_id") != exclude_file_id:
                used.add(f.get("display_name") or f.get("filename", ""))
        for folder in self.list_child_folders(folders_table, owner, parent_id):
            if folder.get("folder_id") != exclude_folder_id:
                used.add(folder.get("name", ""))
        if desired_name not in used:
            return desired_name
        n = 1
        while True:
            candidate = f"{base} ({n}){ext}"
            if candidate not in used:
                return candidate
            n += 1

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

    def remove_group_from_all_files(self, table_name, group_id):
        """从所有文件的 shared_groups 列表中摘除指定 group_id。

        群组解散时调用。若摘除后列表为空，自动把 is_shared 置回 "0"。
        返回受影响的文件数，便于审计。
        """
        affected = 0
        with self._get_connection() as conn:
            table = conn.table(table_name)
            for key, data in table.scan():
                shared_raw = data.get(b"meta:shared_groups", b"").decode()
                if not shared_raw:
                    continue
                groups = [x for x in shared_raw.split(",") if x]
                if group_id not in groups:
                    continue
                groups.remove(group_id)
                new_str = ",".join(groups)
                table.put(key, {
                    b"meta:shared_groups": new_str.encode(),
                    b"meta:is_shared": (b"1" if new_str else b"0"),
                })
                affected += 1
        return affected

    def update_file_sharing(self, table_name, file_id, is_shared, group_ids):
        """更新文件的分享状态与目标群组列表

        is_shared: bool；group_ids: list[str]（清空时传 []）。
        把列存进 meta:is_shared / meta:shared_groups。
        """
        with self._get_connection() as conn:
            table = conn.table(table_name)
            row = table.row(file_id.encode())
            if not row:
                return False
            shared_str = ",".join([g for g in group_ids if g]) if is_shared else ""
            table.put(file_id.encode(), {
                b"meta:is_shared": (b"1" if is_shared and shared_str else b"0"),
                b"meta:shared_groups": shared_str.encode(),
            })
            return True

    # ========== 群组：双表反向索引 ==========
    # cloud_drive_groups        rowkey=group_id          列族 info
    # cloud_drive_group_members rowkey={gid}#{username}  列族 info
    # cloud_drive_user_groups   rowkey={username}#{gid}  列族 info
    # 同一份成员关系按两种 RowKey 各存一份，"群→成员"和"用户→群"都能前缀扫描

    def create_group(self, groups_table, members_table, user_groups_table,
                     name, owner, description=""):
        """创建群组并把 owner 写入两张成员索引表"""
        group_id = uuid.uuid4().hex
        now = str(int(time.time() * 1000))
        with self._get_connection() as conn:
            groups = conn.table(groups_table)
            members = conn.table(members_table)
            user_groups = conn.table(user_groups_table)

            groups.put(group_id.encode(), {
                b"info:name": name.encode(),
                b"info:description": description.encode(),
                b"info:owner": owner.encode(),
                b"info:created_at": now.encode(),
                b"info:member_count": b"1",
            })
            members.put(f"{group_id}#{owner}".encode(), {
                b"info:role": b"owner",
                b"info:joined_at": now.encode(),
            })
            user_groups.put(f"{owner}#{group_id}".encode(), {
                b"info:group_id": group_id.encode(),
                b"info:joined_at": now.encode(),
            })
        return {"group_id": group_id, "name": name, "owner": owner,
                "description": description, "created_at": now, "member_count": 1}

    def get_group(self, groups_table, group_id):
        with self._get_connection() as conn:
            row = conn.table(groups_table).row(group_id.encode())
            if not row:
                return None
            return {
                "group_id": group_id,
                "name": row.get(b"info:name", b"").decode(),
                "description": row.get(b"info:description", b"").decode(),
                "owner": row.get(b"info:owner", b"").decode(),
                "created_at": row.get(b"info:created_at", b"").decode(),
                "member_count": int(row.get(b"info:member_count", b"0").decode() or 0),
            }

    def delete_group(self, groups_table, members_table, user_groups_table, group_id):
        """解散群组：删 groups 行 + 扫成员前缀清两张索引表"""
        with self._get_connection() as conn:
            groups = conn.table(groups_table)
            members = conn.table(members_table)
            user_groups = conn.table(user_groups_table)

            usernames = []
            prefix = f"{group_id}#".encode()
            for key, _ in members.scan(row_prefix=prefix):
                k = key.decode()
                username = k.split("#", 1)[1]
                usernames.append(username)
                members.delete(key)
            for u in usernames:
                user_groups.delete(f"{u}#{group_id}".encode())
            groups.delete(group_id.encode())
        return True

    def add_group_member(self, groups_table, members_table, user_groups_table,
                         group_id, username, role="member"):
        """加成员：双写两张索引表，同步 member_count"""
        now = str(int(time.time() * 1000))
        with self._get_connection() as conn:
            members = conn.table(members_table)
            user_groups = conn.table(user_groups_table)
            groups = conn.table(groups_table)

            if not groups.row(group_id.encode()):
                raise ValueError(f"group {group_id} 不存在")

            mkey = f"{group_id}#{username}".encode()
            if members.row(mkey):
                return False  # 已是成员
            members.put(mkey, {
                b"info:role": role.encode(),
                b"info:joined_at": now.encode(),
            })
            user_groups.put(f"{username}#{group_id}".encode(), {
                b"info:group_id": group_id.encode(),
                b"info:joined_at": now.encode(),
            })
            grow = groups.row(group_id.encode())
            current = int(grow.get(b"info:member_count", b"0").decode() or 0)
            groups.put(group_id.encode(), {b"info:member_count": str(current + 1).encode()})
        return True

    def remove_group_member(self, groups_table, members_table, user_groups_table,
                            group_id, username):
        with self._get_connection() as conn:
            members = conn.table(members_table)
            user_groups = conn.table(user_groups_table)
            groups = conn.table(groups_table)

            mkey = f"{group_id}#{username}".encode()
            if not members.row(mkey):
                return False
            members.delete(mkey)
            user_groups.delete(f"{username}#{group_id}".encode())
            grow = groups.row(group_id.encode())
            current = int(grow.get(b"info:member_count", b"0").decode() or 0)
            groups.put(group_id.encode(), {b"info:member_count": str(max(0, current - 1)).encode()})
        return True

    def list_group_members(self, members_table, group_id):
        """前缀扫 {group_id}# 拿成员列表"""
        result = []
        prefix = f"{group_id}#".encode()
        with self._get_connection() as conn:
            for key, data in conn.table(members_table).scan(row_prefix=prefix):
                k = key.decode()
                username = k.split("#", 1)[1]
                result.append({
                    "username": username,
                    "role": data.get(b"info:role", b"member").decode(),
                    "joined_at": data.get(b"info:joined_at", b"").decode(),
                })
        return result

    def list_user_groups(self, user_groups_table, groups_table, username):
        """前缀扫 {username}# 拿用户加入的群组，再去 groups 表填详情"""
        gids = []
        prefix = f"{username}#".encode()
        with self._get_connection() as conn:
            ug = conn.table(user_groups_table)
            for key, data in ug.scan(row_prefix=prefix):
                gids.append(data.get(b"info:group_id", b"").decode())
        return [g for g in (self.get_group(groups_table, gid) for gid in gids) if g]

    def list_user_group_ids(self, user_groups_table, username):
        """轻量版：只返回 group_id 列表（权限校验/推荐过滤用）"""
        ids = []
        prefix = f"{username}#".encode()
        with self._get_connection() as conn:
            for key, data in conn.table(user_groups_table).scan(row_prefix=prefix):
                gid = data.get(b"info:group_id", b"").decode()
                if gid:
                    ids.append(gid)
        return ids

    def list_all_groups(self, groups_table):
        """管理员视角：列出所有群组"""
        result = []
        with self._get_connection() as conn:
            for key, data in conn.table(groups_table).scan():
                result.append({
                    "group_id": key.decode(),
                    "name": data.get(b"info:name", b"").decode(),
                    "description": data.get(b"info:description", b"").decode(),
                    "owner": data.get(b"info:owner", b"").decode(),
                    "created_at": data.get(b"info:created_at", b"").decode(),
                    "member_count": int(data.get(b"info:member_count", b"0").decode() or 0),
                })
        return result

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
