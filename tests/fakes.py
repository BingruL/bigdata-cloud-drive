"""
内存版假 HBase / HDFS 服务

测试目标：覆盖 *项目原有功能* 的集成测试（auth / files / groups / sharing / stats）。
新增的 Kafka / Spark Streaming / MapReduce 由于强依赖外部基础设施，不在测试范围内。

设计原则：
- 与真实 HBaseService / HDFSService 公开接口同构，路由代码无感切换
- 所有状态保存在 Python dict，单测进程内跨方法可见
- 不依赖 happybase / hdfs / 网络
"""
import json
import time
import uuid


class FakeHBaseService:
    """内存版 HBaseService，覆盖 routes 实际用到的方法。"""

    def __init__(self):
        # tables[table_name][row_key] = {col_name: value}
        self._tables = {}

    def _t(self, name):
        return self._tables.setdefault(name, {})

    # ===== 表管理 =====
    def init_tables(self, table_config):
        for name in table_config:
            self._t(name)

    def ping(self):
        return True

    # ===== 用户 =====
    def create_user(self, table_name, username, password_hash, role="user"):
        t = self._t(table_name)
        if username in t:
            return None
        t[username] = {
            "password": password_hash,
            "role": role,
            "created_at": str(int(time.time() * 1000)),
            "status": "active",
        }
        return {"username": username, "role": role, "status": "active"}

    def get_user(self, table_name, username):
        t = self._t(table_name)
        if username not in t:
            return None
        return {"username": username, **t[username]}

    def list_users(self, table_name):
        return [{"username": u, **info} for u, info in self._t(table_name).items()]

    # ===== 文件 =====
    def save_file_meta(self, table_name, file_id, meta):
        self._t(table_name)[file_id] = dict(meta)
        return file_id

    def get_file_meta(self, table_name, file_id):
        row = self._t(table_name).get(file_id)
        if not row:
            return None
        return {"file_id": file_id, **row}

    def delete_file_meta(self, table_name, file_id):
        self._t(table_name).pop(file_id, None)
        return True

    def list_files(self, table_name, owner=None, file_type=None, keyword=None,
                   page=1, page_size=20, include_deleted=False, only_deleted=False,
                   start_date=None, end_date=None):
        files = []
        for fid, row in self._t(table_name).items():
            info = {"file_id": fid, **row}
            is_deleted = info.get("deleted") == "1"
            if only_deleted:
                if not is_deleted:
                    continue
            elif not include_deleted and is_deleted:
                continue
            if owner and info.get("owner") != owner:
                continue
            if file_type and info.get("type", "").lower() != file_type.lower():
                continue
            if keyword and keyword.lower() not in info.get("filename", "").lower():
                continue
            if start_date is not None or end_date is not None:
                try:
                    ts = int(info.get("created_at", 0) or 0)
                except (TypeError, ValueError):
                    ts = 0
                if start_date is not None and ts < start_date:
                    continue
                if end_date is not None and ts > end_date:
                    continue
            files.append(info)
        if only_deleted:
            files.sort(key=lambda x: x.get("deleted_at", "0"), reverse=True)
        else:
            files.sort(key=lambda x: x.get("created_at", "0"), reverse=True)
        total = len(files)
        start = (page - 1) * page_size
        end = start + page_size
        return {
            "files": files[start:end], "total": total,
            "page": page, "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
        }

    def increment_downloads(self, table_name, file_id):
        row = self._t(table_name).get(file_id)
        if not row:
            return 0
        cur = int(row.get("downloads", "0") or 0) + 1
        row["downloads"] = str(cur)
        return cur

    def soft_delete_file(self, table_name, file_id):
        row = self._t(table_name).get(file_id)
        if not row:
            return False
        row["deleted"] = "1"
        row["deleted_at"] = str(int(time.time() * 1000))
        return True

    def restore_file(self, table_name, file_id):
        row = self._t(table_name).get(file_id)
        if not row:
            return False
        row.pop("deleted", None)
        row.pop("deleted_at", None)
        return True

    def update_file_meta_fields(self, table_name, file_id, fields):
        row = self._t(table_name).get(file_id)
        if not row:
            return False
        row.update({k: str(v) for k, v in fields.items()})
        return True

    def update_file_ai(self, table_name, file_id, summary=None, tags=None):
        row = self._t(table_name).get(file_id)
        if not row:
            return
        if summary is not None:
            row["summary"] = summary
        if tags is not None:
            row["tags"] = tags

    def update_file_sharing(self, table_name, file_id, is_shared, group_ids):
        row = self._t(table_name).get(file_id)
        if not row:
            return False
        shared_str = ",".join([g for g in group_ids if g]) if is_shared else ""
        row["is_shared"] = "1" if is_shared and shared_str else "0"
        row["shared_groups"] = shared_str
        return True

    def remove_group_from_all_files(self, table_name, group_id):
        affected = 0
        for row in self._t(table_name).values():
            shared_raw = row.get("shared_groups", "")
            if not shared_raw:
                continue
            groups = [x for x in shared_raw.split(",") if x]
            if group_id not in groups:
                continue
            groups.remove(group_id)
            new_str = ",".join(groups)
            row["shared_groups"] = new_str
            row["is_shared"] = "1" if new_str else "0"
            affected += 1
        return affected

    def get_all_files_raw(self, table_name, include_deleted=True):
        result = []
        for fid, row in self._t(table_name).items():
            if not include_deleted and row.get("deleted") == "1":
                continue
            result.append({"file_id": fid, **row})
        return result

    def create_folder(self, table_name, folder_id, meta):
        self._t(table_name)[folder_id] = dict(meta)
        return {"folder_id": folder_id, **meta}

    def list_child_folders(self, table_name, owner, parent_id, include_deleted=False, only_deleted=False):
        rows = []
        for folder_id, row in self._t(table_name).items():
            if row.get("owner") != owner:
                continue
            if row.get("parent_id", "root") != parent_id:
                continue
            deleted = row.get("deleted") == "1"
            if only_deleted and not deleted:
                continue
            if not include_deleted and not only_deleted and deleted:
                continue
            rows.append({"folder_id": folder_id, **row, "item_type": "folder"})
        rows.sort(key=lambda r: r.get("name", ""))
        return rows

    def get_folder(self, table_name, folder_id):
        if folder_id == "root":
            return {"folder_id": "root", "name": "全部文件", "parent_id": "", "owner": ""}
        row = self._t(table_name).get(folder_id)
        return {"folder_id": folder_id, **row} if row else None

    def update_folder_fields(self, table_name, folder_id, fields):
        if folder_id == "root":
            return False
        row = self._t(table_name).get(folder_id)
        if not row:
            return False
        row.update({k: str(v) for k, v in fields.items()})
        return True

    def collect_folder_subtree(self, folders_table, files_table, folder_id):
        folders = [
            {"folder_id": fid, **row}
            for fid, row in self._t(folders_table).items()
        ]
        by_parent = {}
        for folder in folders:
            by_parent.setdefault(folder.get("parent_id", "root"), []).append(folder)

        folder_ids = set()
        subtree_folders = []
        stack = [folder_id]
        while stack:
            current_id = stack.pop()
            if current_id in folder_ids:
                continue
            folder_ids.add(current_id)
            folder = next((f for f in folders if f.get("folder_id") == current_id), None)
            if folder:
                subtree_folders.append(folder)
            for child in by_parent.get(current_id, []):
                stack.append(child["folder_id"])

        subtree_files = [
            {"file_id": fid, **row}
            for fid, row in self._t(files_table).items()
            if row.get("parent_id", "root") in folder_ids
        ]
        return {"folders": subtree_folders, "files": subtree_files}

    def is_descendant_folder(self, table_name, ancestor_folder_id, candidate_folder_id):
        if candidate_folder_id == "root":
            return False
        folders = self._t(table_name)
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
        now = str(int(time.time() * 1000))
        subtree = self.collect_folder_subtree(folders_table, files_table, folder_id)
        for folder in subtree["folders"]:
            row = self._t(folders_table).get(folder["folder_id"])
            if row is not None:
                row["deleted"] = "1"
                row["deleted_at"] = now
                row["updated_at"] = now
        for file_info in subtree["files"]:
            row = self._t(files_table).get(file_info["file_id"])
            if row is not None:
                row["deleted"] = "1"
                row["deleted_at"] = now
                row["updated_at"] = now
        return subtree

    def restore_folder_tree(self, folders_table, files_table, folder_id):
        now = str(int(time.time() * 1000))
        subtree = self.collect_folder_subtree(folders_table, files_table, folder_id)
        for folder in subtree["folders"]:
            row = self._t(folders_table).get(folder["folder_id"])
            if row is not None:
                row.pop("deleted", None)
                row.pop("deleted_at", None)
                row["updated_at"] = now
        for file_info in subtree["files"]:
            row = self._t(files_table).get(file_info["file_id"])
            if row is not None:
                row.pop("deleted", None)
                row.pop("deleted_at", None)
                row["updated_at"] = now
        return subtree

    def purge_folder_tree(self, folders_table, files_table, folder_id, hdfs=None):
        subtree = self.collect_folder_subtree(folders_table, files_table, folder_id)
        for file_info in subtree["files"]:
            hdfs_path = file_info.get("hdfs_path")
            if hdfs and hdfs_path:
                hdfs.delete_file(hdfs_path)
            self._t(files_table).pop(file_info["file_id"], None)
        for folder in subtree["folders"]:
            self._t(folders_table).pop(folder["folder_id"], None)
        return subtree

    def resolve_available_name(self, files_table, folders_table, owner, parent_id, desired_name,
                               exclude_file_id=None, exclude_folder_id=None):
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

    # ===== 日志 =====
    def add_log(self, table_name, username, action, detail=""):
        t = self._t(table_name)
        ts = int(time.time() * 1000)
        key = f"{ts}_{uuid.uuid4().hex[:8]}"
        t[key] = {
            "username": username, "action": action,
            "detail": str(detail), "timestamp": str(ts),
        }
        return key

    def get_logs(self, table_name, username=None, action=None, limit=100):
        items = sorted(self._t(table_name).items(), key=lambda kv: kv[0], reverse=True)
        out = []
        for k, v in items:
            if username and v.get("username") != username:
                continue
            if action and v.get("action") != action:
                continue
            out.append({"log_id": k, **v})
            if len(out) >= limit:
                break
        return out

    # ===== 统计缓存 =====
    def save_stats(self, table_name, stat_key, data):
        self._t(table_name)[stat_key] = {
            "value": json.dumps(data, ensure_ascii=False),
            "updated_at": str(int(time.time() * 1000)),
        }

    def get_stats(self, table_name, stat_key):
        row = self._t(table_name).get(stat_key)
        if not row:
            return None
        return {
            "key": stat_key,
            "value": json.loads(row.get("value", "{}")),
            "updated_at": row.get("updated_at", "0"),
        }

    # ===== 群组：双表反向索引 =====
    def create_group(self, groups_table, members_table, user_groups_table,
                     name, owner, description=""):
        gid = uuid.uuid4().hex
        now = str(int(time.time() * 1000))
        self._t(groups_table)[gid] = {
            "name": name, "description": description, "owner": owner,
            "created_at": now, "member_count": "1",
        }
        self._t(members_table)[f"{gid}#{owner}"] = {"role": "owner", "joined_at": now}
        self._t(user_groups_table)[f"{owner}#{gid}"] = {"group_id": gid, "joined_at": now}
        return {"group_id": gid, "name": name, "owner": owner,
                "description": description, "created_at": now, "member_count": 1}

    def get_group(self, groups_table, group_id):
        row = self._t(groups_table).get(group_id)
        if not row:
            return None
        return {"group_id": group_id, **{k: v for k, v in row.items() if k != "member_count"},
                "member_count": int(row.get("member_count", "0") or 0)}

    def delete_group(self, groups_table, members_table, user_groups_table, group_id):
        members = self._t(members_table)
        user_groups = self._t(user_groups_table)
        usernames = []
        for key in [k for k in members if k.startswith(f"{group_id}#")]:
            usernames.append(key.split("#", 1)[1])
            del members[key]
        for u in usernames:
            user_groups.pop(f"{u}#{group_id}", None)
        self._t(groups_table).pop(group_id, None)
        return True

    def add_group_member(self, groups_table, members_table, user_groups_table,
                         group_id, username, role="member"):
        if group_id not in self._t(groups_table):
            raise ValueError(f"group {group_id} 不存在")
        members = self._t(members_table)
        mkey = f"{group_id}#{username}"
        if mkey in members:
            return False
        now = str(int(time.time() * 1000))
        members[mkey] = {"role": role, "joined_at": now}
        self._t(user_groups_table)[f"{username}#{group_id}"] = {"group_id": group_id, "joined_at": now}
        grow = self._t(groups_table).get(group_id)
        if grow:
            grow["member_count"] = str(int(grow.get("member_count", "0") or 0) + 1)
        return True

    def remove_group_member(self, groups_table, members_table, user_groups_table,
                            group_id, username):
        members = self._t(members_table)
        mkey = f"{group_id}#{username}"
        if mkey not in members:
            return False
        del members[mkey]
        self._t(user_groups_table).pop(f"{username}#{group_id}", None)
        grow = self._t(groups_table).get(group_id)
        if grow:
            grow["member_count"] = str(max(0, int(grow.get("member_count", "0") or 0) - 1))
        return True

    def list_group_members(self, members_table, group_id):
        out = []
        for key, info in self._t(members_table).items():
            if key.startswith(f"{group_id}#"):
                out.append({"username": key.split("#", 1)[1],
                            "role": info.get("role", "member"),
                            "joined_at": info.get("joined_at", "")})
        return out

    def list_user_groups(self, user_groups_table, groups_table, username):
        gids = []
        for key, info in self._t(user_groups_table).items():
            if key.startswith(f"{username}#"):
                gid = info.get("group_id")
                if gid:
                    gids.append(gid)
        return [g for g in (self.get_group(groups_table, gid) for gid in gids) if g]

    def list_user_group_ids(self, user_groups_table, username):
        return [info["group_id"] for key, info in self._t(user_groups_table).items()
                if key.startswith(f"{username}#") and info.get("group_id")]

    def list_all_groups(self, groups_table):
        return [{"group_id": gid, **{k: v for k, v in row.items() if k != "member_count"},
                 "member_count": int(row.get("member_count", "0") or 0)}
                for gid, row in self._t(groups_table).items()]


class FakeHDFSService:
    """内存版 HDFSService —— 字节流存 dict，避免真实 HDFS 依赖"""

    def __init__(self):
        self._files = {}  # hdfs_path -> bytes

    def init_directories(self):
        pass

    def ping(self):
        return True

    def upload_file(self, username, file_id, local_path, filename):
        import os
        ext = os.path.splitext(filename)[1]
        hdfs_path = f"/cloud-drive/files/{username}/{file_id}{ext}"
        with open(local_path, "rb") as f:
            self._files[hdfs_path] = f.read()
        return hdfs_path

    def download_file(self, hdfs_path, local_path):
        data = self._files.get(hdfs_path, b"")
        with open(local_path, "wb") as f:
            f.write(data)
        return local_path

    def read_file(self, hdfs_path):
        return self._files.get(hdfs_path, b"")

    def read_text_file(self, hdfs_path, max_bytes=50000):
        data = self._files.get(hdfs_path, b"")[:max_bytes]
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("utf-8", errors="ignore")

    def delete_file(self, hdfs_path):
        return self._files.pop(hdfs_path, None) is not None

    def file_exists(self, hdfs_path):
        return hdfs_path in self._files

    def get_file_size(self, hdfs_path):
        return len(self._files.get(hdfs_path, b""))

    def get_storage_usage(self, username=None):
        if username:
            prefix = f"/cloud-drive/files/{username}/"
            total = sum(len(v) for k, v in self._files.items() if k.startswith(prefix))
            cnt = sum(1 for k in self._files if k.startswith(prefix))
        else:
            total = sum(len(v) for v in self._files.values())
            cnt = len(self._files)
        return {"total_size": total, "file_count": cnt, "dir_count": 0}
