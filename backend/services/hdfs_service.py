"""
HDFS 服务层
负责与 HDFS 交互，处理文件的上传、下载、删除等操作
使用 hdfs 库（WebHDFS REST API）
"""
import os
import logging
from hdfs import InsecureClient

logger = logging.getLogger(__name__)


class HDFSService:
    """HDFS 文件存储服务"""

    def __init__(self, url="http://localhost:9870", user="bingru", root_dir="/cloud-drive"):
        self.url = url
        self.user = user
        self.root_dir = root_dir
        self.client = InsecureClient(self.url, user=self.user)

    def init_directories(self):
        """初始化 HDFS 目录结构"""
        dirs = [
            self.root_dir,
            f"{self.root_dir}/files",
            f"{self.root_dir}/logs",
        ]
        for d in dirs:
            self.client.makedirs(d)
            logger.info(f"HDFS 目录已就绪: {d}")

    def _user_dir(self, username):
        """获取用户文件目录"""
        return f"{self.root_dir}/files/{username}"

    def upload_file(self, username, file_id, local_path, filename):
        """
        上传文件到 HDFS
        返回 HDFS 路径
        """
        user_dir = self._user_dir(username)
        self.client.makedirs(user_dir)

        # 使用 file_id 作为文件名避免冲突，保留原始扩展名
        ext = os.path.splitext(filename)[1]
        hdfs_path = f"{user_dir}/{file_id}{ext}"

        self.client.upload(hdfs_path, local_path, overwrite=True)
        logger.info(f"文件已上传到 HDFS: {hdfs_path}")
        return hdfs_path

    def download_file(self, hdfs_path, local_path):
        """
        从 HDFS 下载文件到本地
        """
        self.client.download(hdfs_path, local_path, overwrite=True)
        logger.info(f"文件已从 HDFS 下载: {hdfs_path} -> {local_path}")
        return local_path

    def read_file(self, hdfs_path):
        """
        读取 HDFS 文件内容（返回字节流）
        用于小文件直接读取
        """
        with self.client.read(hdfs_path) as reader:
            return reader.read()

    def read_text_file(self, hdfs_path, max_bytes=50000):
        """
        读取文本文件内容（用于 AI 摘要）
        限制最大读取字节数
        """
        with self.client.read(hdfs_path, length=max_bytes) as reader:
            content = reader.read()
        try:
            return content.decode("utf-8")
        except UnicodeDecodeError:
            try:
                return content.decode("gbk")
            except UnicodeDecodeError:
                return content.decode("utf-8", errors="ignore")

    def delete_file(self, hdfs_path):
        """删除 HDFS 文件"""
        result = self.client.delete(hdfs_path, recursive=False)
        if result:
            logger.info(f"HDFS 文件已删除: {hdfs_path}")
        else:
            logger.warning(f"HDFS 文件删除失败: {hdfs_path}")
        return result

    def file_exists(self, hdfs_path):
        """检查文件是否存在"""
        status = self.client.status(hdfs_path, strict=False)
        return status is not None

    def get_file_size(self, hdfs_path):
        """获取文件大小"""
        status = self.client.status(hdfs_path, strict=False)
        if status:
            return status["length"]
        return 0

    def get_storage_usage(self, username=None):
        """
        获取存储使用情况
        username: 指定用户，None 则统计全部
        """
        target = self._user_dir(username) if username else f"{self.root_dir}/files"
        try:
            content = self.client.content(target, strict=False)
            if content:
                return {
                    "total_size": content["length"],
                    "file_count": content["fileCount"],
                    "dir_count": content["directoryCount"],
                }
        except Exception as e:
            logger.error(f"获取存储使用情况失败: {e}")
        return {"total_size": 0, "file_count": 0, "dir_count": 0}
