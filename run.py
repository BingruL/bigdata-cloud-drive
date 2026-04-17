#!/usr/bin/env python3
"""
项目启动与初始化脚本
1. 检查并初始化 HBase 表
2. 创建管理员账户
3. 生成测试数据（可选）
4. 启动 Flask 服务

用法:
  python run.py                # 正常启动
  python run.py --init         # 初始化 + 启动
  python run.py --seed         # 初始化 + 生成测试数据 + 启动
"""
import os
import sys
import time
import uuid
import random
import argparse

# 确保能导入 backend 模块
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from backend.config import get_config
from backend.auth.jwt_handler import hash_password


def init_hbase(config):
    """初始化 HBase 表"""
    from backend.services.hbase_service import HBaseService
    print("[初始化] 连接 HBase...")
    hbase = HBaseService(config.HBASE_HOST, config.HBASE_PORT)
    table_config = {
        config.HBASE_TABLE_USERS: {"info": dict()},
        config.HBASE_TABLE_FILES: {"meta": dict()},
        config.HBASE_TABLE_LOGS: {"log": dict()},
        config.HBASE_TABLE_STATS: {"data": dict()},
    }
    hbase.init_tables(table_config)
    print("[初始化] HBase 表创建完成")
    return hbase


def init_hdfs(config):
    """初始化 HDFS 目录"""
    from backend.services.hdfs_service import HDFSService
    print("[初始化] 连接 HDFS...")
    hdfs = HDFSService(config.HDFS_URL, config.HDFS_USER, config.HDFS_ROOT_DIR)
    hdfs.init_directories()
    print("[初始化] HDFS 目录创建完成")
    return hdfs


def create_admin(hbase, config):
    """创建管理员账户"""
    print("[初始化] 创建管理员账户...")
    admin_pass = os.environ.get("ADMIN_PASSWORD", "admin123")
    result = hbase.create_user(
        config.HBASE_TABLE_USERS,
        "admin",
        hash_password(admin_pass),
        role="admin",
    )
    if result:
        print(f"  管理员账户创建成功: admin / {admin_pass}")
    else:
        print("  管理员账户已存在")


def seed_test_data(hbase, hdfs, config):
    """生成测试数据"""
    print("\n[测试数据] 开始生成...")

    # 创建测试用户
    test_users = ["alice", "bob", "charlie", "diana"]
    for user in test_users:
        result = hbase.create_user(
            config.HBASE_TABLE_USERS,
            user,
            hash_password("123456"),
            role="user",
        )
        if result:
            print(f"  创建用户: {user}")

    # 生成测试文件元数据（含标签，用于关系图谱展示）
    test_files = [
        ("项目报告.pdf", "报告,项目管理,文档"),
        ("数据分析.csv", "数据分析,统计,大数据"),
        ("系统设计.md", "系统设计,架构,文档"),
        ("风景照片.jpg", "摄影,风景,自然"),
        ("代码备份.zip", "备份,代码,归档"),
        ("hadoop_config.xml", "Hadoop,配置,大数据"),
        ("spark_job.py", "Spark,大数据,数据处理"),
        ("论文初稿.txt", "论文,学术,文档"),
        ("财务报表.xlsx", "财务,报表,统计"),
        ("会议纪要.pdf", "会议,项目管理,文档"),
        ("需求文档.md", "需求分析,项目管理,文档"),
        ("测试数据.csv", "测试,数据分析,大数据"),
        ("架构图.png", "系统设计,架构,图表"),
        ("日志分析.py", "日志,数据分析,数据处理"),
        ("数据库设计.pdf", "数据库,系统设计,文档"),
        ("用户手册.txt", "手册,文档,用户"),
        ("MapReduce示例.java", "MapReduce,大数据,Hadoop"),
        ("词频统计.py", "统计,数据处理,大数据"),
        ("销售数据.csv", "销售,统计,数据分析"),
        ("产品图片.jpg", "产品,摄影,展示"),
    ]

    now_ts = int(time.time() * 1000)
    for i, (fname, tags) in enumerate(test_files):
        file_id = uuid.uuid4().hex
        owner = random.choice(test_users)
        ftype = fname.split(".")[-1]
        size = random.randint(1024, 50 * 1024 * 1024)
        created = now_ts - random.randint(0, 7 * 86400 * 1000)
        downloads = random.randint(0, 50)

        meta = {
            "filename": fname,
            "size": str(size),
            "type": ftype,
            "owner": owner,
            "hdfs_path": f"/cloud-drive/files/{owner}/{file_id}.{ftype}",
            "created_at": str(created),
            "downloads": str(downloads),
            "summary": "",
            "tags": tags,
        }
        hbase.save_file_meta(config.HBASE_TABLE_FILES, file_id, meta)

    print(f"  生成 {len(test_files)} 个文件记录")

    # 生成操作日志
    actions = ["login", "upload", "download", "download", "download"]
    for _ in range(100):
        user = random.choice(test_users + ["admin"])
        action = random.choice(actions)
        hbase.add_log(config.HBASE_TABLE_LOGS, user, action, f"test_{uuid.uuid4().hex[:6]}")

    print("  生成 100 条操作日志")
    print("[测试数据] 完成!\n")


def main():
    parser = argparse.ArgumentParser(description="智能云盘系统启动脚本")
    parser.add_argument("--init", action="store_true", help="初始化 HBase 表和 HDFS 目录")
    parser.add_argument("--seed", action="store_true", help="生成测试数据")
    parser.add_argument("--port", type=int, default=5000, help="服务端口 (默认 5000)")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址 (默认 0.0.0.0)")
    args = parser.parse_args()

    config = get_config()

    if args.init or args.seed:
        try:
            hbase = init_hbase(config)
            hdfs = init_hdfs(config)
            create_admin(hbase, config)
            if args.seed:
                seed_test_data(hbase, hdfs, config)
        except Exception as e:
            print(f"\n[错误] 初始化失败: {e}")
            print("请确保 HBase Thrift Server 和 HDFS 已启动。")
            print("  启动 HBase: start-hbase.sh && hbase thrift start")
            print("  启动 HDFS:  start-dfs.sh")
            sys.exit(1)

    # 启动 Flask
    print("=" * 50)
    print("  智能云盘系统")
    print(f"  访问地址: http://localhost:{args.port}")
    print("=" * 50)

    from backend.app import create_app
    app = create_app()
    app.run(host=args.host, port=args.port, debug=config.DEBUG)


if __name__ == "__main__":
    main()
