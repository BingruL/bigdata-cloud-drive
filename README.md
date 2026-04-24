# 基于 HBase/HDFS 的智能云盘系统

> 大数据技术基础 · 期末项目

## 一、项目概述

本项目是一个基于 Hadoop 生态的简易云盘系统，集成了分布式存储、用户认证、数据统计分析、AI 智能功能和可视化展示。

**核心特性：**

- **分布式存储**：文件内容存入 HDFS，元数据存入 HBase
- **Token 认证**：JWT 无状态认证 + 角色权限控制
- **分布式计算**：Spark 批量统计分析 + 推荐计算
- **AI 智能**：文件摘要/标签生成 + 个性化推荐
- **数据可视化**：ECharts 仪表盘，多维度图表展示
- **存储配额**：用户级存储配额与实时用量进度条
- **回收站**：软删除机制，支持恢复与彻底删除
- **最近访问**：基于操作日志聚合用户最近使用的文件

**课程知识点覆盖：**

| 项目模块 |
|---------|
| HDFS / HBase 存储 |
| MapReduce / Spark 计算 |
| Flume/Kafka 日志采集 |
| 推荐算法 |
| ECharts 可视化 |
| JWT 认证与访问控制 |

---

## 二、项目结构

```
bigdata-cloud-drive/
├── backend/                    # 后端 Python 代码
│   ├── app.py                  # Flask 主入口（应用工厂）
│   ├── config.py               # 配置文件
│   ├── auth/
│   │   └── jwt_handler.py      # JWT Token 认证模块
│   ├── routes/
│   │   ├── auth_routes.py      # 认证路由（注册/登录/刷新）
│   │   ├── file_routes.py      # 文件管理路由（CRUD + 搜索）
│   │   └── stats_routes.py     # 统计分析 & AI 推荐路由
│   └── services/
│       ├── hbase_service.py    # HBase 数据访问服务
│       ├── hdfs_service.py     # HDFS 文件存储服务
│       ├── ai_service.py       # AI 摘要/推荐服务
│       └── stats_service.py    # 统计计算服务
├── frontend/                   # 前端 Web 界面
│   ├── landing.html            # 炫酷引导页（/）- canvas 粒子背景 + bento 特性 + 对比表
│   ├── index.html              # SPA 主页面（/app）
│   ├── css/style.css           # 样式
│   └── js/app.js               # Vue 3 应用逻辑
├── spark_jobs/                 # Spark 分析作业
│   ├── file_stats.py           # 文件统计分析
│   └── recommendation.py       # 推荐计算
├── run.py                      # 启动脚本
└── docs/                       # 项目文档
```

---

## 三、环境准备

### 3.1 前提条件

确保已安装以下组件：

| 组件 | 版本要求 | 用途 |
|------|---------|------|
| JDK | 1.8+ | Hadoop/HBase 运行环境 |
| Hadoop | 3.x | HDFS 分布式文件系统 |
| HBase | 2.x | NoSQL 元数据存储 |
| Python | 3.8+ | 后端服务 |
| Spark | 3.x | 分布式计算（可选） |

### 3.2 安装 Python 依赖

```bash
cd bigdata-cloud-drive
pip install -r backend/requirements.txt
```

### 3.3 启动 Hadoop 和 HBase

```bash
# 1. 启动 HDFS
start-dfs.sh

# 2. 启动 HBase
start-hbase.sh

# 3. 启动 HBase Thrift Server（Python 客户端连接需要）
hbase thrift start &

# 验证服务
jps   # 应看到 NameNode, DataNode, HMaster, HRegionServer, ThriftServer
```

### 3.4 配置

编辑 `backend/config.py` 或通过环境变量配置：

```bash
export HDFS_URL=http://localhost:9870
export HBASE_HOST=localhost
export HBASE_PORT=9090

# AI 服务（可选，用于文件摘要功能）
# 支持任何兼容 OpenAI API 的服务，如 Ollama
export AI_API_URL=http://localhost:11434/v1
export AI_MODEL=qwen2.5:7b
```

---

## 四、启动运行

### 4.1 首次启动（初始化 + 测试数据）

```bash
python run.py --seed
```

这会：
1. 在 HBase 中创建 4 张表（users, files, logs, stats）
2. 在 HDFS 中创建目录结构
3. 创建管理员账户（admin / admin123）
4. 生成 20 条测试文件记录和 100 条操作日志
5. 启动 Web 服务

### 4.2 正常启动

```bash
python run.py
# 或指定端口
python run.py --port 8080
```

### 4.3 访问系统

浏览器打开 `http://localhost:5000`

- `/` —— 产品引导页（hero + 特性 bento + 对比表），点击"开始使用"进入系统
- `/app` —— 注册 / 登录页（SPA 主入口）

测试账号：

- 管理员：`admin` / `admin123`
- 测试用户：`alice` / `123456`（或 bob, charlie, diana）

---

## 五、功能说明

### 5.1 用户认证（第 10 章 安全）

- **注册**：`POST /api/auth/register`
- **登录**：`POST /api/auth/login` → 返回 JWT Token
- **刷新**：`POST /api/auth/refresh`
- 所有文件操作接口需携带 `Authorization: Bearer <token>`
- 角色权限：普通用户只能管理自己的文件，管理员可查看全部

### 5.2 文件管理（第 3 章 存储）

- **上传**：文件内容 → HDFS，元数据 → HBase；上传前按用户配额校验
- **下载**：HBase 查路径 → HDFS 读文件
- **删除**：软删除，仅在 HBase 元数据打上 `deleted` 标记，HDFS 文件保留
- **回收站**：展示所有软删除文件，支持 **恢复** 或 **彻底删除**（后者才真正清理 HDFS）
- **最近访问**：聚合操作日志中 `download` / `preview` 事件，按最近访问时间排序
- **搜索**：按文件名、类型、时间范围筛选

**HBase 表设计 `cloud_drive_files`：**

| RowKey | 列族 meta |
|--------|----------|
| file_id (UUID) | filename, size, type, owner, hdfs_path, created_at, downloads, summary, tags, **deleted, deleted_at** |

### 5.2.1 存储配额

- 普通用户默认配额 10 GB，管理员 200 GB（可通过 `USER_QUOTA_BYTES` / `ADMIN_QUOTA_BYTES` 环境变量覆盖）
- 上传时实时校验用量（含回收站中文件），超额返回 HTTP 413
- 前端侧边栏底部显示进度条，超 80% 变色提醒

### 5.3 统计分析（第 4/8 章 计算）

实时统计（后端直接计算）：
- 各用户文件数量
- 文件类型分布
- 每日上传趋势
- 存储空间占用
- 热门文件排行

Spark 批量分析（定期运行）：
```bash
spark-submit --master local[*] spark_jobs/file_stats.py
spark-submit --master local[*] spark_jobs/recommendation.py
```

### 5.4 数据可视化（第 9 章 可视化）

Dashboard 包含：
- 概览卡片（文件数、存储、下载、用户数）
- 柱状图：各用户文件数量
- 饼图：文件类型分布
- 折线图：近 7 天上传趋势
- 日历热力图：最近一年用户活跃度（按日聚合操作次数，颜色深浅表示活跃程度）
- 横向柱状图：热门文件 Top 10

### 5.5 AI 智能功能（第 7 章 分析与挖掘）

**文件摘要/标签：**
- 上传文本文件时自动调用 LLM 生成摘要和标签
- 支持手动触发重新生成

**智能推荐：**
- 热门推荐：基于下载次数排序
- 个性化推荐：分析用户偏好类型，推荐同类高热度文件
- 协同过滤：Jaccard 相似度找到相似用户，推荐交叉文件

---

## 六、API 接口文档

### 认证接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/auth/register` | 注册 |
| POST | `/api/auth/login` | 登录，返回 Token |
| POST | `/api/auth/refresh` | 刷新 Token |
| GET  | `/api/auth/me` | 获取当前用户信息 |

### 文件接口（需认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/files/upload` | 上传文件 (multipart/form-data)，受配额限制 |
| GET  | `/api/files/list` | 文件列表（支持分页、筛选，不含回收站） |
| GET  | `/api/files/recent` | 当前用户最近访问的文件（按日志聚合） |
| GET  | `/api/files/trash` | 回收站文件列表 |
| GET  | `/api/files/<id>` | 获取文件详情 |
| GET  | `/api/files/<id>/download` | 下载文件 |
| DELETE | `/api/files/<id>` | 软删除（移入回收站） |
| POST | `/api/files/<id>/restore` | 从回收站恢复 |
| DELETE | `/api/files/<id>/purge` | 彻底删除（清理 HDFS + HBase） |
| GET  | `/api/files/search` | 搜索文件 |
| POST | `/api/files/<id>/summary` | 生成 AI 摘要 |

### 统计接口（需认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/stats/dashboard` | Dashboard 汇总 |
| GET | `/api/stats/user-file-counts` | 用户文件数 |
| GET | `/api/stats/file-type-distribution` | 类型分布 |
| GET | `/api/stats/daily-upload-trend` | 上传趋势 |
| GET | `/api/stats/storage` | 存储统计（全局） |
| GET | `/api/stats/my-storage` | 当前用户配额与用量 |
| GET | `/api/stats/hot-files` | 热门文件 |
| GET | `/api/stats/recent-activity` | 最近动态 |
| GET | `/api/stats/activity-heatmap` | 活跃热力图数据 |

### 推荐接口（需认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/ai/recommend/hot` | 热门推荐 |
| GET | `/api/ai/recommend/personalized` | 个性化推荐 |
| GET | `/api/ai/recommend/similar-users` | 协同过滤推荐 |

---

## 七、技术选型

| 层次 | 技术 | 说明 |
|------|------|------|
| 前端 | Vue 3 + ECharts | SPA 单页应用 + 数据可视化 |
| 后端 | Flask (Python) | RESTful API 服务 |
| 文件存储 | Hadoop HDFS | 分布式大文件存储 |
| 元数据存储 | Apache HBase | 列族数据库，存储文件信息 |
| 分布式计算 | Apache Spark | 批量统计分析和推荐计算 |
| 认证 | JWT | 无状态 Token 认证 |
| AI | LLM API (兼容 OpenAI) | 文件摘要/标签自动生成 |

---

## 八、Spark 作业说明

### file_stats.py — 文件统计

读取 HBase 中所有文件元数据，使用 Spark DataFrame API 计算：

```python
# 各用户文件数量
files_df.groupBy("owner").agg(count("*"), sum("size_long"))

# 文件类型分布
files_df.groupBy("type").agg(count("*"))

# 热门文件
files_df.orderBy(desc("downloads_long")).limit(20)
```

### recommendation.py — 推荐计算

1. 从日志中提取用户下载行为，构建用户-文件交互矩阵
2. 计算用户间 Jaccard 相似度
3. 综合下载总量和近期热度计算文件评分

---

## 九、注意事项

1. **HBase Thrift Server 必须启动**：Python 的 happybase 库通过 Thrift 协议连接 HBase
2. **AI 功能为可选**：如果未配置 AI API，文件摘要功能不可用，但不影响其他功能
3. **Spark 作业需手动运行**：可以配合 crontab 定时执行
4. **生产部署**：建议使用 Gunicorn + Nginx 部署 Flask 服务
