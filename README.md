# 基于 HBase/HDFS 的智能云盘系统

> 大数据技术基础 · 期末项目

## 一、项目概述

本项目是一个基于 Hadoop 生态的简易云盘系统，集成了分布式存储、用户认证、数据统计分析、AI 智能功能和可视化展示。

**核心特性：**

- **分布式存储**：文件内容存入 HDFS，元数据存入 HBase
- **Token 认证**：JWT 无状态认证 + 角色权限控制
- **目录体系**：支持用户私有文件夹树、面包屑浏览、新建文件夹、重命名和移动
- **私有 + 群组分享**：文件默认私有，只有勾选分享到群组后，组内其他成员才可见/下载
- **公开链接分享**：文件主可生成带过期时间和可选提取码的公开下载链接，并可随时撤销
- **在线预览**：文本/图片预览沿用原接口，PDF 通过短期预览 Token 以内联方式打开
- **分布式计算**：Spark 批量统计分析 + 群组内协同过滤推荐
- **AI 智能**：文件摘要/标签生成 + 群组智能推荐
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
│   │   ├── file_routes.py      # 文件管理路由（CRUD + 搜索 + 预览 + 群组分享）
│   │   ├── folder_routes.py    # 文件夹路由（新建/重命名/移动/回收站）
│   │   ├── public_link_routes.py # 公开链接路由（创建/撤销/公开下载）
│   │   ├── group_routes.py     # 群组管理路由（创建/成员/解散）
│   │   └── stats_routes.py     # 统计分析 & AI 推荐路由
│   └── services/
│       ├── hbase_service.py    # HBase 数据访问服务
│       ├── hdfs_service.py     # HDFS 文件存储服务
│       ├── ai_service.py       # AI 摘要/推荐服务
│       └── stats_service.py    # 统计计算服务
├── frontend/                   # 前端 Web 界面
│   ├── landing.html            # 炫酷引导页（/）- canvas 粒子背景 + bento 特性 + 对比表
│   ├── public.html             # 公开链接下载页（/s/<token>）
│   ├── docs.html               # 用户使用文档（/docs）- 侧栏目录 + 分节说明
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

### 4.1 推荐启动（完整链路）

项目根目录下提供了一键完整启动脚本，适合演示和联调：

```bash
cd bigdata-cloud-drive
./scripts/start_full.sh

# 等价 Makefile 命令
make start-full
```

`start_full.sh` 会按需启动并检查：

1. HDFS（NameNode / DataNode）
2. HBase（HMaster / HRegionServer）
3. HBase Thrift Server（默认 `localhost:9090`）
4. Kafka 单节点容器（默认 `cloud-drive-kafka`，端口 `9092`）
5. Flask Web 服务（默认 `http://localhost:5000`）
6. Kafka 日志 consumer
7. Spark Streaming 实时统计作业（如果本机已安装 `spark-submit`）

常用运维命令：

```bash
./scripts/status_full.sh   # 查看 HDFS / HBase / Kafka / Flask / Streaming 状态
./scripts/stop_full.sh     # 停止完整链路

# 等价 Makefile 命令
make status-full
make stop-full
```

日志位置：

```bash
logs/flask.log
logs/kafka-consumer.log
logs/spark-streaming.log
logs/hbase-thrift.log
```

> 若脚本提示 Docker daemon 不可访问，请先启动 Docker Desktop，并确认 WSL integration 已开启。

### 4.2 首次初始化（初始化 + 测试数据）

```bash
python run.py --seed
```

这会：
1. 在 HBase 中创建 9 张表（users, files, logs, stats, groups, group_members, user_groups, folders, public_links）
2. 在 HDFS 中创建目录结构
3. 创建管理员账户（admin / admin123）
4. 生成 20 条测试文件记录、100 条操作日志、2 个示例群组（大数据课程组 / 运维小组）及部分群组共享文件
5. 启动 Web 服务

首次准备演示环境时，建议先执行一次 `python run.py --seed` 完成初始化；之后日常演示再使用 `./scripts/start_full.sh` 启动完整链路。

### 4.3 轻量启动（仅后端 Web 服务）

如果只想启动 Flask 后端，不启用 Kafka consumer / Spark Streaming，可使用：

```bash
python run.py
# 或指定端口
python run.py --port 8080
```

此模式下 Kafka 默认不启用，操作日志会直接写入 HBase，核心上传、下载、权限、看板等功能仍可运行。

### 4.4 访问系统

浏览器打开 `http://localhost:5000`

- `/` —— 产品引导页（hero + 特性 bento + 对比表），点击"开始使用"进入系统
- `/app` —— 注册 / 登录页（SPA 主入口）
- `/docs` —— 用户使用文档（快速开始、文件管理、AI、看板、推荐等完整说明）

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
- **目录**：每个用户拥有私有目录树，支持在当前目录上传、新建文件夹、面包屑返回上级目录
- **重命名 / 移动**：文件和文件夹均支持重命名、移动；同目录重名会自动生成可用名称
- **下载**：HBase 查路径 → HDFS 读文件
- **预览**：文本与图片可直接在弹窗中预览；PDF 使用短期预览 Token 以 `inline` 流打开，能出现在最近访问中
- **公开链接**：文件主可创建公开下载链接，支持过期时间、可选提取码、下载次数统计和撤销
- **删除**：软删除，仅在 HBase 元数据打上 `deleted` 标记，HDFS 文件保留；删除文件夹会递归处理子目录和文件
- **回收站**：展示所有软删除文件/文件夹，支持 **恢复** 或 **彻底删除**（后者才真正清理 HDFS）
- **最近访问**：聚合操作日志中 `download` / `preview` 事件，按最近访问时间排序
- **搜索**：按文件名、类型、时间范围筛选
- **分享**：文件默认私有。文件主可将文件分享到自己所在的一个或多个群组，只有组内成员才能访问

**HBase 表设计 `cloud_drive_files`：**

| RowKey | 列族 meta |
|--------|----------|
| file_id (UUID) | filename, display_name, parent_id, size, type, owner, hdfs_path, created_at, updated_at, downloads, summary, tags, deleted, deleted_at, **is_shared, shared_groups** |

**新增 HBase 表：**

| 表 | RowKey | 用途 |
|----|--------|------|
| `cloud_drive_folders` | `folder_id` | 文件夹元数据，记录 `name`、`parent_id`、`owner`、删除状态和更新时间 |
| `cloud_drive_public_links` | `token` | 公开链接元数据，记录文件、创建者、过期时间、提取码哈希、撤销状态和下载次数 |

### 5.2.2 群组与分享模型

- 所有读取他人文件的接口（download / preview / 详情）统一使用权限规则：`owner == me OR (is_shared == "1" AND 文件的 shared_groups 与我所在群组有交集) OR role == admin`
- 群组采用"双表反向索引"的 HBase 经典建模（见第八节「HBase 数据建模思路」）
- 任何登录用户都可创建群组并成为群主；群主可增删成员、解散群组；普通成员可查看成员列表、主动退出
- 管理员可查看所有群组（`GET /api/groups?all=1`）

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

MapReduce 标签倒排索引（详见 `docs/MAPREDUCE_USAGE.md`）：
```bash
# Hadoop Streaming 版本（一键跑通：HBase → HDFS → MR → HBase）
bash mapreduce_jobs/tag_index/run.sh

# Spark 对照版本（同样逻辑，演示"同一题两种范式"）
spark-submit --master local[*] spark_jobs/tag_index_spark.py
```
完成后前端可调用 `GET /api/files/by-tag/<tag>` 走索引，O(1) 命中而无需 scan 文件元数据全表。

Spark 批量分析（定期运行）：
```bash
spark-submit --master local[*] spark_jobs/file_stats.py
spark-submit --master local[*] spark_jobs/recommendation.py
```

Spark Structured Streaming 实时统计（可选，speed layer，详见 `docs/SPARK_STREAMING_USAGE.md`）：
```bash
spark-submit --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_jobs/streaming_stats.py
```
启动后 Dashboard 顶部"实时面板"开始接收 5 分钟/10 分钟滑动窗口指标，每 2 秒刷新。

### 5.4 数据可视化（第 9 章 可视化）

Dashboard 包含：
- **实时面板**（speed layer）：5 分钟动作计数、10 分钟活跃用户、5 分钟热门文件、最新 30 条事件流。数据由 `streaming_stats.py` 通过 Spark Structured Streaming 从 Kafka 订阅事件后写入 HBase，前端每 2 秒轮询，绿色脉冲圆点指示在线状态
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

**智能推荐（限定群组作用域）：**

推荐的候选文件池 = 我所在群组的"已分享"文件；相似度/偏好语料 = 我所在群组成员的下载/预览行为。

- 群组热门：候选池内按下载次数排序
- 个性化推荐：基于我对候选池文件的类型偏好，推荐同类高热度文件
- 相似成员推荐：在群组成员之间计算 Jaccard 相似度，推荐相似成员下载过而我没下过的文件

Admin 视角下退化为全站（避免管理员视图被群组过滤）；未加入任何群组的普通用户，推荐会返回空并提示加入群组。

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
| GET  | `/api/files/browse` | 浏览当前目录下的文件和文件夹 |
| GET  | `/api/files/recent` | 当前用户最近访问的文件（按日志聚合） |
| GET  | `/api/files/trash` | 回收站文件列表 |
| GET  | `/api/files/<id>` | 获取文件详情 |
| GET  | `/api/files/<id>/download` | 下载文件 |
| PATCH | `/api/files/<id>/rename` | 重命名文件 |
| PATCH | `/api/files/<id>/move` | 移动文件到目标目录 |
| DELETE | `/api/files/<id>` | 软删除（移入回收站） |
| POST | `/api/files/<id>/restore` | 从回收站恢复 |
| DELETE | `/api/files/<id>/purge` | 彻底删除（清理 HDFS + HBase） |
| GET  | `/api/files/search` | 搜索文件 |
| GET  | `/api/files/<id>/preview` | 文本/图片预览 |
| POST | `/api/files/<id>/preview-token` | 创建短期 PDF 预览 Token |
| GET  | `/api/files/<id>/preview-stream` | PDF 流式预览 |
| POST | `/api/files/<id>/summary` | 生成 AI 摘要 |
| POST | `/api/files/<id>/share` | 分享文件到指定群组（body: `{groups: [gid,...]}`，覆盖式） |
| POST | `/api/files/<id>/unshare` | 取消所有分享，恢复私有 |
| GET  | `/api/files/shared` | 列出我所在群组里其他成员分享给我的文件 |
| GET  | `/api/files/by-tag/<tag>` | 按标签查询（走 MR/Spark 离线生成的倒排索引表 `cloud_drive_tag_index`） |

### 文件夹接口（需认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/folders` | 新建文件夹 |
| GET  | `/api/folders/<id>` | 获取文件夹详情 |
| PATCH | `/api/folders/<id>/rename` | 重命名文件夹 |
| PATCH | `/api/folders/<id>/move` | 移动文件夹树，禁止移动到自身或子目录 |
| DELETE | `/api/folders/<id>` | 递归软删除文件夹及其子项 |
| POST | `/api/folders/<id>/restore` | 从回收站恢复文件夹树 |
| DELETE | `/api/folders/<id>/purge` | 彻底删除文件夹树及其 HDFS 文件 |

### 公开链接接口

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/files/<id>/public-links` | 创建公开链接（文件主） |
| GET  | `/api/files/<id>/public-links` | 查看文件已有公开链接 |
| DELETE | `/api/files/<id>/public-links/<token>` | 撤销公开链接 |
| GET  | `/api/public-links/<token>` | 获取公开链接信息 |
| POST | `/api/public-links/<token>/download` | 校验提取码并下载文件 |
| GET  | `/s/<token>` | 浏览器公开下载页 |

### 群组接口（需认证）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/groups` | 创建群组（创建者自动成为群主） |
| GET  | `/api/groups` | 我加入的群组；admin 加 `?all=1` 列出全部 |
| GET  | `/api/groups/<id>` | 群组详情（含成员列表） |
| DELETE | `/api/groups/<id>` | 解散群组（仅群主或 admin） |
| POST | `/api/groups/<id>/members` | 添加成员（仅群主） |
| DELETE | `/api/groups/<id>/members/<username>` | 移除成员（群主）/ 退出群组（本人） |

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
| GET | `/api/stats/realtime` | 实时面板（Spark Streaming 写入的 5min/10min 滑动窗口指标） |

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

1. 扫描 `cloud_drive_files` 保留 `is_shared=1` 的共享文件，扫描 `cloud_drive_group_members` 构建"群组 → 成员集合"
2. 从日志中提取**对共享文件**的下载行为，构建用户-文件交互矩阵
3. **按群组分别**计算组内用户 Jaccard 相似度（天然避免跨群组陌生人相似度）
4. 综合下载总量和近期热度计算共享文件评分

---

## 九、HBase 数据建模思路（设计说明）

本项目有几处刻意采用的 HBase 建模手法，用于展示"因 HBase 不支持 join、写模型固定、列族稀疏存储"所衍生出的设计权衡：

### 9.1 双表反向索引（群组成员关系）

"用户 ↔ 群组"是多对多关系。HBase 不支持 join，我们把同一份成员关系按两种 RowKey 各存一份：

| 用途 | 表 | RowKey | 前缀扫描的含义 |
|------|----|--------|------|
| 查"群→成员" | `cloud_drive_group_members` | `{group_id}#{username}` | 前缀 `{gid}#` 得到该群全部成员 |
| 查"用户→群" | `cloud_drive_user_groups`   | `{username}#{group_id}` | 前缀 `{user}#` 得到该用户加入的所有群 |

写时双写，删时双删；这是 HBase 反向索引的经典做法。代价是写放大，收益是两个方向都能通过 `scan(row_prefix=...)` 在 O(前缀匹配行数) 内读取，避免了 "scan 全表 + 过滤" 的灾难。

### 9.2 复合 RowKey + 前缀扫描

`{gid}#{username}` 的复合键把"一对多"关系压缩到单行。对比关系数据库：

- 关系型：`SELECT username FROM group_members WHERE group_id = ?` 靠索引支持
- HBase：RowKey 天然按字典序排列，前缀 `{gid}#` 的所有行物理上相邻，直接 `scan(row_prefix=...)` 即可

这也是为什么 RowKey 设计是 HBase 建模的第一优先级。

### 9.3 稀疏列（文件分享元信息）

在 `cloud_drive_files` 的 `meta` 列族下增加了 `is_shared` 和 `shared_groups` 两列。绝大多数私有文件 **不会写入这两列**（HBase 不存储空值，物理上不占空间）。

- 关系型：加两列意味着所有行都要分配存储
- HBase：稀疏列族天然对"少数行才有的字段"友好

### 9.4 写放大与最终一致性

双表反向索引需要双写（加成员两次 put、删成员两次 delete）。HBase 本身不支持多行事务，如果第一次 put 成功、第二次失败，两张索引表就会短暂不一致。

生产级处理方案通常是：
1. 业务层重试 + 幂等检测
2. 引入 WAL/队列做最终一致性对账
3. 重度场景使用 Phoenix / Omid 等 HBase 事务层

本项目规模小，暂时接受这种权衡，但在课程答辩中可作为"大数据存储一致性"的讨论点。

---

## 十、Kafka 事件总线（可选）

为了让操作日志走"应用埋点 → 消息队列 → 多消费者"的标准大数据链路，本项目实现了一个可选的 Kafka 事件总线。**未启用时所有事件会直接同步写 HBase，项目仍可独立运行**。

### 10.1 架构

```
Flask 路由 ──► EventBus.log(username, action, detail)
                    │
                    ├─ KAFKA_ENABLED=0  ──► 直接写 HBase cloud_drive_logs（兜底）
                    │
                    └─ KAFKA_ENABLED=1  ──► Kafka topic: cloud_drive_events
                                                  │
                                                  └─► log_consumer 进程 ─► HBase
```

后续可在同一个 topic 上扩展 Spark Streaming 实时大屏、Flume HDFS 冷归档等多个消费者，实现 Lambda 架构的 speed layer。

### 10.2 启动 Kafka

推荐直接使用完整启动脚本，它会自动拉起 Kafka 容器、Flask producer、日志 consumer 和 Spark Streaming：

```bash
./scripts/start_full.sh
```

如果需要单独调试 Kafka 链路，也可以按下面步骤手动启动：

```bash
# 1. 拉起单节点 Kafka（KRaft 模式，无需 ZK）
docker compose -f docker-compose.kafka.yml up -d

# 2. 启用环境变量
export KAFKA_ENABLED=1
export KAFKA_BOOTSTRAP=localhost:9092

# 3. 启动后端（producer 端）
python run.py

# 4. 另起一个终端启动 consumer（落库到 HBase）
python -m backend.workers.log_consumer
```

### 10.3 关键设计

- **降级而非中断**：producer 初始化失败、send 失败均自动回退到 HBase 直写，请求链路永不报错
- **格式统一**：事件 schema 为 `{username, action, detail, timestamp}` JSON
- **at-least-once**：consumer 启用 `auto_offset_reset=earliest` + 自动 commit，简单期末项目场景不做精确一次
- **水平扩容**：再起一个 consumer 进程加入同一个 group 即可分摊分区

---

## 十一、测试 / 压测 / 数据治理

### 11.1 集成测试（pytest）

覆盖项目核心功能（auth / files / groups / sharing / stats），使用内存版假 HBase / HDFS，**无需启动任何中间件**：

```bash
pip install pytest
python -m pytest tests/ -q
```

当前核对输出：`123 passed, 1 warning in ~2s`。完整说明见 `tests/` 目录下的各 `test_*.py`。

> Kafka / Spark Streaming / MapReduce 依赖外部基础设施，主要通过对应的 `docs/*_USAGE.md` 文档手动验证；核心 Web API 使用 Fake HBase / HDFS 覆盖。

### 11.2 性能压测

`scripts/benchmark.py` 用于对 HTTP 接口做并发压测，统计 QPS / p50/p95/p99 延迟，便于演示 Kafka 启用前后的差异：

```bash
# 后端先起来（python run.py --seed）
python3 scripts/benchmark.py --concurrency 10 --total 200 --label "no-kafka"
# 启用 Kafka 后再跑一次：
KAFKA_ENABLED=1 python3 scripts/benchmark.py --label "with-kafka" --csv-out bench.csv
```

### 11.3 数据生命周期治理

`scripts/data_lifecycle.py` 完成三件事：① 90 天前文件迁移到 `/cloud-drive/cold/`；② 回收站超 30 天彻底删除；③ 给 `cloud_drive_logs` 设置 30 天 TTL：

```bash
# 先 dry-run 看下会动哪些文件
python3 scripts/data_lifecycle.py --dry-run

# 真正执行 + 同时设置日志 TTL
python3 scripts/data_lifecycle.py --apply --set-log-ttl
```

可加入 cron 每天凌晨自动运行。

---

## 十二、整体架构

完整的 Lambda 三层架构（批处理 + 速度 + 服务）说明、各组件职责、典型数据流图，以及答辩开场陈述模板，详见 `docs/ARCHITECTURE.md`。

---

## 十三、注意事项

---

1. **HBase Thrift Server 必须启动**：Python 的 happybase 库通过 Thrift 协议连接 HBase
2. **AI 功能为可选**：如果未配置 AI API，文件摘要功能不可用，但不影响其他功能
3. **Kafka 为可选**：未启用时事件直接写 HBase；启用后需同时运行 consumer 进程
4. **Spark 作业需手动运行**：可以配合 crontab 定时执行
5. **生产部署**：建议使用 Gunicorn + Nginx 部署 Flask 服务
