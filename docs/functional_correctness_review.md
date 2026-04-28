# 功能正确性审查报告

审查日期：2026-04-28

## 审查范围

- Flask 后端：认证、文件管理、回收站、群组分享、统计、AI 推荐接口。
- 服务层：HBase/HDFS 数据访问、统计服务、AI 推荐服务。
- 前端主要调用路径：`frontend/js/app.js` 中的 API 使用方式。
- 离线/辅助组件：Spark 作业、MapReduce 标签索引、数据生命周期脚本。
- 自动化验证：运行 `pytest tests/ -q`。

## 验证结果

现有测试结果：`51 passed, 1 warning in 0.84s`。

说明：测试覆盖了 auth/files/groups/sharing/stats 的主要 happy path，但未覆盖回收站文件下载/预览、预览访问日志、搜索时间范围分页、AI 推荐候选池等边界行为。

## 主要发现

### 1. 高：软删除后的文件仍可被详情、下载、预览接口访问

位置：

- `backend/routes/file_routes.py:21` 的 `_can_access()` 只判断 owner/admin/群组共享，不判断 `deleted == "1"`。
- `backend/routes/file_routes.py:251` 的文件详情、`backend/routes/file_routes.py:268` 的下载、`backend/routes/file_routes.py:462` 的预览都只调用 `_can_access()`。
- `backend/routes/file_routes.py:323` 仅在再次删除时判断文件已在回收站。

影响：

- 文件移入回收站后，只要知道 `file_id`，文件所有者、admin、或仍满足共享条件的成员仍可以读取元数据、下载文件或预览内容。
- 下载回收站文件还会继续执行 `increment_downloads()` 和写入 download 日志，破坏回收站语义与统计口径。
- 前端列表隐藏了回收站文件，但后端接口仍可访问，属于后端状态校验缺口。

建议：

- 在所有读取内容/元数据的路径统一拒绝 `deleted == "1"` 文件，除 `/trash`、`/restore`、`/purge` 等回收站专用接口外返回 404 或 410。
- 更稳妥的做法是在 `_can_access()` 之前增加 `_is_active_file()`，或让 `_can_access(meta, allow_deleted=False)` 默认拒绝回收站文件。
- 补充测试：软删除后 `GET /api/files/<id>`、`GET /download`、`GET /preview` 应失败。

### 2. 中高：预览操作不会写入 preview 日志，最近访问功能与实时统计口径不完整

位置：

- `backend/routes/file_routes.py:170` 的最近访问说明依赖 `download / preview` 日志。
- `backend/routes/file_routes.py:183` 到 `backend/routes/file_routes.py:186` 实际也只聚合 `download` 和 `preview`。
- `backend/routes/file_routes.py:488` 到 `backend/routes/file_routes.py:516` 的预览成功分支直接返回响应，没有写入 `EVENT_BUS.log(..., "preview", file_id)`。

影响：

- 用户只预览、不下载的文件不会出现在“最近访问”。
- Spark Streaming 的实时热门文件逻辑包含 `preview` 动作，但后端从不产出该事件，实时面板数据少算预览行为。

建议：

- 在文本、图片、unsupported 三个预览成功返回前统一记录一次 `preview` 日志。
- 补充测试：预览文件后 `/api/files/recent` 应包含该文件。

### 3. 中：搜索接口先分页后按时间范围过滤，导致结果和 total 不正确

位置：

- `backend/routes/file_routes.py:384` 先调用 `hbase.list_files(... page=page, page_size=page_size)`。
- `backend/routes/file_routes.py:390` 到 `backend/routes/file_routes.py:401` 随后只对当前页结果做 `start_date/end_date` 过滤。

影响：

- 如果符合时间范围的文件不在当前页，接口会返回空或少量结果，即使后续页存在匹配数据。
- `total` 被改成当前页过滤后的数量，不代表全量匹配总数，前端分页会错误。

建议：

- 将时间范围过滤下推到 `HBaseService.list_files()`，在分页前完成所有过滤。
- 同时校验 `start_date/end_date/page/page_size`，避免非法参数触发 500。

### 4. 中：群组 AI 推荐候选池包含用户自己的共享文件

位置：

- `backend/routes/stats_routes.py:173` 到 `backend/routes/stats_routes.py:176` 的注释声明候选池“并排除我自己的文件”。
- `backend/routes/stats_routes.py:194` 到 `backend/routes/stats_routes.py:201` 实际只判断 `is_shared` 与群组交集，没有排除 `f.owner == g.current_user`。
- `backend/routes/stats_routes.py:219`、`backend/routes/stats_routes.py:234`、`backend/routes/stats_routes.py:252` 三个推荐接口都使用该候选池。

影响：

- “群组热门”可能推荐用户自己上传并分享到群组的文件。
- 个性化/相似成员推荐虽然部分服务层会过滤 owner，但入口候选池口径不一致，容易造成不同推荐标签行为不一致。

建议：

- 非 admin 分支构建 candidates 时跳过 `f.get("owner") == g.current_user`。
- 补充测试：用户分享自己的文件到所在群组后，推荐结果不应包含该文件。

### 5. 中：admin 向不存在群组添加成员时会写入孤儿成员关系

位置：

- `backend/routes/group_routes.py:113` 直接用 `_is_owner()` 做权限判断，没有先确认群组存在。
- `_is_owner()` 对 admin 直接返回 True，因此 admin 对不存在的 `group_id` 会继续执行。
- `backend/services/hbase_service.py:413` 到 `backend/services/hbase_service.py:423` 在确认 group 行存在前先写入 `members`、`user_groups`，再对 group 行写 `member_count`。

影响：

- admin 调用 `POST /api/groups/<不存在id>/members` 会创建不完整 group 行或孤儿索引，破坏双表反向索引一致性。
- 普通用户对不存在群组添加成员返回 403，而不是更准确的 404。

建议：

- `add_member()` 开头先 `get_group()`，不存在直接 404。
- `HBaseService.add_group_member()` 也应先确认 groups 表中存在 group 行，再写两张成员索引表。

### 6. 中低：多个查询参数直接 `int()` 转换，非法输入会触发 500

位置示例：

- `backend/routes/file_routes.py:177`、`219`、`378`、`379`
- `backend/routes/stats_routes.py:41`、`106`、`115`、`163`、`219`、`234`、`252`

影响：

- 请求如 `?page=abc`、`?top=x`、`?days=-1` 可能返回 500 或产生无意义数据，而不是 400。
- 前端正常输入不容易触发，但外部 API 调用会降低接口可靠性。

建议：

- 增加统一的 `parse_int_arg(name, default, min_value, max_value)`。
- 非法参数返回 400，并限制最大 `limit/top/page_size`。

### 7. 低：冷热分层脚本迁移前未创建目标 HDFS 目录

位置：

- `scripts/data_lifecycle.py:79` 生成 `/cloud-drive/cold/files/...` 新路径。
- `scripts/data_lifecycle.py:87` 直接执行 `hdfs.client.rename(old_path, new_path)`。

影响：

- 如果 `/cloud-drive/cold/files/<user>` 不存在，rename 可能失败，导致冷数据迁移功能在首次运行时不可用。

建议：

- 迁移前创建目标父目录。
- 增加 dry-run 输出中对目标目录存在性的检查。

## 测试覆盖缺口

建议优先补充以下测试：

- 回收站文件不可详情/下载/预览。
- 预览成功后最近访问出现该文件。
- 搜索时间过滤应在分页前执行，并验证 `total/total_pages`。
- AI 推荐不返回当前用户自己的共享文件。
- admin 添加成员到不存在群组应返回 404，且不写成员索引。
- 查询参数非法时返回 400。

## 总体结论

项目的主流程可运行，现有测试也验证了基础功能路径；但回收站状态、访问日志、搜索分页、推荐候选池和群组索引一致性存在明确功能正确性问题。建议先修复第 1、2、3 项，因为它们直接影响用户可见行为和数据统计可信度。
