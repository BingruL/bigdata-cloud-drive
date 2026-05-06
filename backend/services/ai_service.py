"""
AI 服务层
1. 文件摘要与自动标签（调用大语言模型 API）
2. 智能文件推荐（基于下载热度 + 用户偏好）
对应课程第 7 章：大数据分析与挖掘（推荐算法）
"""
import json
import logging
import re
import requests
from collections import Counter, defaultdict

logger = logging.getLogger(__name__)


class AIService:
    """AI 智能分析服务"""

    def __init__(self, api_url, api_key="", model="qwen2.5:7b"):
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self.model = model

    # ========== AI 文件摘要 / 标签 ==========

    def generate_summary(self, text_content, filename=""):
        """
        调用 LLM API 生成文件摘要
        兼容 OpenAI 格式 API（如 Ollama、vLLM、OpenAI）
        """
        if not text_content or len(text_content.strip()) < 50:
            return {"summary": "文件内容过短，无法生成摘要", "tags": []}

        # 截取前 3000 字用于摘要
        content = text_content[:3000]

        prompt = f"""请分析以下文件内容，完成两个任务：
1. 生成一段 100 字以内的中文摘要
2. 给出 3-5 个分类标签（如：技术、财务、合同、报告、笔记、论文等）

文件名：{filename}
文件内容：
{content}

请严格按以下 JSON 格式返回（不要包含其他内容）：
{{"summary": "摘要内容", "tags": ["标签1", "标签2", "标签3"]}}"""

        try:
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            resp = requests.post(
                f"{self.api_url}/chat/completions",
                headers=headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 500,
                },
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            answer = data["choices"][0]["message"]["content"].strip()

            # 尝试解析 JSON
            # 处理可能的 markdown 代码块包裹
            if answer.startswith("```"):
                lines = answer.split("\n")
                answer = "\n".join(lines[1:-1])
            result = json.loads(answer)
            return {
                "summary": result.get("summary", ""),
                "tags": result.get("tags", []),
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"AI API 请求失败: {e}")
            return {"summary": f"AI 服务暂不可用: {str(e)}", "tags": []}
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"AI 返回结果解析失败: {e}")
            return {"summary": "AI 返回格式异常", "tags": []}

    def generate_tags_from_filename(self, filename):
        """
        仅根据文件名推断分类标签（用于无法读取纯文本的二进制文件，
        如 docx / pptx / pdf / xlsx / 图片 / 压缩包等）
        """
        if not filename:
            return {"summary": "", "tags": []}

        prompt = f"""请仅根据以下文件名推断 3-5 个分类标签（如：技术、财务、合同、报告、笔记、论文、课件、图片等）。
不要编造内容摘要，只输出标签。

文件名：{filename}

请严格按以下 JSON 格式返回（不要包含其他内容）：
{{"tags": ["标签1", "标签2", "标签3"]}}"""

        try:
            headers = {"Content-Type": "application/json"}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"

            resp = requests.post(
                f"{self.api_url}/chat/completions",
                headers=headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.3,
                    "max_tokens": 200,
                },
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            answer = data["choices"][0]["message"]["content"].strip()

            if answer.startswith("```"):
                lines = answer.split("\n")
                answer = "\n".join(lines[1:-1])
            result = json.loads(answer)
            return {
                "summary": "",
                "tags": result.get("tags", []),
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"AI API 请求失败: {e}")
            return {"summary": "", "tags": []}
        except (json.JSONDecodeError, KeyError) as e:
            logger.error(f"AI 返回结果解析失败: {e}")
            return {"summary": "", "tags": []}

    # ========== 智能推荐 ==========

    def get_hot_files(self, all_files, top_n=10):
        """
        热门文件推荐
        基于下载次数排序
        """
        files_with_downloads = []
        for f in all_files:
            downloads = int(f.get("downloads", 0))
            files_with_downloads.append({**f, "downloads_int": downloads})

        files_with_downloads.sort(key=lambda x: x["downloads_int"], reverse=True)
        return files_with_downloads[:top_n]

    def get_personalized_recommendations(self, all_files, all_logs,
                                          username, top_n=10):
        """
        个性化推荐
        策略：
        1. 分析用户历史下载的文件类型偏好
        2. 推荐该类型中用户尚未下载的高热度文件
        3. 参考相似用户的行为（简单协同过滤）
        """
        # Step 1: 收集用户的下载记录
        user_downloaded = set()
        user_type_counter = Counter()

        for log in all_logs:
            if log.get("action") == "download" and log.get("username") == username:
                file_id = log.get("detail", "")
                user_downloaded.add(file_id)

        # 从下载的文件中统计类型偏好
        for f in all_files:
            if f["file_id"] in user_downloaded:
                ftype = f.get("type", "other").lower()
                user_type_counter[ftype] += 1

        # Step 2: 找出用户偏好的文件类型
        preferred_types = set()
        if user_type_counter:
            preferred_types = {t for t, _ in user_type_counter.most_common(3)}
        else:
            # 无历史记录，使用全局热门
            return self.get_hot_files(all_files, top_n)

        # Step 3: 推荐用户未下载过的、偏好类型的热门文件
        candidates = []
        for f in all_files:
            if f["file_id"] in user_downloaded:
                continue
            if f.get("owner") == username:
                continue
            ftype = f.get("type", "other").lower()
            downloads = int(f.get("downloads", 0))

            # 偏好类型加权
            score = downloads
            if ftype in preferred_types:
                score *= 2

            candidates.append({**f, "score": score})

        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[:top_n]

    def compute_file_relations(self, all_files, threshold=0.15, max_edges_per_node=8):
        """
        计算文件之间的关联关系（标签 Jaccard 0.5 + 同类型 0.25 + 文件名关键词 Jaccard 0.25）。
        用倒排索引（tag/type/keyword -> file_ids）只对有交集的候选对计算分数，
        避免 N 大时 O(N²) 暴力比较；并对每个节点保留 top-K 条边，
        防止前端力导向图因边数爆炸卡死。
        """
        nodes = []
        edges = []
        _tag_index = defaultdict(list)
        _type_index = defaultdict(list)
        _keyword_index = defaultdict(list)

        # 预处理每个文件的特征
        file_features = []
        for f in all_files:
            tags = set()
            raw_tags = f.get("tags", "")
            if raw_tags:
                tags = {t.strip().lower() for t in raw_tags.split(",") if t.strip()}
            ftype = f.get("type", "").lower()
            filename = f.get("filename", "")
            # 提取文件名关键词（去掉扩展名，按常见分隔符分割）
            name_no_ext = filename.rsplit(".", 1)[0] if "." in filename else filename
            name_keywords = {w.lower() for w in re.split(r'[_\-\s.\u3000]+', name_no_ext) if len(w) >= 2}

            idx = len(file_features)
            file_features.append({
                "file": f,
                "tags": tags,
                "type": ftype,
                "keywords": name_keywords,
            })
            for t in tags:
                _tag_index[t].append(idx)
            if ftype:
                _type_index[ftype].append(idx)
            for kw in name_keywords:
                _keyword_index[kw].append(idx)

            summary_text = f.get("summary", "") or ""
            nodes.append({
                "id": f["file_id"],
                "name": filename,
                "type": ftype,
                "size": int(f.get("size", 0) or 0),
                "owner": f.get("owner", ""),
                "downloads": int(f.get("downloads", 0) or 0),
                "tags": raw_tags,
                "summary": summary_text[:120],
            })

        # 用倒排索引收集候选对，避免 O(N²)。同质组（如全是 pdf）截断到 64 防退化。
        candidate_pairs = set()
        for _index in (_tag_index, _type_index, _keyword_index):
            for ids in _index.values():
                if len(ids) < 2:
                    continue
                ids_capped = ids[:64] if len(ids) > 64 else ids
                for a in range(len(ids_capped)):
                    for b in range(a + 1, len(ids_capped)):
                        x, y = ids_capped[a], ids_capped[b]
                        candidate_pairs.add((x, y) if x < y else (y, x))

        per_node_edges = defaultdict(list)
        for i, j in candidate_pairs:
            fi = file_features[i]
            fj = file_features[j]
            score = 0.0

            if fi["tags"] and fj["tags"]:
                intersection = len(fi["tags"] & fj["tags"])
                union = len(fi["tags"] | fj["tags"])
                if union > 0:
                    score += 0.5 * (intersection / union)

            if fi["type"] and fj["type"] and fi["type"] == fj["type"]:
                score += 0.25

            if fi["keywords"] and fj["keywords"]:
                kw_intersection = len(fi["keywords"] & fj["keywords"])
                kw_union = len(fi["keywords"] | fj["keywords"])
                if kw_union > 0:
                    score += 0.25 * (kw_intersection / kw_union)

            if score >= threshold:
                per_node_edges[i].append((score, j))
                per_node_edges[j].append((score, i))

        # 每个节点保留 top-K 边，避免 hairball 把前端力导向图卡死
        seen = set()
        for i, lst in per_node_edges.items():
            lst.sort(reverse=True)
            for score, j in lst[:max_edges_per_node]:
                key = (i, j) if i < j else (j, i)
                if key in seen:
                    continue
                seen.add(key)
                edges.append({
                    "source": file_features[key[0]]["file"]["file_id"],
                    "target": file_features[key[1]]["file"]["file_id"],
                    "weight": round(score, 3),
                })

        return {"nodes": nodes, "edges": edges}

    def get_related_files(self, all_files, file_id, top_n=10):
        """
        获取与指定文件相关的文件列表
        """
        relations = self.compute_file_relations(all_files, threshold=0.1)
        related = []
        for edge in relations["edges"]:
            if edge["source"] == file_id:
                related.append({"file_id": edge["target"], "weight": edge["weight"]})
            elif edge["target"] == file_id:
                related.append({"file_id": edge["source"], "weight": edge["weight"]})

        related.sort(key=lambda x: x["weight"], reverse=True)

        # 映射回文件信息
        file_map = {f["file_id"]: f for f in all_files}
        results = []
        for r in related[:top_n]:
            if r["file_id"] in file_map:
                results.append({**file_map[r["file_id"]], "relation_score": r["weight"]})
        return results

    def get_similar_users_recommendations(self, all_files, all_logs,
                                           username, top_n=10):
        """
        基于用户相似度的协同过滤推荐
        找到与当前用户下载行为相似的用户，推荐他们下载过但当前用户未下载的文件
        """
        # 构建 用户-文件 下载矩阵
        user_files = defaultdict(set)
        for log in all_logs:
            if log.get("action") == "download":
                user_files[log["username"]].add(log.get("detail", ""))

        current_user_files = user_files.get(username, set())
        if not current_user_files:
            return self.get_hot_files(all_files, top_n)

        # 计算 Jaccard 相似度
        similarity_scores = []
        for other_user, other_files in user_files.items():
            if other_user == username:
                continue
            intersection = len(current_user_files & other_files)
            union = len(current_user_files | other_files)
            if union > 0:
                jaccard = intersection / union
                similarity_scores.append((other_user, jaccard, other_files))

        similarity_scores.sort(key=lambda x: x[1], reverse=True)

        # 从最相似的用户中收集推荐文件
        recommended_ids = Counter()
        for other_user, sim, other_files in similarity_scores[:5]:
            for fid in other_files - current_user_files:
                recommended_ids[fid] += sim

        # 映射回文件信息
        file_map = {f["file_id"]: f for f in all_files}
        results = []
        for fid, score in recommended_ids.most_common(top_n):
            if fid in file_map:
                results.append({**file_map[fid], "recommendation_score": round(score, 3)})

        return results
