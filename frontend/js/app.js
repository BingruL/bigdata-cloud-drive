/**
 * 智能云盘 - 前端应用
 * 基于 Vue 3 + ECharts
 */

const API_BASE = window.location.origin + "/api";

const { createApp, ref, reactive, computed, onMounted, watch, nextTick } = Vue;

const app = createApp({
  setup() {
    // ===== Auth State =====
    const token = ref(localStorage.getItem("cd_token") || "");
    const username = ref(localStorage.getItem("cd_username") || "");
    const userRole = ref(localStorage.getItem("cd_role") || "user");
    const authMode = ref("login");
    const authForm = reactive({ username: "", password: "" });
    const authError = ref("");
    const loading = ref(false);

    // ===== Page State =====
    const currentPage = ref("files");
    const toast = ref(null);

    // ===== File State =====
    const files = ref([]);
    const filePagination = reactive({ page: 1, total: 0, total_pages: 0 });
    const searchKeyword = ref("");
    const filterType = ref("");
    const uploading = ref(false);
    const summaryModal = ref(null);
    const previewModal = ref(null);
    const previewLoading = ref(false);
    const fileViewMode = ref("list"); // "list" 或 "graph"
    const graphData = ref(null);
    const graphLoading = ref(false);
    const selectedGraphFile = ref(null);
    const relatedFiles = ref([]);

    // ===== Dashboard State =====
    const dashboardData = reactive({});

    // ===== Recommend State =====
    const recTab = ref("hot");
    const recommendFiles = ref([]);

    // ===== Logs State =====
    const logs = ref([]);

    // ===== Storage / Recent / Trash State =====
    const storageInfo = reactive({
      used: 0, quota: 0, percent: 0,
      used_readable: "", quota_readable: "",
      active_count: 0, trash_count: 0,
      trash_size: 0, trash_size_readable: "",
    });
    const recentFiles = ref([]);
    const trashFiles = ref([]);

    // ===== Helpers =====
    function showToast(message, type = "info") {
      toast.value = { message, type };
      setTimeout(() => { toast.value = null; }, 3000);
    }

    async function api(path, options = {}) {
      const url = API_BASE + path;
      const headers = { ...(options.headers || {}) };
      if (token.value) headers["Authorization"] = "Bearer " + token.value;
      if (!(options.body instanceof FormData)) headers["Content-Type"] = "application/json";

      try {
        const resp = await fetch(url, { ...options, headers });
        const data = await resp.json();
        if (!resp.ok) {
          if (resp.status === 401) { doLogout(); }
          throw new Error(data.error || "请求失败");
        }
        return data;
      } catch (e) {
        throw e;
      }
    }

    // ===== Auth =====
    async function doLogin() {
      authError.value = "";
      loading.value = true;
      try {
        const data = await api("/auth/login", {
          method: "POST",
          body: JSON.stringify({ username: authForm.username, password: authForm.password }),
        });
        token.value = data.token;
        username.value = data.user.username;
        userRole.value = data.user.role;
        localStorage.setItem("cd_token", data.token);
        localStorage.setItem("cd_username", data.user.username);
        localStorage.setItem("cd_role", data.user.role);
        showToast("登录成功", "success");
        loadFiles();
        loadStorage();
      } catch (e) {
        authError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    async function doRegister() {
      authError.value = "";
      loading.value = true;
      try {
        await api("/auth/register", {
          method: "POST",
          body: JSON.stringify({ username: authForm.username, password: authForm.password }),
        });
        showToast("注册成功，请登录", "success");
        authMode.value = "login";
      } catch (e) {
        authError.value = e.message;
      } finally {
        loading.value = false;
      }
    }

    function doLogout() {
      token.value = "";
      username.value = "";
      userRole.value = "user";
      localStorage.removeItem("cd_token");
      localStorage.removeItem("cd_username");
      localStorage.removeItem("cd_role");
    }

    // ===== Files =====
    async function loadFiles(page = 1) {
      try {
        const params = new URLSearchParams({ page, page_size: 20 });
        if (searchKeyword.value) params.set("keyword", searchKeyword.value);
        if (filterType.value) params.set("type", filterType.value);
        const data = await api("/files/list?" + params.toString());
        files.value = data.files || [];
        filePagination.page = data.page;
        filePagination.total = data.total;
        filePagination.total_pages = data.total_pages;
      } catch (e) {
        showToast("加载文件列表失败: " + e.message, "error");
      }
    }

    function goPage(p) {
      if (p >= 1 && p <= filePagination.total_pages) loadFiles(p);
    }

    async function doUpload(event) {
      const fileList = event.target.files;
      if (!fileList.length) return;
      uploading.value = true;
      try {
        for (const file of fileList) {
          const formData = new FormData();
          formData.append("file", file);
          await api("/files/upload", { method: "POST", body: formData });
        }
        showToast(`${fileList.length} 个文件上传成功`, "success");
        loadFiles();
        loadStorage();
      } catch (e) {
        showToast("上传失败: " + e.message, "error");
      } finally {
        uploading.value = false;
        event.target.value = "";
      }
    }

    async function doDownload(f) {
      try {
        const resp = await fetch(API_BASE + `/files/${f.file_id}/download`, {
          headers: { Authorization: "Bearer " + token.value },
        });
        if (!resp.ok) throw new Error("下载失败");
        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = f.filename;
        a.click();
        URL.revokeObjectURL(url);
        showToast("下载成功", "success");
        // 刷新列表更新下载次数
        setTimeout(() => loadFiles(filePagination.page), 500);
      } catch (e) {
        showToast("下载失败: " + e.message, "error");
      }
    }

    async function doDelete(f) {
      if (!confirm(`将 "${f.filename}" 移至回收站？\n可在"回收站"中恢复或彻底删除。`)) return;
      try {
        await api(`/files/${f.file_id}`, { method: "DELETE" });
        showToast("已移至回收站", "success");
        loadFiles(filePagination.page);
        loadStorage();
      } catch (e) {
        showToast("删除失败: " + e.message, "error");
      }
    }

    async function doGenerateSummary(f) {
      // 如果已有摘要，直接显示
      if (f.summary) {
        summaryModal.value = f;
        return;
      }
      showToast("正在生成 AI 摘要...", "info");
      try {
        const data = await api(`/files/${f.file_id}/summary`, { method: "POST" });
        f.summary = data.summary;
        f.tags = data.tags ? data.tags.join(",") : "";
        summaryModal.value = f;
        showToast("AI 摘要生成完成", "success");
      } catch (e) {
        showToast("摘要生成失败: " + e.message, "error");
      }
    }

    // ===== File Preview =====
    async function doPreview(f) {
      previewLoading.value = true;
      previewModal.value = { filename: f.filename, type: "loading" };
      try {
        const data = await api(`/files/${f.file_id}/preview`);
        previewModal.value = data;
      } catch (e) {
        previewModal.value = { filename: f.filename, type: "unsupported", message: "预览加载失败: " + e.message };
      } finally {
        previewLoading.value = false;
      }
    }

    // ===== File Graph =====
    async function loadFileGraph() {
      graphLoading.value = true;
      try {
        const data = await api("/ai/file-relations?threshold=0.15");
        graphData.value = data;
        await nextTick();
        renderFileGraph(data);
      } catch (e) {
        showToast("加载文件关系图谱失败: " + e.message, "error");
      } finally {
        graphLoading.value = false;
      }
    }

    function renderFileGraph(data) {
      const el = document.getElementById("file-graph");
      if (!el || !data) return;

      const chart = echarts.init(el);

      // 文件类型颜色映射
      const typeColors = {
        pdf: "#e53e3e", txt: "#4f6ef7", md: "#4f6ef7", doc: "#3b5ce4", docx: "#3b5ce4",
        jpg: "#e67e22", jpeg: "#e67e22", png: "#e67e22", gif: "#e67e22", svg: "#e67e22",
        zip: "#805ad5", rar: "#805ad5", "7z": "#805ad5",
        py: "#38a169", java: "#38a169", js: "#38a169", ts: "#38a169", html: "#0d9488", css: "#0d9488",
        csv: "#dd6b20", xlsx: "#dd6b20", xls: "#dd6b20", json: "#d53f8c", xml: "#d53f8c",
        mp3: "#5a67d8", mp4: "#5a67d8", avi: "#5a67d8",
      };

      const categories = [...new Set(data.nodes.map(n => n.type || "other"))];
      const categoryList = categories.map(c => ({
        name: c.toUpperCase(),
        itemStyle: { color: typeColors[c] || "#64748b" },
      }));

      const nodes = data.nodes.map(n => ({
        id: n.id,
        name: n.name,
        symbolSize: Math.min(Math.max(20, Math.sqrt(n.size / 1024) * 2), 60),
        category: categories.indexOf(n.type || "other"),
        itemStyle: { color: typeColors[n.type] || "#64748b" },
        label: {
          show: true,
          formatter: n.name.length > 10 ? n.name.substring(0, 10) + "..." : n.name,
          fontSize: 11,
          color: "#4a5568",
        },
        _raw: n,
      }));

      const edges = data.edges.map(e => ({
        source: e.source,
        target: e.target,
        lineStyle: {
          width: Math.max(1, e.weight * 6),
          opacity: 0.4 + e.weight * 0.4,
          color: "#b0bdd0",
          curveness: 0.15,
        },
      }));

      chart.setOption({
        backgroundColor: "transparent",
        tooltip: {
          trigger: "item",
          formatter: function (params) {
            if (params.dataType === "node") {
              const d = params.data._raw;
              let html = `<div style="font-weight:600;margin-bottom:4px">${d.name}</div>`;
              html += `<div>类型: ${(d.type || "未知").toUpperCase()}</div>`;
              html += `<div>大小: ${formatSize(d.size)}</div>`;
              html += `<div>所有者: ${d.owner}</div>`;
              html += `<div>下载: ${d.downloads} 次</div>`;
              if (d.tags) html += `<div>标签: ${d.tags}</div>`;
              if (d.summary) html += `<div style="max-width:250px;margin-top:4px;color:#718096">${d.summary.substring(0, 80)}...</div>`;
              return html;
            }
            if (params.dataType === "edge") {
              return `关联强度: ${(params.data.lineStyle.width / 6).toFixed(2)}`;
            }
          },
        },
        legend: {
          data: categoryList.map(c => c.name),
          textStyle: { color: "#4a5568", fontSize: 12 },
          bottom: 10,
          selectedMode: "multiple",
        },
        animationDurationUpdate: 500,
        series: [{
          type: "graph",
          layout: "force",
          data: nodes,
          edges: edges,
          categories: categoryList,
          roam: true,
          draggable: true,
          force: {
            repulsion: 300,
            edgeLength: [80, 200],
            gravity: 0.1,
            layoutAnimation: true,
          },
          emphasis: {
            focus: "adjacency",
            lineStyle: { width: 4, color: "#4f6ef7" },
            itemStyle: { shadowBlur: 10, shadowColor: "rgba(79,110,247,0.35)" },
          },
          label: { position: "bottom" },
          lineStyle: { curveness: 0.15 },
        }],
      });

      chart.on("click", function (params) {
        if (params.dataType === "node") {
          selectedGraphFile.value = params.data._raw;
          loadRelatedFiles(params.data.id);
        }
      });
    }

    async function loadRelatedFiles(fileId) {
      try {
        const data = await api(`/ai/related-files/${fileId}?top=10`);
        relatedFiles.value = Array.isArray(data) ? data : [];
      } catch (e) {
        relatedFiles.value = [];
      }
    }

    function switchFileView(mode) {
      fileViewMode.value = mode;
      if (mode === "graph") {
        loadFileGraph();
      }
    }

    // ===== Dashboard =====
    async function loadDashboard() {
      try {
        const summary = await api("/stats/dashboard");
        Object.assign(dashboardData, summary);

        await nextTick();
        await renderCharts();
      } catch (e) {
        showToast("加载看板数据失败: " + e.message, "error");
      }
    }

    async function renderCharts() {
      const chartTheme = {
        backgroundColor: "transparent",
        textStyle: { color: "#4a5568" },
      };

      // 用户文件数
      try {
        const userCounts = await api("/stats/user-file-counts");
        const el = document.getElementById("chart-user-files");
        if (el) {
          const chart = echarts.init(el, null, { renderer: "canvas" });
          chart.setOption({
            ...chartTheme,
            tooltip: { trigger: "axis" },
            xAxis: {
              type: "category",
              data: userCounts.map(i => i.username),
              axisLabel: { color: "#4a5568" },
              axisLine: { lineStyle: { color: "#e5e9f0" } },
            },
            yAxis: {
              type: "value",
              axisLabel: { color: "#a0aec0" },
              splitLine: { lineStyle: { color: "#e5e9f0" } },
            },
            series: [{
              type: "bar",
              data: userCounts.map(i => i.count),
              itemStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                  { offset: 0, color: "#6366f1" },
                  { offset: 1, color: "#4f6ef7" },
                ]),
                borderRadius: [6, 6, 0, 0],
              },
              barWidth: "40%",
            }],
            grid: { left: 40, right: 20, top: 20, bottom: 30 },
          });
        }
      } catch (e) { console.warn("user chart error", e); }

      // 文件类型分布
      try {
        const typeDist = await api("/stats/file-type-distribution");
        const el = document.getElementById("chart-file-types");
        if (el) {
          const chart = echarts.init(el);
          const colors = ["#4f6ef7", "#38a169", "#e67e22", "#805ad5", "#e53e3e", "#0d9488", "#dd6b20", "#d53f8c"];
          chart.setOption({
            ...chartTheme,
            tooltip: { trigger: "item", formatter: "{b}: {c} ({d}%)" },
            series: [{
              type: "pie", radius: ["40%", "70%"],
              data: typeDist.map((i, idx) => ({
                name: i.type.toUpperCase(), value: i.count,
                itemStyle: { color: colors[idx % colors.length] },
              })),
              label: { color: "#4a5568", fontSize: 12 },
              emphasis: { itemStyle: { shadowBlur: 10, shadowColor: "rgba(0,0,0,0.15)" } },
            }],
          });
        }
      } catch (e) { console.warn("type chart error", e); }

      // 日上传趋势
      try {
        const dailyTrend = await api("/stats/daily-upload-trend?days=7");
        const el = document.getElementById("chart-daily-trend");
        if (el) {
          const chart = echarts.init(el);
          chart.setOption({
            ...chartTheme,
            tooltip: { trigger: "axis" },
            xAxis: {
              type: "category",
              data: dailyTrend.map(i => i.date.slice(5)),
              axisLabel: { color: "#4a5568" },
              axisLine: { lineStyle: { color: "#e5e9f0" } },
            },
            yAxis: {
              type: "value", minInterval: 1,
              axisLabel: { color: "#a0aec0" },
              splitLine: { lineStyle: { color: "#e5e9f0" } },
            },
            series: [{
              type: "line", data: dailyTrend.map(i => i.count),
              smooth: true,
              areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: "rgba(56,161,105,0.25)" },
                { offset: 1, color: "rgba(56,161,105,0)" },
              ])},
              lineStyle: { color: "#38a169", width: 2.5 },
              itemStyle: { color: "#38a169" },
            }],
            grid: { left: 40, right: 20, top: 20, bottom: 30 },
          });
        }
      } catch (e) { console.warn("daily chart error", e); }

      // 活跃热力图
      try {
        const heatmapData = await api("/stats/activity-heatmap?days=365");
        const el = document.getElementById("chart-activity-heatmap");
        if (el) {
          const chart = echarts.init(el);

          // 计算日期范围：最近一年
          const now = new Date();
          const yearAgo = new Date(now);
          yearAgo.setFullYear(yearAgo.getFullYear() - 1);
          const rangeStart = yearAgo.toISOString().slice(0, 10);
          const rangeEnd = now.toISOString().slice(0, 10);

          const data = heatmapData.map(d => [d.date, d.count]);
          const maxCount = Math.max(...heatmapData.map(d => d.count), 1);

          chart.setOption({
            ...chartTheme,
            tooltip: {
              formatter: function (params) {
                return params.value[0] + '<br/>操作次数: ' + params.value[1];
              },
            },
            visualMap: {
              min: 0,
              max: maxCount,
              type: "piecewise",
              orient: "horizontal",
              left: "center",
              bottom: 0,
              pieces: [
                { lte: 0, color: "#ebedf0", label: "0" },
                { gt: 0, lte: Math.ceil(maxCount * 0.25), color: "#c6e48b" },
                { gt: Math.ceil(maxCount * 0.25), lte: Math.ceil(maxCount * 0.5), color: "#7bc96f" },
                { gt: Math.ceil(maxCount * 0.5), lte: Math.ceil(maxCount * 0.75), color: "#239a3b" },
                { gt: Math.ceil(maxCount * 0.75), color: "#196127" },
              ],
              textStyle: { color: "#4a5568" },
            },
            calendar: {
              top: 20,
              left: 50,
              right: 30,
              bottom: 40,
              range: [rangeStart, rangeEnd],
              cellSize: ["auto", 15],
              splitLine: { show: false },
              itemStyle: {
                borderWidth: 3,
                borderColor: "#fff",
                borderRadius: 3,
              },
              yearLabel: { show: false },
              monthLabel: {
                color: "#4a5568",
                fontSize: 12,
                nameMap: "ZH",
              },
              dayLabel: {
                color: "#a0aec0",
                fontSize: 10,
                nameMap: "ZH",
                firstDay: 1,
              },
            },
            series: [{
              type: "heatmap",
              coordinateSystem: "calendar",
              data: data,
              itemStyle: { borderRadius: 3 },
            }],
          });
        }
      } catch (e) { console.warn("heatmap chart error", e); }

      // 热门文件
      try {
        const hotFiles = await api("/stats/hot-files?top=10");
        const el = document.getElementById("chart-hot-files");
        if (el) {
          const chart = echarts.init(el);
          const sorted = [...hotFiles].reverse();
          chart.setOption({
            ...chartTheme,
            tooltip: { trigger: "axis" },
            xAxis: {
              type: "value",
              axisLabel: { color: "#a0aec0" },
              splitLine: { lineStyle: { color: "#e5e9f0" } },
            },
            yAxis: {
              type: "category",
              data: sorted.map(i => (i.filename || "").substring(0, 20)),
              axisLabel: { color: "#4a5568", fontSize: 11 },
              axisLine: { lineStyle: { color: "#e5e9f0" } },
            },
            series: [{
              type: "bar",
              data: sorted.map(i => parseInt(i.downloads || i.downloads_int || 0)),
              itemStyle: {
                color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [
                  { offset: 0, color: "#e67e22" },
                  { offset: 1, color: "#f6ad55" },
                ]),
                borderRadius: [0, 6, 6, 0],
              },
              barWidth: "50%",
            }],
            grid: { left: 140, right: 30, top: 10, bottom: 20 },
          });
        }
      } catch (e) { console.warn("hot files chart error", e); }
    }

    // ===== Recommend =====
    async function loadRecommend() {
      try {
        const endpoints = {
          hot: "/ai/recommend/hot?top=10",
          personal: "/ai/recommend/personalized?top=10",
          similar: "/ai/recommend/similar-users?top=10",
        };
        const data = await api(endpoints[recTab.value]);
        recommendFiles.value = Array.isArray(data) ? data : [];
      } catch (e) {
        showToast("加载推荐失败: " + e.message, "error");
        recommendFiles.value = [];
      }
    }

    // ===== Storage quota =====
    async function loadStorage() {
      try {
        const data = await api("/stats/my-storage");
        Object.assign(storageInfo, data);
      } catch (e) { /* silent */ }
    }

    // ===== Recent files =====
    async function loadRecent() {
      try {
        const data = await api("/files/recent?limit=50");
        recentFiles.value = data.files || [];
      } catch (e) {
        showToast("加载最近访问失败: " + e.message, "error");
      }
    }

    // ===== Trash =====
    async function loadTrash() {
      try {
        const data = await api("/files/trash?page=1&page_size=100");
        trashFiles.value = data.files || [];
      } catch (e) {
        showToast("加载回收站失败: " + e.message, "error");
      }
    }

    async function doRestore(f) {
      try {
        await api(`/files/${f.file_id}/restore`, { method: "POST" });
        showToast("文件已恢复", "success");
        loadTrash();
        loadStorage();
      } catch (e) {
        showToast("恢复失败: " + e.message, "error");
      }
    }

    async function doPurge(f) {
      if (!confirm(`确定彻底删除 "${f.filename}" 吗？\n此操作将从 HDFS 永久移除，无法恢复。`)) return;
      try {
        await api(`/files/${f.file_id}/purge`, { method: "DELETE" });
        showToast("文件已彻底删除", "success");
        loadTrash();
        loadStorage();
      } catch (e) {
        showToast("彻底删除失败: " + e.message, "error");
      }
    }

    // ===== Logs =====
    async function loadLogs() {
      try {
        const data = await api("/stats/recent-activity?limit=100");
        logs.value = Array.isArray(data) ? data : [];
      } catch (e) {
        showToast("加载日志失败: " + e.message, "error");
      }
    }

    // ===== Formatters =====
    function formatSize(bytes) {
      bytes = parseInt(bytes || 0);
      if (bytes < 1024) return bytes + " B";
      if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + " KB";
      if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + " MB";
      return (bytes / (1024 * 1024 * 1024)).toFixed(2) + " GB";
    }

    function formatTime(ts) {
      if (!ts) return "—";
      try {
        const d = new Date(parseInt(ts));
        return d.toLocaleString("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit" });
      } catch { return "—"; }
    }

    function getFileIcon(type) {
      const icons = {
        pdf: "📄", txt: "📝", md: "📝", doc: "📃", docx: "📃",
        jpg: "🖼", jpeg: "🖼", png: "🖼", gif: "🖼", svg: "🖼",
        zip: "📦", rar: "📦", "7z": "📦", tar: "📦", gz: "📦",
        py: "🐍", java: "☕", js: "⚡", ts: "⚡", html: "🌐", css: "🎨",
        csv: "📊", xlsx: "📊", xls: "📊", json: "📋", xml: "📋",
        mp3: "🎵", mp4: "🎬", avi: "🎬", wav: "🎵",
      };
      return icons[(type || "").toLowerCase()] || "📁";
    }

    function actionLabel(action) {
      const m = { login: "登录", register: "注册", upload: "上传", download: "下载", delete: "删除" };
      return m[action] || action;
    }

    // ===== Page Watcher =====
    watch(currentPage, (page) => {
      nextTick(() => {
        if (typeof lucide !== "undefined") lucide.createIcons();
      });
      if (page === "files") loadFiles();
      if (page === "recent") loadRecent();
      if (page === "trash") loadTrash();
      if (page === "dashboard") loadDashboard();
      if (page === "recommend") loadRecommend();
      if (page === "logs") loadLogs();
    });

    // ===== Init =====
    onMounted(() => {
      if (token.value) {
        loadFiles();
        loadStorage();
      }
      nextTick(() => {
        if (typeof lucide !== "undefined") lucide.createIcons();
      });
    });

    return {
      token, username, userRole, authMode, authForm, authError, loading,
      currentPage, toast,
      files, filePagination, searchKeyword, filterType, uploading, summaryModal,
      previewModal, previewLoading,
      fileViewMode, graphData, graphLoading, selectedGraphFile, relatedFiles,
      dashboardData,
      recTab, recommendFiles,
      logs,
      storageInfo, recentFiles, trashFiles,
      showToast, doLogin, doRegister, doLogout,
      loadFiles, goPage, doUpload, doDownload, doDelete, doGenerateSummary, doPreview,
      switchFileView, loadRelatedFiles,
      loadRecommend,
      doRestore, doPurge,
      formatSize, formatTime, getFileIcon, actionLabel,
    };
  },
});

app.mount("#app");
