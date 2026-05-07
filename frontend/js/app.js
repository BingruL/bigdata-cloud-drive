/**
 * 智能云盘 - 前端应用
 * 基于 Vue 3 + ECharts
 */

const API_BASE = window.location.origin + "/api";

const { createApp, ref, reactive, computed, onMounted, onUpdated, watch, nextTick } = Vue;

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
    const currentFolderId = ref("root");
    const breadcrumbs = ref([{ folder_id: "root", name: "全部文件" }]);
    const items = ref([]);
    const moveModal = ref(null);
    const renameModal = ref(null);
    const newFolderModal = ref(null);
    const filePagination = reactive({ page: 1, total: 0, total_pages: 0 });
    const searchKeyword = ref("");
    const filterType = ref("");
    const uploading = ref(false);
    const summaryModal = ref(null);
    const previewModal = ref(null);
    const previewLoading = ref(false);
    const fileViewMode = ref("list"); // "list" / "grid" / "graph"
    const gridSortOptions = [
      { k: "filename",   l: "文件名" },
      { k: "type",       l: "类型" },
      { k: "size",       l: "大小" },
      { k: "created_at", l: "上传时间" },
      { k: "downloads",  l: "下载次数" },
    ];
    const graphData = ref(null);
    const graphLoading = ref(false);
    const selectedGraphFile = ref(null);
    const relatedFiles = ref([]);

    // ===== Dashboard State =====
    const dashboardData = reactive({});

    // ===== Realtime（Spark Streaming 实时面板）=====
    const realtimeData = reactive({
      streaming_online: false,
      action_counts: {},
      active_users: { count: 0, users: [] },
      hot_files: [],
      event_stream: [],
      updated_at: null,
    });
    let realtimeTimer = null;

    // ===== Recommend State =====
    const recTab = ref("hot");
    const recommendFiles = ref([]);
    const recommendScope = ref("");

    // ===== Group / Sharing State =====
    const myGroups = ref([]);                // 我所在的群组列表
    const groupDetail = ref(null);           // 当前查看的群组详情（含成员）
    const newGroupForm = reactive({ name: "", description: "" });
    const newMemberName = ref("");
    const sharedFiles = ref([]);             // 群组共享给我的文件
    const shareModal = ref(null);            // { file, selected: Set<gid> }

    // ===== Logs State =====
    const logs = ref([]);

    // ===== Sort / Selection State =====
    const sortKey = ref("created_at");  // filename | type | size | created_at | downloads
    const sortDir = ref("desc");         // "asc" | "desc"
    const selectedIds = ref(new Set());
    const selectedIdsVersion = ref(0);   // bump to trigger computed re-eval on Set mutation

    function toggleSort(key) {
      if (sortKey.value === key) {
        sortDir.value = sortDir.value === "asc" ? "desc" : "asc";
      } else {
        sortKey.value = key;
        sortDir.value = "asc";
      }
    }

    function isFolder(item) {
      return item && (item.item_type === "folder" || (!!item.folder_id && !item.file_id));
    }

    function isFile(item) {
      return item && !isFolder(item);
    }

    function itemId(item) {
      return isFolder(item) ? item.folder_id : item.file_id;
    }

    function itemName(item) {
      if (!item) return "";
      return isFolder(item) ? (item.name || "") : (item.display_name || item.filename || "");
    }

    function visibleBrowseItems() {
      const keyword = (searchKeyword.value || "").trim().toLowerCase();
      const type = (filterType.value || "").trim().toLowerCase();
      return (items.value || []).filter(item => {
        const name = itemName(item).toLowerCase();
        const matchesKeyword = !keyword || name.includes(keyword);
        const matchesType = !type || (isFile(item) && (item.type || "").toLowerCase() === type);
        return matchesKeyword && matchesType;
      });
    }

    function syncVisibleFiles() {
      files.value = visibleBrowseItems().filter(isFile);
    }

    const sortedItems = computed(() => {
      const list = visibleBrowseItems();
      const key = sortKey.value;
      const dir = sortDir.value === "asc" ? 1 : -1;
      list.sort((a, b) => {
        if (isFolder(a) !== isFolder(b)) return isFolder(a) ? -1 : 1;
        let va = key === "filename" ? itemName(a) : a[key];
        let vb = key === "filename" ? itemName(b) : b[key];
        if (key === "size" || key === "downloads" || key === "created_at") {
          va = parseInt(va || 0); vb = parseInt(vb || 0);
        } else {
          va = (va || "").toString().toLowerCase();
          vb = (vb || "").toString().toLowerCase();
        }
        if (va < vb) return -1 * dir;
        if (va > vb) return  1 * dir;
        return 0;
      });
      return list;
    });

    const sortedFiles = computed(() => sortedItems.value.filter(isFile));

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
        const params = new URLSearchParams({ parent_id: currentFolderId.value || "root" });
        const data = await api("/files/browse?" + params.toString());
        items.value = data.items || [];
        const nextBreadcrumbs = normalizeBreadcrumbs(data.breadcrumbs || []);
        breadcrumbs.value = currentFolderId.value !== "root" && nextBreadcrumbs.length <= 1
          ? breadcrumbs.value
          : nextBreadcrumbs;
        syncVisibleFiles();
        filePagination.page = 1;
        filePagination.total = sortedItems.value.length;
        filePagination.total_pages = 1;
      } catch (e) {
        showToast("加载文件列表失败: " + e.message, "error");
      }
    }

    function goPage(p) {
      if (p >= 1 && p <= filePagination.total_pages) loadFiles(p);
    }

    function normalizeBreadcrumbs(raw) {
      const crumbs = Array.isArray(raw) && raw.length ? raw : [{ folder_id: "root", name: "全部文件" }];
      if (crumbs[0].folder_id !== "root") crumbs.unshift({ folder_id: "root", name: "全部文件" });
      return crumbs.map(c => ({ folder_id: c.folder_id || "root", name: c.name || "全部文件" }));
    }

    function openFolder(folder) {
      if (!isFolder(folder)) return;
      currentFolderId.value = folder.folder_id;
      const existing = breadcrumbs.value.findIndex(c => c.folder_id === folder.folder_id);
      if (existing >= 0) {
        breadcrumbs.value = breadcrumbs.value.slice(0, existing + 1);
      } else {
        breadcrumbs.value = [...breadcrumbs.value, { folder_id: folder.folder_id, name: itemName(folder) }];
      }
      clearSelection();
      loadFiles();
    }

    function openBreadcrumb(crumb, idx) {
      currentFolderId.value = crumb.folder_id || "root";
      breadcrumbs.value = breadcrumbs.value.slice(0, idx + 1);
      clearSelection();
      loadFiles();
    }

    async function doUpload(event) {
      const fileList = event.target.files;
      if (!fileList.length) return;
      uploading.value = true;
      try {
        for (const file of fileList) {
          const formData = new FormData();
          formData.append("file", file);
          formData.append("parent_id", currentFolderId.value || "root");
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

    async function doDownload(f, opts = {}) {
      try {
        const resp = await fetch(API_BASE + `/files/${f.file_id}/download`, {
          headers: { Authorization: "Bearer " + token.value },
        });
        if (!resp.ok) throw new Error("下载失败");
        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = itemName(f) || f.filename;
        document.body.appendChild(a);
        a.click();
        a.remove();
        // 推迟回收 blob URL，避免在浏览器写盘前提前释放
        setTimeout(() => URL.revokeObjectURL(url), 5000);
        if (!opts.silent) {
          showToast("已开始下载", "success");
          setTimeout(() => loadFiles(filePagination.page), 500);
        }
      } catch (e) {
        if (!opts.silent) showToast("下载失败: " + e.message, "error");
        throw e;
      }
    }

    async function doDelete(f) {
      const name = itemName(f);
      if (!confirm(`将 "${name}" 移至回收站？\n可在"回收站"中恢复或彻底删除。`)) return;
      try {
        const path = isFolder(f) ? `/folders/${f.folder_id}` : `/files/${f.file_id}`;
        await api(path, { method: "DELETE" });
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
      previewModal.value = { filename: itemName(f), type: "loading" };
      try {
        const data = await api(`/files/${f.file_id}/preview`);
        previewModal.value = data;
      } catch (e) {
        previewModal.value = { filename: itemName(f), type: "unsupported", message: "预览加载失败: " + e.message };
      } finally {
        previewLoading.value = false;
      }
    }

    function openNewFolderModal() {
      newFolderModal.value = { name: "" };
    }

    async function confirmCreateFolder() {
      const name = (newFolderModal.value && newFolderModal.value.name || "").trim();
      if (!name) { showToast("文件夹名称不能为空", "error"); return; }
      try {
        await api("/folders", {
          method: "POST",
          body: JSON.stringify({ name, parent_id: currentFolderId.value || "root" }),
        });
        newFolderModal.value = null;
        showToast("文件夹已创建", "success");
        loadFiles();
      } catch (e) {
        showToast("创建文件夹失败: " + e.message, "error");
      }
    }

    function openRenameModal(item) {
      renameModal.value = { item, name: itemName(item) };
    }

    async function confirmRename() {
      if (!renameModal.value) return;
      const name = (renameModal.value.name || "").trim();
      if (!name) { showToast("名称不能为空", "error"); return; }
      const item = renameModal.value.item;
      const path = isFolder(item) ? `/folders/${item.folder_id}/rename` : `/files/${item.file_id}/rename`;
      try {
        await api(path, { method: "PATCH", body: JSON.stringify({ name }) });
        renameModal.value = null;
        showToast("名称已更新", "success");
        loadFiles();
      } catch (e) {
        showToast("重命名失败: " + e.message, "error");
      }
    }

    function openMoveModal(item) {
      moveModal.value = { item, targetMode: "root", targetId: "" };
    }

    function moveTargetParentId() {
      if (!moveModal.value) return "root";
      if (moveModal.value.targetMode === "current") return currentFolderId.value || "root";
      if (moveModal.value.targetMode === "custom") return (moveModal.value.targetId || "").trim() || "root";
      return "root";
    }

    async function confirmMove() {
      if (!moveModal.value) return;
      const item = moveModal.value.item;
      const parent_id = moveTargetParentId();
      const path = isFolder(item) ? `/folders/${item.folder_id}/move` : `/files/${item.file_id}/move`;
      try {
        await api(path, { method: "PATCH", body: JSON.stringify({ parent_id }) });
        moveModal.value = null;
        showToast("已移动", "success");
        clearSelection();
        loadFiles();
      } catch (e) {
        showToast("移动失败: " + e.message, "error");
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
      const nodeCount = (data.nodes || []).length;
      const edgeCount = (data.edges || []).length;
      const heavy = nodeCount > 120 || edgeCount > 400;

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

      // 统计每个节点的度数（用于按重要性缩放）
      const degree = {};
      (data.edges || []).forEach(e => {
        degree[e.source] = (degree[e.source] || 0) + 1;
        degree[e.target] = (degree[e.target] || 0) + 1;
      });
      const maxDeg = Math.max(1, ...Object.values(degree));

      const nodes = data.nodes.map(n => {
        const d = degree[n.id] || 0;
        const color = typeColors[n.type] || "#64748b";
        // 节点大小：度数 + 文件大小综合
        const sizeBase = 14 + (d / maxDeg) * 32 + Math.min(12, Math.sqrt(n.size / 1024 / 64));
        return {
          id: n.id,
          name: n.name,
          symbolSize: Math.max(12, Math.min(56, sizeBase)),
          category: categories.indexOf(n.type || "other"),
          itemStyle: {
            color,
            borderColor: "rgba(255,255,255,0.85)",
            borderWidth: 1.5,
          },
          label: {
            show: !heavy,
            position: "right",
            formatter: n.name.length > 14 ? n.name.substring(0, 14) + "…" : n.name,
            fontSize: 10,
            color,
            fontWeight: d > maxDeg * 0.5 ? "bold" : "normal",
          },
          _raw: n,
          _degree: d,
        };
      });

      const edges = data.edges.map(e => ({
        source: e.source,
        target: e.target,
        lineStyle: {
          width: Math.max(0.8, e.weight * 3.5),
          opacity: 0.55,
          color: "source",
          curveness: 0,
        },
      }));

      chart.setOption({
        backgroundColor: "transparent",
        animation: !heavy,
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
              html += `<div>关联数: ${params.data._degree}</div>`;
              if (d.tags) html += `<div>标签: ${d.tags}</div>`;
              if (d.summary) html += `<div style="max-width:250px;margin-top:4px;color:#718096">${d.summary.substring(0, 80)}...</div>`;
              return html;
            }
            if (params.dataType === "edge") {
              return `关联强度: ${(params.data.lineStyle.width / 3.5).toFixed(2)}`;
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
          draggable: !heavy,
          // 让力导布局充满整个画布
          left: 30,
          right: 30,
          top: 50,
          bottom: 60,
          force: {
            // 大幅增强斥力，避免节点堆叠
            repulsion: heavy ? 200 : 600,
            edgeLength: heavy ? [30, 80] : [60, 140],
            gravity: 0.08,
            friction: 0.35,
            layoutAnimation: !heavy,
          },
          emphasis: {
            focus: "adjacency",
            scale: true,
            lineStyle: { width: 3, opacity: 0.9 },
            label: { fontWeight: "bold", fontSize: 12 },
            itemStyle: { shadowBlur: 14, shadowColor: "rgba(79,110,247,0.45)" },
          },
          label: { position: "right" },
          lineStyle: { curveness: 0 },
          autoCurveness: false,
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

    // ===== Realtime polling =====
    async function loadRealtime() {
      try {
        const r = await api("/stats/realtime");
        realtimeData.streaming_online = !!r.streaming_online;
        realtimeData.action_counts = r.realtime_action_counts || {};
        realtimeData.active_users = r.realtime_active_users || { count: 0, users: [] };
        realtimeData.hot_files = r.realtime_hot_files || [];
        realtimeData.event_stream = r.realtime_event_stream || [];
        realtimeData.updated_at = r.updated_at || null;
      } catch (_) { /* 静默：streaming 未启动属于正常情况 */ }
    }

    function startRealtimePolling() {
      stopRealtimePolling();
      loadRealtime();
      realtimeTimer = setInterval(loadRealtime, 2000);
    }

    function stopRealtimePolling() {
      if (realtimeTimer) { clearInterval(realtimeTimer); realtimeTimer = null; }
    }

    function realtimeTotalActions() {
      return Object.values(realtimeData.action_counts || {}).reduce((a, b) => a + b, 0);
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
              data: sorted.map(i => (itemName(i) || "").substring(0, 20)),
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
        // 后端新接口返回 {scope, items}；旧版兼容 Array
        if (Array.isArray(data)) {
          recommendFiles.value = data;
          recommendScope.value = "";
        } else {
          recommendFiles.value = data.items || [];
          recommendScope.value = data.scope || "";
        }
      } catch (e) {
        showToast("加载推荐失败: " + e.message, "error");
        recommendFiles.value = [];
        recommendScope.value = "";
      }
    }

    // ===== Groups =====
    async function loadGroups() {
      try {
        const data = await api("/groups");
        myGroups.value = data.groups || [];
      } catch (e) {
        showToast("加载群组失败: " + e.message, "error");
      }
    }

    async function doCreateGroup() {
      const name = (newGroupForm.name || "").trim();
      if (!name) { showToast("群组名称不能为空", "error"); return; }
      try {
        await api("/groups", {
          method: "POST",
          body: JSON.stringify({ name, description: newGroupForm.description || "" }),
        });
        showToast("群组已创建", "success");
        newGroupForm.name = "";
        newGroupForm.description = "";
        loadGroups();
      } catch (e) {
        showToast("创建失败: " + e.message, "error");
      }
    }

    async function openGroupDetail(gid) {
      try {
        groupDetail.value = await api(`/groups/${gid}`);
        newMemberName.value = "";
      } catch (e) {
        showToast("加载群组详情失败: " + e.message, "error");
      }
    }

    async function doAddMember() {
      const u = (newMemberName.value || "").trim();
      if (!u || !groupDetail.value) return;
      try {
        await api(`/groups/${groupDetail.value.group_id}/members`, {
          method: "POST",
          body: JSON.stringify({ username: u }),
        });
        showToast("成员已添加", "success");
        newMemberName.value = "";
        openGroupDetail(groupDetail.value.group_id);
        loadGroups();
      } catch (e) {
        showToast("添加失败: " + e.message, "error");
      }
    }

    async function doRemoveMember(uname) {
      if (!groupDetail.value) return;
      const gid = groupDetail.value.group_id;
      const isSelf = uname === username.value;
      const msg = isSelf ? "确定退出该群组？" : `将 ${uname} 移出群组？`;
      if (!confirm(msg)) return;
      try {
        await api(`/groups/${gid}/members/${encodeURIComponent(uname)}`, { method: "DELETE" });
        showToast(isSelf ? "已退出群组" : "已移除成员", "success");
        if (isSelf) {
          groupDetail.value = null;
          loadGroups();
        } else {
          openGroupDetail(gid);
          loadGroups();
        }
      } catch (e) {
        showToast("操作失败: " + e.message, "error");
      }
    }

    async function doDeleteGroup(gid) {
      if (!confirm("确定解散该群组？所有成员关系将被清理，已分享到该群组的文件会失去对组员的访问权。")) return;
      try {
        await api(`/groups/${gid}`, { method: "DELETE" });
        showToast("群组已解散", "success");
        groupDetail.value = null;
        loadGroups();
      } catch (e) {
        showToast("解散失败: " + e.message, "error");
      }
    }

    function isGroupOwner(g) {
      return g && g.owner === username.value;
    }

    // ===== Sharing =====
    async function loadShared() {
      try {
        const data = await api("/files/shared?page=1&page_size=100");
        sharedFiles.value = data.files || [];
      } catch (e) {
        showToast("加载群组共享失败: " + e.message, "error");
      }
    }

    async function openShareModal(f) {
      // 打开前确保群组列表已加载
      if (!myGroups.value.length) await loadGroups();
      const current = new Set((f.shared_groups || "").split(",").filter(x => x));
      shareModal.value = { file: f, selected: current, version: 0 };
    }

    function toggleShareGroup(gid) {
      if (!shareModal.value) return;
      const s = shareModal.value.selected;
      if (s.has(gid)) s.delete(gid); else s.add(gid);
      shareModal.value.version++;
    }

    function isShareGroupChecked(gid) {
      if (!shareModal.value) return false;
      shareModal.value.version;
      return shareModal.value.selected.has(gid);
    }

    async function confirmShare() {
      if (!shareModal.value) return;
      const groups = [...shareModal.value.selected];
      const fid = shareModal.value.file.file_id;
      try {
        if (groups.length === 0) {
          await api(`/files/${fid}/unshare`, { method: "POST" });
          showToast("已取消分享", "success");
        } else {
          await api(`/files/${fid}/share`, {
            method: "POST",
            body: JSON.stringify({ groups }),
          });
          showToast("分享设置已更新", "success");
        }
        shareModal.value = null;
        loadFiles(filePagination.page);
      } catch (e) {
        showToast("操作失败: " + e.message, "error");
      }
    }

    // ===== Selection helpers =====
    function isSelected(id) {
      selectedIdsVersion.value;  // subscribe
      return selectedIds.value.has(id);
    }
    function toggleSelect(id) {
      if (selectedIds.value.has(id)) selectedIds.value.delete(id);
      else selectedIds.value.add(id);
      selectedIdsVersion.value++;
    }
    function clearSelection() {
      selectedIds.value.clear();
      selectedIdsVersion.value++;
    }
    function toggleSelectAll(list) {
      const ids = list.filter(isFile).map(f => f.file_id);
      const allChosen = ids.every(id => selectedIds.value.has(id));
      if (allChosen) ids.forEach(id => selectedIds.value.delete(id));
      else ids.forEach(id => selectedIds.value.add(id));
      selectedIdsVersion.value++;
    }
    const selectionCount = computed(() => { selectedIdsVersion.value; return selectedIds.value.size; });

    watch([items, searchKeyword, filterType], () => {
      syncVisibleFiles();
      filePagination.total = sortedItems.value.length;
    });

    async function doBatchDelete() {
      const ids = [...selectedIds.value];
      if (!ids.length) return;
      if (!confirm(`将选中的 ${ids.length} 个文件移至回收站？`)) return;
      let ok = 0, fail = 0;
      for (const id of ids) {
        try { await api(`/files/${id}`, { method: "DELETE" }); ok++; }
        catch { fail++; }
      }
      showToast(`已移至回收站 ${ok} 个${fail ? `（失败 ${fail}）` : ""}`, fail ? "error" : "success");
      clearSelection();
      loadFiles(filePagination.page);
      loadStorage();
    }

    async function doBatchDownload() {
      const ids = [...selectedIds.value];
      if (!ids.length) return;
      let ok = 0, fail = 0;
      for (const id of ids) {
        const f = files.value.find(x => x.file_id === id);
        if (!f) continue;
        try { await doDownload(f, { silent: true }); ok++; }
        catch { fail++; }
      }
      const word = ids.length > 1 ? `${ok} 个文件已开始下载` : "已开始下载";
      showToast(word + (fail ? `（失败 ${fail}）` : ""), fail ? "error" : "success");
      clearSelection();
      setTimeout(() => loadFiles(filePagination.page), 500);
    }

    async function doBatchRestore() {
      const ids = [...selectedIds.value];
      if (!ids.length) return;
      let ok = 0, fail = 0;
      for (const id of ids) {
        try { await api(`/files/${id}/restore`, { method: "POST" }); ok++; }
        catch { fail++; }
      }
      showToast(`已恢复 ${ok} 个${fail ? `（失败 ${fail}）` : ""}`, fail ? "error" : "success");
      clearSelection();
      loadTrash();
      loadStorage();
    }

    async function doBatchPurge() {
      const ids = [...selectedIds.value];
      if (!ids.length) return;
      if (!confirm(`彻底删除选中的 ${ids.length} 个文件？此操作不可恢复。`)) return;
      let ok = 0, fail = 0;
      for (const id of ids) {
        try { await api(`/files/${id}/purge`, { method: "DELETE" }); ok++; }
        catch { fail++; }
      }
      showToast(`已彻底删除 ${ok} 个${fail ? `（失败 ${fail}）` : ""}`, fail ? "error" : "success");
      clearSelection();
      loadTrash();
      loadStorage();
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
      if (!confirm(`确定彻底删除 "${itemName(f)}" 吗？\n此操作将从 HDFS 永久移除，无法恢复。`)) return;
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
        pdf: "📕", txt: "📝", md: "📝", rtf: "📝",
        doc: "📘", docx: "📘",
        ppt: "📙", pptx: "📙", key: "📙",
        xls: "📗", xlsx: "📗", csv: "📗", numbers: "📗", tsv: "📗",
        jpg: "🖼", jpeg: "🖼", png: "🖼", gif: "🖼", svg: "🖼", bmp: "🖼", webp: "🖼", ico: "🖼",
        zip: "📦", rar: "📦", "7z": "📦", tar: "📦", gz: "📦", bz2: "📦", xz: "📦",
        py: "🐍", java: "☕", js: "⚡", ts: "⚡", jsx: "⚡", tsx: "⚡",
        html: "🌐", htm: "🌐", css: "🎨", scss: "🎨", less: "🎨",
        c: "🔧", cpp: "🔧", h: "🔧", hpp: "🔧", go: "🐹", rs: "🦀", rb: "💎", php: "🐘", sh: "🖥", bat: "🖥",
        json: "📋", xml: "📋", yaml: "📋", yml: "📋", toml: "📋", ini: "📋",
        sql: "🗄", db: "🗄", sqlite: "🗄",
        mp3: "🎵", wav: "🎵", flac: "🎵", aac: "🎵", ogg: "🎵", m4a: "🎵",
        mp4: "🎬", avi: "🎬", mov: "🎬", mkv: "🎬", webm: "🎬", flv: "🎬",
        ttf: "🔤", otf: "🔤", woff: "🔤", woff2: "🔤",
        exe: "⚙", dll: "⚙", apk: "📱", iso: "💿",
      };
      return icons[(type || "").toLowerCase()] || "📄";
    }

    function actionLabel(action) {
      const m = {
        login: "登录", register: "注册", upload: "上传", download: "下载", delete: "删除",
        share: "分享", unshare: "取消分享", restore: "恢复", purge: "彻底删除",
        group_create: "创建群组", group_delete: "解散群组",
        group_add_member: "加入成员", group_remove_member: "移除成员", group_leave: "退出群组",
        preview: "预览",
      };
      return m[action] || action;
    }

    function recommendScopeLabel(scope) {
      if (scope === "no_group") return "你尚未加入任何群组，无可推荐内容。请先在「我的群组」中创建或加入群组。";
      if (scope === "admin") return "管理员视角：基于全站文件计算";
      if (scope === "group") return "基于你所在群组的共享文件池与成员行为";
      return "";
    }

    // 任何一次 DOM 更新后重新处理 lucide 图标（v-for 列表刷新会插入新的 <i data-lucide="...">）
    onUpdated(() => {
      if (typeof lucide !== "undefined") lucide.createIcons();
    });

    // ===== Page Watcher =====
    watch(currentPage, (page, prev) => {
      clearSelection();
      nextTick(() => {
        if (typeof lucide !== "undefined") lucide.createIcons();
      });
      if (prev === "dashboard" && page !== "dashboard") stopRealtimePolling();
      if (page === "files") loadFiles();
      if (page === "recent") loadRecent();
      if (page === "trash") loadTrash();
      if (page === "dashboard") { loadDashboard(); startRealtimePolling(); }
      if (page === "recommend") loadRecommend();
      if (page === "logs") loadLogs();
      if (page === "groups") { loadGroups(); groupDetail.value = null; }
      if (page === "shared") { loadShared(); loadGroups(); }
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
      files, currentFolderId, breadcrumbs, items, moveModal, renameModal, newFolderModal,
      filePagination, searchKeyword, filterType, uploading, summaryModal,
      previewModal, previewLoading,
      fileViewMode, gridSortOptions, graphData, graphLoading, selectedGraphFile, relatedFiles,
      dashboardData,
      realtimeData, realtimeTotalActions,
      recTab, recommendFiles, recommendScope, recommendScopeLabel,
      myGroups, groupDetail, newGroupForm, newMemberName, sharedFiles, shareModal,
      loadGroups, doCreateGroup, openGroupDetail, doAddMember, doRemoveMember,
      doDeleteGroup, isGroupOwner,
      loadShared, openShareModal, toggleShareGroup, isShareGroupChecked, confirmShare,
      logs,
      storageInfo, recentFiles, trashFiles,
      sortKey, sortDir, toggleSort, sortedFiles, sortedItems,
      isSelected, toggleSelect, toggleSelectAll, clearSelection, selectionCount,
      doBatchDelete, doBatchDownload, doBatchRestore, doBatchPurge,
      showToast, doLogin, doRegister, doLogout,
      loadFiles, goPage, openFolder, openBreadcrumb, itemName, itemId, isFolder, isFile,
      doUpload, doDownload, doDelete, doGenerateSummary, doPreview,
      openNewFolderModal, confirmCreateFolder, openRenameModal, confirmRename, openMoveModal, confirmMove,
      switchFileView, loadRelatedFiles,
      loadRecommend,
      doRestore, doPurge,
      formatSize, formatTime, getFileIcon, actionLabel,
    };
  },
});

app.mount("#app");
