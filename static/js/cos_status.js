// static/js/cos_status.js

(function () {
    const COS_TARGET_LABELS = {
        dayahead_node_price: "日前节点电价",
        realtime_node_price: "实时节点电价",
        info_disclose_forecast: "信息披露预测",
        info_disclose_actual: "信息披露实际"
    };

    function extractDateFromKey(key) {
        if (!key) return null;
        const s = String(key);
        const m1 = s.match(/(\d{4}-\d{1,2}-\d{1,2})/);
        if (m1) return m1[1];
        const m2 = s.match(/(\d{8})/);
        if (m2) return `${m2[1].slice(0, 4)}-${m2[1].slice(4, 6)}-${m2[1].slice(6, 8)}`;
        return null;
    }

    function formatLocalTime(isoStr) {
        if (!isoStr) return "未知";
        const d = new Date(isoStr);
        if (isNaN(d.getTime())) return isoStr;
        return d.toLocaleString();
    }

    function showStatusAlert(message) {
        if (typeof window.showAlert === "function") {
            window.showAlert(message, "success");
            return;
        }
        const container = document.body;
        const alert = document.createElement("div");
        alert.className = "alert alert-success";
        alert.style.position = "fixed";
        alert.style.top = "16px";
        alert.style.right = "16px";
        alert.style.zIndex = "1080";
        alert.textContent = message;
        container.appendChild(alert);
        setTimeout(() => {
            alert.style.opacity = "0";
            setTimeout(() => alert.remove(), 300);
        }, 3000);
    }

    function normalizeStatus(rawStatus) {
        const status = String(rawStatus || "").toLowerCase();
        if (status === "done") return { icon: "✅", text: "已完成" };
        if (status === "failed") return { icon: "❌", text: "失败" };
        if (status === "waiting") return { icon: "⏳", text: "等待数据" };
        if (status === "attempting" || status === "running") return { icon: "⏳", text: "导入中" };
        if (!status) return { icon: "—", text: "未导入" };
        return { icon: "⚠️", text: "未知" };
    }

    function loadCosDailyStatus() {
        // 支持两种DOM结构：旧版（文件管理页）和新版（首页）
        const banner = document.getElementById("refresh-banner");
        const textEl =
            document.getElementById("refresh-status-text") ||
            document.getElementById("monitor-status-text");
        const detailEl =
            document.getElementById("refresh-status-details") ||
            document.getElementById("monitor-status-details");
        const dotEl =
            document.getElementById("refresh-status-dot") ||
            document.getElementById("monitor-dot");
        if (!textEl || !dotEl) return;

        // 在加载过程中给新版首页的圆点一个轻微的“读取中”提示
        if (dotEl.classList) {
            dotEl.classList.add("pulsing");
        }

        fetch("/api/cos_daily/status")
            .then(res => res.json())
            .then(data => {
                if (!data || data.status !== "ok") {
                    const statusMsg = data && data.status ? `状态：${data.status}` : "无法获取状态";
                    const extraMsg = data && data.message ? `（${String(data.message)}）` : "";
                    textEl.textContent = `每日自动更新未运行或无状态文件。${statusMsg}${extraMsg}`;
                    const parts = [];
                    Object.keys(COS_TARGET_LABELS).forEach(key => {
                        const label = COS_TARGET_LABELS[key];
                        const statusInfo = normalizeStatus("");
                        parts.push(`${label}：${statusInfo.icon} ${statusInfo.text}`);
                    });
                    if (detailEl) detailEl.textContent = parts.join(" ｜ ");
                    applyDotStatus(dotEl, { level: "neutral" });
                    return;
                }

                const lastSuccess = data.last_success_at ? formatLocalTime(data.last_success_at) : "暂无";
                const note = data && data.enabled === false ? "（自动任务已关闭，仅展示最近状态）" : "";
                textEl.textContent = `最近刷新：${lastSuccess}（监控日：${data.day || "未知"}）${note}`;

                const targets = data.targets || {};
                const parts = [];
                let hasFail = false;
                let hasDone = false;
                Object.keys(COS_TARGET_LABELS).forEach(key => {
                    const label = COS_TARGET_LABELS[key];
                    const info = targets[key] || {};
                    const statusInfo = normalizeStatus(info.status);
                    if (String(info.status).toLowerCase() === "failed") hasFail = true;
                    if (String(info.status).toLowerCase() === "done") hasDone = true;
                    const dateHint = extractDateFromKey(info.key) || "";
                    const suffix = dateHint ? `（${dateHint}）` : "";
                    parts.push(`${label}：${statusInfo.icon} ${statusInfo.text}${suffix}`);
                });
                if (detailEl) detailEl.textContent = parts.join(" ｜ ");
                applyDotStatus(dotEl, { level: hasFail ? "error" : (hasDone ? "ok" : "neutral") });

                const lastNotified = localStorage.getItem("cos_last_success_at");
                if (data.last_success_at && data.last_success_at !== lastNotified) {
                    const targetsText = (data.last_success_targets || []).map(t => COS_TARGET_LABELS[t] || t).join(", ");
                    const toastMsg = targetsText ? `✅ 数据已刷新：${targetsText}` : "✅ 数据已刷新";
                    showStatusAlert(toastMsg);
                    localStorage.setItem("cos_last_success_at", data.last_success_at);
                }
            })
            .catch(() => {
                textEl.textContent = "状态获取失败（网络或服务异常）";
                if (detailEl) detailEl.textContent = "";
                applyDotStatus(dotEl, { level: "neutral" });
            });
    }

    function applyDotStatus(dotEl, { level }) {
        // 新版首页使用 class 控制（active / warning / error），旧版使用内联背景色即可
        const isNewDot = dotEl && dotEl.id === "monitor-dot";
        const color =
            level === "error" ? "#ef4444" :
            level === "ok" ? "#22c55e" :
            "#94a3b8";
        const shadow =
            level === "error" ? "rgba(239, 68, 68, 0.18)" :
            level === "ok" ? "rgba(34, 197, 94, 0.18)" :
            "rgba(148, 163, 184, 0.18)";

        if (!dotEl) return;
        if (dotEl.classList) {
            dotEl.classList.remove("active", "warning", "error", "pulsing");
            if (isNewDot) {
                if (level === "ok") dotEl.classList.add("active");
                else if (level === "error") dotEl.classList.add("error");
            }
        }
        dotEl.style.background = color;
        // 兼容旧版 dot 是 span 的情况
        if (dotEl.style) dotEl.style.background = color;
        if (dotEl.style) dotEl.style.boxShadow = `0 0 0 4px ${shadow}`;
    }

    function setupCosDailyStatus() {
        loadCosDailyStatus();
        setInterval(loadCosDailyStatus, 60000);
    }

    document.addEventListener("DOMContentLoaded", setupCosDailyStatus);
})();
