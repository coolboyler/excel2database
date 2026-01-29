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
        if (status === "attempting" || status === "running") return { icon: "⏳", text: "导入中" };
        if (!status) return { icon: "—", text: "未导入" };
        return { icon: "⚠️", text: "未知" };
    }

    function loadCosDailyStatus() {
        const banner = document.getElementById("refresh-banner");
        const textEl = document.getElementById("refresh-status-text");
        const detailEl = document.getElementById("refresh-status-details");
        const dotEl = document.getElementById("refresh-status-dot");
        if (!banner || !textEl || !detailEl || !dotEl) return;

        fetch("/api/cos_daily/status")
            .then(res => res.json())
            .then(data => {
                if (!data || data.status !== "ok") {
                    const msg = data && data.status ? `状态：${data.status}` : "无法获取状态";
                    textEl.textContent = `每日自动更新未运行或无状态文件。${msg}`;
                    const parts = [];
                    Object.keys(COS_TARGET_LABELS).forEach(key => {
                        const label = COS_TARGET_LABELS[key];
                        const statusInfo = normalizeStatus("");
                        parts.push(`${label}：${statusInfo.icon} ${statusInfo.text}`);
                    });
                    detailEl.textContent = parts.join(" ｜ ");
                    dotEl.style.background = "#94a3b8";
                    return;
                }

                const lastSuccess = data.last_success_at ? formatLocalTime(data.last_success_at) : "暂无";
                textEl.textContent = `最近刷新：${lastSuccess}（监控日：${data.day || "未知"}）`;

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
                detailEl.textContent = parts.join(" ｜ ");
                dotEl.style.background = hasFail ? "#ef4444" : (hasDone ? "#22c55e" : "#94a3b8");

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
                detailEl.textContent = "";
                dotEl.style.background = "#94a3b8";
            });
    }

    function setupCosDailyStatus() {
        loadCosDailyStatus();
        setInterval(loadCosDailyStatus, 60000);
    }

    document.addEventListener("DOMContentLoaded", setupCosDailyStatus);
})();
