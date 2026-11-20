"use strict";

const el = (id) => document.getElementById(id);

let manifest = null;
let running = false;

function formatBytes(n) {
  if (n == null) return "—";
  const units = ["B", "KiB", "MiB", "GiB"];
  let value = n;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  const digits = unit === 0 ? 0 : value >= 100 ? 0 : value >= 10 ? 1 : 2;
  return `${value.toFixed(digits)} ${units[unit]} (${n.toLocaleString("zh-CN")} 字节)`;
}

function setProgress(bytes, total, state) {
  const pct = total ? Math.min(100, (bytes / total) * 100) : 0;
  const bar = el("bar-fill");
  bar.style.width = `${pct.toFixed(2)}%`;
  bar.className = `bar-fill${state === "ok" ? " ok" : state === "bad" ? " bad" : ""}`;
  el("stat-bytes").textContent = total
    ? `${bytes.toLocaleString("zh-CN")} / ${total.toLocaleString("zh-CN")}`
    : bytes.toLocaleString("zh-CN");
}

function elapsedLabel(startedAt) {
  if (!startedAt) return "—";
  const ms = performance.now() - startedAt;
  return `${(ms / 1000).toFixed(2)} 秒`;
}

async function loadManifest() {
  const response = await fetch("/api/manifest", { cache: "no-store" });
  if (!response.ok) throw new Error(`manifest HTTP ${response.status}`);
  manifest = await response.json();
  el("package-name").textContent = manifest.name;
  el("pkg-version").textContent = manifest.version;
  el("pkg-size").textContent = formatBytes(manifest.size);
  el("pkg-chunks").textContent =
    `${manifest.chunk_count} 块 × ${manifest.chunk_size / 1024} KiB（末块较短）`;
  el("pkg-sha").textContent = manifest.sha256;
  el("download-link").href = manifest.download_path;
  el("download-link").setAttribute("download", manifest.name);
}

function mirrorCard(mirror) {
  const card = document.createElement("div");
  card.className = "mirror";
  const online = mirror.online
    ? '<span class="badge on">在线</span>'
    : '<span class="badge off">不可达</span>';
  const fault = mirror.fault || { mode: "none", from_chunk: 1 };
  const faulty =
    fault.mode !== "none"
      ? `<span class="badge faulty">${fault.mode} @ 第 ${fault.from_chunk} 块</span>`
      : '<span class="badge">正常</span>';

  card.innerHTML = `
    <h3>${mirror.role === "primary" ? "主镜像" : "备用镜像"} ${online} ${faulty}</h3>
    <p class="muted small mono">${mirror.url}</p>
    ${mirror.online ? "" : `<p class="small" style="color:var(--bad)">${mirror.error || ""}</p>`}
    <label>故障方式
      <select data-role="mode">
        <option value="none"${fault.mode === "none" ? " selected" : ""}>none（正常）</option>
        <option value="truncate"${fault.mode === "truncate" ? " selected" : ""}>truncate（半块断流）</option>
        <option value="corrupt"${fault.mode === "corrupt" ? " selected" : ""}>corrupt（等长坏块）</option>
      </select>
    </label>
    <label>起始分块
      <input type="number" min="0" max="${manifest ? manifest.chunk_count - 1 : 96}" value="${fault.from_chunk}">
    </label>
    <div class="fault-row">
      <button type="button" class="button" data-action="apply">应用故障</button>
      <button type="button" class="button" data-action="clear">恢复正常</button>
    </div>
  `;

  const modeSelect = card.querySelector('select[data-role="mode"]');
  const chunkInput = card.querySelector('label input');
  if (!mirror.online) {
    modeSelect.disabled = true;
    chunkInput.disabled = true;
    card.querySelectorAll("button").forEach((b) => (b.disabled = true));
  }

  card.querySelector('[data-action="apply"]').addEventListener("click", async () => {
    const mode = modeSelect.value;
    const from = Number.parseInt(chunkInput.value, 10) || 0;
    await setFault(mirror.role, mode, from);
  });
  card.querySelector('[data-action="clear"]').addEventListener("click", () => setFault(mirror.role, "none", 1));

  return card;
}

async function loadMirrors() {
  const grid = el("mirror-grid");
  try {
    const response = await fetch("/api/mirrors", { cache: "no-store" });
    const data = await response.json();
    grid.replaceChildren(...data.mirrors.map(mirrorCard));
  } catch (error) {
    grid.innerHTML = `<p style="color:var(--bad)">读取镜像状态失败：${error}</p>`;
  }
}

async function setFault(role, mode, fromChunk) {
  const response = await fetch(`/api/mirrors/${role}/fault`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ mode, from_chunk: fromChunk }),
  });
  if (!response.ok) {
    const body = await response.text();
    el("stat-error").textContent = `设置故障失败：${body}`;
    return;
  }
  await loadMirrors();
  await loadStats();
}

async function loadStats() {
  try {
    const response = await fetch("/api/stats", { cache: "no-store" });
    const stats = await response.json();
    el("stats").textContent = JSON.stringify(stats, null, 2);
  } catch (error) {
    el("stats").textContent = `读取统计失败：${error}`;
  }
}

async function sha256Hex(buffer) {
  if (!crypto.subtle) return null;
  const digest = await crypto.subtle.digest("SHA-256", buffer);
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

async function downloadAndVerify() {
  if (!manifest || running) return;
  running = true;
  const button = el("verify-button");
  button.disabled = true;
  el("stat-error").textContent = "";
  el("stat-result").textContent = "进行中…";
  el("stat-blocks").textContent = "0";
  el("progress-text").textContent = "正在从网关接收数据…";
  el("stat-elapsed").textContent = "—";
  setProgress(0, manifest.size, "run");

  const startedAt = performance.now();
  let received = 0;
  const pieces = [];

  try {
    const response = await fetch(manifest.download_path, { cache: "no-store" });

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      throw new Error(
        `网关返回 HTTP ${response.status}：${body.detail || body.error || "下载被拒绝"}`
      );
    }

    const declared = Number.parseInt(response.headers.get("content-length") || "", 10);
    const downloadId = response.headers.get("x-download-id") || "—";
    el("progress-text").textContent =
      `下载 ID ${downloadId}，声明长度 ${declared.toLocaleString("zh-CN")} 字节…`;

    const reader = response.body.getReader();
    let lastPaint = 0;
    const paintProgress = (state) => {
      setProgress(received, manifest.size, state);
      el("stat-blocks").textContent =
        received === manifest.size
          ? String(manifest.chunk_count)
          : `${Math.floor(received / manifest.chunk_size)} / ${manifest.chunk_count}`;
      el("stat-elapsed").textContent = elapsedLabel(startedAt);
    };
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        pieces.push(value);
        received += value.length;
        const now = performance.now();
        if (now - lastPaint > 100) {
          lastPaint = now;
          paintProgress("run");
        }
      }
    } catch (streamError) {
      // The gateway aborts the body when neither mirror can supply a verified
      // block, so the browser sees a truncated response rather than an EOF.
      throw new Error(
        `连接在 ${received.toLocaleString("zh-CN")} 字节处中断` +
          (Number.isFinite(declared)
            ? `（网关声明 ${declared.toLocaleString("zh-CN")} 字节）`
            : "") +
          `：${streamError.message || streamError}`
      );
    }

    paintProgress("run");

    if (Number.isFinite(declared) && received !== declared) {
      throw new Error(
        `连接在 ${received.toLocaleString("zh-CN")} 字节处提前结束，声明为 ${declared.toLocaleString("zh-CN")} 字节`
      );
    }
    if (received !== manifest.size) {
      throw new Error(
        `收到 ${received.toLocaleString("zh-CN")} 字节，清单要求 ${manifest.size.toLocaleString("zh-CN")} 字节`
      );
    }

    el("progress-text").textContent = "字节数完整，正在核对整包 SHA-256…";
    const payload = new Uint8Array(received);
    let offset = 0;
    for (const piece of pieces) {
      payload.set(piece, offset);
      offset += piece.length;
    }
    const digest = await sha256Hex(payload);
    if (digest !== null && digest !== manifest.sha256) {
      throw new Error(`整包 SHA-256 不符：${digest}`);
    }

    setProgress(received, manifest.size, "ok");
    el("stat-blocks").textContent = String(manifest.chunk_count);
    el("stat-result").textContent = digest
      ? "成功（长度与 SHA-256 均通过）"
      : "成功（长度通过；当前环境无 WebCrypto，未核对 SHA-256）";
    el("progress-text").textContent = "下载完成。";
  } catch (error) {
    setProgress(received, manifest.size, "bad");
    el("stat-result").textContent = "失败";
    el("progress-text").textContent =
      "下载未完整完成：网关未能从两台镜像取得可验证的块，或连接中断。";
    el("stat-error").textContent = String(error.message || error);
  } finally {
    el("stat-elapsed").textContent = elapsedLabel(startedAt);
    button.disabled = false;
    running = false;
    await loadStats();
  }
}

el("verify-button").addEventListener("click", downloadAndVerify);
el("refresh-mirrors").addEventListener("click", loadMirrors);
el("refresh-stats").addEventListener("click", loadStats);

loadManifest().then(loadMirrors).then(loadStats);
