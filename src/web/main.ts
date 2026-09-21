import "./style.css";
import type { CrashPoint, LabStatus } from "../shared/types.js";

const $ = <T extends HTMLElement>(id: string): T => {
  const el = document.getElementById(id);
  if (!el) throw new Error(`缺少元素: ${id}`);
  return el as T;
};

async function api<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`/api${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const json = (await res.json()) as T & { error?: string };
  if (!res.ok) {
    const err = new Error(json.error ?? `HTTP ${res.status}`) as Error & {
      data?: unknown;
    };
    err.data = json;
    throw err;
  }
  return json;
}

let toastTimer: number | undefined;
function toast(message: string, isError = false): void {
  const el = $<HTMLDivElement>("toast");
  el.textContent = message;
  el.classList.remove("hidden", "error");
  if (isError) el.classList.add("error");
  window.clearTimeout(toastTimer);
  toastTimer = window.setTimeout(() => el.classList.add("hidden"), 3500);
}

async function refresh(): Promise<void> {
  try {
    const status = await api<LabStatus>("/status");
    render(status);
  } catch (err) {
    toast(err instanceof Error ? err.message : String(err), true);
  }
}

function render(s: LabStatus): void {
  const stateEl = $<HTMLSpanElement>("run-state");
  const labels: Record<LabStatus["runState"], string> = {
    running: "运行中",
    paused: "已暂停",
    crashed: "已崩溃（等待恢复）",
    needsMigration: "需要显式版本迁移",
  };
  stateEl.textContent = labels[s.runState];
  stateEl.className = `badge ${s.runState}`;
  $<HTMLSpanElement>("proc-version").textContent = `v${s.processorVersion}`;
  $<HTMLSpanElement>("state-hash").textContent = s.stateHash.slice(0, 16);
  $<HTMLSpanElement>("stat-deliveries").textContent = String(s.stats.deliveries);
  $<HTMLSpanElement>("stat-effective").textContent = String(s.stats.effectiveChanges);
  $<HTMLSpanElement>("stat-replayed").textContent = String(s.replayedCommits);
  $<HTMLSpanElement>("stat-recovered").textContent =
    s.recoveredFromSeq === null ? "(无)" : `#${s.recoveredFromSeq}`;

  $<HTMLInputElement>("cfg-every").value = String(s.config.checkpointEvery);
  $<HTMLInputElement>("cfg-interval").value = String(s.config.checkpointIntervalMs);
  $<HTMLDivElement>("migration-box").classList.toggle(
    "hidden",
    s.runState !== "needsMigration",
  );

  const pbody = $<HTMLTableSectionElement>("partitions-body");
  pbody.innerHTML = "";
  for (const p of s.partitions) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${p.id}</td><td>${p.hwm}</td><td>${p.displayedCursor}</td><td>${p.committedOffset}</td>`;
    pbody.appendChild(tr);
  }

  const abody = $<HTMLTableSectionElement>("aggregates-body");
  abody.innerHTML = "";
  for (const a of s.aggregates) {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${a.key}</td><td>${a.sum}</td><td>${a.count}</td><td>${a.sumSq}</td>`;
    abody.appendChild(tr);
  }

  const cpList = $<HTMLUListElement>("checkpoint-list");
  cpList.innerHTML = "";
  for (const cp of s.checkpoints) {
    const li = document.createElement("li");
    li.textContent = cp.valid
      ? `#${cp.seq}  v${cp.processorVersion}  ${cp.createdAt}  (${cp.file})`
      : `${cp.file} —— 损坏/写了一半，恢复时忽略`;
    if (!cp.valid) li.classList.add("invalid");
    cpList.appendChild(li);
  }

  const mList = $<HTMLUListElement>("migration-list");
  mList.innerHTML = "";
  for (const m of s.migrations) {
    const li = document.createElement("li");
    li.textContent = m.ok
      ? `${m.at}  v${m.fromVersion} → v${m.toVersion}  #${m.sourceSeq} → #${m.targetSeq} 成功`
      : `${m.at}  v${m.fromVersion} → v${m.toVersion}  失败：${m.reason ?? ""}（原检查点未改动）`;
    if (!m.ok) li.classList.add("invalid");
    mList.appendChild(li);
  }

  $<HTMLSpanElement>("crash-info").textContent = s.lastCrash
    ? `上次崩溃点：${s.lastCrash.point} @ ${s.lastCrash.at}`
    : "";
}

async function action(path: string, body?: unknown, okText?: string): Promise<void> {
  try {
    await api(path, body ?? {});
    if (okText) toast(okText);
    await refresh();
  } catch (err) {
    const e = err as Error & { data?: { crashed?: boolean; point?: CrashPoint } };
    if (e.data?.crashed) {
      toast(`进程在「${e.data.point}」崩溃——请点击重启恢复`, true);
    } else {
      toast(e.message, true);
    }
    await refresh();
  }
}

$<HTMLButtonElement>("btn-add-partition").addEventListener("click", () =>
  action("/partitions", {}, "已创建分区"),
);
$<HTMLButtonElement>("btn-sample").addEventListener("click", () =>
  action("/sample-events", { perPartition: 3 }, "已填充示例事件"),
);
$<HTMLFormElement>("append-form").addEventListener("submit", (ev) => {
  ev.preventDefault();
  const idem = $<HTMLInputElement>("in-idem").value.trim();
  void action(
    "/events",
    {
      partition: Number($<HTMLInputElement>("in-partition").value),
      key: $<HTMLInputElement>("in-key").value,
      value: Number($<HTMLInputElement>("in-value").value),
      idemKey: idem || undefined,
    },
    "事件已追加",
  );
});
$<HTMLButtonElement>("btn-start").addEventListener("click", () => action("/start", {}));
$<HTMLButtonElement>("btn-pause").addEventListener("click", () => action("/pause", {}));
$<HTMLButtonElement>("btn-step").addEventListener("click", () => action("/step", {}));
$<HTMLButtonElement>("btn-batch").addEventListener("click", () =>
  action("/batch", { count: 5 }),
);
$<HTMLButtonElement>("btn-checkpoint").addEventListener("click", () =>
  action("/checkpoint", {}, "检查点已建立"),
);
$<HTMLButtonElement>("btn-recover").addEventListener("click", () =>
  action("/recover", {}, "已从最后完整检查点恢复"),
);
$<HTMLButtonElement>("btn-migrate-v2").addEventListener("click", () =>
  action("/migrate", { toVersion: 2 }, "迁移成功"),
);
$<HTMLButtonElement>("btn-fail-migrate").addEventListener("click", () =>
  action("/migrate", { toVersion: 99 }, undefined),
);
$<HTMLButtonElement>("btn-force-v1").addEventListener("click", () =>
  action("/force-version", { version: 1 }, "处理器版本已设为 v1（重启/下次检查点生效）"),
);
$<HTMLButtonElement>("btn-force-v2").addEventListener("click", () =>
  action("/force-version", { version: 2 }, "处理器版本已设为 v2（重启/下次检查点生效）"),
);
$<HTMLFormElement>("config-form").addEventListener("submit", (ev) => {
  ev.preventDefault();
  void action(
    "/config",
    {
      checkpointEvery: Number($<HTMLInputElement>("cfg-every").value),
      checkpointIntervalMs: Number($<HTMLInputElement>("cfg-interval").value),
    },
    "配置已保存",
  );
});

document.querySelectorAll<HTMLButtonElement>("button[data-crash]").forEach((btn) => {
  btn.addEventListener("click", () => {
    const point = btn.dataset.crash as CrashPoint;
    void action("/arm-crash", { point }, `已武装崩溃点：${point}`);
  });
});

$<HTMLButtonElement>("btn-export").addEventListener("click", async () => {
  try {
    const bundle = await api<unknown>("/export");
    const blob = new Blob([JSON.stringify(bundle, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `checkpoint-lab-export-${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(url);
    toast("实验已导出");
    await refresh();
  } catch (err) {
    toast(err instanceof Error ? err.message : String(err), true);
  }
});

$<HTMLButtonElement>("btn-reset").addEventListener("click", () => {
  if (window.confirm("确定清空所有持久化数据？")) {
    void action("/reset", {}, "实例已重置");
  }
});

void refresh();
window.setInterval(() => void refresh(), 1000);
