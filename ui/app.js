"use strict";

// ---------- 小工具 ----------

const $ = (sel, el = document) => el.querySelector(sel);

// 所有来自接口的文字都走 textContent（createTextNode），不拼 innerHTML
function h(tag, attrs, ...kids) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v == null || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "style") node.style.cssText = v;
    else if (k.startsWith("on")) node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v === true ? "" : v);
  }
  for (const kid of kids.flat()) {
    if (kid == null || kid === false) continue;
    node.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return node;
}

const DASH = "–";
const fmt = {
  pct: (x, d = 0) => (x == null ? DASH : `${(x * 100).toFixed(d)}%`),
  signedPct: (x, d = 1) => (x == null ? DASH : `${x >= 0 ? "+" : "−"}${Math.abs(x * 100).toFixed(d)}%`),
  p: (x, d = 2) => (x == null ? DASH : Number(x).toFixed(d)),
  brier: (x) => (x == null ? DASH : Number(x).toFixed(3)),
  money(x) {
    if (x == null) return DASH;
    const a = Math.abs(x), sign = x < 0 ? "−" : "";
    if (a >= 1e6) return `${sign}$${(a / 1e6).toFixed(1)}M`;
    if (a >= 1e4) return `${sign}$${(a / 1e3).toFixed(1)}K`;
    return `${sign}$${a.toLocaleString("en-US", { maximumFractionDigits: a < 100 ? 2 : 0 })}`;
  },
  signedMoney: (x) => (x == null ? DASH : (x >= 0 ? "+" : "") + fmt.money(x)),
  int: (x) => (x == null ? DASH : Math.round(x).toLocaleString("en-US")),
  price(x) {
    if (x == null) return DASH;
    const d = x >= 1000 ? 2 : x >= 10 ? 3 : 4;
    return Number(x).toLocaleString("en-US", { minimumFractionDigits: d, maximumFractionDigits: d });
  },
  clock(sec) {
    if (sec == null) return DASH;
    const v = Math.max(0, Math.round(sec));
    return `${Math.floor(v / 60)}:${String(v % 60).padStart(2, "0")}`;
  },
  bp: (x) => (x == null ? DASH : `${x >= 0 ? "+" : "−"}${Math.abs(x).toFixed(1)} bp`),
  time: (ts) => new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false }),
  stamp: (ts) => new Date(ts * 1000).toLocaleString("zh-CN", {
    hour12: false, month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit",
  }),
};
const signClass = (x) => (x == null || x === 0 ? "" : x > 0 ? "pos" : "neg");

async function getJSON(url) {
  const r = await fetch(url, { cache: "no-store" });
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || `HTTP ${r.status}`);
  return body;
}

// ---------- 提示框 ----------

const tip = $("#tooltip");
function showTip(evt, title, rows, anchor) {
  tip.replaceChildren();
  if (title) tip.append(h("div", { class: "tip-title" }, title));
  for (const r of rows) {
    tip.append(h("div", { class: "tip-row" },
      r.color ? h("span", { class: r.kind === "dot" ? "key-dot" : "key-line", style: `background:${r.color}` }) : null,
      h("strong", {}, r.value), h("span", { class: "tip-label" }, r.label)));
  }
  tip.hidden = false;
  let cx = evt && evt.clientX, cy = evt && evt.clientY;
  if (cx == null || (cx === 0 && cy === 0)) {  // 键盘聚焦：贴着元素
    const box = (anchor || evt.target).getBoundingClientRect();
    cx = box.left + box.width / 2; cy = box.top;
  }
  const pad = 14, w = tip.offsetWidth, ht = tip.offsetHeight;
  let x = cx + pad, y = cy + pad;
  if (x + w > innerWidth - 8) x = cx - w - pad;
  if (y + ht > innerHeight - 8) y = cy - ht - pad;
  tip.style.left = `${Math.max(8, x)}px`;
  tip.style.top = `${Math.max(8, y)}px`;
}
function hideTip() { tip.hidden = true; }

// ---------- 通用组件 ----------

function table(head, rows, opts = {}) {
  if (!rows.length) return h("div", { class: "empty" }, opts.empty || "暂无数据");
  const numeric = opts.numeric || [];
  return h("table", {},
    h("thead", {}, h("tr", {}, head.map((t, i) => h("th", { class: numeric[i] ? "num" : null, scope: "col" }, t)))),
    h("tbody", {}, rows.map((row) => h("tr", {}, row.map((cell, i) => {
      const attrs = { class: [numeric[i] ? "num" : "", cell && cell.cls ? cell.cls : ""].join(" ").trim() || null };
      const content = cell && typeof cell === "object" && !(cell instanceof Node) ? cell.v : cell;
      return h("td", attrs, content);
    })))));
}

function legendRow(items) {
  return h("div", { class: "legend" }, items.map((it) => h("span", {},
    h("span", { class: `key-${it.kind || "line"}`, style: `background:${it.color}` }), it.label)));
}

function kpi(label, value, note, cls) {
  return h("div", { class: "kpi" }, h("div", { class: "label" }, label),
    h("div", { class: `value ${cls || ""}` }, value), note ? h("div", { class: "note" }, note) : null);
}

const observers = new WeakMap();
function mount(container, node) {
  for (const old of container.querySelectorAll(".chart")) observers.get(old)?.disconnect();
  container.replaceChildren(node);
}

// 图表卡片：标题、图例、图/表切换。draw(width) 返回图的节点，宽度变化时重画
function chartCard({ title, sub, legend, draw, tableView }) {
  const plot = h("div", { class: "plot" });
  const tableWrap = h("div", { class: "table-wrap", hidden: true }, tableView);
  const toggle = h("button", { class: "ghost small", type: "button", "aria-pressed": "false" }, "表格");
  toggle.addEventListener("click", () => {
    const on = toggle.getAttribute("aria-pressed") !== "true";
    toggle.setAttribute("aria-pressed", String(on));
    toggle.textContent = on ? "图表" : "表格";
    tableWrap.hidden = !on;
    plot.hidden = on;
  });
  const card = h("figure", { class: "card chart" },
    h("div", { class: "card-head" }, h("div", {}, h("h3", {}, title), sub ? h("p", { class: "sub" }, sub) : null), toggle),
    legend ? legendRow(legend) : null, plot, tableWrap);
  const ro = new ResizeObserver(() => {
    const w = Math.floor(plot.clientWidth);
    if (w > 0 && w !== plot._w) { plot._w = w; plot.replaceChildren(draw(w)); }
  });
  ro.observe(plot);
  observers.set(card, ro);
  return card;
}

// 横向条形图（HTML 实现，标签可以省略号截断）
function barList(rows, { valueFmt, tipRows }) {
  const max = Math.max(...rows.map((r) => r.value), 1);
  return h("div", { class: "bars" }, rows.map((r) => {
    const row = h("div", { class: "bar-row", tabindex: 0, "aria-label": `${r.label}：${valueFmt(r.value)}` },
      h("div", { class: "bar-label", title: r.label }, r.label),
      h("div", { class: "bar-track" },
        h("div", { class: "bar", style: `width:${Math.max(0.5, (r.value / max) * 82)}%` }),
        h("span", { class: "bar-value" }, valueFmt(r.value))));
    const rowsForTip = tipRows(r);
    row.addEventListener("pointermove", (e) => showTip(e, r.label, rowsForTip, row));
    row.addEventListener("pointerleave", hideTip);
    row.addEventListener("focus", (e) => showTip(e, r.label, rowsForTip, row));
    row.addEventListener("blur", hideTip);
    return row;
  }));
}

// ---------- 影子交易台 ----------

const VERDICTS = {
  candidate: "候选", no_edge: "没有优势", rules_unclear: "规则不清", dispute_risk: "争议风险",
  too_little_info: "信息不足", not_picked: "没被选中",
};

function renderDesk(d) {
  if (!d.exists) {
    $("#desk-kpis").replaceChildren(h("div", { class: "card empty" }, "还没有 desk.db。先运行 ",
      h("code", {}, ".venv/bin/python main.py"), "。"));
    return;
  }
  const lastAgo = d.last_ts ? (Date.now() / 1000 - d.last_ts) / 60 : null;
  $("#desk-kpis").replaceChildren(
    kpi("虚拟现金", fmt.money(d.cash), `本金 ${fmt.money(d.bank)} · 持仓占用 ${fmt.money(d.locked)}`),
    kpi("已实现盈亏", fmt.signedMoney(d.realized), `已平 ${d.closed.length} 笔`, signClass(d.realized)),
    kpi("持仓", fmt.int(d.open.length), "笔，按真实盘口模拟"),
    kpi("Jev 判断", fmt.int(d.judgments), `花费约 $${d.cost.toFixed(4)}${lastAgo != null ? ` · 最近一次 ${Math.round(lastAgo)} 分钟前` : ""}`),
  );

  // p_jev 存的是第一个结果的概率；买的是第二个结果时换成 1 − p，和“买入”那一列对上
  mount($("#open-positions"), table(["市场", "买入", "股数", "成本价", "公允价", "Jev（买入方）", "投入", "开仓"],
    d.open.map((p) => [{ v: p.question, cls: "q" }, h("span", { class: "pill" }, p.outcome),
      { v: p.shares.toFixed(1), cls: "num" }, { v: fmt.p(p.entry_price, 3), cls: "num" },
      { v: fmt.p(p.fair, 3), cls: "num" }, { v: fmt.p(p.side === 1 ? 1 - p.p_jev : p.p_jev), cls: "num" },
      { v: fmt.money(p.shares * p.entry_price + p.entry_fee), cls: "num" }, { v: fmt.stamp(p.opened_ts), cls: "num" }]),
    { numeric: [false, false, true, true, true, true, true, true], empty: "目前没有持仓" }));

  const verdicts = d.verdicts.map((v) => ({ label: VERDICTS[v.verdict] || v.verdict, value: v.n }));
  mount($("#verdict-card"), chartCard({
    title: "Jev 判断的结论分布",
    sub: "每次判断后，被哪一道关拦下，或者成为候选。",
    draw: () => verdicts.length ? barList(verdicts, {
      valueFmt: fmt.int, tipRows: (v) => [{ value: fmt.int(v.value), label: "次" }],
    }) : h("div", { class: "empty" }, "还没有判断"),
    tableView: table(["结论", "次数"], verdicts.map((v) => [v.label, { v: fmt.int(v.value), cls: "num" }]),
      { numeric: [false, true] }),
  }));

  const cal = d.calibration;
  $("#desk-calibration").replaceChildren(cal.n ? table(["", "Brier"], [
    ["市场中间价", { v: fmt.brier(cal.market), cls: "num" }],
    ["Jev 原始", { v: fmt.brier(cal.jev), cls: "num" }],
    ["收缩后（交易用）", { v: fmt.brier(cal.fair), cls: "num" }],
  ], { numeric: [false, true] }) : h("div", { class: "empty" }, "还没有已结算的市场。市场到期结算后会自动补上结果。"),
  ...(cal.n ? [h("p", { class: "sub" }, `已结算 ${cal.n} 个市场${cal.n < 50 ? "，样本少于 50，先别下结论" : ""}。`)] : []));

  mount($("#recent"), table(["时间", "市场", "市场价", "Jev p", "规则", "争议", "信息", "结论"],
    d.recent.map((j) => [{ v: fmt.stamp(j.ts), cls: "num" }, { v: j.question, cls: "q" },
      { v: fmt.p(j.mid_a, 3), cls: "num" }, { v: fmt.p(j.p_a), cls: "num" }, { v: fmt.p(j.rules_clear), cls: "num" },
      { v: fmt.p(j.dispute_risk), cls: "num" }, { v: fmt.p(j.info, 1), cls: "num" },
      h("span", { class: "pill" }, VERDICTS[j.verdict] || j.verdict)]),
    { numeric: [true, false, true, true, true, true, true, false], empty: "还没有判断" }));

  mount($("#closed-positions"), table(["市场", "买入", "原因", "成本价", "退出价", "盈亏", "平仓"],
    d.closed.map((p) => [{ v: p.question, cls: "q" }, h("span", { class: "pill" }, p.outcome), p.exit_reason,
      { v: fmt.p(p.entry_price, 3), cls: "num" }, { v: fmt.p(p.exit_price, 3), cls: "num" },
      { v: fmt.signedMoney(p.pnl), cls: `num ${signClass(p.pnl)}` }, { v: fmt.stamp(p.closed_ts), cls: "num" }]),
    { numeric: [false, false, false, true, true, true, true], empty: "还没有平仓" }));
}

async function loadDesk() {
  try {
    renderDesk(await getJSON("/api/desk"));
  } catch (e) {
    $("#desk-kpis").replaceChildren(h("div", { class: "card error" }, `读取 desk.db 失败：${e.message}`));
  }
}

// ---------- 主题、轮询 ----------

const themes = ["auto", "light", "dark"];
const themeLabel = { auto: "◐ 自动", light: "☀ 浅色", dark: "☾ 深色" };
function applyTheme(t) {
  if (t === "auto") document.documentElement.removeAttribute("data-theme");
  else document.documentElement.setAttribute("data-theme", t);
  $("#theme").textContent = themeLabel[t];
}
let theme = "auto";
try { theme = localStorage.getItem("jev-theme") || "auto"; } catch (e) { /* 无痕模式等 */ }
applyTheme(theme);
$("#theme").addEventListener("click", () => {
  theme = themes[(themes.indexOf(theme) + 1) % themes.length];
  applyTheme(theme);
  try { localStorage.setItem("jev-theme", theme); } catch (e) { /* ignore */ }
});

loadDesk();
setInterval(() => { if (!document.hidden) loadDesk(); }, 30_000);
