'use strict';
/* renderer.js — Chimera 桌面端渲染进程。
 * 实时规则（v0.2.1 性能优化后）：
 *  - 新消息增量追加：WS 直播 group_message 自带 body，直接把这一条 append
 *    进当前打开的群聊；无 body 的（断线补发 / SSE 路）用 ?since=lastTs 只拉
 *    新消息追加；不再每条消息全量重拉 100 条 + 全量 innerHTML 重渲染。
 *  - 未读徽章：unread 事件 / receipt ack 带全量计数时校准；其他群的新消息
 *    本地 +1；打开群聊时发 receipt 清掉该群未读。
 *  - loadAll 只在登录 / 启动时跑，实时事件不再触发它（原来每事件约 390ms、
 *    3 个 HTTP 请求；现在事件只改自己那一项）。
 *  - 消息窗口有界（v0.2.1+1）：聊天区 DOM 最多留 300 条。打开会话初拉 100 条；
 *    有更早消息时顶上出现“加载更早的消息”，点一次用 ?before_id= 往前翻一页
 *    （服务端 paginate 支持 before_id + limit，群聊与私聊同口径）；追加新消息
 *    超出上限时从顶上裁掉最老的节点与 id（长会话 DOM 不再无界长大）。
 *    滚动补偿：前翻时按插入高度调 scrollTop，视图不动；新消息只在贴底时才
 *    自动滚到底，正在往上翻旧消息时不抢滚动位置。
 *  - 私聊初拉也带 limit:100：此前 dmHistory 不带 limit，服务端 paginate 无
 *    limit 时全量返回，长私聊会在打开时全量 innerHTML 重渲染。
 *  - 打开群聊即清本地未读徽章（此前要等第一条实时事件 seq 到才清）。
 * 断线重连靠主进程的 RealtimeClient（WS 优先、SSE 回落，lastSeq 续接）。
 * 不轮询（节点面板 5 秒定时器已删，节点状态由 node:state / node:sync 事件推）。 */
const $ = (id) => document.getElementById(id);

// 消息窗口：DOM 里最多留多少条消息；每页向前翻多少条（服务端 paginate
// 的 limit 上限就是 100，再大也会被截到 100）。
const MAX_RENDERED = 300;
const PAGE_SIZE = 100;
// 贴底判定：离底部不到这么多像素就认为在看最新消息，新消息到时自动滚到底。
const NEAR_BOTTOM_PX = 120;

const state = {
  me: null,
  groups: [],
  dms: [],
  accounts: [],
  tab: 'groups',
  open: null, // { kind: 'group'|'dm', id, name }
  lastTs: null,
  lastSeq: null, // 最近一个实时事件的 seq（发 receipt 清未读用）
  renderedIds: new Set(), // 当前打开会话已渲染的消息 id（增量去重）
  unread: {}, // gid -> 未读数（本地维护，unread 事件 / receipt ack 校准）
  pending: [], // { name, base64 } 待发送图片
  nodeStatus: null,
  hasMoreOlder: false, // 顶上“加载更早的消息”是否可点（服务端 has_more）
  loadingOlder: false, // 正在向前翻页
};

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

// 视觉小件：头像按名字哈希取 6 档渐变，时间只显示 HH:MM / MM-DD HH:MM。
function avatarClass(name) {
  let h = 0; const s = String(name || '?');
  for (const ch of s) h = (h * 31 + ch.codePointAt(0)) >>> 0;
  return 'av-' + (h % 6);
}
function avatarInitial(name) {
  const s = String(name || '?').trim();
  return s ? s[0].toUpperCase() : '?';
}
function avatarHtml(name, extra) {
  return `<span class="avatar ${extra || ''} ${avatarClass(name)}">${esc(avatarInitial(name))}</span>`;
}
function fmtTime(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return String(ts);
  const now = new Date();
  const hhmm = String(d.getHours()).padStart(2, '0') + ':' + String(d.getMinutes()).padStart(2, '0');
  const sameDay = d.toDateString() === now.toDateString();
  if (sameDay) return hhmm;
  return (d.getMonth() + 1) + '-' + d.getDate() + ' ' + hhmm;
}
function setMeChip() {
  const av = $('whoamiAvatar'); const chip = $('whoami');
  if (!av || !chip) return;
  if (state.me) {
    av.textContent = avatarInitial(state.me.username);
    av.className = 'avatar avatar-sm ' + avatarClass(state.me.username);
    av.classList.remove('hidden');
    chip.classList.remove('hidden');
  } else {
    av.classList.add('hidden');
    chip.classList.add('hidden');
  }
}

function show(view) {
  $('view-login').classList.toggle('hidden', view !== 'login');
  $('view-main').classList.toggle('hidden', view !== 'main');
}

// 传输状态指示器（v0.2.1）：只按主进程推来的 realtime:transport 画，
// state ∈ connecting | ws | sse | disconnected | session-expired。
// 颜色：WS 绿、SSE 琥珀、连接中灰、断开/会话失效红。
const TRANSPORT_UI = {
  connecting: { cls: 'off', label: '连接中…' },
  ws: { cls: 'on', label: 'WS 已连接' },
  sse: { cls: 'warn', label: 'SSE 回落' },
  disconnected: { cls: 'bad', label: '已断开' },
  'session-expired': { cls: 'bad', label: '会话失效，请重新登录' },
};
function setTransport(state, detail) {
  const ui = TRANSPORT_UI[state] || TRANSPORT_UI.disconnected;
  $('sseDot').className = 'dot ' + ui.cls;
  $('sseText').textContent = detail || ui.label;
}

// ---------- session ----------
async function boot() {
  const sess = await window.chimera.session.get();
  if (sess) {
    state.me = { username: sess.username };
    $('whoami').textContent = sess.username;
    setMeChip();
    $('logoutBtn').classList.remove('hidden');
    show('main');
    await loadAll();
  } else {
    show('login');
  }
}

$('loginBtn').addEventListener('click', async () => {
  const username = $('loginUser').value.trim();
  const password = $('loginPass').value;
  $('loginErr').textContent = '';
  if (!username || !password) { $('loginErr').textContent = '请填写用户名和密码'; return; }
  try {
    const r = await window.chimera.session.login(username, password);
    state.me = { username: r.username };
    $('whoami').textContent = r.username;
    setMeChip();
    $('logoutBtn').classList.remove('hidden');
    show('main');
    await loadAll();
  } catch (err) {
    $('loginErr').textContent = '登录失败：' + (err.message || err);
  }
});
$('loginPass').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('loginBtn').click(); });

$('logoutBtn').addEventListener('click', async () => {
  await window.chimera.session.logout();
  state.me = null; state.open = null;
  $('whoami').textContent = '';
  setMeChip();
  $('logoutBtn').classList.add('hidden');
  show('login');
});

// ---------- nav ----------
document.querySelectorAll('.nav-btn').forEach((b) => {
  b.addEventListener('click', () => {
    document.querySelectorAll('.nav-btn').forEach((x) => x.classList.remove('active'));
    b.classList.add('active');
    const v = b.dataset.view;
    $('pane-conv').classList.toggle('hidden', v !== 'conv');
    $('pane-posts').classList.toggle('hidden', v !== 'posts');
    $('pane-node').classList.toggle('hidden', v !== 'node');
    if (v === 'posts') loadPosts();
    if (v === 'node') refreshNodePanel();
  });
});

// ---------- conv list ----------
$('tabGroups').addEventListener('click', () => { state.tab = 'groups'; syncTabs(); renderConvList(); });
$('tabFriends').addEventListener('click', () => { state.tab = 'friends'; syncTabs(); renderConvList(); });
$('convSearch').addEventListener('input', renderConvList);

function syncTabs() {
  $('tabGroups').classList.toggle('active', state.tab === 'groups');
  $('tabFriends').classList.toggle('active', state.tab === 'friends');
}

async function loadAll() {
  try {
    const [groups, accounts] = await Promise.all([
      window.chimera.api.groups(),
      window.chimera.api.accounts().catch(() => ({ accounts: [] })),
    ]);
    state.groups = groups.groups || groups || [];
    state.accounts = accounts.accounts || [];
    try { state.dms = (await window.chimera.api.dms()).dms || []; } catch { state.dms = []; }
    renderConvList();
  } catch (err) {
    $('convItems').innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

function renderConvList() {
  const q = $('convSearch').value.trim().toLowerCase();
  const box = $('convItems');
  if (state.tab === 'groups') {
    const list = state.groups.filter((g) => !q || (g.name || '').toLowerCase().includes(q));
    box.innerHTML = list.map((g) => `
      <div class="item" data-kind="group" data-id="${esc(g.id)}">
        ${avatarHtml(g.name)}
        <div class="item-main"><div class="item-name"><b>${esc(g.name)}</b><span class="unread" data-unread="${esc(g.id)}"></span></div>
        <span class="item-sub">${g.kind === 'chat' ? '聊天群' : esc(g.kind || '')}${g.private ? ' · 私密' : ''}</span></div>
      </div>`).join('') || '<div class="empty-note">没有群组</div>';
  } else {
    const humans = state.accounts.filter((a) => a.type === 'human' && a.name !== state.me?.username);
    const ais = state.accounts.filter((a) => a.type === 'ai');
    const row = (a) => `
      <div class="item" data-kind="dm" data-id="${esc(a.name)}">
        ${avatarHtml(a.name)}
        <div class="item-main"><div class="item-name"><b>${esc(a.name)}</b></div>
        <span class="item-sub">${a.type === 'human' ? '人类' : 'AI'}${a.online ? ' · 在线' : ''}</span></div>
      </div>`;
    const all = [...humans, ...ais].filter((a) => !q || (a.name || '').toLowerCase().includes(q));
    box.innerHTML = (all.map(row).join('')) || '<div class="empty-note">没有好友</div>';
  }
  box.querySelectorAll('.item').forEach((el) => {
    el.addEventListener('click', () => openConv(el.dataset.kind, el.dataset.id));
    if (state.open) el.classList.toggle('active', el.dataset.kind === state.open.kind && el.dataset.id === state.open.id);
  });
  paintUnreadBadges();
}

// ---------- unread badges ----------
// 服务端 unread 事件 / receipt ack 的口径：{ groups: { gid: n }, total, mentions }。
// 全量到达时校准本地计数；其他群的新消息只本地 +1（下次全量到达再校准）。
function paintUnreadBadges() {
  document.querySelectorAll('[data-unread]').forEach((el) => {
    const n = Number(state.unread[el.dataset.unread] || 0);
    el.textContent = n > 0 ? (n > 99 ? '99+' : String(n)) : '';
  });
}

function applyUnread(unread) {
  if (!unread || typeof unread !== 'object') return;
  state.unread = { ...(unread.groups || {}) };
  paintUnreadBadges();
}

function bumpUnread(gid, delta) {
  const n = Math.max(0, Number(state.unread[gid] || 0) + delta);
  state.unread[gid] = n;
  paintUnreadBadges();
}

// 打开群聊即清未读：本地徽章先清（打开就代表已读）；WS 路走 receipt
// （回 ack 带全量 unread），SSE 回落时 sendReceipt 返回 false，改走 REST
// POST /api/read。receipt 发不出去时下次 unread 事件再校准，不挡阅读。
async function markGroupRead(gid) {
  if (state.unread[gid]) { state.unread[gid] = 0; paintUnreadBadges(); }
  if (!Number.isFinite(state.lastSeq)) return;
  const seq = state.lastSeq;
  try {
    const viaWs = await window.chimera.realtime.sendReceipt({ groupId: gid, seq });
    if (viaWs === false) {
      const r = await window.chimera.api.read(gid, seq);
      if (r && r.unread) applyUnread(r.unread);
    }
  } catch { /* 已读上报失败不挡阅读；下次 unread 事件校准 */ }
}

async function openConv(kind, id) {
  const name = kind === 'group'
    ? (state.groups.find((g) => g.id === id)?.name || id)
    : id;
  state.open = { kind, id, name };
  state.lastTs = null;
  state.renderedIds = new Set();
  state.hasMoreOlder = false;
  state.loadingOlder = false;
  syncOlderButton();
  const ca = $('chatAvatar');
  if (ca) { ca.textContent = avatarInitial(name); ca.className = 'avatar ' + avatarClass(name); ca.classList.remove('hidden'); }
  document.querySelectorAll('#convItems .item').forEach((el) => {
    el.classList.toggle('active', el.dataset.kind === kind && el.dataset.id === id);
  });
  $('chatTitle').textContent = name;
  $('chatMeta').textContent = kind === 'group' ? '群聊' : '私聊';
  $('chatInputRow').classList.remove('hidden');
  $('chatBox').innerHTML = '<div class="muted">加载中…</div>';
  await loadMessages();
  if (kind === 'group') markGroupRead(id);
}

async function loadMessages() {
  if (!state.open) return;
  try {
    let data;
    if (state.open.kind === 'group') {
      data = await window.chimera.api.groupMessages(state.open.id, { limit: PAGE_SIZE });
      renderMessages(data.messages || data || []);
    } else {
      // 私聊也要带 limit：服务端 paginate 无 limit 时全量返回，长私聊
      // 打开时会把全部历史一次 innerHTML 重渲染。
      data = await window.chimera.api.dmHistory(state.open.id, { limit: PAGE_SIZE });
      renderMessages(data.messages || data || []);
    }
    state.hasMoreOlder = !!(data && data.has_more);
    syncOlderButton();
  } catch (err) {
    state.hasMoreOlder = false;
    syncOlderButton();
    $('chatBox').innerHTML = `<div class="muted">加载失败：${esc(err.message || err)}</div>`;
  }
}

// 向前翻一页：用最老一条的 id 当 before_id，服务端只回它之前的 PAGE_SIZE 条。
// 显式按钮触发（不做滚到顶自动翻，免得误触与滚动跳动）；翻页时按插入高度
// 补偿 scrollTop，读到的位置不动。已到 DOM 上限时不再提供翻页。
async function loadOlderMessages() {
  if (!state.open || state.loadingOlder || !state.hasMoreOlder) return;
  const box = $('chatBox');
  const first = box.querySelector('.msg');
  const oldestId = first && first.dataset ? first.dataset.id : null;
  if (!oldestId) return;
  if (box.querySelectorAll('.msg').length >= MAX_RENDERED) {
    state.hasMoreOlder = false;
    syncOlderButton();
    return;
  }
  state.loadingOlder = true;
  syncOlderButton();
  try {
    let data;
    if (state.open.kind === 'group') {
      data = await window.chimera.api.groupMessages(state.open.id, { before_id: oldestId, limit: PAGE_SIZE });
    } else {
      data = await window.chimera.api.dmHistory(state.open.id, { before_id: oldestId, limit: PAGE_SIZE });
    }
    const older = data.messages || data || [];
    if (!older.length) {
      state.hasMoreOlder = false;
    } else {
      prependMessages(older);
      state.hasMoreOlder = !!(data && data.has_more);
    }
  } catch { /* 翻页失败不清空已有内容，按钮还在，下次再点 */ }
  finally {
    state.loadingOlder = false;
    syncOlderButton();
  }
}

function syncOlderButton() {
  const row = $('chatOlderRow');
  const btn = $('chatOlderBtn');
  if (!row || !btn) return;
  const show = !!state.open && (state.hasMoreOlder || state.loadingOlder);
  row.classList.toggle('hidden', !show);
  btn.disabled = state.loadingOlder || !state.hasMoreOlder;
  btn.textContent = state.loadingOlder ? '加载中…' : '加载更早的消息';
}

$('chatOlderBtn').addEventListener('click', loadOlderMessages);

// 只拉上次时间戳之后的新消息（服务端 ?since= 按 ts > 过滤），追加进当前视图。
// fullFallback 为真且一无所获时全量重拉一次（防两条消息 ts 撞到同一毫秒被漏掉）。
async function fetchNewMessages(fullFallback) {
  if (!state.open) return false;
  try {
    let data;
    if (state.open.kind === 'group') {
      data = await window.chimera.api.groupMessages(state.open.id, { since: state.lastTs || undefined });
    } else {
      data = await window.chimera.api.dmHistory(state.open.id, { since: state.lastTs || undefined });
    }
    const msgs = data.messages || data || [];
    if (!msgs.length) {
      if (fullFallback) await loadMessages();
      return false;
    }
    appendMessages(msgs);
    return true;
  } catch (err) {
    if (fullFallback) { try { await loadMessages(); } catch { /* 全量重拉也失败就等下次事件 */ } }
    return false;
  }
}

function messageHtml(m) {
  const atts = (m.attachments || []).map((a) =>
    `<a href="${esc(a.url)}" target="_blank"><img class="thumb" src="${esc(a.url)}" alt="图片"></a>`).join('');
  const author = m.author || m.from || '?';
  const mine = !!state.me && author === state.me.username;
  // data-id 供窗口裁剪与向前翻页（before_id）找最老一条用。
  return `<div class="msg${mine ? ' mine' : ''}" data-id="${esc(m.id || '')}">${avatarHtml(author)}<div class="bubble"><div class="msg-head"><b>${esc(author)}</b><time>${esc(fmtTime(m.ts))}</time></div><div class="msg-body">${esc(m.body || '')}</div>${atts}</div></div>`;
}

function renderMessages(msgs) {
  const box = $('chatBox');
  // 防御性上限：服务端万一回得比窗口大，只渲染最新的 MAX_RENDERED 条，
  // 其余算“更早的消息”，顶上按钮可翻。
  const list = msgs.length > MAX_RENDERED ? msgs.slice(msgs.length - MAX_RENDERED) : msgs;
  if (msgs.length > MAX_RENDERED) state.hasMoreOlder = true;
  box.innerHTML = list.map((m) => messageHtml(m)).join('') || '<div class="chat-empty" data-empty="1"><img src="assets/brand.png" alt="奇美拉"><b>暂无消息</b><span>发第一条消息，打个招呼吧。</span></div>';
  state.renderedIds = new Set(list.map((m) => m.id).filter(Boolean));
  box.scrollTop = box.scrollHeight;
  if (list.length) state.lastTs = list[list.length - 1].ts;
}

// 增量追加：按 id 去重（自己发的消息回声事件与发送后拉取可能撞上同一条）。
// 只在贴底时自动滚到底；正在往上翻旧消息时不抢滚动位置。
// DOM 上限：追加后超出 MAX_RENDERED 时从顶上裁掉最老的节点与 id，
// 长会话里 DOM 与 renderedIds 不再无界长大。
function appendMessages(msgs) {
  const box = $('chatBox');
  const empty = box.querySelector('[data-empty]');
  if (empty) empty.remove();
  const nearBottom = box.scrollHeight - box.scrollTop - box.clientHeight < NEAR_BOTTOM_PX;
  let added = false;
  for (const m of msgs) {
    const id = m.id;
    if (id && state.renderedIds.has(id)) continue;
    if (id) state.renderedIds.add(id);
    box.insertAdjacentHTML('beforeend', messageHtml(m));
    if (m.ts) state.lastTs = m.ts; // 消息按时间正序，最后一条即最新水位
    added = true;
  }
  if (added) {
    let over = box.querySelectorAll('.msg').length - MAX_RENDERED;
    while (over > 0) {
      const first = box.querySelector('.msg');
      if (!first) break;
      if (first.dataset && first.dataset.id) state.renderedIds.delete(first.dataset.id);
      first.remove();
      over -= 1;
    }
    if (nearBottom) box.scrollTop = box.scrollHeight;
  }
}

// 向前追加（加载更早的消息）：插到顶上，按插入高度补偿 scrollTop，
// 用户读到的位置不动；只补 id，不动 lastTs（水位永远是最新一条）。
function prependMessages(msgs) {
  const box = $('chatBox');
  const empty = box.querySelector('[data-empty]');
  if (empty) empty.remove();
  const beforeHeight = box.scrollHeight;
  box.insertAdjacentHTML('afterbegin', msgs.map((m) => messageHtml(m)).join(''));
  for (const m of msgs) if (m.id) state.renderedIds.add(m.id);
  box.scrollTop += box.scrollHeight - beforeHeight;
}

// WS 直播事件自带 body/attachments（服务端 wsExtra），直接拼成消息对象追加。
function messageFromEvent(data) {
  return {
    id: data.id,
    author: data.author,
    author_type: data.author_type,
    ts: data.ts,
    body: data.body || '',
    attachments: Array.isArray(data.attachments) ? data.attachments : [],
  };
}

// ---------- send (text + images in one message) ----------
function renderPending() {
  const box = $('chatPending');
  if (!state.pending.length) { box.classList.add('hidden'); box.innerHTML = ''; return; }
  box.classList.remove('hidden');
  box.innerHTML = state.pending.map((p, i) =>
    `<span class="chip">${esc(p.name)} <a href="#" data-rm="${i}">✕</a></span>`).join('');
  box.querySelectorAll('[data-rm]').forEach((a) => {
    a.addEventListener('click', (e) => { e.preventDefault(); state.pending.splice(Number(a.dataset.rm), 1); renderPending(); });
  });
}

// 分块转 base64：32KB 一块，避开 btoa(String.fromCharCode(...bytes))
// 一次展开整个文件会撞 V8 参数上限（120KB 以上图片直接 RangeError）。
function arrayBufferToBase64(buf) {
  const bytes = new Uint8Array(buf);
  const CHUNK = 0x8000;
  let binary = '';
  for (let i = 0; i < bytes.length; i += CHUNK) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK));
  }
  return btoa(binary);
}

async function fileToPending(file) {
  const buf = await file.arrayBuffer();
  const b64 = arrayBufferToBase64(buf);
  state.pending.push({ name: file.name, base64: b64 });
  renderPending();
}

$('chatImageBtn').addEventListener('click', () => {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = 'image/*';
  input.multiple = true;
  input.onchange = () => {
    [...input.files].forEach((f) => {
      fileToPending(f).catch((err) => alert('图片读取失败：' + (err.message || err)));
    });
  };
  input.click();
});

$('chatInput').addEventListener('paste', async (e) => {
  const files = [...(e.clipboardData?.files || [])].filter((f) => f.type.startsWith('image/'));
  if (!files.length) return;
  e.preventDefault();
  for (const f of files) {
    try { await fileToPending(f); }
    catch (err) { alert('图片读取失败：' + (err.message || err)); return; }
  }
});

async function sendCurrent() {
  if (!state.open) return;
  const body = $('chatInput').value.trim();
  if (!body && !state.pending.length) return;
  const attachments = [];
  for (const p of state.pending) {
    try {
      const up = await window.chimera.api.uploadBlob(p.name, p.base64);
      if (up?.upload?.id) attachments.push(up.upload.id);
    } catch (err) {
      alert('图片上传失败：' + (err.message || err));
      return;
    }
  }
  try {
    if (state.open.kind === 'group') {
      await window.chimera.api.sendGroupMessage(state.open.id, body, attachments);
    } else {
      await window.chimera.api.sendDm(state.open.id, body, attachments);
    }
    $('chatInput').value = '';
    state.pending = [];
    renderPending();
    // 自己发的消息：增量拉一次即可（回声事件到时按 id 去重，不会 double）。
    await fetchNewMessages(false);
  } catch (err) {
    alert('发送失败：' + (err.message || err));
  }
}

$('chatSendBtn').addEventListener('click', sendCurrent);
$('chatInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') sendCurrent(); });

// ---------- posts ----------
$('postsRefresh').addEventListener('click', loadPosts);
async function loadPosts() {
  try {
    const data = await window.chimera.api.posts({ limit: 30 });
    const posts = data.posts || [];
    $('postsList').innerHTML = posts.map((p) => `
      <div class="post-card"><b>${esc(p.title || '(无标题)')}</b>
      <div class="post-meta">${avatarHtml(p.author || '?', 'avatar-sm')}<span>${esc(p.author || '')} · ${esc(fmtTime(p.ts))}</span></div>
      <div class="post-excerpt">${esc((p.body || '').slice(0, 200))}</div></div>`).join('')
      || '<div class="empty-note">暂无帖子</div>';
  } catch (err) {
    $('postsList').innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

// ---------- node panel ----------
async function refreshNodePanel() {
  try {
    const st = await window.chimera.node.status();
    state.nodeStatus = st;
    const on = st.state === 'running' || st.state === 'starting';
    $('nodeToggle').checked = on || st.enabled;
    $('nodeStateText').textContent =
      st.state === 'running' ? '节点运行中' :
      st.state === 'starting' ? '节点启动中…' :
      st.state === 'stopping' ? '节点停止中…' :
      st.state === 'error' ? '节点出错：' + (st.lastError || '') :
      '节点已关闭';
    $('nodeStateSub').textContent = on
      ? `只读副本档 · 端口 ${st.port} · 已同步 ${st.headers} 条头${st.l2Head ? ' · L2 高度 ' + st.l2Head.height : ' · L2 链头不可达'}`
      : '开启后开始同步社区副本';
    $('nodeNavBadge').textContent = on ? '开' : '关';
    $('nodeNavBadge').className = 'node-badge ' + (on ? 'on' : 'off');
    $('nodeStats').innerHTML = `
      <div class="stat-card"><div class="stat-label">状态</div><div class="stat-value">${esc(st.state)}</div></div>
      <div class="stat-card"><div class="stat-label">档位</div><div class="stat-value">${esc(st.tier)}</div></div>
      <div class="stat-card"><div class="stat-label">运行时长</div><div class="stat-value">${st.uptimeSec}s</div></div>
      <div class="stat-card"><div class="stat-label">本地副本头</div><div class="stat-value">${st.headers} 条</div></div>
      <div class="stat-card"><div class="stat-label">已知 peer</div><div class="stat-value">${st.peers}（在线 ${st.peersOnline}）</div></div>
      <div class="stat-card"><div class="stat-label">上次同步</div><div class="stat-value">${esc(st.lastSyncAt || '—')}（${st.lastSyncOk === false ? '失败' : '正常'}）</div></div>
      <div class="stat-card"><div class="stat-label">论坛身份</div><div class="stat-value">${st.hasForumIdentity ? '已绑定' : '未绑定（仅本地跑）'}</div></div>`;
  } catch (err) {
    $('nodeStateText').textContent = '节点状态读取失败：' + (err.message || err);
  }
}

$('nodeToggle').addEventListener('change', async (e) => {
  const want = e.target.checked;
  $('nodeToggle').disabled = true;
  try {
    await window.chimera.node.setEnabled(want);
  } catch (err) {
    alert((want ? '启动' : '停止') + '失败：' + (err.message || err));
    e.target.checked = !want;
  } finally {
    $('nodeToggle').disabled = false;
    await refreshNodePanel();
  }
});

$('nodeSyncBtn').addEventListener('click', async () => {
  try { await window.chimera.node.syncNow(); } catch (err) { alert('同步失败：' + (err.message || err)); }
  await refreshNodePanel();
});

$('nodeHeadersBtn').addEventListener('click', async () => {
  try {
    const headers = await window.chimera.node.headers(10);
    const log = $('nodeLog');
    log.textContent = headers.map((h) => JSON.stringify(h)).join('\n') || '(本地副本还是空的)';
  } catch (err) { alert('读取失败：' + (err.message || err)); }
});

// ---------- SSE wiring (main process owns the connection) ----------
// 事件只改自己那一项，不再全量 loadAll（原来每事件约 390ms、3 个 HTTP 请求）。
// 指示器只走 realtime:transport（v0.2.1）：onSseConnect/onSseDisconnect
// 不再画指示器，避免两路状态打架。
window.chimera.onRealtimeTransport(({ state, detail }) => setTransport(state, detail));
window.chimera.onSseTruncated(() => { if (state.open) loadMessages(); });
window.chimera.onSseEvent(async ({ event, data, seq }) => {
  if (Number.isFinite(seq)) state.lastSeq = seq;
  if (event === 'unread') { applyUnread(data); return; }
  if (event === 'receipt') { if (data && data.unread) applyUnread(data.unread); return; }
  if (event === 'group_message') {
    if (state.open?.kind === 'group' && data?.group_id === state.open.id) {
      if (data && data.body !== undefined && data.body !== null) {
        appendMessages([messageFromEvent(data)]); // WS 直播自带 body，直接追加
      } else {
        await fetchNewMessages(true); // 断线补发 / SSE 路无 body：增量拉取
      }
      markGroupRead(state.open.id); // 正看着这个群，顺手清未读
      return;
    }
    if (data?.group_id) bumpUnread(data.group_id, 1); // 别的群：徽章本地 +1
    return;
  }
  if (event === 'post_created' || event === 'reply_created') {
    if (!$('pane-posts').classList.contains('hidden')) loadPosts();
    return;
  }
  if (event === 'mention') {
    // 私聊没有专属事件；打开着私聊时顺带增量拉一次，别的不动。
    if (state.open?.kind === 'dm') await fetchNewMessages(false);
    return;
  }
  // presence / typing / pong 等临时消息：不刷新列表、不打扰阅读。
});
window.chimera.onNodeState(() => refreshNodePanel());
window.chimera.onNodeSync(() => refreshNodePanel());
window.chimera.onNodeLog((line) => {
  const log = $('nodeLog');
  log.textContent += line + '\n';
  log.scrollTop = log.scrollHeight;
});

boot();
