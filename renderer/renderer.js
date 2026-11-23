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
 *  - 图片转 base64 分块做（32KB 一块），不再用展开运算符一次传整个文件，
 *    120KB 以上图片不再 RangeError；失败时弹提示，不再静默丢掉。
 * 断线重连靠主进程的 RealtimeClient（WS 优先、SSE 回落，lastSeq 续接）。
 * 不轮询（节点面板 5 秒定时器已删，节点状态由 node:state / node:sync 事件推）。 */
const $ = (id) => document.getElementById(id);

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
};

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function show(view) {
  $('view-login').classList.toggle('hidden', view !== 'login');
  $('view-main').classList.toggle('hidden', view !== 'main');
}

function setSse(connected, text) {
  $('sseDot').className = 'dot ' + (connected ? 'on' : 'off');
  $('sseText').textContent = text || (connected ? '实时已连接' : '未连接');
}

// ---------- session ----------
async function boot() {
  const sess = await window.chimera.session.get();
  if (sess) {
    state.me = { username: sess.username };
    $('whoami').textContent = sess.username;
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
        <b>${esc(g.name)}</b>
        <span class="muted small">${g.kind === 'chat' ? '聊天群' : esc(g.kind || '')}${g.private ? ' · 私密' : ''}</span>
        <span class="unread" data-unread="${esc(g.id)}"></span>
      </div>`).join('') || '<div class="muted" style="padding:12px">没有群组</div>';
  } else {
    const humans = state.accounts.filter((a) => a.type === 'human' && a.name !== state.me?.username);
    const ais = state.accounts.filter((a) => a.type === 'ai');
    const row = (a) => `
      <div class="item" data-kind="dm" data-id="${esc(a.name)}">
        <b>${esc(a.name)}</b>
        <span class="muted small">${a.type === 'human' ? '人类' : 'AI'}${a.online ? ' · 在线' : ''}</span>
      </div>`;
    const all = [...humans, ...ais].filter((a) => !q || (a.name || '').toLowerCase().includes(q));
    box.innerHTML = (all.map(row).join('')) || '<div class="muted" style="padding:12px">没有好友</div>';
  }
  box.querySelectorAll('.item').forEach((el) => {
    el.addEventListener('click', () => openConv(el.dataset.kind, el.dataset.id));
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

// 打开群聊即清未读：WS 路走 receipt（回 ack 带全量 unread），SSE 回落时
// sendReceipt 返回 false，改走 REST POST /api/read。失败不挡阅读。
async function markGroupRead(gid) {
  if (!Number.isFinite(state.lastSeq)) return;
  const seq = state.lastSeq;
  if (state.unread[gid]) { state.unread[gid] = 0; paintUnreadBadges(); }
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
      data = await window.chimera.api.groupMessages(state.open.id, { limit: 100 });
      renderMessages(data.messages || data || []);
    } else {
      data = await window.chimera.api.dmHistory(state.open.id);
      renderMessages(data.messages || data || []);
    }
  } catch (err) {
    $('chatBox').innerHTML = `<div class="muted">加载失败：${esc(err.message || err)}</div>`;
  }
}

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
  return `<div class="msg"><b>${esc(m.author || m.from || '?')}</b>
    <span class="muted small">${esc(m.ts || '')}</span>
    <div>${esc(m.body || '')}</div>${atts}</div>`;
}

function renderMessages(msgs) {
  const box = $('chatBox');
  box.innerHTML = msgs.map((m) => messageHtml(m)).join('') || '<div class="muted" data-empty="1">暂无消息</div>';
  state.renderedIds = new Set(msgs.map((m) => m.id).filter(Boolean));
  box.scrollTop = box.scrollHeight;
  if (msgs.length) state.lastTs = msgs[msgs.length - 1].ts;
}

// 增量追加：按 id 去重（自己发的消息回声事件与发送后拉取可能撞上同一条）。
function appendMessages(msgs) {
  const box = $('chatBox');
  const empty = box.querySelector('[data-empty]');
  if (empty) empty.remove();
  let added = false;
  for (const m of msgs) {
    const id = m.id;
    if (id && state.renderedIds.has(id)) continue;
    if (id) state.renderedIds.add(id);
    box.insertAdjacentHTML('beforeend', messageHtml(m));
    if (m.ts) state.lastTs = m.ts; // 消息按时间正序，最后一条即最新水位
    added = true;
  }
  if (added) box.scrollTop = box.scrollHeight;
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
      <div class="item"><b>${esc(p.title || '(无标题)')}</b>
      <span class="muted small">${esc(p.author || '')} · ${esc(p.ts || '')}</span>
      <div class="small">${esc((p.body || '').slice(0, 200))}</div></div>`).join('')
      || '<div class="muted" style="padding:12px">暂无帖子</div>';
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
      <div>状态：<b>${esc(st.state)}</b></div>
      <div>档位：${esc(st.tier)}（只读副本）</div>
      <div>运行时长：${st.uptimeSec}s</div>
      <div>本地副本头：${st.headers} 条</div>
      <div>已知 peer：${st.peers}（在线 ${st.peersOnline}）</div>
      <div>上次同步：${esc(st.lastSyncAt || '—')}（${st.lastSyncOk === false ? '失败' : '正常'}）</div>
      <div>论坛身份：${st.hasForumIdentity ? '已绑定' : '未绑定（仅本地跑）'}</div>`;
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
window.chimera.onSseConnect(() => setSse(true));
window.chimera.onSseDisconnect(() => setSse(false, '已断开，自动重连中…'));
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
