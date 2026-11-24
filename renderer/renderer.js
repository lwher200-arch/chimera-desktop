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
  boards: [], // GET /api/boards 全量板块（常驻，不依赖帖子）
  postsBoard: null, // 当前帖子板块过滤（null = 全部）
  community: null, // GET /api/communities/:id 详情缓存
  communities: [],
  teamLedger: [],
  teamLoaded: false,
  lang: 'zh', // 翻译目标语言，默认中文（localStorage chimera_lang）
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

// 未加入判定（v0.3.1）：publicGroup 带完整 members 数组，按当前登录人
// （人类账号）是否在里面算，不另调接口。
function isGroupMember(g, me) {
  if (!g || !me || !me.username) return false;
  return (g.members || []).some((m) => m.name === me.username && m.type === 'human');
}

// 帖子板块过滤（v0.3.1）：客户端按 board 过滤已拉的 30 篇，null = 全部。
function filterPostsByBoard(posts, board) {
  if (!board) return posts || [];
  return (posts || []).filter((p) => p.board === board);
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
// 主区六 pane（首页/会话/帖子/节点/社区/团队）切换：顶栏导航与首页快捷卡共用。
function showPane(v) {
  document.querySelectorAll('.nav-btn').forEach((x) => x.classList.toggle('active', x.dataset.view === v));
  $('pane-home').classList.toggle('hidden', v !== 'home');
  $('pane-conv').classList.toggle('hidden', v !== 'conv');
  $('pane-posts').classList.toggle('hidden', v !== 'posts');
  $('pane-node').classList.toggle('hidden', v !== 'node');
  $('pane-community').classList.toggle('hidden', v !== 'community');
  $('pane-team').classList.toggle('hidden', v !== 'team');
  if (v === 'posts') loadPosts();
  if (v === 'node') refreshNodePanel();
  if (v === 'community') loadCommunity();
  if (v === 'team') loadTeam();
}
document.querySelectorAll('.nav-btn').forEach((b) => {
  b.addEventListener('click', () => showPane(b.dataset.view));
});
document.querySelectorAll('.home-card').forEach((b) => {
  b.addEventListener('click', () => showPane(b.dataset.view));
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
  loadBoards(); // 板块常驻：帖子拉不拉得到都不影响板块显示
  try {
    const [groups, accounts] = await Promise.all([
      window.chimera.api.groups(),
      window.chimera.api.accounts().catch(() => ({ accounts: [] })),
    ]);
    state.groups = groups.groups || groups || [];
    state.accounts = accounts.accounts || [];
    try { state.dms = (await window.chimera.api.dms()).dms || []; } catch { state.dms = []; }
    renderConvList();
    renderRightbar();
  } catch (err) {
    $('convItems').innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

// 板块常驻（v0.3.1）：直接取 GET /api/boards 全量板块；拉失败明说原因、
// 留重试，不留 stale 的“加载帖子后显示”。
async function loadBoards() {
  try {
    const data = await window.chimera.api.boards();
    state.boards = data.boards || [];
    renderBoardList();
  } catch (err) {
    $('boardList').innerHTML = `<div class="muted small" style="padding:0 10px">板块加载失败：${esc(err.message || err)} <a href="#" id="boardRetry">重试</a></div>`;
    const r = $('boardRetry');
    if (r) r.addEventListener('click', (e) => { e.preventDefault(); loadBoards(); });
  }
}

function renderBoardList() {
  const all = [{ id: null, name: '全部板块' }, ...state.boards];
  $('boardList').innerHTML = all.map((b) => `
    <div class="board-item${state.postsBoard === b.id ? ' active' : ''}" data-board="${esc(b.id || '')}">${esc(b.name)}</div>`).join('')
    || '<div class="muted small" style="padding:0 10px">暂无板块</div>';
  $('boardList').querySelectorAll('.board-item').forEach((el) => {
    el.addEventListener('click', () => {
      state.postsBoard = el.dataset.board || null;
      renderBoardList();
      showPane('posts');
    });
  });
}

function renderConvList() {
  const q = $('convSearch').value.trim().toLowerCase();
  const box = $('convItems');
  if (state.tab === 'groups') {
    const list = state.groups.filter((g) => !q || (g.name || '').toLowerCase().includes(q));
    box.innerHTML = list.map((g) => {
      const member = isGroupMember(g, state.me);
      return `
      <div class="item" data-kind="group" data-id="${esc(g.id)}">
        ${avatarHtml(g.name)}
        <div class="item-main"><div class="item-name"><b>${esc(g.name)}</b>${member ? '' : '<span class="badge-not-joined">未加入</span>'}<span class="unread" data-unread="${esc(g.id)}"></span></div>
        <span class="item-sub">${g.kind === 'chat' ? '聊天群' : esc(g.kind || '')}${g.private ? ' · 私密' : ''}</span></div>
      </div>`;
    }).join('') || '<div class="empty-note">没有群组</div>';
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

// ---------- 右侧栏：在线账号（与网页版 #rightbar 同口径，用 loadAll 已拉的 accounts） ----------
function renderRightbar() {
  const box = $('rightbarBody');
  if (!box) return;
  const online = state.accounts.filter((a) => a.online);
  box.innerHTML = online.map((a) => `
    <div class="online-item"><b>${esc(a.name)}</b>
    <span class="muted small"> ${a.type === 'human' ? '人类' : 'AI'}</span></div>`).join('')
    || '<div class="muted small">当前没有账号在线</div>';
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
  // 会话列表在左侧栏常驻，点会话时先切回会话面板（在帖子/节点页点会话也一样）。
  showPane('conv');
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
  // 未加入的群：消息照常看，输入框禁用、横幅给加入按钮（v0.3.1）。
  const member = kind === 'group'
    ? isGroupMember(state.groups.find((g) => g.id === id), state.me)
    : true;
  syncJoinBanner(kind, member);
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

// 未加入横幅（v0.3.1，v0.3.2 改走 /join）：非成员看得到消息、发不了言；
// 加入按钮走 POST /api/groups/:id/join。服务端口径：公开群任何登录用户
// 可自加；私密群回 403——失败时把服务端原话展示出来，不自己编理由。
function syncJoinBanner(kind, member) {
  const banner = $('chatJoinBanner');
  const row = $('chatInputRow');
  if (kind !== 'group' || member) {
    banner.classList.add('hidden');
    row.classList.remove('hidden');
    return;
  }
  row.classList.add('hidden');
  banner.classList.remove('hidden');
  $('chatJoinText').textContent = '你还不是这个群的成员，只能看不能发言。';
  $('chatJoinErr').textContent = '';
  $('chatJoinBtn').disabled = false;
  $('chatJoinBtn').textContent = '加入该群';
}

$('chatJoinBtn').addEventListener('click', async () => {
  if (!state.open || state.open.kind !== 'group') return;
  const btn = $('chatJoinBtn');
  btn.disabled = true;
  btn.textContent = '加入中…';
  $('chatJoinErr').textContent = '';
  try {
    await window.chimera.api.joinGroup(state.open.id);
    // 成功：重拉群列表（成员身份已变），再打开一次。
    const groups = await window.chimera.api.groups();
    state.groups = groups.groups || groups || [];
    renderConvList();
    await openConv('group', state.open.id);
  } catch (err) {
    $('chatJoinErr').textContent = '加入失败：' + (err.message || err) + '（公开群可直接加入；私密群须由成员或站长拉你入群）';
    btn.disabled = false;
    btn.textContent = '加入该群';
  }
});

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
  // 翻译占位（v0.3.1）：经 window.ChimeraTranslate 取，不经参数传——
  // test/smoke.js §6 抽函数跑时 window 不存在，typeof 守卫下只留原文。
  const trFn = (typeof window !== 'undefined' && window.ChimeraTranslate) ? window.ChimeraTranslate.placeholder : null;
  const tr = trFn ? trFn(m.body || '') : '';
  // data-id 供窗口裁剪与向前翻页（before_id）找最老一条用。
  return `<div class="msg${mine ? ' mine' : ''}" data-id="${esc(m.id || '')}">${avatarHtml(author)}<div class="bubble"><div class="msg-head"><b>${esc(author)}</b><time>${esc(fmtTime(m.ts))}</time></div><div class="msg-body">${esc(m.body || '')}</div>${tr}${atts}</div></div>`;
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
  // 未加入的群没有发消息的入口：空态文案改成加入提示，免得误导。
  const emptyTip = box.querySelector('[data-empty] span');
  if (emptyTip && state.open?.kind === 'group' && !isGroupMember((state.groups || []).find((g) => g.id === state.open.id), state.me)) {
    emptyTip.textContent = '加入后才能发言。';
  }
  if (typeof window !== 'undefined' && window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
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
    if (typeof window !== 'undefined' && window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
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
  if (typeof window !== 'undefined' && window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
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
  // 未加入的群不发：横幅已禁用输入，这里是第二道防线（防快捷键绕过）。
  if (state.open.kind === 'group' && !isGroupMember(state.groups.find((g) => g.id === state.open.id), state.me)) {
    alert('你还不是这个群的成员，加入后才能发言。');
    return;
  }
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
  const label = $('postsBoardLabel');
  const boardName = state.postsBoard
    ? (state.boards.find((b) => b.id === state.postsBoard)?.name || state.postsBoard)
    : '';
  if (label) label.textContent = boardName ? `板块：${boardName}` : '';
  try {
    const data = await window.chimera.api.posts({ limit: 30 });
    const posts = filterPostsByBoard(data.posts || [], state.postsBoard);
    $('postsList').innerHTML = posts.map((p) => {
      const excerpt = (p.body || '').slice(0, 200);
      return `
      <div class="post-card"><b>${esc(p.title || '(无标题)')}</b>
      <div class="post-meta">${avatarHtml(p.author || '?', 'avatar-sm')}<span>${esc(p.author || '')} · ${esc(fmtTime(p.ts))}</span></div>
      <div class="post-excerpt">${esc(excerpt)}</div>${trPlaceholder(excerpt)}</div>`;
    }).join('')
      || `<div class="empty-note">${boardName ? '该板块暂无帖子' : '暂无帖子'}</div>`;
    if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations($('postsList'));
  } catch (err) {
    // 帖子拉失败只影响帖子列表本身：板块栏是 loadBoards 常驻的，不动它。
    $('postsList').innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

// ---------- 社区 / 团队（v0.3.1，只读 v1，不做管理操作） ----------
async function loadCommunity() {
  const box = $('communityBody');
  if (state.community) { renderCommunity(); return; }
  box.innerHTML = '<div class="muted">加载中…</div>';
  try {
    const list = await window.chimera.api.communities();
    state.communities = list.communities || [];
    const eterna = state.communities.find((c) => c.name === 'eternatide') || state.communities[0];
    if (!eterna) { box.innerHTML = '<div class="empty-note">暂无社区</div>'; return; }
    const detail = await window.chimera.api.community(eterna.id);
    state.community = detail.community || detail;
    renderCommunity();
  } catch (err) {
    box.innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

function renderCommunity() {
  const box = $('communityBody');
  const c = state.community;
  if (!c) return;
  const members = c.members || [];
  const groups = c.groups || [];
  const modules = c.modules_enabled || [];
  const anns = c.announcements || [];
  box.innerHTML = `
    <div class="comm-head"><h2>${esc(c.title || c.name || '社区')}</h2>
    <p class="muted">${esc(c.description || '')}</p>
    <div class="comm-meta muted small">创建人 ${esc(c.creator || '')} · 成员 ${members.length} 人 · 下挂 ${groups.length} 个群</div></div>
    <h3>模块</h3>
    <div class="chip-row">${modules.map((m) => `<span class="chip">${esc(m)}</span>`).join('') || '<span class="muted small">无</span>'}</div>
    <h3>公告</h3>
    ${anns.map((a) => `<div class="ann-card"><div class="post-meta">${avatarHtml(a.author || '?', 'avatar-sm')}<span>${esc(a.author || '')} · ${esc(fmtTime(a.ts))}</span></div><div>${esc(a.body || '')}</div>${trPlaceholder(a.body || '')}</div>`).join('') || '<div class="muted small">暂无公告</div>'}
    <h3>成员</h3>
    <div class="member-list">${members.map((m) => `<div class="member-item">${avatarHtml(m.name)}<div class="item-main"><div class="item-name"><b>${esc(m.name)}</b><span class="muted small">${esc(m.role || 'member')} · ${m.type === 'human' ? '人类' : 'AI'}</span></div></div></div>`).join('')}</div>
    <h3>下挂群组</h3>
    <div class="group-list">${groups.map((g) => `<div class="item" data-gid="${esc(g.id)}">${avatarHtml(g.name)}<div class="item-main"><div class="item-name"><b>${esc(g.name)}</b></div><span class="item-sub">${esc(g.kind || '')}${g.visibility === 'private' ? ' · 私密' : ''}</span></div></div>`).join('') || '<div class="muted small">暂无群组</div>'}</div>`;
  box.querySelectorAll('[data-gid]').forEach((el) => {
    el.addEventListener('click', () => openConv('group', el.dataset.gid));
  });
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
}

$('communityRefresh').addEventListener('click', () => {
  state.community = null;
  loadCommunity();
});

async function loadTeam() {
  const box = $('teamBody');
  if (state.teamLoaded) { renderTeam(); return; }
  box.innerHTML = '<div class="muted">加载中…</div>';
  try {
    if (!state.community) {
      const list = await window.chimera.api.communities();
      state.communities = list.communities || [];
      const eterna = state.communities.find((c) => c.name === 'eternatide') || state.communities[0];
      if (eterna) {
        const detail = await window.chimera.api.community(eterna.id);
        state.community = detail.community || detail;
      }
    }
    const id = state.community && state.community.id;
    if (!id) { box.innerHTML = '<div class="empty-note">暂无团队</div>'; return; }
    const data = await window.chimera.api.communityLedger(id, 50);
    state.teamLedger = data.items || [];
    state.teamLoaded = true;
    renderTeam();
  } catch (err) {
    box.innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

function renderTeam() {
  const box = $('teamBody');
  const c = state.community;
  const items = state.teamLedger || [];
  box.innerHTML = `
    <div class="comm-head"><h2>团队账本${c ? ' · ' + esc(c.title || c.name || '') : ''}</h2>
    <p class="muted">只读：账本条目由论坛服务端记账，桌面端不写账本。最近 ${items.length} 条。</p></div>
    ${items.map((it) => `<div class="ledger-item"><div class="ledger-kind">${esc(it.kind || '')}</div><div class="ledger-body">${esc(it.summary || it.body || '')}</div><div class="muted small">${esc(it.actor || it.by || '')} · ${esc(fmtTime(it.ts))}</div></div>`).join('') || '<div class="empty-note">暂无账本条目</div>'}`;
}

$('teamRefresh').addEventListener('click', () => {
  state.teamLedger = [];
  state.teamLoaded = false;
  loadTeam();
});

// ---------- node panel ----------
async function refreshNodePanel() {
  try {
    const st = await window.chimera.node.status();
    state.nodeStatus = st;
    const on = st.state === 'running' || st.state === 'starting';
    $('nodeToggle').checked = on || st.enabled;
    $('nodeStateText').textContent =
      st.state === 'running' ? '漂流站点运行中' :
      st.state === 'starting' ? '漂流站点启动中…' :
      st.state === 'stopping' ? '漂流站点停止中…' :
      st.state === 'error' ? '漂流站点出错：' + (st.lastError || '') :
      '漂流站点已停靠';
    const up = st.upload || {};
    $('nodeStateSub').textContent = on
      ? `同步 + 上传 · 脱敏同步 · 端口 ${st.port} · 已同步 ${st.headers} 条头${st.l2Head ? ' · L2 高度 ' + st.l2Head.height : ' · L2 链头不可达'} · 已上传 ${up.lastUploadedSeq || 0} 条${up.lastUploadAt ? ' · 最近上传 ' + fmtTime(up.lastUploadAt) : ''}${up.chainOk === false ? ' · 链自检异常' : ''}`
      : '开启后成为一个漂流站点：同步并上传脱敏的哈希与元数据，随时可停';
    $('nodeNavBadge').textContent = on ? '开' : '关';
    $('nodeNavBadge').className = 'node-badge ' + (on ? 'on' : 'off');
    $('nodeStats').innerHTML = `
      <div class="stat-card"><div class="stat-label">状态</div><div class="stat-value">${esc(st.state)}</div></div>
      <div class="stat-card"><div class="stat-label">档位</div><div class="stat-value">${esc(st.tier === 'sync-upload' ? '同步 + 上传' : st.tier)}</div></div>
      <div class="stat-card"><div class="stat-label">运行时长</div><div class="stat-value">${st.uptimeSec}s</div></div>
      <div class="stat-card"><div class="stat-label">本地副本头</div><div class="stat-value">${st.headers} 条</div></div>
      <div class="stat-card"><div class="stat-label">已知 peer</div><div class="stat-value">${st.peers}（在线 ${st.peersOnline}）</div></div>
      <div class="stat-card"><div class="stat-label">上次同步</div><div class="stat-value">${esc(fmtTime(st.lastSyncAt) || '—')}（${st.lastSyncOk === false ? '失败' : '正常'}）</div></div>
      <div class="stat-card"><div class="stat-label">已上传</div><div class="stat-value">${up.lastUploadedSeq || 0} 条</div></div>
      <div class="stat-card"><div class="stat-label">最近上传</div><div class="stat-value">${esc(fmtTime(up.lastUploadAt) || '—')}</div></div>
      <div class="stat-card"><div class="stat-label">上传失败</div><div class="stat-value">${up.failures || 0} 次</div></div>
      <div class="stat-card"><div class="stat-label">链自检</div><div class="stat-value">${up.chainOk === false ? '异常' : '正常'}</div></div>
      <div class="stat-card"><div class="stat-label">论坛身份</div><div class="stat-value">${st.hasForumIdentity ? '已绑定' + (st.nodeAccountName ? '（' + esc(st.nodeAccountName) + '）' : '') : '未绑定（仅本地跑）'}</div></div>`;
    // 激活即分配（v0.3.3）：未绑定时露激活按钮；已绑定时只展示账号名。
    const actBtn = $('nodeActivateBtn');
    const actText = $('nodeActivateText');
    if (st.hasForumIdentity) {
      actText.textContent = '论坛身份：' + (st.nodeAccountName || '已绑定') + '（归你所有；绑定你的 AI 可共享这个节点账号）';
      actBtn.classList.add('hidden');
    } else {
      actText.textContent = '还没绑定论坛身份，节点只能本地跑，不上传。点“激活节点”，用当前登录的人类账号自动分配一个节点账号（归你所有），token 只存本机。';
      actBtn.classList.remove('hidden');
    }
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

// 激活即分配（v0.3.3）：人类登录态调服务端 /api/node/activate，分配到的
// 节点账号名由主进程存进 state，token 只写本机 0600 文件。
$('nodeActivateBtn').addEventListener('click', async () => {
  const btn = $('nodeActivateBtn');
  btn.disabled = true;
  try {
    const data = await window.chimera.node.activate();
    const name = data && data.account && data.account.name;
    await refreshNodePanel();
    if (name) alert('节点已激活，分配到的节点账号：' + name);
  } catch (err) { alert('激活失败：' + (err.message || err)); }
  finally { btn.disabled = false; }
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

// ---------- 自适应翻译接线（v0.3.1） ----------
// trPlaceholder：帖子摘要/公告的翻译占位；群消息走 messageHtml 内联取。
function trPlaceholder(text) {
  if (typeof window === 'undefined' || !window.ChimeraTranslate) return '';
  return window.ChimeraTranslate.placeholder(text);
}

function syncLangBtn() {
  const b = $('langToggle');
  if (!b) return;
  b.textContent = state.lang === 'zh' ? '翻译：中文' : 'Translate: EN';
  b.title = state.lang === 'zh' ? '点按切换翻译目标为英文' : 'Click to switch translation target to Chinese';
}

if (typeof window !== 'undefined' && window.ChimeraTranslate) {
  window.ChimeraTranslate.init();
  window.ChimeraTranslate.setBatchFn(async (items, target) => {
    const d = await window.chimera.api.translateBatch(items, target);
    if (!d || !d.ok || !Array.isArray(d.results)) throw new Error('翻译批量接口返回异常');
    return d.results;
  });
  state.lang = window.ChimeraTranslate.viewerLang();
}
syncLangBtn();

$('langToggle').addEventListener('click', async () => {
  const next = state.lang === 'zh' ? 'en' : 'zh';
  if (window.ChimeraTranslate) window.ChimeraTranslate.setViewerLang(next);
  state.lang = next;
  syncLangBtn();
  // 目标语言变了：已渲染的译文框按新目标重译（重拉当前视图即可）。
  if (state.open) await loadMessages();
  if (!$('pane-posts').classList.contains('hidden')) await loadPosts();
  if (!$('pane-community').classList.contains('hidden')) renderCommunity();
  if (!$('pane-team').classList.contains('hidden')) renderTeam();
});

boot();
