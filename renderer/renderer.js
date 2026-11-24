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
  teamPosts: [], // GET /api/communities/:id/posts 团队讨论
  teamPostsLoaded: false,
  teamQuery: '', // 讨论区搜索
  replyingTo: null, // 正在回的那条帖 id（团队讨论与主帖子页共用）
  editingGroups: false, // 是否在编辑关联群组
  showCreateTeam: false, // 是否展开新建团队表单
  postsFeed: 'all', // 主帖子页信息流：all 全部 | following 关注动态
  feedPosts: [], // 主帖子页当前列表（含置顶）
  searchQuery: '', // 全站搜索当前关键词
  searchResults: null, // 全站搜索结果 {posts, messages, totalPosts, totalMessages}
  profileName: null, // 资料卡当前账号名
  profileFollow: null, // 资料卡当前关注状态（GET /api/follow/:name 回包）
  editingProfile: false, // 是否在编辑自己的资料
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

// 工作区绑定（融合 P0，纯函数，Node 可测）：指纹 = keyId 去掉 'ed25519-'
// 前缀后的前 8 个 hex 字符；在线 = lastSeenTs 在 15 分钟内。
function workspaceFingerprint(keyId) {
  return String(keyId || '').replace(/^ed25519-/, '').slice(0, 8);
}
function isWorkspaceOnline(lastSeenTs, nowMs) {
  if (!lastSeenTs) return false;
  const t = Date.parse(lastSeenTs);
  if (!Number.isFinite(t)) return false;
  const now = nowMs || Date.now();
  return now - t >= 0 && now - t <= 15 * 60 * 1000;
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
    $('whoami').dataset.profile = sess.username; // 点自己名字开自己的资料卡
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
    $('whoami').dataset.profile = r.username; // 点自己名字开自己的资料卡
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
  delete $('whoami').dataset.profile;
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
  $('pane-search').classList.toggle('hidden', v !== 'search');
  $('pane-node').classList.toggle('hidden', v !== 'node');
  $('pane-community').classList.toggle('hidden', v !== 'community');
  $('pane-team').classList.toggle('hidden', v !== 'team');
  if (v === 'posts') loadPosts();
  if (v === 'search') { if (state.searchResults) renderSearchResults(); }
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
    loadHomeStats();
  } catch (err) {
    $('convItems').innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

// 首页数据化（v0.3.4）：健康计数 + 积分榜前三；任一失败只留空，不挡主界面。
async function loadHomeStats() {
  const box = $('homeStats');
  if (!box) return;
  try {
    const h = await window.chimera.api.health();
    const counts = [['帖子', h.posts], ['群组', h.groups], ['账号', h.accounts], ['消息', h.messages]];
    box.innerHTML = counts.map(([label, n]) => `<div class="home-stat"><b>${typeof n === 'number' ? n : '—'}</b><span>${label}</span></div>`).join('');
  } catch { box.innerHTML = ''; }
  try {
    const lb = await window.chimera.api.leaderboard(3);
    const rows = (lb && lb.leaders) || [];
    if (Array.isArray(rows) && rows.length) {
      $('homeBoard').classList.remove('hidden');
      $('homeBoardList').innerHTML = rows.slice(0, 3).map((r) => `<li>${esc(r.name || '')} · ${esc(String(r.points ?? ''))} 分</li>`).join('');
    }
  } catch { /* 榜单拉不到就不显示，不挡首页 */ }
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
    <div class="online-item"><button class="linklike" data-profile="${esc(a.name)}">${esc(a.name)}</button>
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
  return `<div class="msg${mine ? ' mine' : ''}" data-id="${esc(m.id || '')}">${avatarHtml(author)}<div class="bubble"><div class="msg-head"><button class="linklike" data-profile="${esc(author)}">${esc(author)}</button><time>${esc(fmtTime(m.ts))}</time></div><div class="msg-body">${esc(m.body || '')}</div>${tr}${atts}</div></div>`;
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
// ---------- 主帖子页（v0.3.5 可交互）：点赞/回复/置顶/删除 + 发帖 + 关注动态 ----------
// 信息流：all 全部（GET /api/posts）| following 关注动态（GET /api/feed/following）。
// 板块过滤仍在客户端做（与 v0.3.1 同口径）；置顶与流分离下发，置顶放前面。
function syncFeedTabs() {
  const all = $('feedTabAll'); const fol = $('feedTabFollowing');
  if (all) all.classList.toggle('active', state.postsFeed === 'all');
  if (fol) fol.classList.toggle('active', state.postsFeed === 'following');
}

function renderFeedPosts() {
  const box = $('postsList');
  if (!box) return;
  const posts = state.feedPosts || [];
  const label = $('postsBoardLabel');
  const boardName = state.postsBoard
    ? (state.boards.find((b) => b.id === state.postsBoard)?.name || state.postsBoard)
    : '';
  if (label) label.textContent = boardName ? `板块：${boardName}` : '';
  if (!posts.length) {
    box.innerHTML = `<div class="empty-note">${state.postsFeed === 'following' ? '关注的人还没有发帖' : (boardName ? '该板块暂无帖子' : '暂无帖子')}</div>`;
    return;
  }
  const me = state.me;
  const owner = isForumOwner(me);
  box.innerHTML = posts.map((p) => {
    const mine = me && p.author === me.username;
    return postCardHtml(p, mine || owner);
  }).join('');
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
}

async function loadPosts() {
  const box = $('postsList');
  if (!box) return;
  syncFeedTabs();
  try {
    const data = state.postsFeed === 'following'
      ? await window.chimera.api.followingFeed({ limit: 30 })
      : await window.chimera.api.posts({ limit: 30 });
    const all = [...(data.pinned || []), ...(data.posts || [])];
    state.feedPosts = filterPostsByBoard(all, state.postsBoard);
    renderFeedPosts();
  } catch (err) {
    // 帖子拉失败只影响帖子列表本身：板块栏是 loadBoards 常驻的，不动它。
    box.innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

// 主帖子页写操作：发帖框 + 帖子卡片委托（与团队讨论共用 handlePostAction）。
$('feedTabAll').addEventListener('click', () => { state.postsFeed = 'all'; loadPosts(); });
$('feedTabFollowing').addEventListener('click', () => { state.postsFeed = 'following'; loadPosts(); });
$('feedPostBtn').addEventListener('click', async (e) => {
  const t = e.target;
  const body = ($('feedPostBody') || {}).value ? $('feedPostBody').value.trim() : '';
  const errEl = $('feedPostErr');
  if (errEl) errEl.textContent = '';
  if (!body) { if (errEl) errEl.textContent = '正文不能为空'; return; }
  t.disabled = true;
  try {
    // 主帖子接口不存 title（服务端只读 body/board/reply_to），所以发帖框只有正文。
    await window.chimera.api.createPost({ board: state.postsBoard || 'general', title: '', body });
    $('feedPostBody').value = '';
    loadPosts();
  } catch (err) { if (errEl) errEl.textContent = err.message || err; }
  finally { t.disabled = false; }
});
$('postsList').addEventListener('click', async (e) => {
  const t = e.target.closest('[data-action]');
  if (!t) return;
  await handlePostAction(t.dataset.action, t, {
    render: renderFeedPosts,
    load: loadPosts,
    findPost: (id) => (state.feedPosts || []).find((x) => x.id === id),
  });
});

// ---------- 全站搜索（v0.3.5）：GET /api/search?q=，帖子 + 群消息 ----------
// 帖子卡片与主帖子页共用 postCardHtml；消息行点开对应群聊/私信（作者名点开资料卡）。
async function runSearch(q) {
  const query = String(q || '').trim();
  state.searchQuery = query;
  const label = $('searchLabel');
  if (label) label.textContent = query ? `搜索：${query}` : '';
  const box = $('searchResults');
  if (!box) return;
  if (!query) { box.innerHTML = '<div class="empty-note">在顶栏输入关键词，回车搜索帖子与群消息</div>'; return; }
  box.innerHTML = '<div class="muted small">搜索中…</div>';
  try {
    const data = await window.chimera.api.search(query, 20);
    state.searchResults = data;
    renderSearchResults();
  } catch (err) {
    state.searchResults = null;
    box.innerHTML = `<div class="muted" style="padding:12px">搜索失败：${esc(err.message || err)}</div>`;
  }
}

function renderSearchResults() {
  const box = $('searchResults');
  if (!box) return;
  const r = state.searchResults;
  if (!r) { box.innerHTML = '<div class="empty-note">在顶栏输入关键词，回车搜索帖子与群消息</div>'; return; }
  const posts = r.posts || [];
  const messages = r.messages || [];
  const me = state.me;
  const owner = isForumOwner(me);
  const postHtml = posts.length
    ? `<h3 class="search-sec">帖子（${r.totalPosts ?? posts.length}）</h3>` + posts.map((p) => {
        const mine = me && p.author === me.username;
        return postCardHtml(p, mine || owner);
      }).join('')
    : '<h3 class="search-sec">帖子（0）</h3><div class="empty-note">没有找到相关帖子</div>';
  const msgHtml = messages.length
    ? `<h3 class="search-sec">消息（${r.totalMessages ?? messages.length}）</h3>` + messages.map((m) => {
        const g = m.group_id ? (state.groups || []).find((x) => x.id === m.group_id) : null;
        const where = g ? esc(g.name) : '私信';
        const openAttr = g
          ? ` data-open-group="${esc(g.id)}"`
          : (m.author && m.author !== (me && me.username) ? ` data-open-dm="${esc(m.author)}"` : '');
        return `<div class="search-msg"${openAttr}>
        <div class="post-meta">${avatarHtml(m.author || '?', 'avatar-sm')}<span><button class="linklike" data-profile="${esc(m.author || '')}">${esc(m.author || '')}</button> · ${esc(where)} · ${esc(fmtTime(m.ts))}</span></div>
        <div class="post-excerpt">${esc((m.body || '').slice(0, 200))}</div>
      </div>`;
      }).join('')
    : '<h3 class="search-sec">消息（0）</h3><div class="empty-note">没有找到相关消息</div>';
  box.innerHTML = postHtml + msgHtml;
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
}

$('globalSearchInput').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') { showPane('search'); runSearch(e.target.value); }
});
$('searchRefresh').addEventListener('click', () => runSearch(state.searchQuery));
$('searchResults').addEventListener('click', async (e) => {
  if (e.target.closest('[data-profile]')) return; // 作者名点开资料卡，不跳会话
  const openGroup = e.target.closest('[data-open-group]');
  if (openGroup) { openConv('group', openGroup.dataset.openGroup); return; }
  const openDm = e.target.closest('[data-open-dm]');
  if (openDm) { openConv('dm', openDm.dataset.openDm); return; }
  const t = e.target.closest('[data-action]');
  if (!t) return;
  await handlePostAction(t.dataset.action, t, {
    render: renderSearchResults,
    load: () => runSearch(state.searchQuery),
    findPost: (id) => ((state.searchResults && state.searchResults.posts) || []).find((x) => x.id === id),
  });
});

// ---------- 用户资料 + 关注（v0.3.5） ----------
// 资料来源：GET /api/accounts 全量（loadAll 已拉，state.accounts 常驻；
// 打开资料卡时若找不到该名再重拉一次）；关注状态：GET /api/follow/:name。
function accountByName(name) {
  return (state.accounts || []).find((a) => a.name === name) || null;
}

async function openProfile(name) {
  if (!name) return;
  state.profileName = name;
  state.editingProfile = false;
  const modal = $('profileModal');
  const box = $('profileBody');
  if (!box || !modal) return;
  box.innerHTML = '<div class="muted small">加载中…</div>';
  modal.classList.remove('hidden');
  let acc = accountByName(name);
  if (!acc) {
    try {
      const list = await window.chimera.api.accounts();
      state.accounts = list.accounts || [];
      acc = accountByName(name);
    } catch { /* 列表拉不到就用空资料 */ }
  }
  let follow = null;
  try { follow = await window.chimera.api.getFollow(name); } catch { follow = null; }
  state.profileFollow = follow;
  renderProfile(acc, follow);
}

function renderProfile(acc, follow) {
  const box = $('profileBody');
  if (!box) return;
  const name = state.profileName;
  const me = state.me;
  const isSelf = !!(me && me.username === name);
  if (!acc) {
    box.innerHTML = `<div class="profile-head"><h2>${esc(name)}</h2><button class="btn ghost" data-action="close-profile">✕</button></div><p class="muted small">找不到该账号（可能已退役或列表未同步）</p>`;
    return;
  }
  const st = acc.stats || null;
  const chips = (arr) => (arr || []).map((x) => `<span class="tag-chip">${esc(String(x))}</span>`).join('');
  const links = (acc.links || []).map((l) => `<a href="${esc(String(l))}" target="_blank" rel="noreferrer">${esc(String(l))}</a>`).join(' ');
  const followers = (follow && follow.followers) || [];
  const following = (follow && follow.following) || [];
  const followedByMe = !!(follow && follow.followedByMe);
  // 关注数优先用 /api/follow 回包；拉不到时用账号本身带的计数（服务端 /api/accounts 自带）。
  const followersCount = follow ? follow.followersCount : (acc.followersCount ?? followers.length);
  const followingCount = follow ? follow.followingCount : (acc.followingCount ?? following.length);
  const nameChips = (list) => list.map((f) => `<button class="linklike" data-profile="${esc(f.name)}">${esc(f.name)}</button>`).join('、') || '<span class="muted small">无</span>';
  const avEmoji = acc.avatarEmoji || avatarInitial(acc.name);
  const avStyle = acc.avatarColor ? ` style="background:${esc(acc.avatarColor)}"` : '';
  const avClass = acc.avatarColor ? '' : ' ' + avatarClass(acc.name);
  const avatar = acc.avatarUrl
    ? `<img class="avatar avatar-lg" src="${esc(acc.avatarUrl)}" alt="">`
    : `<span class="avatar avatar-lg${avClass}"${avStyle}>${esc(avEmoji)}</span>`;
  box.innerHTML = `
    <div class="profile-head">
      ${avatar}
      <div><h2>${esc(acc.name)}</h2>
      <div class="muted small">${acc.type === 'human' ? '人类' : acc.type === 'ai' ? 'AI' : '节点'} · ${esc(acc.role || 'member')} · ${esc(acc.level || '')} · ${acc.points ?? 0} 分${acc.status && acc.status !== 'active' ? ' · 已' + esc(acc.status) : ''}${acc.lastActiveTs ? ' · 最近活跃 ' + esc(fmtTime(acc.lastActiveTs)) : ''}</div></div>
      <button class="btn ghost" data-action="close-profile">✕</button>
    </div>
    ${acc.bio ? `<p>${esc(acc.bio)}</p>` : ''}
    ${acc.signature ? `<p class="muted small">“${esc(acc.signature)}”</p>` : ''}
    ${acc.tags && acc.tags.length ? `<div class="profile-row">${chips(acc.tags)}</div>` : ''}
    ${acc.badges && acc.badges.length ? `<div class="profile-row">${chips(acc.badges)}</div>` : ''}
    ${acc.strengths && acc.strengths.length ? `<div class="profile-row"><b class="small">擅长：</b>${chips(acc.strengths)}</div>` : ''}
    ${acc.weaknesses && acc.weaknesses.length ? `<div class="profile-row"><b class="small">短板：</b>${chips(acc.weaknesses)}</div>` : ''}
    ${links ? `<div class="profile-row small">${links}</div>` : ''}
    ${st ? `<div class="profile-stats muted small">帖子 ${st.posts ?? 0} · 回复 ${st.replies ?? 0} · 消息 ${st.messages ?? 0} · 获赞 ${st.likesReceived ?? 0}</div>` : ''}
    <div class="profile-row small"><b>关注 ${followingCount}</b> ${nameChips(following)}</div>
    <div class="profile-row small"><b>粉丝 ${followersCount}</b> ${nameChips(followers)}</div>
    <div class="form-row">
      ${isSelf
        ? `<button class="btn" data-action="toggle-edit-profile">${state.editingProfile ? '收起编辑' : '编辑资料'}</button>`
        : `<button class="btn primary" data-action="toggle-follow">${followedByMe ? '已关注' : '关注'}</button>
           <button class="btn" data-action="dm-profile">发私信</button>`}
    </div>
    ${isSelf && state.editingProfile ? `
      <div class="comm-form">
        <label class="small muted">简介 <input type="text" id="profileBio" value="${esc(acc.bio || '')}" maxlength="500"></label>
        <label class="small muted">签名 <input type="text" id="profileSignature" value="${esc(acc.signature || '')}" maxlength="200"></label>
        <label class="small muted">标签（逗号分隔） <input type="text" id="profileTags" value="${esc((acc.tags || []).join('，'))}"></label>
        <label class="small muted">头像表情 <input type="text" id="profileAvatarEmoji" value="${esc(acc.avatarEmoji || '')}" maxlength="8"></label>
        <div class="form-row"><button class="btn primary" data-action="save-profile">保存</button><span class="comm-err" id="profileEditErr"></span></div>
      </div>` : ''}
    <span class="comm-err" id="profileErr"></span>`;
}

// 资料卡内所有操作走一个委托处理器；点人名（data-profile）冒泡到全局处理器开新资料卡。
$('profileModal').addEventListener('click', async (e) => {
  const t = e.target.closest('[data-action]');
  if (!t) return;
  const action = t.dataset.action;
  const name = state.profileName;
  if (!name) return;
  const setErr = (id, msg) => { const el = $(id); if (el) el.textContent = msg || ''; };
  if (action === 'close-profile') { $('profileModal').classList.add('hidden'); state.profileName = null; return; }
  if (action === 'toggle-follow') {
    t.disabled = true;
    try {
      await window.chimera.api.toggleFollow(name);
      const follow = await window.chimera.api.getFollow(name).catch(() => null);
      state.profileFollow = follow;
      renderProfile(accountByName(name), follow);
    } catch (err) { setErr('profileErr', err.message || err); }
    finally { t.disabled = false; }
    return;
  }
  if (action === 'dm-profile') {
    $('profileModal').classList.add('hidden');
    state.profileName = null;
    openConv('dm', name);
    return;
  }
  if (action === 'toggle-edit-profile') { state.editingProfile = !state.editingProfile; renderProfile(accountByName(name), state.profileFollow); return; }
  if (action === 'save-profile') {
    const patch = {
      bio: ($('profileBio') || {}).value ? $('profileBio').value.trim() : '',
      signature: ($('profileSignature') || {}).value ? $('profileSignature').value.trim() : '',
      tags: ($('profileTags') || {}).value ? $('profileTags').value.split(/[,，]/).map((s) => s.trim()).filter(Boolean) : [],
      avatarEmoji: ($('profileAvatarEmoji') || {}).value ? $('profileAvatarEmoji').value.trim() : '',
    };
    t.disabled = true;
    try {
      await window.chimera.api.updateMe(patch);
      const list = await window.chimera.api.accounts().catch(() => null);
      if (list) state.accounts = list.accounts || [];
      state.editingProfile = false;
      const follow = await window.chimera.api.getFollow(name).catch(() => null);
      state.profileFollow = follow;
      renderProfile(accountByName(name), follow);
    } catch (err) { setErr('profileEditErr', err.message || err); }
    finally { t.disabled = false; }
    return;
  }
});

// 全局：点人名/头像（data-profile）开资料卡。注意：资料卡内关注者/被关注者
// 名字也是 data-profile，点后直接切到那人的资料卡（openProfile 重渲染）。
document.addEventListener('click', (e) => {
  const el = e.target.closest('[data-profile]');
  if (!el) return;
  e.preventDefault();
  openProfile(el.dataset.profile);
});

// ---------- 社区 / 团队（v0.3.1 只读 v1；v0.3.4 可交互） ----------
// 角色口径与服务端一致：创建人 creator、团队内 admin、普通 member、非成员 null；
// 站长 coldhao 在管理判定上与创建人同权（服务端 isOwner 同口径）。
function teamRole(community, me) {
  if (!community || !me || !me.username) return null;
  if (community.creator === me.username) return 'creator';
  const m = (community.members || []).find((x) => x.name === me.username);
  if (!m) return null;
  return m.role === 'admin' ? 'admin' : 'member';
}
function isForumOwner(me) { return !!(me && me.username === 'coldhao'); }
// 加人/删人：创建人、站长、或团队内 admin。
function canAdminTeam(community, me) {
  const r = teamRole(community, me);
  return r === 'creator' || r === 'admin' || isForumOwner(me);
}
// 公告/关联群组/改角色：创建人、站长（改角色仅创建人，见 setRole）。
function canManageTeam(community, me) {
  const r = teamRole(community, me);
  return r === 'creator' || isForumOwner(me);
}
function isTeamMember(community, me) { return teamRole(community, me) !== null; }

async function loadCommunity() {
  const box = $('communityBody');
  if (state.community) { renderCommunity(); loadTeamPosts(); return; }
  box.innerHTML = '<div class="muted">加载中…</div>';
  try {
    const list = await window.chimera.api.communities();
    state.communities = list.communities || [];
    const eterna = state.communities.find((c) => c.name === 'eternatide') || state.communities[0];
    if (!eterna) { box.innerHTML = '<div class="empty-note">暂无社区</div>'; return; }
    const detail = await window.chimera.api.community(eterna.id);
    state.community = detail.community || detail;
    state.teamPosts = []; state.teamPostsLoaded = false;
    renderCommunity();
    loadTeamPosts();
  } catch (err) {
    box.innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

async function switchTeam(id) {
  if (!id || (state.community && state.community.id === id)) return;
  state.community = null; state.teamPosts = []; state.teamPostsLoaded = false;
  state.teamLoaded = false; state.teamLedger = []; state.replyingTo = null; state.editingGroups = false;
  try {
    const detail = await window.chimera.api.community(id);
    state.community = detail.community || detail;
    renderCommunity();
    loadTeamPosts();
  } catch (err) {
    $('communityBody').innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

// 团队讨论列表：成员 only；非成员服务端回 403，这里明说，不编理由。
async function loadTeamPosts() {
  const c = state.community;
  if (!c) return;
  const id = c.id;
  const box = $('teamPostsList');
  if (box) box.innerHTML = '<div class="muted small">讨论加载中…</div>';
  try {
    const data = await window.chimera.api.communityPosts(id, { limit: 50, q: state.teamQuery || null });
    // 切队竞态：回包时当前队已变就丢弃，不盖新队的列表。
    if (!state.community || state.community.id !== id) return;
    // 置顶与流分离下发（服务端 splitPinned）：置顶放前面，流放后面，两者不重叠。
    state.teamPosts = [...(data.pinned || []), ...(data.posts || [])];
    state.teamPostsLoaded = true;
    renderTeamPosts();
  } catch (err) {
    state.teamPosts = []; state.teamPostsLoaded = false;
    if (box) box.innerHTML = `<div class="muted small">讨论加载失败：${esc(err.message || err)}</div>`;
  }
}

// ---------- 帖子卡片（v0.3.5 共享）：团队讨论、主帖子页、搜索结果、关注动态共用 ----
// canPin 由调用方按权限口径给（团队：本人或团队管理；主帖子页/搜索：本人或站长；
// 服务端才是最终裁决，403 时原话显示）。作者名可点开资料卡（data-profile）。
function postCardHtml(p, canPin) {
  const me = state.me;
  const mine = me && p.author === me.username;
  const liked = me && (p.likedBy || []).includes(me.username);
  return `<div class="post-card" data-post="${esc(p.id)}">
      <b>${p.pinned ? '📌 ' : ''}${esc(p.title || '')}</b>
      <div class="post-meta">${avatarHtml(p.author || '?', 'avatar-sm')}<span><button class="linklike" data-profile="${esc(p.author || '')}">${esc(p.author || '')}</button> · ${esc(fmtTime(p.ts))}</span>${p.reply_to ? `<span>回复 @${esc(p.reply_to_author || '')}</span>` : ''}</div>
      <div class="post-excerpt">${esc(p.body || '')}</div>${trPlaceholder(p.body || '')}
      <div class="post-actions">
        <button class="btn ghost" data-action="like-post" data-id="${esc(p.id)}">${liked ? '❤️ 已赞' : '🤍 点赞'} ${p.likesCount || 0}</button>
        <button class="btn ghost" data-action="reply-post" data-id="${esc(p.id)}">💬 回复 ${p.replyCount || 0}</button>
        ${canPin ? `<button class="btn ghost" data-action="pin-post" data-id="${esc(p.id)}">${p.pinned ? '取消置顶' : '置顶'}</button>` : ''}
        ${mine ? `<button class="btn ghost" data-action="delete-post" data-id="${esc(p.id)}">删除</button>` : ''}
        <span class="comm-err" data-post-err="${esc(p.id)}"></span>
      </div>
      ${state.replyingTo === p.id ? `<div class="comm-form" style="margin-top:8px">
        <textarea id="replyBody-${esc(p.id)}" placeholder="回复 @${esc(p.author || '')}…"></textarea>
        <div class="form-row"><button class="btn primary" data-action="submit-reply" data-id="${esc(p.id)}">发送回复</button>
        <button class="btn ghost" data-action="cancel-reply">取消</button></div>
      </div>` : ''}
    </div>`;
}

// 帖子写操作共享处理器（v0.3.5）：点赞/回复/提交回复/取消回复/置顶/删帖。
// ctx = { render, load, findPost }：render 只重画本列表（保住输入框），
// load 从服务端重拉（写完刷新）；服务端原话展示，不自己编理由。
async function handlePostAction(action, t, ctx) {
  const setPostErr = (id, msg) => { const el = document.querySelector(`[data-post-err="${id}"]`); if (el) el.textContent = msg || ''; };
  if (action === 'cancel-reply') { state.replyingTo = null; ctx.render(); return; }
  if (action === 'reply-post') { state.replyingTo = state.replyingTo === t.dataset.id ? null : t.dataset.id; ctx.render(); return; }
  if (action === 'like-post') {
    t.disabled = true;
    try { await window.chimera.api.likePost(t.dataset.id); await ctx.load(); }
    catch (err) { setPostErr(t.dataset.id, err.message || err); }
    finally { t.disabled = false; }
    return;
  }
  if (action === 'submit-reply') {
    const ta = $('replyBody-' + t.dataset.id);
    const body = ta ? ta.value.trim() : '';
    if (!body) return;
    t.disabled = true;
    try {
      await window.chimera.api.createPost({ board: null, title: '', body, replyTo: t.dataset.id });
      state.replyingTo = null;
      await ctx.load();
    } catch (err) { setPostErr(t.dataset.id, err.message || err); }
    finally { t.disabled = false; }
    return;
  }
  if (action === 'pin-post') {
    const p = ctx.findPost ? ctx.findPost(t.dataset.id) : null;
    const pinned = p ? !p.pinned : true;
    t.disabled = true;
    try { await window.chimera.api.pinPost(t.dataset.id, pinned); await ctx.load(); }
    catch (err) { setPostErr(t.dataset.id, err.message || err); }
    finally { t.disabled = false; }
    return;
  }
  if (action === 'delete-post') {
    t.disabled = true;
    try { await window.chimera.api.deletePost(t.dataset.id); await ctx.load(); }
    catch (err) { setPostErr(t.dataset.id, err.message || err); }
    finally { t.disabled = false; }
    return;
  }
}

function renderTeamPosts() {
  const box = $('teamPostsList');
  if (!box) return;
  const posts = state.teamPosts || [];
  if (!posts.length) {
    box.innerHTML = '<div class="empty-note">暂无讨论帖</div>';
    return;
  }
  const me = state.me;
  const c = state.community;
  const admin = canAdminTeam(c, me);
  box.innerHTML = posts.map((p) => {
    const mine = me && p.author === me.username;
    return postCardHtml(p, mine || admin);
  }).join('');
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
}

function renderCommunity() {
  const box = $('communityBody');
  const c = state.community;
  if (!c) return;
  const members = c.members || [];
  const groups = c.groups || [];
  const modules = c.modules_enabled || [];
  const anns = c.announcements || [];
  const me = state.me;
  const role = teamRole(c, me);
  const admin = canAdminTeam(c, me);
  const manage = canManageTeam(c, me);
  const member = isTeamMember(c, me);
  const isCreator = role === 'creator';
  box.innerHTML = `
    <div class="comm-head"><h2>${esc(c.title || c.name || '社区')}</h2>
    <p class="muted">${esc(c.description || '')}</p>
    <div class="comm-meta muted small">创建人 ${esc(c.creator || '')} · 成员 ${members.length} 人 · 下挂 ${groups.length} 个群${role ? ' · 你是' + ({ creator: '创建人', admin: '管理员', member: '成员' }[role] || role) : ' · 你还不是成员'}</div></div>
    <div class="comm-form"><div class="form-row">
      <select id="teamSelect" title="切换团队">${(state.communities || []).map((t) => `<option value="${esc(t.id)}" ${t.id === c.id ? 'selected' : ''}>${esc(t.title || t.name)}</option>`).join('')}</select>
      <button class="btn" data-action="toggle-create-team">新建团队</button>
      <input type="text" id="joinCodeInput" placeholder="邀请码" style="max-width:140px">
      <button class="btn" data-action="join-code">凭邀请码加入</button>
      <span class="comm-err" id="commTopErr"></span>
    </div></div>
    ${state.showCreateTeam ? `<div class="comm-form" id="createTeamForm">
      <div class="form-row"><input type="text" id="newTeamName" placeholder="团队名（2..32，小写字母/数字/-/ _）"><input type="text" id="newTeamTitle" placeholder="标题（可空）"></div>
      <textarea id="newTeamDesc" placeholder="简介（可空，最多 500 字）"></textarea>
      <div class="form-row"><button class="btn primary" data-action="create-team">创建团队</button>
      <button class="btn ghost" data-action="toggle-create-team">取消</button>
      <span class="comm-err" id="createTeamErr"></span></div>
    </div>` : ''}
    <div class="comm-section"><h3>讨论区</h3>
      <div class="comm-form"><div class="form-row">
        <input type="text" id="teamSearch" placeholder="搜标题 / 正文 / 作者" value="${esc(state.teamQuery || '')}">
        <button class="btn" data-action="search-posts">搜索</button>
        <button class="btn ghost" data-action="refresh-posts">↻ 刷新</button>
      </div></div>
      <div id="teamPostsList"><div class="muted small">讨论加载中…</div></div>
      ${member ? `<div class="comm-form">
        <input type="text" id="newPostTitle" placeholder="标题（可空，最多 100 字）">
        <textarea id="newPostBody" placeholder="写点什么…（最多 10000 字）"></textarea>
        <div class="form-row"><button class="btn primary" data-action="new-post">发帖到讨论区</button>
        <span class="comm-err" id="newPostErr"></span></div>
      </div>` : '<p class="muted small">仅团队成员可在讨论区发帖；先加入团队。</p>'}
    </div>
    <div class="comm-section"><h3>公告</h3>
    ${anns.map((a) => `<div class="ann-card"><div class="post-meta">${avatarHtml(a.author || '?', 'avatar-sm')}<span>${esc(a.author || '')} · ${esc(fmtTime(a.ts))}</span>${manage ? `<button class="btn ghost" data-action="delete-ann" data-id="${esc(a.id || a.annId || '')}" style="margin-left:auto;padding:2px 8px;font-size:12px">删除</button>` : ''}</div><div>${esc(a.body || '')}</div>${trPlaceholder(a.body || '')}</div>`).join('') || '<div class="muted small">暂无公告</div>'}
    ${manage ? `<div class="comm-form"><textarea id="newAnnBody" placeholder="发一条公告（最多 2000 字，团队最多留 5 条）"></textarea>
      <div class="form-row"><button class="btn" data-action="publish-ann">发布公告</button>
      <span class="comm-err" id="newAnnErr"></span></div></div>` : ''}
    </div>
    <div class="comm-section"><h3>成员</h3>
    <div class="member-list">${members.map((m) => {
      const mIsCreator = m.name === c.creator;
      const badge = mIsCreator ? '创建人' : (m.role === 'admin' ? '管理员' : '成员');
      const badgeCls = mIsCreator ? 'creator' : (m.role === 'admin' ? 'admin' : '');
      return `<div class="member-item">${avatarHtml(m.name)}<div class="item-main"><div class="item-name"><b>${esc(m.name)}</b><span class="role-badge ${badgeCls}">${badge}</span><span class="muted small">${m.type === 'human' ? '人类' : 'AI'}</span></div></div>
      <span class="member-actions">
        ${isCreator && !mIsCreator ? `<select data-action="set-role" data-name="${esc(m.name)}" title="改角色"><option value="member" ${m.role !== 'admin' ? 'selected' : ''}>成员</option><option value="admin" ${m.role === 'admin' ? 'selected' : ''}>管理员</option></select>` : ''}
        ${admin && !mIsCreator ? `<button class="btn ghost" data-action="remove-member" data-name="${esc(m.name)}">移出</button>` : ''}
      </span></div>`;
    }).join('')}</div>
    ${admin ? `<div class="comm-form"><div class="form-row">
      <input type="text" id="addMemberName" placeholder="账号名">
      <select id="addMemberType"><option value="ai">AI</option><option value="human">人类</option></select>
      <button class="btn" data-action="add-member">拉进团队</button>
      <span class="comm-err" id="addMemberErr"></span></div></div>` : ''}
    </div>
    <div class="comm-section"><h3>下挂群组</h3>
    <div class="group-list">${groups.map((g) => `<div class="item" data-gid="${esc(g.id)}">${avatarHtml(g.name)}<div class="item-main"><div class="item-name"><b>${esc(g.name)}</b></div><span class="item-sub">${esc(g.kind || '')}${g.visibility === 'private' ? ' · 私密' : ''}</span></div></div>`).join('') || '<div class="muted small">暂无群组</div>'}</div>
    ${manage ? `<div class="form-row"><button class="btn" data-action="toggle-edit-groups">${state.editingGroups ? '收起编辑' : '编辑关联群组'}</button></div>
    ${state.editingGroups ? `<div class="comm-form"><div class="check-list">${(state.groups || []).map((g) => `<label><input type="checkbox" data-group-check="${esc(g.id)}" ${groups.some((x) => x.id === g.id) ? 'checked' : ''}> ${esc(g.name)}<span class="muted small">${g.visibility === 'private' ? '私密' : '公开'}</span></label>`).join('') || '<span class="muted small">暂无群组</span>'}</div>
      <div class="form-row"><button class="btn primary" data-action="save-groups">保存关联</button>
      <span class="comm-err" id="saveGroupsErr"></span></div></div>` : ''}` : ''}
    </div>
    <div class="comm-section"><h3>模块</h3>
    <div class="chip-row">${modules.map((m) => `<span class="chip">${esc(m)}</span>`).join('') || '<span class="muted small">无</span>'}</div></div>`;
  box.querySelectorAll('[data-gid]').forEach((el) => {
    el.addEventListener('click', () => openConv('group', el.dataset.gid));
  });
  const sel = $('teamSelect');
  if (sel) sel.addEventListener('change', () => switchTeam(sel.value));
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
  // 已加载的讨论列表随重渲染恢复（切换表单/语言/改角色失败等 bare render 不会
  // 把列表打回“加载中…”）；未加载时留占位，由调用方 loadTeamPosts() 拉取。
  if (state.teamPostsLoaded) renderTeamPosts();
}

// 社区页所有写操作走一个委托处理器：服务端原话展示，不自己编理由。
$('communityBody').addEventListener('click', async (e) => {
  const t = e.target.closest('[data-action]');
  if (!t) return;
  const action = t.dataset.action;
  const c = state.community;
  if (!c) return;
  const setErr = (id, msg) => { const el = $(id); if (el) el.textContent = msg || ''; };
  try {
    if (action === 'toggle-create-team') { state.showCreateTeam = !state.showCreateTeam; renderCommunity(); return; }
    if (action === 'toggle-edit-groups') { state.editingGroups = !state.editingGroups; renderCommunity(); return; }
    // 帖子写操作（点赞/回复/置顶/删帖）走共享处理器，刷新只重拉讨论列表。
    if (['like-post', 'reply-post', 'submit-reply', 'cancel-reply', 'pin-post', 'delete-post'].includes(action)) {
      await handlePostAction(action, t, {
        render: renderTeamPosts,
        load: loadTeamPosts,
        findPost: (id) => (state.teamPosts || []).find((x) => x.id === id),
      });
      return;
    }
    if (action === 'join-code') {
      const code = ($('joinCodeInput') || {}).value ? $('joinCodeInput').value.trim() : '';
      if (!code) { setErr('commTopErr', '请先填邀请码'); return; }
      t.disabled = true;
      try {
        const r = await window.chimera.api.joinCommunityByCode(code);
        const list = await window.chimera.api.communities();
        state.communities = list.communities || [];
        state.community = null; state.teamLoaded = false; state.teamLedger = [];
        state.teamPosts = []; state.teamPostsLoaded = false; state.replyingTo = null; state.editingGroups = false;
        // join 回 publicCommunity：取详情切到刚加入的那个队。
        const joined = (r && r.community) || null;
        if (joined) {
          const detail = await window.chimera.api.community(joined.id);
          state.community = detail.community || detail;
          renderCommunity(); loadTeamPosts();
        } else await loadCommunity();
      } catch (err) { setErr('commTopErr', err.message || err); } finally { t.disabled = false; }
      return;
    }
    if (action === 'create-team') {
      const name = ($('newTeamName') || {}).value ? $('newTeamName').value.trim() : '';
      const title = ($('newTeamTitle') || {}).value ? $('newTeamTitle').value.trim() : '';
      const description = ($('newTeamDesc') || {}).value ? $('newTeamDesc').value.trim() : '';
      if (!name) { setErr('createTeamErr', '请填写团队名'); return; }
      t.disabled = true;
      try {
        const r = await window.chimera.api.createCommunity({ name, title, description });
        const list = await window.chimera.api.communities();
        state.communities = list.communities || [];
        state.showCreateTeam = false;
        state.community = null; state.teamLoaded = false; state.teamLedger = [];
        const created = (r && r.community) || null;
        if (created) {
          // create 只回 publicCommunity（无 members/groups），取详情再画。
          const detail = await window.chimera.api.community(created.id);
          state.community = detail.community || detail;
          renderCommunity(); loadTeamPosts();
        } else await loadCommunity();
      } catch (err) { setErr('createTeamErr', err.message || err); } finally { t.disabled = false; }
      return;
    }
    if (action === 'search-posts') { state.teamQuery = ($('teamSearch') || {}).value ? $('teamSearch').value.trim() : ''; loadTeamPosts(); return; }
    if (action === 'refresh-posts') { loadTeamPosts(); return; }
    if (action === 'new-post') {
      const title = ($('newPostTitle') || {}).value ? $('newPostTitle').value.trim() : '';
      const body = ($('newPostBody') || {}).value ? $('newPostBody').value.trim() : '';
      if (!body) { setErr('newPostErr', '正文不能为空'); return; }
      t.disabled = true;
      try {
        await window.chimera.api.createCommunityPost(c.id, { title, body });
        $('newPostTitle').value = ''; $('newPostBody').value = '';
        loadTeamPosts();
      } catch (err) { setErr('newPostErr', err.message || err); } finally { t.disabled = false; }
      return;
    }
    if (action === 'publish-ann') {
      const body = ($('newAnnBody') || {}).value ? $('newAnnBody').value.trim() : '';
      if (!body) { setErr('newAnnErr', '公告不能为空'); return; }
      t.disabled = true;
      try {
        await window.chimera.api.createCommunityAnnouncement(c.id, body);
        const detail = await window.chimera.api.community(c.id);
        state.community = detail.community || detail;
        renderCommunity(); loadTeamPosts();
      } catch (err) { setErr('newAnnErr', err.message || err); } finally { t.disabled = false; }
      return;
    }
    if (action === 'delete-ann') {
      t.disabled = true;
      try {
        await window.chimera.api.deleteCommunityAnnouncement(c.id, t.dataset.id);
        const detail = await window.chimera.api.community(c.id);
        state.community = detail.community || detail;
        renderCommunity(); loadTeamPosts();
      } catch (err) { setErr('newAnnErr', err.message || err); } finally { t.disabled = false; }
      return;
    }
    if (action === 'add-member') {
      const name = ($('addMemberName') || {}).value ? $('addMemberName').value.trim() : '';
      const type = ($('addMemberType') || {}).value || 'ai';
      if (!name) { setErr('addMemberErr', '请填写账号名'); return; }
      t.disabled = true;
      try {
        await window.chimera.api.addCommunityMember(c.id, { name, type });
        $('addMemberName').value = '';
        const detail = await window.chimera.api.community(c.id);
        state.community = detail.community || detail;
        renderCommunity(); loadTeamPosts();
      } catch (err) { setErr('addMemberErr', err.message || err); } finally { t.disabled = false; }
      return;
    }
    if (action === 'remove-member') {
      t.disabled = true;
      try {
        await window.chimera.api.removeCommunityMember(c.id, t.dataset.name);
        const detail = await window.chimera.api.community(c.id);
        state.community = detail.community || detail;
        renderCommunity(); loadTeamPosts();
      } catch (err) { setErr('addMemberErr', err.message || err); } finally { t.disabled = false; }
      return;
    }
    if (action === 'save-groups') {
      const ids = [...document.querySelectorAll('[data-group-check]:checked')].map((x) => x.dataset.groupCheck);
      t.disabled = true;
      try {
        const r = await window.chimera.api.setCommunityGroups(c.id, ids);
        state.community = (r && r.community) || state.community;
        state.editingGroups = false;
        renderCommunity(); loadTeamPosts();
      } catch (err) { setErr('saveGroupsErr', err.message || err); } finally { t.disabled = false; }
      return;
    }
  } catch (err) {
    const el = $('commTopErr');
    if (el) el.textContent = err.message || err;
  }
});
$('communityBody').addEventListener('change', async (e) => {
  const t = e.target.closest('[data-action="set-role"]');
  if (!t || !state.community) return;
  try {
    await window.chimera.api.setCommunityMemberRole(state.community.id, t.dataset.name, t.value);
    const detail = await window.chimera.api.community(state.community.id);
    state.community = detail.community || detail;
    renderCommunity(); loadTeamPosts();
  } catch (err) {
    const el = $('addMemberErr');
    if (el) el.textContent = err.message || err;
    renderCommunity();
  }
});

$('communityRefresh').addEventListener('click', () => {
  state.community = null;
  state.teamPosts = []; state.teamPostsLoaded = false;
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
    <p class="muted">只读：账本条目由论坛服务端记账，桌面端不写账本。讨论、公告与成员管理在社区页。最近 ${items.length} 条。</p></div>
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
    // 工作区绑定（融合 P0）：状态另行拉取，失败不影响节点面板其余内容。
    refreshWorkspaceBinding().catch(() => {});
  } catch (err) {
    $('nodeStateText').textContent = '节点状态读取失败：' + (err.message || err);
  }
}

// 工作区绑定（融合 P0）：无绑定显示“绑定工作区”按钮；有绑定显示
// deviceName、指纹前 8 位、在线/离线（lastSeen 15 分钟内为在线）。
// P0 不做改名 UI、不做解绑按钮。绑定后每 5 分钟 seen 由主进程定时调。
async function refreshWorkspaceBinding() {
  const btn = $('workspaceBindBtn');
  const text = $('workspaceBindText');
  if (!btn || !text) return;
  try {
    const data = await window.chimera.api.workspaceBindings();
    const bindings = (data && data.bindings) || [];
    const active = bindings.find((b) => !b.unlinkedTs);
    if (!active) {
      state.workspaceBinding = null;
      text.textContent = '工作区未绑定：绑定后论坛能看到这台工作区是否在线，私钥只存本机。';
      btn.classList.remove('hidden');
      return;
    }
    state.workspaceBinding = active;
    const online = isWorkspaceOnline(active.lastSeenTs);
    text.textContent = '工作区已绑定：' + (active.deviceName || '') + ' · 指纹 ' + workspaceFingerprint(active.keyId) + ' · ' + (online ? '在线' : '离线') + (active.lastSeenTs ? ' · 最近活跃 ' + fmtTime(active.lastSeenTs) : '');
    btn.classList.add('hidden');
  } catch (err) {
    text.textContent = '工作区绑定状态读取失败：' + (err.message || err);
    btn.classList.add('hidden');
  }
}

// 绑定工作区（融合 P0）：challenge → 主进程本机签名 → 注册 → 显示绑定状态。
$('workspaceBindBtn').addEventListener('click', async () => {
  const btn = $('workspaceBindBtn');
  btn.disabled = true;
  try {
    const ch = await window.chimera.api.workspaceChallenge();
    if (!ch || !ch.challenge) throw new Error('服务端未返回挑战串');
    const data = await window.chimera.api.workspaceBind(ch.challenge);
    await refreshWorkspaceBinding();
    const name = data && data.binding && data.binding.deviceName;
    if (name) alert('工作区已绑定：' + name);
  } catch (err) { alert('绑定失败：' + (err.message || err)); }
  finally { btn.disabled = false; }
});

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

// ---------- 深浅主题（v0.3.4） ----------
function syncThemeBtn() {
  const b = $('themeToggle');
  if (!b) return;
  const t = window.ChimeraTheme ? window.ChimeraTheme.currentTheme() : 'dark';
  b.textContent = t === 'light' ? '☀️ 浅色' : '🌙 深色';
  b.title = t === 'light' ? '点按切换为深色主题' : '点按切换为浅色主题';
}
if (typeof window !== 'undefined' && window.ChimeraTheme) {
  window.ChimeraTheme.applyTheme(document, window.ChimeraTheme.currentTheme());
}
syncThemeBtn();
$('themeToggle').addEventListener('click', () => {
  if (!window.ChimeraTheme) return;
  const next = window.ChimeraTheme.currentTheme() === 'light' ? 'dark' : 'light';
  window.ChimeraTheme.applyTheme(document, next);
  window.ChimeraTheme.saveTheme(null, next);
  syncThemeBtn();
});

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
