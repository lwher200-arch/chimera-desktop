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

// ---------- 界面文案自适应翻译（v0.3.7） ----------
// chrome 文案走 ChimeraTranslate.ui / uiFmt，与内容翻译共用 chimera_lang；
// 模块缺失时（Node 抽函数测试）回落为 key 本身，不抛错。
function ui(key) {
  if (typeof window !== 'undefined' && window.ChimeraTranslate && window.ChimeraTranslate.ui) return window.ChimeraTranslate.ui(key);
  return key;
}
function uiFmt(key) {
  if (typeof window !== 'undefined' && window.ChimeraTranslate && window.ChimeraTranslate.uiFmt) return window.ChimeraTranslate.uiFmt.apply(window.ChimeraTranslate, arguments);
  return key;
}

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
  lastTransport: null, // 最近一次 realtime:transport，供语言切换时重画指示器
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
  connecting: { cls: 'off', key: 'top.connecting' },
  ws: { cls: 'on', key: 'top.wsConnected' },
  sse: { cls: 'warn', key: 'top.sseFallback' },
  disconnected: { cls: 'bad', key: 'top.disconnected' },
  'session-expired': { cls: 'bad', key: 'top.sessionExpired' },
};
// detail 是主进程拼的中文串（如“WS 已连接（公网）”）；只取括号里的网络
// 后缀（公网/尾网）翻出来接在文案后面，其余主进程措辞不逐字翻。
function transportSuffix(detail) {
  const m = String(detail || '').match(/（([^）]+)）/);
  if (!m) return '';
  if (m[1] === '公网') return ui('top.netPublic');
  if (m[1] === '尾网') return ui('top.netTail');
  return '（' + m[1] + '）';
}
function setTransport(st, detail) {
  const t = TRANSPORT_UI[st] || TRANSPORT_UI.disconnected;
  state.lastTransport = { state: st, detail: detail || null };
  $('sseDot').className = 'dot ' + t.cls;
  $('sseText').textContent = ui(t.key) + transportSuffix(detail);
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
  if (!username || !password) { $('loginErr').textContent = ui('login.emptyErr'); return; }
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
    $('loginErr').textContent = ui('login.failErr') + (err.message || err);
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
    $('convItems').innerHTML = `<div class="muted" style="padding:12px">${ui('common.loadFail')}${esc(err.message || err)}</div>`;
  }
}

// 首页数据化（v0.3.4）：健康计数 + 积分榜前三；任一失败只留空，不挡主界面。
async function loadHomeStats() {
  const box = $('homeStats');
  if (!box) return;
  try {
    const h = await window.chimera.api.health();
    const counts = [[ui('home.statPosts'), h.posts], [ui('home.statGroups'), h.groups], [ui('home.statAccounts'), h.accounts], [ui('home.statMsgs'), h.messages]];
    box.innerHTML = counts.map(([label, n]) => `<div class="home-stat"><b>${typeof n === 'number' ? n : '—'}</b><span>${label}</span></div>`).join('');
  } catch { box.innerHTML = ''; }
  try {
    const lb = await window.chimera.api.leaderboard(3);
    const rows = (lb && lb.leaders) || [];
    if (Array.isArray(rows) && rows.length) {
      $('homeBoard').classList.remove('hidden');
      $('homeBoardList').innerHTML = rows.slice(0, 3).map((r) => `<li>${esc(r.name || '')} · ${esc(uiFmt('home.points', r.points ?? ''))}</li>`).join('');
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
    $('boardList').innerHTML = `<div class="muted small" style="padding:0 10px">${ui('side.boardsFail')}${esc(err.message || err)} <a href="#" id="boardRetry">${ui('side.retry')}</a></div>`;
    const r = $('boardRetry');
    if (r) r.addEventListener('click', (e) => { e.preventDefault(); loadBoards(); });
  }
}

function renderBoardList() {
  const all = [{ id: null, name: ui('side.allBoards') }, ...state.boards];
  $('boardList').innerHTML = all.map((b) => `
    <div class="board-item${state.postsBoard === b.id ? ' active' : ''}" data-board="${esc(b.id || '')}">${b.id ? `<span data-tr-name="${esc(b.name)}">${esc(b.name)}</span>` : esc(b.name)}</div>`).join('')
    || '<div class="muted small" style="padding:0 10px">' + ui('side.noBoards') + '</div>';
  $('boardList').querySelectorAll('.board-item').forEach((el) => {
    el.addEventListener('click', () => {
      state.postsBoard = el.dataset.board || null;
      renderBoardList();
      showPane('posts');
    });
  });
  if (window.ChimeraTranslate && window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
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
        <div class="item-main"><div class="item-name"><b data-tr-name="${esc(g.name)}">${esc(g.name)}</b>${member ? '' : '<span class="badge-not-joined">' + ui('side.notJoined') + '</span>'}<span class="unread" data-unread="${esc(g.id)}"></span></div>
        <span class="item-sub">${g.kind === 'chat' ? ui('side.groupChat') : esc(g.kind || '')}${g.private ? ui('side.private') : ''}</span></div>
      </div>`;
    }).join('') || '<div class="empty-note">' + ui('side.noGroups') + '</div>';
  } else {
    const humans = state.accounts.filter((a) => a.type === 'human' && a.name !== state.me?.username);
    const ais = state.accounts.filter((a) => a.type === 'ai');
    const row = (a) => `
      <div class="item" data-kind="dm" data-id="${esc(a.name)}">
        ${avatarHtml(a.name)}
        <div class="item-main"><div class="item-name"><b>${esc(a.name)}</b></div>
        <span class="item-sub">${a.type === 'human' ? ui('side.human') : 'AI'}${a.online ? ui('side.online') : ''}</span></div>
      </div>`;
    const all = [...humans, ...ais].filter((a) => !q || (a.name || '').toLowerCase().includes(q));
    box.innerHTML = (all.map(row).join('')) || '<div class="empty-note">' + ui('side.noFriends') + '</div>';
  }
  box.querySelectorAll('.item').forEach((el) => {
    el.addEventListener('click', () => openConv(el.dataset.kind, el.dataset.id));
    if (state.open) el.classList.toggle('active', el.dataset.kind === state.open.kind && el.dataset.id === state.open.id);
  });
  paintUnreadBadges();
  if (window.ChimeraTranslate && window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
}

// ---------- 右侧栏：在线账号（与网页版 #rightbar 同口径，用 loadAll 已拉的 accounts） ----------
function renderRightbar() {
  const box = $('rightbarBody');
  if (!box) return;
  const online = state.accounts.filter((a) => a.online);
  box.innerHTML = online.map((a) => `
    <div class="online-item"><button class="linklike" data-profile="${esc(a.name)}">${esc(a.name)}</button>
    <span class="muted small"> ${a.type === 'human' ? ui('side.human') : 'AI'}</span></div>`).join('')
    || '<div class="muted small">' + ui('side.noOnline') + '</div>';
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
  // 群名跟随语言（v0.3.8），私信人名不动。
  if (kind === 'group') $('chatTitle').setAttribute('data-tr-name', name);
  else $('chatTitle').removeAttribute('data-tr-name');
  if (window.ChimeraTranslate && window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
  $('chatMeta').textContent = kind === 'group' ? ui('chat.metaGroup') : ui('chat.metaDm');
  // 未加入的群：消息照常看，输入框禁用、横幅给加入按钮（v0.3.1）。
  const member = kind === 'group'
    ? isGroupMember(state.groups.find((g) => g.id === id), state.me)
    : true;
  syncJoinBanner(kind, member);
  $('chatBox').innerHTML = '<div class="muted">' + ui('chat.loading') + '</div>';
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
    $('chatBox').innerHTML = `<div class="muted">${ui('chat.loadFail')}${esc(err.message || err)}</div>`;
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
  btn.textContent = state.loadingOlder ? ui('chat.loading') : ui('chat.loadOlder');
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
  $('chatJoinText').textContent = ui('chat.joinText');
  $('chatJoinErr').textContent = '';
  $('chatJoinBtn').disabled = false;
  $('chatJoinBtn').textContent = ui('chat.joinBtn');
}

$('chatJoinBtn').addEventListener('click', async () => {
  if (!state.open || state.open.kind !== 'group') return;
  const btn = $('chatJoinBtn');
  btn.disabled = true;
  btn.textContent = ui('chat.joining');
  $('chatJoinErr').textContent = '';
  try {
    await window.chimera.api.joinGroup(state.open.id);
    // 成功：重拉群列表（成员身份已变），再打开一次。
    const groups = await window.chimera.api.groups();
    state.groups = groups.groups || groups || [];
    renderConvList();
    await openConv('group', state.open.id);
  } catch (err) {
    $('chatJoinErr').textContent = ui('chat.joinErr') + (err.message || err) + ui('chat.joinErrHint');
    btn.disabled = false;
    btn.textContent = ui('chat.joinBtn');
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
    `<a href="${esc(a.url)}" target="_blank"><img class="thumb" src="${esc(a.url)}" alt="${ui('chat.imgAlt')}"></a>`).join('');
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
  box.innerHTML = list.map((m) => messageHtml(m)).join('') || '<div class="chat-empty" data-empty="1"><img src="assets/brand.png" alt="奇美拉"><b>' + ui('chat.noMsg') + '</b><span>' + ui('chat.noMsgSub') + '</span></div>';
  state.renderedIds = new Set(list.map((m) => m.id).filter(Boolean));
  box.scrollTop = box.scrollHeight;
  if (list.length) state.lastTs = list[list.length - 1].ts;
  // 未加入的群没有发消息的入口：空态文案改成加入提示，免得误导。
  const emptyTip = box.querySelector('[data-empty] span');
  if (emptyTip && state.open?.kind === 'group' && !isGroupMember((state.groups || []).find((g) => g.id === state.open.id), state.me)) {
    emptyTip.textContent = ui('chat.joinToPost');
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
      fileToPending(f).catch((err) => alert(ui('chat.imgReadFail') + (err.message || err)));
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
    catch (err) { alert(ui('chat.imgReadFail') + (err.message || err)); return; }
  }
});

async function sendCurrent() {
  if (!state.open) return;
  // 未加入的群不发：横幅已禁用输入，这里是第二道防线（防快捷键绕过）。
  if (state.open.kind === 'group' && !isGroupMember(state.groups.find((g) => g.id === state.open.id), state.me)) {
    alert(ui('chat.notMemberAlert'));
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
      alert(ui('chat.imgUploadFail') + (err.message || err));
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
    alert(ui('chat.sendFail') + (err.message || err));
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
  if (label) label.innerHTML = boardName ? esc(ui('posts.boardLabel')) + `<span data-tr-name="${esc(boardName)}">${esc(boardName)}</span>` : '';
  if (!posts.length) {
    box.innerHTML = `<div class="empty-note">${state.postsFeed === 'following' ? ui('posts.emptyFollowing') : (boardName ? ui('posts.emptyBoard') : ui('posts.empty'))}</div>`;
    return;
  }
  const me = state.me;
  const owner = isForumOwner(me);
  box.innerHTML = posts.map((p) => {
    const mine = me && p.author === me.username;
    return postCardHtml(p, mine || owner);
  }).join('');
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
  if (window.ChimeraTranslate && window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
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
    box.innerHTML = `<div class="muted" style="padding:12px">${ui('posts.loadFail')}${esc(err.message || err)}</div>`;
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
  if (!body) { if (errEl) errEl.textContent = ui('posts.emptyBodyErr'); return; }
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
  if (label) label.textContent = query ? ui('search.label') + query : '';
  const box = $('searchResults');
  if (!box) return;
  if (!query) { box.innerHTML = '<div class="empty-note">' + ui('search.hint') + '</div>'; return; }
  box.innerHTML = '<div class="muted small">' + ui('search.searching') + '</div>';
  try {
    const data = await window.chimera.api.search(query, 20);
    state.searchResults = data;
    renderSearchResults();
  } catch (err) {
    state.searchResults = null;
    box.innerHTML = `<div class="muted" style="padding:12px">${ui('search.loadFail')}${esc(err.message || err)}</div>`;
  }
}

function renderSearchResults() {
  const box = $('searchResults');
  if (!box) return;
  const r = state.searchResults;
  if (!r) { box.innerHTML = '<div class="empty-note">' + ui('search.hint') + '</div>'; return; }
  const posts = r.posts || [];
  const messages = r.messages || [];
  const me = state.me;
  const owner = isForumOwner(me);
  const postHtml = posts.length
    ? `<h3 class="search-sec">${uiFmt('search.postsSec', r.totalPosts ?? posts.length)}</h3>` + posts.map((p) => {
        const mine = me && p.author === me.username;
        return postCardHtml(p, mine || owner);
      }).join('')
    : '<h3 class="search-sec">' + uiFmt('search.postsSec', 0) + '</h3><div class="empty-note">' + ui('search.noPosts') + '</div>';
  const msgHtml = messages.length
    ? `<h3 class="search-sec">${uiFmt('search.msgsSec', r.totalMessages ?? messages.length)}</h3>` + messages.map((m) => {
        const g = m.group_id ? (state.groups || []).find((x) => x.id === m.group_id) : null;
        const where = g ? esc(g.name) : ui('search.dmWhere');
        const openAttr = g
          ? ` data-open-group="${esc(g.id)}"`
          : (m.author && m.author !== (me && me.username) ? ` data-open-dm="${esc(m.author)}"` : '');
        return `<div class="search-msg"${openAttr}>
        <div class="post-meta">${avatarHtml(m.author || '?', 'avatar-sm')}<span><button class="linklike" data-profile="${esc(m.author || '')}">${esc(m.author || '')}</button> · ${esc(where)} · ${esc(fmtTime(m.ts))}</span></div>
        <div class="post-excerpt">${esc((m.body || '').slice(0, 200))}</div>${trPlaceholder((m.body || '').slice(0, 200))}
      </div>`;
      }).join('')
    : '<h3 class="search-sec">' + uiFmt('search.msgsSec', 0) + '</h3><div class="empty-note">' + ui('search.noMsgs') + '</div>';
  box.innerHTML = postHtml + msgHtml;
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
  if (window.ChimeraTranslate && window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
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
  box.innerHTML = '<div class="muted small">' + ui('common.loading') + '</div>';
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
    box.innerHTML = `<div class="profile-head"><h2>${esc(name)}</h2><button class="btn ghost" data-action="close-profile">✕</button></div><p class="muted small">${ui('profile.notFound')}</p>`;
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
  const nameChips = (list) => list.map((f) => `<button class="linklike" data-profile="${esc(f.name)}">${esc(f.name)}</button>`).join(ui('common.listSep')) || '<span class="muted small">' + ui('profile.none') + '</span>';
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
      <div class="muted small">${acc.type === 'human' ? ui('profile.human') : acc.type === 'ai' ? ui('profile.ai') : ui('profile.node')} · ${esc(acc.role || 'member')} · ${esc(acc.level || '')} · ${uiFmt('profile.points', acc.points ?? 0)}${acc.status && acc.status !== 'active' ? ' · 已' + esc(acc.status) : ''}${acc.lastActiveTs ? ui('profile.lastActive') + esc(fmtTime(acc.lastActiveTs)) : ''}</div></div>
      <button class="btn ghost" data-action="close-profile">✕</button>
    </div>
    ${acc.bio ? `<p>${esc(acc.bio)}</p>` : ''}
    ${acc.signature ? `<p class="muted small">“${esc(acc.signature)}”</p>` : ''}
    ${acc.tags && acc.tags.length ? `<div class="profile-row">${chips(acc.tags)}</div>` : ''}
    ${acc.badges && acc.badges.length ? `<div class="profile-row">${chips(acc.badges)}</div>` : ''}
    ${acc.strengths && acc.strengths.length ? `<div class="profile-row"><b class="small">${ui('profile.strengths')}</b>${chips(acc.strengths)}</div>` : ''}
    ${acc.weaknesses && acc.weaknesses.length ? `<div class="profile-row"><b class="small">${ui('profile.weaknesses')}</b>${chips(acc.weaknesses)}</div>` : ''}
    ${links ? `<div class="profile-row small">${links}</div>` : ''}
    ${st ? `<div class="profile-stats muted small">${uiFmt('profile.stats', st.posts ?? 0, st.replies ?? 0, st.messages ?? 0, st.likesReceived ?? 0)}</div>` : ''}
    <div class="profile-row small"><b>${uiFmt('profile.following', followingCount)}</b> ${nameChips(following)}</div>
    <div class="profile-row small"><b>${uiFmt('profile.followers', followersCount)}</b> ${nameChips(followers)}</div>
    <div class="form-row">
      ${isSelf
        ? `<button class="btn" data-action="toggle-edit-profile">${state.editingProfile ? ui('profile.collapseEdit') : ui('profile.edit')}</button>`
        : `<button class="btn primary" data-action="toggle-follow">${followedByMe ? ui('profile.followed') : ui('profile.follow')}</button>\n
           <button class="btn" data-action="dm-profile">${ui('profile.dm')}</button>`}
    </div>
    ${isSelf && state.editingProfile ? `
      <div class="comm-form">
        <label class="small muted">${ui('profile.bio')} <input type="text" id="profileBio" value="${esc(acc.bio || '')}" maxlength="500"></label>
        <label class="small muted">${ui('profile.signature')} <input type="text" id="profileSignature" value="${esc(acc.signature || '')}" maxlength="200"></label>
        <label class="small muted">${ui('profile.tags')} <input type="text" id="profileTags" value="${esc((acc.tags || []).join('，'))}"></label>
        <label class="small muted">${ui('profile.avatarEmoji')} <input type="text" id="profileAvatarEmoji" value="${esc(acc.avatarEmoji || '')}" maxlength="8"></label>
        <div class="form-row"><button class="btn primary" data-action="save-profile">${ui('profile.save')}</button><span class="comm-err" id="profileEditErr"></span></div>
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
  box.innerHTML = '<div class="muted">' + ui('community.loading') + '</div>';
  try {
    const list = await window.chimera.api.communities();
    state.communities = list.communities || [];
    const eterna = state.communities.find((c) => c.name === 'eternatide') || state.communities[0];
    if (!eterna) { box.innerHTML = '<div class="empty-note">' + ui('community.noCommunity') + '</div>'; return; }
    const detail = await window.chimera.api.community(eterna.id);
    state.community = detail.community || detail;
    state.teamPosts = []; state.teamPostsLoaded = false;
    renderCommunity();
    loadTeamPosts();
  } catch (err) {
    box.innerHTML = `<div class="muted" style="padding:12px">${ui('community.loadFail')}${esc(err.message || err)}</div>`;
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
    $('communityBody').innerHTML = `<div class="muted" style="padding:12px">${ui('community.loadFail')}${esc(err.message || err)}</div>`;
  }
}

// 团队讨论列表：成员 only；非成员服务端回 403，这里明说，不编理由。
async function loadTeamPosts() {
  const c = state.community;
  if (!c) return;
  const id = c.id;
  const box = $('teamPostsList');
  if (box) box.innerHTML = '<div class="muted small">' + ui('community.postsLoading') + '</div>';
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
    if (box) box.innerHTML = `<div class="muted small">${ui('community.postsFail')}${esc(err.message || err)}</div>`;
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
      <div class="post-meta">${avatarHtml(p.author || '?', 'avatar-sm')}<span><button class="linklike" data-profile="${esc(p.author || '')}">${esc(p.author || '')}</button> · ${esc(fmtTime(p.ts))}</span>${p.reply_to ? `<span>${ui('posts.replyTo')}${esc(p.reply_to_author || '')}</span>` : ''}</div>
      <div class="post-excerpt">${esc(p.body || '')}</div>${trPlaceholder(p.body || '')}
      <div class="post-actions">
        <button class="btn ghost" data-action="like-post" data-id="${esc(p.id)}">${liked ? ui('posts.liked') : ui('posts.like')} ${p.likesCount || 0}</button>
        <button class="btn ghost" data-action="reply-post" data-id="${esc(p.id)}">${ui('posts.reply')} ${p.replyCount || 0}</button>
        ${canPin ? `<button class="btn ghost" data-action="pin-post" data-id="${esc(p.id)}">${p.pinned ? ui('posts.unpin') : ui('posts.pin')}</button>` : ''}
        ${mine ? `<button class="btn ghost" data-action="delete-post" data-id="${esc(p.id)}">${ui('posts.delete')}</button>` : ''}
        <span class="comm-err" data-post-err="${esc(p.id)}"></span>
      </div>
      ${state.replyingTo === p.id ? `<div class="comm-form" style="margin-top:8px">
        <textarea id="replyBody-${esc(p.id)}" placeholder="${ui('posts.replyTo')}${esc(p.author || '')}…"></textarea>
        <div class="form-row"><button class="btn primary" data-action="submit-reply" data-id="${esc(p.id)}">${ui('posts.sendReply')}</button>
        <button class="btn ghost" data-action="cancel-reply">${ui('posts.cancel')}</button></div>
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
    box.innerHTML = '<div class="empty-note">' + ui('community.noPosts') + '</div>';
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
  if (window.ChimeraTranslate && window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
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
    <div class="comm-head"><h2>${esc(c.title || c.name || ui('community.title'))}</h2>
    <p class="muted">${esc(c.description || '')}</p>
    <div class="comm-meta muted small">${uiFmt('community.meta', esc(c.creator || ''), members.length, groups.length)}${role ? uiFmt('community.roleSuffix', ({ creator: ui('community.roleCreator'), admin: ui('community.roleAdmin'), member: ui('community.roleMember') }[role] || role)) : ui('community.notMember')}</div></div>
    <div class="comm-form"><div class="form-row">
      <select id="teamSelect" title="${ui('community.switchTeam')}">${(state.communities || []).map((t) => `<option value="${esc(t.id)}" ${t.id === c.id ? 'selected' : ''}>${esc(t.title || t.name)}</option>`).join('')}</select>
      <button class="btn" data-action="toggle-create-team">${ui('community.newTeam')}</button>
      <input type="text" id="joinCodeInput" placeholder="${ui('community.inviteCodePh')}" style="max-width:140px">
      <button class="btn" data-action="join-code">${ui('community.joinCode')}</button>
      <span class="comm-err" id="commTopErr"></span>
    </div></div>
    ${state.showCreateTeam ? `<div class="comm-form" id="createTeamForm">
      <div class="form-row"><input type="text" id="newTeamName" placeholder="${ui('community.teamNamePh')}"><input type="text" id="newTeamTitle" placeholder="${ui('community.teamTitlePh')}"></div>
      <textarea id="newTeamDesc" placeholder="${ui('community.teamDescPh')}"></textarea>
      <div class="form-row"><button class="btn primary" data-action="create-team">${ui('community.createTeam')}</button>
      <button class="btn ghost" data-action="toggle-create-team">${ui('community.cancel')}</button>
      <span class="comm-err" id="createTeamErr"></span></div>
    </div>` : ''}
    <div class="comm-section"><h3>${ui('community.discussion')}</h3>
      <div class="comm-form"><div class="form-row">
        <input type="text" id="teamSearch" placeholder="${ui('community.searchPh')}" value="${esc(state.teamQuery || '')}">
        <button class="btn" data-action="search-posts">${ui('community.searchBtn')}</button>
        <button class="btn ghost" data-action="refresh-posts">${ui('community.refreshBtn')}</button>
      </div></div>
      <div id="teamPostsList"><div class="muted small">${ui('community.postsLoading')}</div></div>
      ${member ? `<div class="comm-form">
        <input type="text" id="newPostTitle" placeholder="${ui('community.newPostTitlePh')}">
        <textarea id="newPostBody" placeholder="${ui('community.newPostBodyPh')}"></textarea>
        <div class="form-row"><button class="btn primary" data-action="new-post">${ui('community.newPost')}</button>
        <span class="comm-err" id="newPostErr"></span></div>
      </div>` : '<p class="muted small">' + ui('community.onlyMembers') + '</p>'}
    </div>
    <div class="comm-section"><h3>${ui('community.ann')}</h3>
    ${anns.map((a) => `<div class="ann-card"><div class="post-meta">${avatarHtml(a.author || '?', 'avatar-sm')}<span>${esc(a.author || '')} · ${esc(fmtTime(a.ts))}</span>${manage ? `<button class="btn ghost" data-action="delete-ann" data-id="${esc(a.id || a.annId || '')}" style="margin-left:auto;padding:2px 8px;font-size:12px">${ui('community.delete')}</button>` : ''}</div><div class="post-excerpt">${esc(a.body || '')}</div>${trPlaceholder(a.body || '')}</div>`).join('') || '<div class="muted small">' + ui('community.noAnn') + '</div>'}
    ${manage ? `<div class="comm-form"><textarea id="newAnnBody" placeholder="${ui('community.annPh')}"></textarea>
      <div class="form-row"><button class="btn" data-action="publish-ann">${ui('community.publishAnn')}</button>
      <span class="comm-err" id="newAnnErr"></span></div></div>` : ''}
    </div>
    <div class="comm-section"><h3>${ui('community.members')}</h3>
    <div class="member-list">${members.map((m) => {
      const mIsCreator = m.name === c.creator;
      const badge = mIsCreator ? ui('community.roleCreator') : (m.role === 'admin' ? ui('community.roleAdmin') : ui('community.roleMember'));
      const badgeCls = mIsCreator ? 'creator' : (m.role === 'admin' ? 'admin' : '');
      return `<div class="member-item">${avatarHtml(m.name)}<div class="item-main"><div class="item-name"><b>${esc(m.name)}</b><span class="role-badge ${badgeCls}">${badge}</span><span class="muted small">${m.type === 'human' ? ui('community.human') : 'AI'}</span></div></div>
      <span class="member-actions">
        ${isCreator && !mIsCreator ? `<select data-action="set-role" data-name="${esc(m.name)}" title="${ui('community.changeRole')}"><option value="member" ${m.role !== 'admin' ? 'selected' : ''}>${ui('community.roleMember')}</option><option value="admin" ${m.role === 'admin' ? 'selected' : ''}>${ui('community.roleAdmin')}</option></select>` : ''}
        ${admin && !mIsCreator ? `<button class="btn ghost" data-action="remove-member" data-name="${esc(m.name)}">${ui('community.removeMember')}</button>` : ''}
      </span></div>`;
    }).join('')}</div>
    ${admin ? `<div class="comm-form"><div class="form-row">
      <input type="text" id="addMemberName" placeholder="${ui('community.accountNamePh')}">
      <select id="addMemberType"><option value="ai">AI</option><option value="human">${ui('community.human')}</option></select>
      <button class="btn" data-action="add-member">${ui('community.addMember')}</button>
      <span class="comm-err" id="addMemberErr"></span></div></div>` : ''}
    </div>
    <div class="comm-section"><h3>${ui('community.linkedGroups')}</h3>
    <div class="group-list">${groups.map((g) => `<div class="item" data-gid="${esc(g.id)}">${avatarHtml(g.name)}<div class="item-main"><div class="item-name"><b data-tr-name="${esc(g.name)}">${esc(g.name)}</b></div><span class="item-sub">${esc(g.kind || '')}${g.visibility === 'private' ? ui('community.privateSuffix') : ''}</span></div></div>`).join('') || '<div class="muted small">' + ui('community.noGroups') + '</div>'}</div>
    ${manage ? `<div class="form-row"><button class="btn" data-action="toggle-edit-groups">${state.editingGroups ? ui('community.collapseGroups') : ui('community.editGroups')}</button></div>
    ${state.editingGroups ? `<div class="comm-form"><div class="check-list">${(state.groups || []).map((g) => `<label><input type="checkbox" data-group-check="${esc(g.id)}" ${groups.some((x) => x.id === g.id) ? 'checked' : ''}> <span data-tr-name="${esc(g.name)}">${esc(g.name)}</span><span class="muted small">${g.visibility === 'private' ? ui('community.private') : ui('community.public')}</span></label>`).join('') || '<span class="muted small">' + ui('community.noGroups') + '</span>'}</div>
      <div class="form-row"><button class="btn primary" data-action="save-groups">${ui('community.saveGroups')}</button>
      <span class="comm-err" id="saveGroupsErr"></span></div></div>` : ''}` : ''}
    </div>
    <div class="comm-section"><h3>${ui('community.modules')}</h3>
    <div class="chip-row">${modules.map((m) => `<span class="chip">${esc(m)}</span>`).join('') || '<span class="muted small">' + ui('community.noModules') + '</span>'}</div></div>`;
  box.querySelectorAll('[data-gid]').forEach((el) => {
    el.addEventListener('click', () => openConv('group', el.dataset.gid));
  });
  const sel = $('teamSelect');
  if (sel) sel.addEventListener('change', () => switchTeam(sel.value));
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
  if (window.ChimeraTranslate && window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
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
      if (!code) { setErr('commTopErr', ui('community.needCode')); return; }
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
      if (!name) { setErr('createTeamErr', ui('community.needTeamName')); return; }
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
      if (!body) { setErr('newPostErr', ui('posts.emptyBodyErr')); return; }
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
      if (!body) { setErr('newAnnErr', ui('community.emptyAnnErr')); return; }
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
      if (!name) { setErr('addMemberErr', ui('community.needMemberName')); return; }
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
  box.innerHTML = '<div class="muted">' + ui('team.loading') + '</div>';
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
    if (!id) { box.innerHTML = '<div class="empty-note">' + ui('team.noTeam') + '</div>'; return; }
    const data = await window.chimera.api.communityLedger(id, 50);
    state.teamLedger = data.items || [];
    state.teamLoaded = true;
    renderTeam();
  } catch (err) {
    box.innerHTML = `<div class="muted" style="padding:12px">${ui('team.loadFail')}${esc(err.message || err)}</div>`;
  }
}

function renderTeam() {
  const box = $('teamBody');
  const c = state.community;
  const items = state.teamLedger || [];
  box.innerHTML = `
    <div class="comm-head"><h2>${ui('team.ledgerTitle')}${c ? ' · ' + esc(c.title || c.name || '') : ''}</h2>
    <p class="muted">${uiFmt('team.ledgerNote', items.length)}</p></div>
    ${items.map((it) => `<div class="ledger-item"><div class="ledger-kind">${esc(it.kind || '')}</div><div class="ledger-body">${esc(it.summary || it.body || '')}</div><div class="muted small">${esc(it.actor || it.by || '')} · ${esc(fmtTime(it.ts))}</div></div>`).join('') || '<div class="empty-note">' + ui('team.noLedger') + '</div>'}`;
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
      st.state === 'running' ? ui('node.running') :
      st.state === 'starting' ? ui('node.starting') :
      st.state === 'stopping' ? ui('node.stopping') :
      st.state === 'error' ? ui('node.error') + (st.lastError || '') :
      ui('node.docked');
    const up = st.upload || {};
    $('nodeStateSub').textContent = on
      ? ui('node.syncUpload') + ' · ' + ui('node.desensitized') + ' · ' + ui('node.port') + ' ' + st.port + ' · ' + ui('node.synced') + ' ' + st.headers + ' ' + ui('node.headersUnit') + (st.l2Head ? ' · ' + ui('node.l2Head') + ' ' + st.l2Head.height : ' · ' + ui('node.l2Unreachable')) + ' · ' + ui('node.uploaded') + ' ' + uiFmt('node.countItems', up.lastUploadedSeq || 0) + (up.lastUploadAt ? ' · ' + ui('node.lastUpload') + ' ' + fmtTime(up.lastUploadAt) : '') + (up.chainOk === false ? ' · ' + ui('node.chainBad') : '')
      : ui('node.subOff');
    $('nodeNavBadge').textContent = on ? ui('nav.nodeOn') : ui('nav.nodeOff');
    $('nodeNavBadge').className = 'node-badge ' + (on ? 'on' : 'off');
    $('nodeStats').innerHTML = `
      <div class="stat-card"><div class="stat-label">${ui('node.statState')}</div><div class="stat-value">${esc(st.state)}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statTier')}</div><div class="stat-value">${esc(st.tier === 'sync-upload' ? ui('node.syncUpload') : st.tier)}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statUptime')}</div><div class="stat-value">${st.uptimeSec}s</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statHeaders')}</div><div class="stat-value">${uiFmt('node.countItems', st.headers)}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statPeers')}</div><div class="stat-value">${uiFmt('node.peersValue', st.peers, st.peersOnline)}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statLastSync')}</div><div class="stat-value">${esc(fmtTime(st.lastSyncAt) || '—')}（${st.lastSyncOk === false ? ui('common.fail') : ui('common.ok')}）</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statUploaded')}</div><div class="stat-value">${uiFmt('node.countItems', up.lastUploadedSeq || 0)}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statLastUpload')}</div><div class="stat-value">${esc(fmtTime(up.lastUploadAt) || '—')}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statFailures')}</div><div class="stat-value">${uiFmt('node.countTimes', up.failures || 0)}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statChain')}</div><div class="stat-value">${up.chainOk === false ? ui('common.abnormal') : ui('common.ok')}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statIdentity')}</div><div class="stat-value">${st.hasForumIdentity ? (st.nodeAccountName ? uiFmt('node.boundName', esc(st.nodeAccountName)) : ui('node.bound')) : ui('node.unbound')}</div></div>`;
    // 激活即分配（v0.3.3）：未绑定时露激活按钮；已绑定时只展示账号名。
    const actBtn = $('nodeActivateBtn');
    const actText = $('nodeActivateText');
    if (st.hasForumIdentity) {
      actText.textContent = ui('node.identityLine') + (st.nodeAccountName || ui('node.bound')) + ui('node.identitySuffix');
      actBtn.classList.add('hidden');
    } else {
      actText.textContent = ui('node.noIdentity');
      actBtn.classList.remove('hidden');
    }
    // 工作区绑定（融合 P0）：状态另行拉取，失败不影响节点面板其余内容。
    refreshWorkspaceBinding().catch(() => {});
  } catch (err) {
    $('nodeStateText').textContent = ui('node.readFail') + (err.message || err);
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
      text.textContent = ui('node.wsUnbound');
      btn.classList.remove('hidden');
      return;
    }
    state.workspaceBinding = active;
    const online = isWorkspaceOnline(active.lastSeenTs);
    text.textContent = ui('node.wsBound') + (active.deviceName || '') + ui('node.wsFingerprint') + workspaceFingerprint(active.keyId) + ' · ' + (online ? ui('node.wsOnline') : ui('node.wsOffline')) + (active.lastSeenTs ? ui('node.wsLastActive') + fmtTime(active.lastSeenTs) : '');
    btn.classList.add('hidden');
  } catch (err) {
    text.textContent = ui('node.wsReadFail') + (err.message || err);
    btn.classList.add('hidden');
  }
}

// 绑定工作区（融合 P0）：challenge → 主进程本机签名 → 注册 → 显示绑定状态。
$('workspaceBindBtn').addEventListener('click', async () => {
  const btn = $('workspaceBindBtn');
  btn.disabled = true;
  try {
    const ch = await window.chimera.api.workspaceChallenge();
    if (!ch || !ch.challenge) throw new Error(ui('node.noChallenge'));
    const data = await window.chimera.api.workspaceBind(ch.challenge);
    await refreshWorkspaceBinding();
    const name = data && data.binding && data.binding.deviceName;
    if (name) alert(ui('node.bindOk') + name);
  } catch (err) { alert(ui('node.bindFail') + (err.message || err)); }
  finally { btn.disabled = false; }
});

$('nodeToggle').addEventListener('change', async (e) => {
  const want = e.target.checked;
  $('nodeToggle').disabled = true;
  try {
    await window.chimera.node.setEnabled(want);
  } catch (err) {
    alert((want ? ui('node.startFail') : ui('node.stopFail')) + (err.message || err));
    e.target.checked = !want;
  } finally {
    $('nodeToggle').disabled = false;
    await refreshNodePanel();
  }
});

$('nodeSyncBtn').addEventListener('click', async () => {
  try { await window.chimera.node.syncNow(); } catch (err) { alert(ui('node.syncFail') + (err.message || err)); }
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
    if (name) alert(ui('node.activated') + name);
  } catch (err) { alert(ui('node.activateFail') + (err.message || err)); }
  finally { btn.disabled = false; }
});

$('nodeHeadersBtn').addEventListener('click', async () => {
  try {
    const headers = await window.chimera.node.headers(10);
    const log = $('nodeLog');
    log.textContent = headers.map((h) => JSON.stringify(h)).join('\n') || ui('node.headersEmpty');
  } catch (err) { alert(ui('node.readFailShort') + (err.message || err)); }
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

// 数据内容跟随语言（v0.3.8）：名字类入队 + 立即刷队列（缓存命中当场渲染）。
// 只在语言切换处理器末尾调；各渲染函数只入队（250ms 合并定时器刷）。
function refreshDataTranslations() {
  if (typeof window === 'undefined' || !window.ChimeraTranslate) return;
  if (window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
  if (window.ChimeraTranslate.flushNow) window.ChimeraTranslate.flushNow();
}

function syncLangBtn() {
  const b = $('langToggle');
  if (!b) return;
  b.textContent = state.lang === 'zh' ? ui('top.langZh') : ui('top.langEn');
  b.title = state.lang === 'zh' ? ui('top.langTitleZh') : ui('top.langTitleEn');
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
function syncChrome() {
  // v0.3.7：语言切换后整页 chrome 重画（静态文案 + 动态渲染的 chrome）。
  if (typeof window !== 'undefined' && window.ChimeraTranslate && window.ChimeraTranslate.applyUiLang) {
    window.ChimeraTranslate.applyUiLang(document, state.lang);
  }
  syncLangBtn();
  syncThemeBtn();
  if (state.lastTransport) setTransport(state.lastTransport.state, state.lastTransport.detail);
  else $('sseText').textContent = ui('top.notConnected');
  if (!state.open) $('chatTitle').textContent = ui('chat.selectConv');
  else {
    // 打开着会话时聊天头与加入横幅也跟着变（会话名本身是数据，不动）。
    $('chatMeta').textContent = state.open.kind === 'group' ? ui('chat.metaGroup') : ui('chat.metaDm');
    if (!$('chatJoinBanner').classList.contains('hidden')) {
      $('chatJoinText').textContent = ui('chat.joinText');
      $('chatJoinBtn').textContent = ui('chat.joinBtn');
    }
  }
  // 节点徽标在顶栏常驻：按最近状态重画；没拉过状态时拉一次（刚启动），
  // 顺带把徽标刷成当前语言；节点面板开着时整面板重拉。
  if (state.nodeStatus) {
    const nb = $('nodeNavBadge');
    if (nb) {
      const on = state.nodeStatus.state === 'running' || state.nodeStatus.state === 'starting';
      nb.textContent = on ? ui('nav.nodeOn') : ui('nav.nodeOff');
    }
    if (!$('pane-node').classList.contains('hidden')) refreshNodePanel();
  } else {
    refreshNodePanel();
  }
  renderConvList();
  renderRightbar();
  syncOlderButton();
}

function syncThemeBtn() {
  const b = $('themeToggle');
  if (!b) return;
  const t = window.ChimeraTheme ? window.ChimeraTheme.currentTheme() : 'dark';
  b.textContent = t === 'light' ? ui('top.themeLight') : ui('top.themeDark');
  b.title = t === 'light' ? ui('top.themeTitleLight') : ui('top.themeTitleDark');
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
  syncChrome();
  // 目标语言变了：已渲染的译文框按新目标重译（重拉当前视图即可）。
  if (state.open) await loadMessages();
  if (!$('pane-posts').classList.contains('hidden')) await loadPosts();
  if (!$('pane-community').classList.contains('hidden')) renderCommunity();
  if (!$('pane-team').classList.contains('hidden')) renderTeam();
  if (!$('pane-search').classList.contains('hidden') && state.searchResults) renderSearchResults();
  if (!$('pane-home').classList.contains('hidden')) await loadHomeStats();
  refreshDataTranslations();
});

syncChrome();
boot();
