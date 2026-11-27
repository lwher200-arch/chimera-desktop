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

// 主进程中文 throw 改错误码（v0.3.13）：IPC 只透传 message，code 嵌在
// 消息里（ERR_CODE 或 ERR_CODE: 细节）。有码且字典有对应 err.* 文案时
// 翻译显示；无码/无对应文案时原样显示服务端原话，不自己编理由。
function errText(err) {
  const m = String((err && err.message) || err || '');
  const mm = m.match(/^(ERR_[A-Z0-9_]+)(?::\s?(.*))?$/s);
  if (!mm) return m;
  const key = {
    ERR_LOGIN_NO_SESSION: 'err.loginNoSession',
    ERR_NEED_LOGIN: 'err.needLogin',
    ERR_BIND_NO_CHALLENGE: 'err.bindNoChallenge',
    ERR_BIND_ALREADY_BOUND: 'err.bindAlreadyBound',
    ERR_DO_REGISTER_FAILED: 'err.doRegisterFailed',
    ERR_NODE_ALREADY_ACTIVATED: 'err.nodeAlreadyActivated',
    ERR_NODE_ACTIVATE_NO_ACCOUNT: 'err.nodeActivateNoAccount',
    ERR_NODE_TOKEN_OUTSIDE_DIR: 'err.nodeTokenOutsideDir',
  }[mm[1]];
  if (!key) return m;
  if (mm[2] !== undefined && mm[2] !== '') return uiFmt(key, mm[2]);
  return ui(key);
}

// 消息窗口：DOM 里最多留多少条消息；每页向前翻多少条（服务端 paginate
// 的 limit 上限就是 100，再大也会被截到 100）。
const MAX_RENDERED = 300;
const PAGE_SIZE = 100;
// 主帖子页：每页拉多少条（与旧版首屏 30 条一致）；与消息列表同口径：
// before_id 向前翻 + since 增量拉新（服务端 paginate / since 均已核实）。
const FEED_PAGE_SIZE = 30;
const FEED_MAX_AUTO_PAGES = 5; // “加载更多”一次点击最多自动续拉几页（板块过滤整页被滤掉时）
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
  feedHasMore: false, // 服务端 has_more：是否还有更早的帖子可翻
  feedLoadingOlder: false, // 正在向前翻页
  feedCursor: null, // 已拉的最老一条流帖 id（翻页 before_id，取自原始回包、过滤前）
  feedNewestTs: null, // 已见最新一条流帖的 ts（增量拉新 since 水位）
  searchQuery: '', // 全站搜索当前关键词
  searchResults: null, // 全站搜索结果 {posts, messages, totalPosts, totalMessages}
  homeHealth: null, // 首页统计最近一次 /api/health 回包（语言切换只重画不重拉）
  homeLeaders: [], // 首页积分榜最近一次 leaders
  profileName: null, // 资料卡当前账号名
  profileFollow: null, // 资料卡当前关注状态（GET /api/follow/:name 回包）
  editingProfile: false, // 是否在编辑自己的资料
  lang: 'zh', // 翻译目标语言，默认中文（localStorage chimera_lang）
  lastTransport: null, // 最近一次 realtime:transport，供语言切换时重画指示器
  postsDraftBoard: null, // 帖子编辑框当前草稿所属板块（切板时先存旧板草稿再恢复新板）
  notif: { items: [], unread: 0, readTs: null, mentionEnabled: true, loaded: false }, // 通知中心（v0.3.15，只做 @提醒）
  notifTimer: null, // 通知 60 秒轮询定时器（登录后起，登出/过期停）
  settings: null, // 设置页最近一次只读数据（loadSettings 回包）
  outbox: { pending: 0, failed: 0, total: 0, entries: [] }, // 发件箱最近状态（主进程推 + 主动拉）
  outboxFlash: false, // 刚入箱时状态行前加“已存入发件箱”提示，5 秒后消
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

// 运行时长格式化（v0.3.13）：秒数按量级折成小时/分/秒，不过字典，不裸显 's'。
function fmtUptime(sec) {
  const s = Math.max(0, Math.floor(Number(sec) || 0));
  if (s >= 3600) return uiFmt('node.uptimeHours', Math.floor(s / 3600), Math.floor((s % 3600) / 60));
  if (s >= 60) return uiFmt('node.uptimeMins', Math.floor(s / 60), s % 60);
  return uiFmt('node.uptimeSecs', s);
}

// 输入防抖（v0.3.13）：高频 input 事件只在停顿后跑一次，避免每敲一字
// 就 innerHTML 重建整个会话列表。返回的函数带 .cancel()（测试/卸载用）。
function debounce(fn, ms) {
  let timer = null;
  function debounced(...args) {
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => { timer = null; fn(...args); }, ms);
  }
  debounced.cancel = () => { if (timer) { clearTimeout(timer); timer = null; } };
  return debounced;
}

// 资料卡 role/status 过字典（v0.3.13）：服务端回英文原值，中文模式
// 不应裸显英文、英文模式不应带中文前缀；未知值原样显示，不编造。
function profileRoleText(role) {
  if (role === 'admin') return ui('profile.roleAdmin');
  if (role === 'creator') return ui('profile.roleCreator');
  if (role === 'member' || !role) return ui('profile.roleMember');
  return String(role);
}
function profileStatusText(status) {
  if (status === 'suspended') return ui('profile.statusSuspended');
  if (status === 'retired') return ui('profile.statusRetired');
  return String(status || '');
}

// 节点状态卡状态值过字典（v0.3.13）：运行中/启动中/停止中/出错/已停靠；
// 未知状态原样显示，不编造。
function nodeStateText(s) {
  if (s === 'running') return ui('node.stateRunning');
  if (s === 'starting') return ui('node.stateStarting');
  if (s === 'stopping') return ui('node.stateStopping');
  if (s === 'error') return ui('node.stateError');
  if (s === 'off' || !s) return ui('node.stateOff');
  return String(s);
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

// 帖子列表纯函数（v0.3.14，可单测）：置顶恒在前，流按时间正序。
// mergeOlderFeed：更早一页（已按板过滤）插到现有流之前、置顶之后；按 id 去重。
function mergeOlderFeed(existing, older) {
  const seen = new Set((existing || []).map((p) => p && p.id).filter(Boolean));
  const fresh = (older || []).filter((p) => p && p.id && !seen.has(p.id));
  if (!fresh.length) return (existing || []).slice();
  const pinned = (existing || []).filter((p) => p.pinned);
  const stream = (existing || []).filter((p) => !p.pinned);
  return [...pinned, ...fresh, ...stream];
}
// mergeNewerFeed：增量拉到的新帖（已按板过滤）追加到末尾（流时间正序）；按 id 去重。
function mergeNewerFeed(existing, newer) {
  const seen = new Set((existing || []).map((p) => p && p.id).filter(Boolean));
  const fresh = (newer || []).filter((p) => p && p.id && !seen.has(p.id));
  return (existing || []).concat(fresh);
}
// feedCursorOf：翻页游标 = 原始回包流帖里最老一条的 id（过滤前取，免得
// 板块过滤把最老几条滤掉后游标错位、反复拉同一页）。回包流帖按时间正序。
function feedCursorOf(pagePosts) {
  const list = pagePosts || [];
  if (!list.length) return null;
  return list[0].id || null;
}
// feedNewestTsOf：增量水位 = 原始回包流帖里最新一条的 ts（过滤前取）。
function feedNewestTsOf(pagePosts) {
  const list = pagePosts || [];
  let ts = null;
  for (const p of list) {
    if (p && p.ts && (!ts || Date.parse(p.ts) > Date.parse(ts))) ts = p.ts;
  }
  return ts;
}

// 通知中心（v0.3.15，只做 @提醒）与草稿：纯函数，Node 可测。
// filterMentionNotifications：服务端回全类型（mention/reply/like/follow），
// 桌面端只展示 type === 'mention' 的条目（含帖子 @ 与群消息 @）。
function filterMentionNotifications(items) {
  return (items || []).filter((n) => n && n.type === 'mention');
}
// notifUnreadCount：@未读 = ts 晚于本地水位 readTs 的 @条目数；
// 水位为空（首跑、服务端 publicAccount 不暴露水位）时全部计未读，
// 点一次“全部已读”后水位即写到当前时间，后续计数即准；
// 时间戳解析失败的条目计未读（宁可多提醒，不漏 @）。
function notifUnreadCount(mentions, readTs) {
  const list = mentions || [];
  if (!readTs) return list.length;
  const water = Date.parse(readTs);
  if (!Number.isFinite(water)) return list.length;
  let n = 0;
  for (const m of list) {
    const t = Date.parse(m.ts || '');
    if (!Number.isFinite(t) || t > water) n += 1;
  }
  return n;
}
// notifVerb：@提醒动词（与网页版 notif.verb_mention 同口径）；非 @ 类型不渲染。
function notifVerb(n) {
  if (n && n.type === 'mention') return ui('notif.verbMention');
  return '';
}
// notifTarget：点条目跳哪——有 postId 看帖子、有 groupId 进群聊、
// 否则看 @人资料卡（关注类无 postId/groupId）。
function notifTarget(n) {
  if (!n) return { kind: 'profile', name: '' };
  if (n.postId) return { kind: 'post', id: n.postId };
  if (n.groupId) return { kind: 'group', id: n.groupId };
  return { kind: 'profile', name: (n.actor && n.actor.name) || '' };
}

// 草稿（v0.3.15）：没发出去的文字，按作用域存 localStorage，跨重启恢复；
// 关应用不自动发（与发件箱已提交的消息严格区分）。storage 经 window 传，
// Node 测试可注入假 localStorage。
function draftKey(scope) { return 'chimera_draft:' + String(scope || ''); }
function draftStorage(win) {
  try {
    const w = win || (typeof window !== 'undefined' ? window : null);
    return (w && w.localStorage) || null;
  } catch { return null; }
}
function loadDraft(scope, win) {
  const st = draftStorage(win);
  if (!st) return '';
  try { return st.getItem(draftKey(scope)) || ''; } catch { return ''; }
}
function saveDraft(scope, text, win) {
  const st = draftStorage(win);
  if (!st) return;
  try {
    if (text) st.setItem(draftKey(scope), String(text));
    else st.removeItem(draftKey(scope));
  } catch { /* 草稿存失败不挡输入 */ }
}
function clearDraft(scope, win) { saveDraft(scope, '', win); }
// draftKeyForOpen：当前打开会话的草稿作用域（依赖 state，Node 可测时传 state）。
function draftKeyForOpen() {
  return state.open ? 'conv:' + state.open.kind + ':' + state.open.id : null;
}
// countDrafts：设置页只读展示——本地有几个作用域存了非空草稿。
function countDrafts(win) {
  const st = draftStorage(win);
  if (!st) return 0;
  try {
    let n = 0;
    for (let i = 0; i < st.length; i += 1) {
      const k = st.key(i);
      if (String(k || '').startsWith('chimera_draft:') && st.getItem(k)) n += 1;
    }
    return n;
  } catch { return 0; }
}
// outboxStatusText：发件箱状态行文案（纯函数）。flash 为真时前加
// “已存入发件箱，联网后自动发送”；失败条目带本地丢弃按钮（只丢本地
// 副本，不触服务端）。entries 里 dead 的才算失败。
function outboxStatusText(st, flash) {
  const s = st || { pending: 0, failed: 0, total: 0, entries: [] };
  const parts = [];
  if (flash) parts.push(ui('outbox.queued'));
  if (s.pending > 0) parts.push(uiFmt('outbox.pending', s.pending));
  const failed = (s.entries || []).filter((e) => e && e.dead);
  if (failed.length) {
    parts.push(uiFmt('outbox.failed', failed.length));
    for (const e of failed) {
      const preview = String(e.body || '').slice(0, 30);
      const title = ui('outbox.failedTitle') + (preview ? '：' + preview : '') + (e.lastError ? ' — ' + e.lastError : '');
      parts.push('<span class="failed-entry">' + esc(title) + ' <button class="btn ghost" data-discard-outbox="' + esc(e.clientId || '') + '">' + ui('outbox.discard') + '</button></span>');
    }
  }
  return parts.join(' · ');
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
  if (st === 'session-expired') resetToLogin();
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
    $('loginErr').textContent = ui('login.failErr') + errText(err);
  }
});
$('loginPass').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('loginBtn').click(); });

// 会话失效回登录（v0.3.13）：登出与 401 过期共用；只清本地 UI 状态，
// 过期时不调 logout（服务端会话已死，登出接口也会 401）。
function resetToLogin() {
  state.me = null; state.open = null;
  stopNotifTimer();
  state.notif = { items: [], unread: 0, readTs: null, mentionEnabled: true, loaded: false };
  state.settings = null;
  paintNotifBadge();
  $('whoami').textContent = '';
  delete $('whoami').dataset.profile;
  setMeChip();
  $('logoutBtn').classList.add('hidden');
  show('login');
}

$('logoutBtn').addEventListener('click', async () => {
  await window.chimera.session.logout();
  resetToLogin();
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
  $('pane-notif').classList.toggle('hidden', v !== 'notif');
  $('pane-settings').classList.toggle('hidden', v !== 'settings');
  if (v === 'posts') {
    // 切板/切页时先把当前框里的草稿存回旧板，再恢复新板的草稿。
    // 框空时不存：空串会清键，而首开时框空正是因为草稿还没恢复。
    const curPostDraft = ($('feedPostBody') || {}).value || '';
    if (curPostDraft) saveDraft('post:' + (state.postsDraftBoard || 'all'), curPostDraft);
    loadPosts();
    state.postsDraftBoard = state.postsBoard || 'all';
    const fb = $('feedPostBody');
    if (fb) fb.value = loadDraft('post:' + state.postsDraftBoard);
  }
  if (v === 'notif') loadNotifications();
  if (v === 'settings') loadSettings();
  if (v === 'search') { if (state.searchResults) renderSearchResults(); }
  if (v === 'node') refreshNodePanel();
  if (v === 'community') loadCommunity();
  if (v === 'team') loadTeam();
  layoutTopNav();
}
document.querySelectorAll('.nav-btn').forEach((b) => {
  if (!b.dataset.view) return; // “更多”按钮无 data-view，自己管开关菜单
  b.addEventListener('click', () => showPane(b.dataset.view));
});
document.querySelectorAll('.home-card').forEach((b) => {
  b.addEventListener('click', () => showPane(b.dataset.view));
});

// 窄屏导航收进“更多”（v0.3.13）：溢出项恒为后缀；纯函数可单测。
// widths 是各导航按钮宽（含 gap），moreWidth 是“更多”按钮宽（含 gap）。
const NAV_ORDER = ['home', 'conv', 'posts', 'search', 'community', 'team', 'node'];
function greedyNavFit(widths, containerWidth, moreWidth) {
  let total = widths.reduce((a, b) => a + b, 0);
  let n = widths.length;
  while (n > 0 && total + (n < widths.length ? moreWidth : 0) > containerWidth) {
    n -= 1;
    total -= widths[n];
  }
  return { visible: widths.slice(0, n).map((_, i) => i), overflow: widths.slice(n).map((_, i) => i + n) };
}

function layoutTopNav() {
  const nav = document.querySelector('.topnav-links');
  const menu = $('navMoreMenu');
  const moreBtn = $('navMoreBtn');
  if (!nav || !menu || !moreBtn) return;
  // 先把所有导航按钮归位（从菜单里拿回来），按固定顺序排进 nav。
  const btns = NAV_ORDER.map((v) => nav.querySelector(`[data-view="${v}"]`) || menu.querySelector(`[data-view="${v}"]`)).filter(Boolean);
  btns.forEach((b) => nav.insertBefore(b, moreBtn));
  const widths = btns.map((b) => (b.offsetWidth || 0) + 4);
  const fit = greedyNavFit(widths, nav.clientWidth, (moreBtn.offsetWidth || 0) + 4);
  const overflowSet = new Set(fit.overflow);
  btns.forEach((b, i) => { if (overflowSet.has(i)) menu.appendChild(b); });
  moreBtn.classList.toggle('hidden', fit.overflow.length === 0);
  if (fit.overflow.length === 0) menu.classList.add('hidden');
  // 当前页的按钮被收进菜单时，“更多”按钮标 active，给用户一个位置提示。
  moreBtn.classList.toggle('active', btns.some((b, i) => overflowSet.has(i) && b.classList.contains('active')));
}

$('navMoreBtn').addEventListener('click', (e) => {
  e.stopPropagation();
  const menu = $('navMoreMenu');
  if (menu.classList.contains('hidden')) {
    const r = $('navMoreBtn').getBoundingClientRect();
    menu.style.top = `${r.bottom + 6}px`;
    menu.style.left = `${Math.max(8, r.left)}px`;
    menu.classList.remove('hidden');
  } else {
    menu.classList.add('hidden');
  }
});
$('navMoreMenu').addEventListener('click', () => $('navMoreMenu').classList.add('hidden'));
document.addEventListener('click', (e) => {
  const menu = $('navMoreMenu');
  if (!menu || menu.classList.contains('hidden')) return;
  if (menu.contains(e.target) || $('navMoreBtn').contains(e.target)) return;
  menu.classList.add('hidden');
});
window.addEventListener('resize', layoutTopNav);
window.addEventListener('load', layoutTopNav);

// ---------- conv list ----------
$('tabGroups').addEventListener('click', () => { state.tab = 'groups'; syncTabs(); renderConvList(); });
$('tabFriends').addEventListener('click', () => { state.tab = 'friends'; syncTabs(); renderConvList(); });
$('convSearch').addEventListener('input', debounce(renderConvList, 150));

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
    loadNotifications(); // 通知徽标：登录后即刷一次，之后 60 秒轮询
    startNotifTimer();
    refreshOutboxStatus(); // 发件箱状态行：上次离线未发的条目一进应用就可见
  } catch (err) {
    $('convItems').innerHTML = `<div class="muted" style="padding:12px">${ui('common.loadFail')}${esc(err.message || err)}</div>`;
  }
}

// 首页数据化（v0.3.4）：健康计数 + 积分榜前三；任一失败只留空，不挡主界面。
// 拉取与渲染已拆开（v0.3.13）：语言切换只调 renderHomeStats 同步重画，
// 不重拉接口；失败不清空已有内容（缓存留上次成功的）。
async function loadHomeStats() {
  try {
    state.homeHealth = await window.chimera.api.health();
  } catch { /* 健康拉不到就留上次缓存，不清屏、不挡首页 */ }
  try {
    const lb = await window.chimera.api.leaderboard(3);
    state.homeLeaders = (lb && lb.leaders) || [];
  } catch { /* 榜单拉不到就不显示，不挡首页 */ }
  renderHomeStats();
}

function renderHomeStats() {
  const box = $('homeStats');
  if (!box) return;
  const h = state.homeHealth;
  if (h) {
    const counts = [[ui('home.statPosts'), h.posts], [ui('home.statGroups'), h.groups], [ui('home.statAccounts'), h.accounts], [ui('home.statMsgs'), h.messages]];
    box.innerHTML = counts.map(([label, n]) => `<div class="home-stat"><b>${typeof n === 'number' ? n : '—'}</b><span>${label}</span></div>`).join('');
  }
  const rows = state.homeLeaders || [];
  if (Array.isArray(rows) && rows.length) {
    $('homeBoard').classList.remove('hidden');
    $('homeBoardList').innerHTML = rows.slice(0, 3).map((r) => `<li>${esc(r.name || '')} · ${esc(uiFmt('home.points', r.points ?? ''))}</li>`).join('');
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
  // 切会话前先把当前框里的草稿存回旧会话（300ms 防抖可能还没跑）。
  if (state.open) saveDraft(draftKeyForOpen(), $('chatInput').value);
  // 会话列表在左侧栏常驻，点会话时先切回会话面板（在帖子/节点页点会话也一样）。
  showPane('conv');
  const name = kind === 'group'
    ? (state.groups.find((g) => g.id === id)?.name || id)
    : id;
  state.open = { kind, id, name };
  $('chatInput').value = loadDraft(draftKeyForOpen()); // 草稿跨重启恢复（v0.3.15）
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
  box.innerHTML = list.map((m) => messageHtml(m)).join('') || '<div class="chat-empty" data-empty="1"><img src="assets/brand.png" alt="' + ui('brand.name') + '"><b>' + ui('chat.noMsg') + '</b><span>' + ui('chat.noMsgSub') + '</span></div>';
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
    let r;
    if (state.open.kind === 'group') {
      r = await window.chimera.api.sendGroupMessage(state.open.id, body, attachments);
    } else {
      r = await window.chimera.api.sendDm(state.open.id, body, attachments);
    }
    $('chatInput').value = '';
    clearDraft(draftKeyForOpen()); // 发出去的文字不再是草稿（入箱也算提交）
    state.pending = [];
    renderPending();
    if (r && r.queued) {
      // 离线入箱：服务端没收到，不拉新消息；重连后主进程自动发。
      queueFlash();
      await refreshOutboxStatus();
      return;
    }
    // 自己发的消息：增量拉一次即可（回声事件到时按 id 去重，不会 double）。
    await fetchNewMessages(false);
  } catch (err) {
    alert(ui('chat.sendFail') + (err.message || err));
  }
}

$('chatSendBtn').addEventListener('click', sendCurrent);
$('chatInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') sendCurrent(); });
// 草稿（v0.3.15）：打字防抖存草稿，切会话前、发送后即清；跨重启恢复。
$('chatInput').addEventListener('input', debounce(() => {
  const k = draftKeyForOpen();
  if (k) saveDraft(k, $('chatInput').value);
}, 300));

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

// 帖子页统一取页：all 走 GET /api/posts，following 走 /api/feed/following；
// 两者同走服务端 splitPinned/paginate（before_id + limit + since 均支持）。
async function fetchFeedPage(opts) {
  return state.postsFeed === 'following'
    ? window.chimera.api.followingFeed(opts)
    : window.chimera.api.posts(opts);
}

async function loadPosts() {
  const box = $('postsList');
  if (!box) return;
  syncFeedTabs();
  try {
    const data = await fetchFeedPage({ limit: FEED_PAGE_SIZE });
    const all = [...(data.pinned || []), ...(data.posts || [])];
    state.feedPosts = filterPostsByBoard(all, state.postsBoard);
    // 游标与水位取自原始回包（过滤前），免得板块过滤后错位。
    state.feedCursor = feedCursorOf(data.posts || []);
    const newest = feedNewestTsOf(data.posts || []);
    if (newest) state.feedNewestTs = newest;
    state.feedHasMore = !!(data && data.has_more);
    renderFeedPosts();
    syncFeedOlderButton();
  } catch (err) {
    // 帖子拉失败只影响帖子列表本身：板块栏是 loadBoards 常驻的，不动它。
    box.innerHTML = `<div class="muted" style="padding:12px">${ui('posts.loadFail')}${esc(err.message || err)}</div>`;
  }
}

// 向前翻一页：用最老一条流帖的 id 当 before_id，服务端只回它之前的
// FEED_PAGE_SIZE 条；新卡插到置顶之后、现有流之前（不重建整个列表，
// 只给新卡入队翻译）。板块过滤整页被滤掉时自动续拉（最多 5 页），
// 免得点一次什么都没多出来。DOM 不设上限：总量受服务端帖子数约束
// （约 500 张卡），且 v0.3.14 起屏外翻译占位不再入队，成本只有 DOM 节点。
async function loadOlderPosts() {
  if (state.feedLoadingOlder || !state.feedHasMore || !state.feedCursor) return;
  state.feedLoadingOlder = true;
  syncFeedOlderButton();
  try {
    let added = [];
    let pages = 0;
    while (pages < FEED_MAX_AUTO_PAGES) {
      pages += 1;
      const data = await fetchFeedPage({ limit: FEED_PAGE_SIZE, before_id: state.feedCursor });
      const raw = data.posts || [];
      state.feedCursor = feedCursorOf(raw) || state.feedCursor;
      state.feedHasMore = !!(data && data.has_more);
      const visible = filterPostsByBoard(raw, state.postsBoard);
      if (visible.length) { added = visible; break; }
      if (!state.feedHasMore || !raw.length) break;
    }
    if (added.length) {
      // 防御去重：before_id 游标保证页与页不重叠，但刷新并发时仍可能撞上
      // 已有卡；只插确实新的卡，DOM 与 state.feedPosts 保持一致。
      const seenIds = new Set(state.feedPosts.map((x) => x && x.id).filter(Boolean));
      const fresh = added.filter((x) => x && x.id && !seenIds.has(x.id));
      if (!fresh.length) return;
      const firstStreamId = (state.feedPosts.find((p) => !p.pinned) || {}).id || null;
      state.feedPosts = mergeOlderFeed(state.feedPosts, fresh);
      insertFeedCards(fresh, firstStreamId);
    }
  } catch { /* 翻页失败不清空已有内容，按钮还在，下次再点 */ }
  finally {
    state.feedLoadingOlder = false;
    syncFeedOlderButton();
  }
}

function syncFeedOlderButton() {
  const row = $('feedOlderRow');
  const btn = $('feedOlderBtn');
  if (!row || !btn) return;
  const show = state.feedHasMore || state.feedLoadingOlder;
  row.classList.toggle('hidden', !show);
  btn.disabled = state.feedLoadingOlder || !state.feedHasMore;
  btn.textContent = state.feedLoadingOlder ? ui('posts.loading') : ui('posts.loadOlder');
}

$('feedOlderBtn').addEventListener('click', loadOlderPosts);

// 只插新卡：更早一页插到第一张流卡之前（置顶之后），新帖追加到末尾；
// 只给新卡入队翻译，不重扫整个列表。
function insertFeedCards(posts, beforeId) {
  const box = $('postsList');
  if (!box || !posts.length) return;
  const empty = box.querySelector('.empty-note');
  if (empty) empty.remove();
  const me = state.me;
  const owner = isForumOwner(me);
  const html = posts.map((p) => {
    const mine = me && p.author === me.username;
    return postCardHtml(p, mine || owner);
  }).join('');
  if (beforeId) {
    const anchor = box.querySelector('[data-post="' + beforeId + '"]');
    if (anchor && anchor.insertAdjacentHTML) anchor.insertAdjacentHTML('beforebegin', html);
    else box.insertAdjacentHTML('afterbegin', html);
  } else {
    box.insertAdjacentHTML('beforeend', html);
  }
  if (window.ChimeraTranslate) window.ChimeraTranslate.observeTranslations(box);
  if (window.ChimeraTranslate && window.ChimeraTranslate.observeNameTranslations) window.ChimeraTranslate.observeNameTranslations(document);
}

// 增量拉新：SSE 新帖事件只拉 since= 水位之后的新帖，追加到末尾，不重建
// 整个列表、不重入队已有卡的翻译。since 回包不带置顶（服务端 hasFilter
// 口径），正好只要流帖。raw 为空时全量重拉一次（防同毫秒撞 ts 漏帖）；
// raw 有数但全被板块过滤掉时是预期内，不重拉。
async function fetchNewFeedPosts() {
  if (!state.feedNewestTs) { await loadPosts(); return; }
  try {
    const data = await fetchFeedPage({ since: state.feedNewestTs });
    const raw = data.posts || [];
    if (!raw.length) { await loadPosts(); return; }
    const newest = feedNewestTsOf(raw);
    if (newest) state.feedNewestTs = newest;
    const visible = filterPostsByBoard(raw, state.postsBoard);
    // 防御去重：刷新并发时 since 回包可能撞上已在列表里的卡，只插新的。
    const seenIds = new Set(state.feedPosts.map((x) => x && x.id).filter(Boolean));
    const fresh = visible.filter((x) => x && x.id && !seenIds.has(x.id));
    if (!fresh.length) return;
    state.feedPosts = mergeNewerFeed(state.feedPosts, fresh);
    insertFeedCards(fresh, null);
  } catch { try { await loadPosts(); } catch { /* 下次事件再试 */ } }
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
    const r = await window.chimera.api.createPost({ board: state.postsBoard || 'general', title: '', body });
    $('feedPostBody').value = '';
    clearDraft('post:' + (state.postsBoard || 'all')); // 发出去的文字不再是草稿（入箱也算提交）
    if (r && r.queued) {
      // 离线入箱：服务端没收到，不拉新帖；重连后主进程自动发。
      queueFlash();
      await refreshOutboxStatus();
      return;
    }
    loadPosts();
  } catch (err) { if (errEl) errEl.textContent = err.message || err; }
  finally { t.disabled = false; }
});
// 草稿（v0.3.15）：帖子编辑框按板块存草稿，跨重启恢复。
$('feedPostBody').addEventListener('input', debounce(() => {
  state.postsDraftBoard = state.postsBoard || 'all';
  saveDraft('post:' + state.postsDraftBoard, $('feedPostBody').value);
}, 300));
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
      <div class="muted small">${acc.type === 'human' ? ui('profile.human') : acc.type === 'ai' ? ui('profile.ai') : ui('profile.node')} · ${esc(profileRoleText(acc.role))} · ${esc(acc.level || '')} · ${uiFmt('profile.points', acc.points ?? 0)}${acc.status && acc.status !== 'active' ? ' · ' + esc(profileStatusText(acc.status)) : ''}${acc.lastActiveTs ? ui('profile.lastActive') + esc(fmtTime(acc.lastActiveTs)) : ''}</div></div>
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
      const r = await window.chimera.api.createPost({ board: null, title: '', body, replyTo: t.dataset.id });
      state.replyingTo = null;
      if (r && r.queued) {
        // 离线入箱：服务端没收到，不拉新帖；重连后主进程自动发。
        queueFlash();
        await refreshOutboxStatus();
        ctx.render();
        return;
      }
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
        <div id="teamOutboxLine" class="outbox-line hidden"></div>
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
  renderOutboxLine(); // 团队表单是 innerHTML 重建，状态行每次重画后补回来
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
        const r = await window.chimera.api.createCommunityPost(c.id, { title, body });
        $('newPostTitle').value = ''; $('newPostBody').value = '';
        if (r && r.queued) {
          // 离线入箱：服务端没收到，不拉新帖；重连后主进程自动发。
          queueFlash();
          await refreshOutboxStatus();
          return;
        }
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
      <div class="stat-card"><div class="stat-label">${ui('node.statState')}</div><div class="stat-value">${esc(nodeStateText(st.state))}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statTier')}</div><div class="stat-value">${esc(st.tier === 'sync-upload' ? ui('node.syncUpload') : st.tier)}</div></div>
      <div class="stat-card"><div class="stat-label">${ui('node.statUptime')}</div><div class="stat-value">${fmtUptime(st.uptimeSec)}</div></div>
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
  } catch (err) { alert(ui('node.bindFail') + errText(err)); }
  finally { btn.disabled = false; }
});

$('nodeToggle').addEventListener('change', async (e) => {
  const want = e.target.checked;
  $('nodeToggle').disabled = true;
  try {
    await window.chimera.node.setEnabled(want);
  } catch (err) {
    alert((want ? ui('node.startFail') : ui('node.stopFail')) + errText(err));
    e.target.checked = !want;
  } finally {
    $('nodeToggle').disabled = false;
    await refreshNodePanel();
  }
});

$('nodeSyncBtn').addEventListener('click', async () => {
  try { await window.chimera.node.syncNow(); } catch (err) { alert(ui('node.syncFail') + errText(err)); }
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
  } catch (err) { alert(ui('node.activateFail') + errText(err)); }
  finally { btn.disabled = false; }
});

$('nodeHeadersBtn').addEventListener('click', async () => {
  try {
    const headers = await window.chimera.node.headers(10);
    const log = $('nodeLog');
    log.textContent = headers.map((h) => JSON.stringify(h)).join('\n') || ui('node.headersEmpty');
  } catch (err) { alert(ui('node.readFailShort') + (err.message || err)); }
});

// ---------- 通知中心（v0.3.15，只做 @提醒） ----------
// 入口：顶栏铃铛（角标 = @未读数，99+ 封顶）；列表只显示 type === 'mention'；
// 未读按本地水位算（服务端 publicAccount 不暴露 notifReadTs，首跑全部计未读）；
// 全部已读调 POST /api/notifications/read，服务端水位与本地水位同步写当前时间。
// 401 走 v0.3.13 的 session:expired（guardApi），这里不另起一套。
// 刷新：打开面板时 + 登录后每 60 秒（与网页版通知 60 秒常驻同口径）；
// mention 事件到时也刷一次（@人实时到，徽标不用等 60 秒）。
async function loadNotifications() {
  if (!state.me) return;
  try {
    const data = await window.chimera.api.notifications();
    state.notif.items = filterMentionNotifications(data.notifications || []);
    state.notif.readTs = data.readTs || state.notif.readTs;
    state.notif.mentionEnabled = data.mentionEnabled !== false;
    state.notif.unread = notifUnreadCount(state.notif.items, state.notif.readTs);
    state.notif.loaded = true;
    paintNotifBadge();
    renderNotifList();
  } catch (err) {
    const box = $('notifList');
    if (box && !$('pane-notif').classList.contains('hidden')) {
      box.innerHTML = '<div class="muted" style="padding:12px">' + ui('notif.loadFail') + esc(err.message || err) + '</div>';
    }
  }
}

function paintNotifBadge() {
  const badge = $('notifBadge');
  if (!badge) return;
  const n = state.notif.mentionEnabled ? (state.notif.unread || 0) : 0;
  badge.textContent = n > 0 ? (n > 99 ? '99+' : String(n)) : '';
}

function renderNotifList() {
  const box = $('notifList');
  if (!box) return;
  const items = state.notif.items || [];
  if (!state.notif.loaded) {
    box.innerHTML = '<div class="muted small">' + ui('common.loading') + '</div>';
    return;
  }
  if (!items.length) {
    box.innerHTML = '<div class="empty-note">' + ui('notif.empty') + '</div>';
    return;
  }
  const water = state.notif.readTs ? Date.parse(state.notif.readTs) : null;
  box.innerHTML = items.map((n) => {
    const t = Date.parse(n.ts || '');
    const isUnread = !Number.isFinite(water) || !Number.isFinite(t) || t > water;
    const actor = (n.actor && n.actor.name) || '?';
    const tgt = notifTarget(n);
    return '<div class="notif-item' + (isUnread ? ' unread-item' : '') + '" data-target-kind="' + esc(tgt.kind) + '" data-target-id="' + esc(tgt.id || '') + '" data-target-name="' + esc(tgt.name || '') + '">'
      + avatarHtml(actor, 'avatar-sm')
      + '<div class="item-main"><div class="item-name"><button class="linklike" data-profile="' + esc(actor) + '">' + esc(actor) + '</button><span>' + esc(notifVerb(n)) + '</span></div>'
      + (n.preview ? '<div class="notif-preview">' + esc(n.preview) + '</div>' : '')
      + '<div class="notif-time">' + esc(fmtTime(n.ts)) + '</div></div>'
      + '</div>';
  }).join('');
}

function startNotifTimer() {
  stopNotifTimer();
  state.notifTimer = setInterval(() => { loadNotifications().catch(() => {}); }, 60 * 1000);
}
function stopNotifTimer() {
  if (state.notifTimer) { clearInterval(state.notifTimer); state.notifTimer = null; }
}

$('notifBell').addEventListener('click', () => showPane('notif'));
$('notifRefresh').addEventListener('click', loadNotifications);
$('notifMarkReadBtn').addEventListener('click', async () => {
  const btn = $('notifMarkReadBtn');
  btn.disabled = true;
  try {
    const data = await window.chimera.api.markNotificationsRead();
    state.notif.readTs = (data && data.readTs) || new Date().toISOString();
    state.notif.unread = 0;
    paintNotifBadge();
    renderNotifList();
  } catch (err) {
    // 标记已读失败不清空徽标，下次再点；不弹 alert 挡阅读。
  } finally { btn.disabled = false; }
});
// 点条目跳哪（v0.3.15 修复轮：抽成纯函数可单测）：帖子 → 帖子页，
// 群聊 → 打开该群，否则 → @人资料卡；无有效目标回 null。
function notifJumpTarget(kind, id, name) {
  if (kind === 'post') return { pane: 'posts' };
  if (kind === 'group' && id) return { conv: ['group', id] };
  if (kind === 'profile' && name) return { profile: name };
  return null;
}
// 点条目跳转。人名按钮走全局资料卡处理，这里跳过（避免一次点击开两个地方）。
$('notifList').addEventListener('click', (e) => {
  if (e.target.closest('[data-profile]')) return;
  const item = e.target.closest('.notif-item');
  if (!item) return;
  const jump = notifJumpTarget(item.dataset.targetKind, item.dataset.targetId, item.dataset.targetName);
  if (!jump) return;
  if (jump.pane) { showPane(jump.pane); return; }
  if (jump.conv) { openConv(jump.conv[0], jump.conv[1]); return; }
  if (jump.profile) { openProfile(jump.profile); }
});

// ---------- 设置（v0.3.15，只读） ----------
// 只读展示：账号（state.accounts 里的本人行：名字/类型/等级/积分）、
// 工作区绑定（api:workspace-bindings 活跃行 + accounts 的 hasWorkspace/
// workspaceOnline）、通知偏好（本地 @提醒开关，只展示不给开关）、
// 发件箱计数（api:outbox-status）、草稿计数（本地 localStorage 统计）、
// 会话有效期（session:get）。本页没有任何写操作：不调 rotate/revoke/
// unbind/updateMe，不设任何动作按钮；刷新按钮只重拉只读数据。
async function loadSettings() {
  const box = $('settingsBody');
  if (!box) return;
  try {
    const [sess, accounts, bindings, outboxStatus] = await Promise.all([
      window.chimera.session.get().catch(() => null),
      window.chimera.api.accounts().catch(() => ({ accounts: [] })),
      window.chimera.api.workspaceBindings().catch(() => ({ bindings: [] })),
      window.chimera.api.outboxStatus().catch(() => ({ pending: 0, failed: 0, total: 0 })),
    ]);
    if (accounts && accounts.accounts) state.accounts = accounts.accounts;
    const me = state.me && state.me.username;
    const acc = me ? accountByName(me) : null;
    const rows = (bindings && bindings.bindings) || [];
    const active = rows.find((b) => !b.unlinkedTs) || null;
    state.settings = { sess, acc, active, outboxStatus, draftCount: countDrafts() };
    renderSettings();
  } catch (err) {
    box.innerHTML = '<div class="muted" style="padding:12px">' + ui('settings.loadFail') + esc(err.message || err) + '</div>';
  }
}

function renderSettings() {
  const box = $('settingsBody');
  if (!box) return;
  const s = state.settings;
  if (!s) {
    box.innerHTML = '<div class="muted small">' + ui('common.loading') + '</div>';
    return;
  }
  const acc = s.acc;
  const ob = s.outboxStatus || { pending: 0, failed: 0, total: 0 };
  const mentionOn = state.notif.mentionEnabled !== false;
  const sess = s.sess;
  const expires = sess && sess.expiresAt ? fmtTime(new Date(sess.expiresAt).toISOString()) : '—';
  const card = (label, value) => '<div class="stat-card"><div class="stat-label">' + esc(label) + '</div><div class="stat-value">' + esc(value) + '</div></div>';
  const typeText = acc ? (acc.type === 'human' ? ui('profile.human') : acc.type === 'ai' ? ui('profile.ai') : ui('profile.node')) : '';
  const wsBound = acc ? !!acc.hasWorkspace : !!s.active;
  box.innerHTML = ''
    + '<div class="settings-sec"><h3>' + ui('settings.account') + '</h3><div class="settings-grid">'
    + (acc
      ? card(ui('settings.name'), acc.name || '') + card(ui('settings.type'), typeText) + card(ui('settings.level'), acc.level || '—') + card(ui('settings.points'), String(acc.points ?? 0))
      : '<div class="muted small">' + ui('settings.noAccount') + '</div>')
    + '</div></div>'
    + '<div class="settings-sec"><h3>' + ui('settings.workspace') + '</h3><div class="settings-grid">'
    + card(ui('settings.bindState'), wsBound ? ui('settings.bound') : ui('settings.unbound'))
    + card(ui('settings.onlineState'), acc && acc.workspaceOnline ? ui('settings.online') : ui('settings.offline'))
    + (s.active ? card(ui('settings.device'), s.active.deviceName || '') + card(ui('settings.fingerprint'), workspaceFingerprint(s.active.keyId)) : '')
    + '</div></div>'
    + '<div class="settings-sec"><h3>' + ui('settings.notif') + '</h3><div class="settings-grid">'
    + card(ui('settings.mention'), mentionOn ? ui('settings.mentionOn') : ui('settings.mentionOff'))
    + '</div></div>'
    + '<div class="settings-sec"><h3>' + ui('settings.outbox') + '</h3><div class="settings-grid">'
    + card(ui('settings.pending'), String(ob.pending || 0))
    + card(ui('settings.failed'), String(ob.failed || 0))
    + card(ui('settings.drafts'), String(s.draftCount || 0))
    + '</div></div>'
    + '<div class="settings-sec"><h3>' + ui('settings.session') + '</h3><div class="settings-grid">'
    + card(ui('settings.name'), (sess && sess.username) || (state.me && state.me.username) || '')
    + card(ui('settings.expires'), expires)
    + '</div></div>';
}

$('settingsBtn').addEventListener('click', () => showPane('settings'));
$('settingsRefresh').addEventListener('click', loadSettings);

// ---------- 发件箱状态行（v0.3.15） ----------
// 聊天框、帖子编辑器、团队讨论三处共用：有待发/失败条目时显示，
// 无条目时隐藏。失败条目的丢弃按钮只丢本地副本（主进程 discard）。
async function refreshOutboxStatus() {
  try {
    const st = await window.chimera.api.outboxStatus();
    state.outbox = st || { pending: 0, failed: 0, total: 0, entries: [] };
  } catch { /* 状态拉不到就留上次，不挡界面 */ }
  renderOutboxLine();
}

function renderOutboxLine() {
  const text = outboxStatusText(state.outbox, state.outboxFlash);
  for (const id of ['chatOutboxLine', 'feedOutboxLine', 'teamOutboxLine']) {
    const el = $(id);
    if (!el) continue;
    if (!text) { el.classList.add('hidden'); el.innerHTML = ''; continue; }
    el.classList.remove('hidden');
    el.innerHTML = text;
  }
  document.querySelectorAll('[data-discard-outbox]').forEach((btn) => {
    btn.addEventListener('click', async () => {
      btn.disabled = true;
      try { await window.chimera.api.outboxDiscard(btn.dataset.discardOutbox); await refreshOutboxStatus(); }
      catch { btn.disabled = false; }
    });
  });
}

window.chimera.onOutboxStatus((st) => {
  state.outbox = st || { pending: 0, failed: 0, total: 0, entries: [] };
  renderOutboxLine();
});

// queueFlash：一次发送入箱后，状态行前 5 秒显示“已存入发件箱”提示，
// 之后只显示计数。主进程入箱后立即推状态，提示与计数同框出现。
function queueFlash() {
  state.outboxFlash = true;
  setTimeout(() => { state.outboxFlash = false; renderOutboxLine(); }, 5000);
}

// ---------- SSE wiring (main process owns the connection) ----------
// 事件只改自己那一项，不再全量 loadAll（原来每事件约 390ms、3 个 HTTP 请求）。
// 指示器只走 realtime:transport（v0.2.1）：onSseConnect/onSseDisconnect
// 不再画指示器，避免两路状态打架。
window.chimera.onRealtimeTransport(({ state, detail }) => setTransport(state, detail));
// 会话过期（v0.3.13）：主进程 401 判定后推 session:expired，渲染进程回登录页。
window.chimera.onSessionExpired(() => resetToLogin());
// 更新已暂存（v0.3.13）：顶栏非打断提示，点一下隐藏，不弹窗、不打断。
window.chimera.onUpdateStaged(({ version }) => {
  const pill = $('updatePill');
  if (!pill) return;
  pill.textContent = uiFmt('top.updateReady', version);
  pill.title = ui('top.updateReadyTitle');
  pill.classList.remove('hidden');
});
$('updatePill').addEventListener('click', () => $('updatePill').classList.add('hidden'));
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
    if (!$('pane-posts').classList.contains('hidden')) fetchNewFeedPosts();
    return;
  }
  if (event === 'mention') {
    // 私聊没有专属事件；打开着私聊时顺带增量拉一次，别的不动。
    if (state.open?.kind === 'dm') await fetchNewMessages(false);
    loadNotifications().catch(() => {}); // @到实时刷徽标，不等 60 秒轮询
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
  document.title = ui('top.title'); // 窗口标题跟随语言（v0.3.13）
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
  renderBoardList(); // v0.3.9：“全部板块”是 chrome（字典 side.allBoards），
  // 但板块列表只在 loadBoards/点版块时重画，syncChrome 没它，语言切换后
  // 仍留启动时的语言；真实版块名靠 data-tr-name 名字队列翻，唯独它漏网。
  renderRightbar();
  syncOlderButton();
  renderHomeStats(); // 首页统计卡跟随语言（v0.3.13）：只重画，不重拉
  layoutTopNav(); // 窄屏导航重算可见项（v0.3.13）
  if (!$('pane-notif').classList.contains('hidden')) renderNotifList(); // 通知列表跟随语言：只重画，不重拉
  if (!$('pane-settings').classList.contains('hidden')) renderSettings(); // 设置页跟随语言：只重画，不重拉
  renderOutboxLine(); // 发件箱状态行跟随语言：只重画，不重拉
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
  if (!$('pane-home').classList.contains('hidden')) renderHomeStats();
  refreshDataTranslations();
});

syncChrome();
boot();
