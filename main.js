'use strict';
/**
 * main.js — Chimera 桌面端主进程。
 *
 * 职责：
 *  - 窗口与会话：safeStorage 存 session token（7 天），密码绝不落盘。
 *  - 实时内核 v2：RealtimeClient 常驻主进程（WS 优先，WS 不可用时回落 SSE），
 *    事件经 IPC 推给渲染进程；断线按 lastSeq 续接，truncated 时通知渲染进程
 *    全量重拉。WS 的 group_message 带 body，渲染进程直接追加渲染；
 *    无 body 的（断线补发 / SSE 路）渲染进程用 ?since= 增量拉取。
 *  - 轻节点：NodeManager（lib/nodeManager.js），开关持久化在 store；
 *    启动时若 node.enabled 为真则自动起节点（用户上次的选择）。
 *
 * 渲染进程只能走 preload 暴露的 window.chimera，不能直接 require。
 */
const path = require('path');
const { app, BrowserWindow, ipcMain, safeStorage } = require('electron');
const { Store } = require('./lib/store');
const { ForumClient } = require('./lib/forumClient');
const { RealtimeClient } = require('./lib/realtimeClient');
const { NodeManager } = require('./lib/nodeManager');
const { ServerResolver, PRIMARY_BASE } = require('./lib/serverResolver');
const { AppUpdater } = require('./lib/appUpdater');
const workspaceBinding = require('./lib/workspaceBinding');
const doRegistry = require('./lib/doRegistry');
const os = require('node:os');

const API_BASE = process.env.CHIMERA_API_BASE || PRIMARY_BASE;

// 服务端 URL 解析（单例）：主 URL 公网域名、备 URL 尾网 het。
// ForumClient（REST，渲染进程的数据层）与 RealtimeClient（WS/SSE）
// 共用这一个 resolver——主 URL 挂了自动切尾网，恢复后切回，
// 两路不各走各的。更新检查（AppUpdater 的清单）也走它。
const resolver = new ServerResolver({ primary: API_BASE });
resolver.on('switch', ({ from, to }) => {
  console.log(`[server] 基地址切换：${from} -> ${to}（${resolver.label()}）`);
});

let win = null;
let store = null;
let client = null;
let realtime = null;
let nodeManager = null;
let updater = null;
let lastTransport = null; // 最近一次 realtime:transport，供窗口加载完后补发
// 工作区绑定（融合 P0）：活跃绑定 id + 每 5 分钟 seen 心跳定时器。
// seen 只在“有活跃绑定且会话有效”时调，失败静默等下次；私钥/签名绝不进日志。
let workspaceActiveBindingId = null;
let workspaceSeenTimer = null;

function startWorkspaceSeenTimer() {
  if (workspaceSeenTimer) return;
  workspaceSeenTimer = setInterval(async () => {
    if (!workspaceActiveBindingId) return;
    const token = sessionToken();
    if (!token) return; // 会话无效时不调，等下次
    try {
      await client.workspaceSeen(token, workspaceActiveBindingId);
    } catch { /* 失败静默等下次 */ }
  }, 5 * 60 * 1000);
  if (workspaceSeenTimer.unref) workspaceSeenTimer.unref();
}

function stopWorkspaceSeenTimer() {
  if (workspaceSeenTimer) { clearInterval(workspaceSeenTimer); workspaceSeenTimer = null; }
  workspaceActiveBindingId = null;
}

function userDataFile(name) {
  return path.join(app.getPath('userData'), name);
}

function encToken(token) {
  if (!safeStorage.isEncryptionAvailable()) return Buffer.from(token, 'utf8').toString('base64');
  return safeStorage.encryptString(token).toString('base64');
}

function decToken(enc) {
  if (!enc) return null;
  try {
    if (!safeStorage.isEncryptionAvailable()) return Buffer.from(enc, 'base64').toString('utf8');
    return safeStorage.decryptString(Buffer.from(enc, 'base64'));
  } catch { return null; }
}

function sessionToken() {
  const s = store.state.session;
  if (!s || !s.encToken) return null;
  if (s.expiresAt && Date.now() > s.expiresAt) return null;
  return decToken(s.encToken);
}

// 错误码（v0.3.13）：IPC 只透传 message，code 嵌在消息里供渲染层翻译；
// 格式 ERR_CODE 或 ERR_CODE: 细节。未知错误不走这里，原样抛。
function codedError(code, detail) {
  const e = new Error(detail ? `${code}: ${detail}` : code);
  e.code = code;
  return e;
}

// 会话过期（v0.3.13）：REST 层遇 401 即清会话、回登录页。
// onSessionExpired 只推一次 session:expired（登录/登出后复位），
// 再把实时流重启为匿名；渲染层收到事件后调 resetToLogin()。
let sessionExpiredNotified = false;

function onSessionExpired() {
  if (store.state.session) store.update((s) => { s.session = null; });
  stopWorkspaceSeenTimer();
  if (!sessionExpiredNotified) {
    sessionExpiredNotified = true;
    sendToRenderer('session:expired', {});
  }
  startRealtime(); // 退回匿名流
}

// guardApi 包一层 api 处理器：401 时先触发 onSessionExpired() 再原样抛错。
// 公共免 token 接口（health/leaderboard/accounts/boards）不包——
// 它们本来就不要 token，401 与会话无关。
function guardApi(fn) {
  return async (...args) => {
    try {
      return await fn(...args);
    } catch (err) {
      if (err && err.status === 401) onSessionExpired();
      throw err;
    }
  };
}

function apiHandle(channel, fn) {
  ipcMain.handle(channel, guardApi(fn));
}

function createWindow() {
  const w = store.state.prefs.window || { width: 1180, height: 760 };
  win = new BrowserWindow({
    width: w.width,
    height: w.height,
    minWidth: 900,
    minHeight: 560,
    title: 'Chimera 桌面端',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });
  win.loadFile(path.join(__dirname, 'renderer', 'index.html'));
  // 渲染进程订阅 realtime:transport 可能晚于 startRealtime 的第一次
  // 'connecting'：加载完后把最近的状态补发一次，指示器不留“未连接”。
  win.webContents.on('did-finish-load', () => {
    if (lastTransport) sendToRenderer('realtime:transport', lastTransport);
  });
  win.on('close', () => {
    try {
      const [width, height] = win.getSize();
      store.update((s) => { s.prefs.window = { width, height }; });
    } catch { /* 关窗时存窗口大小失败不阻塞退出 */ }
  });
  win.on('closed', () => { win = null; });
}

function sendToRenderer(channel, payload) {
  if (win && !win.isDestroyed()) win.webContents.send(channel, payload);
}

// ---- realtime: kernel v2, WS-first + SSE fallback; main process owns it ----
// IPC 通道名保持 sse:* 不变，渲染进程不用改；connect payload 多带 transport
//（'ws' | 'sse'），渲染进程忽略多余字段。
// 状态指示器走新通道 realtime:transport（v0.2.1）：RealtimeClient 每次
// 传输状态变化都推 {state, detail}，渲染进程只按这个画指示器。
function sendTransport(payload) {
  lastTransport = payload;
  sendToRenderer('realtime:transport', payload);
}

function startRealtime() {
  stopRealtime();
  const token = sessionToken();
  realtime = new RealtimeClient(API_BASE, {
    token,
    resolver,
    // 会话失效检测（v0.2.1）：只有 /api/me 回 401/403 才算会话死了；
    // 没 token 时不注入（匿名流没有会话可死）。
    checkSession: token ? async () => { await client.getMe(sessionToken()); } : undefined,
  });
  realtime.on('connect', ({ transport }) => {
    console.log('[realtime] transport:', transport);
    sendToRenderer('sse:connect', { at: new Date().toISOString(), transport });
  });
  realtime.on('disconnect', (err) => sendToRenderer('sse:disconnect', { error: String(err && err.message || err) }));
  realtime.on('transport', (payload) => {
    console.log('[realtime] state:', payload.state, payload.detail || '');
    sendTransport(payload);
  });
  realtime.on('session-expired', ({ reason }) => {
    console.log('[realtime] session expired:', reason);
    onSessionExpired();
  });
  realtime.on('truncated', (data) => sendToRenderer('sse:truncated', data));
  realtime.on('event', ({ event, data, seq }) => sendToRenderer('sse:event', { event, data, seq }));
  // SSE/WS seq 缺口：断点续拉——节点立即同步一次重拉当前计数，不等 30 秒周期。
  realtime.on('gap', ({ expected, got, transport }) => {
    console.log(`[realtime] seq 缺口：${transport} ${expected}..${got - 1} 缺失，触发节点即时重拉`);
    if (nodeManager && nodeManager.state === 'running') {
      nodeManager.syncOnce().catch(() => {});
    }
  });
  realtime.start();
}

function stopRealtime() {
  if (realtime) { try { realtime.stop(); } catch {} realtime = null; }
}

// ---- light node: switch is persisted; auto-start on launch if enabled ----
async function initNode() {
  nodeManager = new NodeManager({
    nodeDir: userDataFile('node'),
    apiBase: API_BASE,
    config: {
      enabled: store.state.node.enabled,
      port: store.state.node.port,
      seeds: store.state.node.seeds && store.state.node.seeds.length
        ? store.state.node.seeds
        : [API_BASE],
      // v0.3.10 (d)：token 路径一律按当前 userData 派生，绝不读 state.json 里
      // 存的老路径——R3 就是验收 profile 抄了 live state.json，空链 + live
      // token 发 seq-1 创世把服务端链清了。测试 profile 的 userData 下没有
      // token 文件，节点自然只本地跑、不上传；NodeManager 构造里还有一道
      // 显性 guard（token 必须在 nodeDir 内，配错即抛）。
      forumTokenFile: path.join(userDataFile('node'), 'forum-token'),
    },
    onLog: (line) => sendToRenderer('node:log', line),
  });
  nodeManager.on('state', (s) => sendToRenderer('node:state', { state: s, status: nodeManager.status() }));
  nodeManager.on('sync', (st) => sendToRenderer('node:sync', st));
  nodeManager.on('upload', (st) => sendToRenderer('node:sync', st));
  nodeManager.on('chain-broken', () => sendToRenderer('node:sync', nodeManager.status()));
  nodeManager.on('chain-rebased', () => sendToRenderer('node:sync', nodeManager.status()));
  if (store.state.node.enabled) {
    try {
      await nodeManager.start();
    } catch (err) {
      nodeManager.log(`开机自启节点失败（开关保持开，下次启动重试）：${err.message}`);
    }
  }
}

// ---- IPC ----
function registerIpc() {
  ipcMain.handle('session:get', () => {
    const s = store.state.session;
    return s ? { username: s.username, expiresAt: s.expiresAt } : null;
  });

  ipcMain.handle('auth:login', async (_e, { username, password }) => {
    const data = await client.humanLogin(username, password);
    if (!data || !data.sessionToken) throw codedError('ERR_LOGIN_NO_SESSION');
    store.update((s) => {
      s.session = {
        username: data.username || username,
        encToken: encToken(data.sessionToken),
        expiresAt: Date.now() + 7 * 24 * 3600 * 1000,
      };
    });
    sessionExpiredNotified = false; // 新会话：过期通知复位
    startRealtime();
    return { username: data.username || username };
  });

  ipcMain.handle('auth:logout', async () => {
    const token = sessionToken();
    if (token) { try { await client.humanLogout(token); } catch {} }
    store.update((s) => { s.session = null; });
    sessionExpiredNotified = false; // 已登出：过期通知复位
    startRealtime(); // 退回匿名流
    return true;
  });

  ipcMain.handle('api:health', () => client.getHealth());
  ipcMain.handle('api:leaderboard', (_e, limit) => client.getLeaderboard(limit || 20));
  apiHandle('api:me', () => client.getMe(sessionToken()));
  ipcMain.handle('api:accounts', () => client.getAccounts());
  apiHandle('api:groups', () => client.getGroups(sessionToken()));
  apiHandle('api:group-messages', (_e, { groupId, since, limit, before_id }) =>
    client.getGroupMessages(groupId, sessionToken(), { since, limit, before_id }));
  apiHandle('api:send-group-message', (_e, { groupId, body, attachments }) =>
    client.sendGroupMessage(groupId, sessionToken(), body, attachments || []));
  apiHandle('api:read', (_e, { groupId, seq }) =>
    client.markGroupRead(sessionToken(), groupId, seq));
  apiHandle('api:dms', () => client.getDms(sessionToken()));
  apiHandle('api:dm-history', (_e, { peer, since, limit, before_id }) =>
    client.getDmHistory(peer, sessionToken(), { since, limit, before_id }));
  apiHandle('api:send-dm', (_e, { peer, body, attachments }) =>
    client.sendDm(peer, sessionToken(), body, attachments || []));
  apiHandle('api:posts', (_e, opts) => client.getPosts(sessionToken(), opts || {}));
  // 翻译（v0.3.1）：单条 + 批量，走 /api/translate（带 session token）。
  // 429 归一化（v0.3.13）：服务端单条限流回 HTTP 429、批量回 200+rate_limited；
  // 渲染层只认 per-item rate_limited（65 秒自动重试），这里把 HTTP 429
  // 也折成同样形状，渲染层不用分两路。
  apiHandle('api:translate', async (_e, { text, target, source }) => {
    try {
      return await client.translate(sessionToken(), text, target, source || null);
    } catch (err) {
      if (err && err.status === 429) return { ok: false, error: 'rate_limited' };
      throw err;
    }
  });
  apiHandle('api:translate-batch', async (_e, { items, target }) => {
    try {
      return await client.translateBatch(sessionToken(), items, target);
    } catch (err) {
      if (err && err.status === 429) return { ok: true, results: (items || []).map(() => ({ error: 'rate_limited' })) };
      throw err;
    }
  });
  // 板块常驻（v0.3.1）：直接取全部板块，不再等帖子拉回来。
  ipcMain.handle('api:boards', () => client.getBoards());
  // 社区 / 团队（v0.3.1，只读）。
  apiHandle('api:communities', () => client.getCommunities(sessionToken()));
  apiHandle('api:community', (_e, { id }) => client.getCommunity(sessionToken(), id));
  apiHandle('api:community-ledger', (_e, { id, limit }) =>
    client.getCommunityLedger(sessionToken(), id, { limit }));
  // 社区 / 团队（v0.3.4，可交互）：服务端按创建人/站长/admin/成员放行，失败原样抛错、渲染进程展示原话。
  apiHandle('api:create-community', (_e, { name, title, description }) =>
    client.createCommunity(sessionToken(), { name, title, description }));
  apiHandle('api:join-community-code', (_e, { code }) =>
    client.joinCommunityByCode(sessionToken(), code));
  apiHandle('api:community-posts', (_e, { id, limit, q }) =>
    client.getCommunityPosts(sessionToken(), id, { limit, q }));
  apiHandle('api:create-community-post', (_e, { id, title, body }) =>
    client.createCommunityPost(sessionToken(), id, { title, body }));
  apiHandle('api:create-community-announcement', (_e, { id, body }) =>
    client.createCommunityAnnouncement(sessionToken(), id, body));
  apiHandle('api:delete-community-announcement', (_e, { id, annId }) =>
    client.deleteCommunityAnnouncement(sessionToken(), id, annId));
  apiHandle('api:add-community-member', (_e, { id, name, type }) =>
    client.addCommunityMember(sessionToken(), id, { name, type }));
  apiHandle('api:remove-community-member', (_e, { id, name }) =>
    client.removeCommunityMember(sessionToken(), id, name));
  apiHandle('api:set-community-member-role', (_e, { id, name, role }) =>
    client.setCommunityMemberRole(sessionToken(), id, name, role));
  apiHandle('api:set-community-groups', (_e, { id, groups }) =>
    client.setCommunityGroups(sessionToken(), id, groups));
  // 帖子互动（v0.3.4）：点赞 / 置顶 / 删帖 + 回复（createPost 已有，reply_to 继承 team_id）。
  apiHandle('api:create-post', (_e, { board, title, body, replyTo }) =>
    client.createPost(sessionToken(), { board, title, body, replyTo }));
  apiHandle('api:like-post', (_e, { id }) => client.likePost(sessionToken(), id));
  apiHandle('api:pin-post', (_e, { id, pinned }) => client.pinPost(sessionToken(), id, pinned));
  apiHandle('api:delete-post', (_e, { id }) => client.deletePost(sessionToken(), id));
  // v0.3.5：全站搜索 / 关注 / 关注动态 / 改自己资料。
  apiHandle('api:search', (_e, { q, limit }) => client.search(sessionToken(), q, limit || 20));
  apiHandle('api:follow', (_e, { name }) => client.getFollow(sessionToken(), name));
  apiHandle('api:toggle-follow', (_e, { name }) => client.toggleFollow(sessionToken(), name));
  apiHandle('api:following-feed', (_e, { limit, before_id, since }) => client.getFollowingFeed(sessionToken(), { limit, before_id, since }));
  apiHandle('api:update-me', (_e, { patch }) => client.updateMe(sessionToken(), patch));
  // ---- 工作区绑定（融合 P0）：登记 + 只读在线状态 ----
  // api:workspace-challenge：取一次性挑战（5 分钟过期）。
  apiHandle('api:workspace-challenge', () => client.workspaceChallenge(sessionToken()));
  // api:workspace-bind：challenge → 本机签名 → 注册，全程在主进程完成
  // （私钥只在主进程读，渲染进程拿不到私钥与签名原文）。
  // 渲染进程只传 challenge；keyId/公钥/设备名由本机密钥派生。
  // 设备名默认 os.hostname()，P0 不做改名 UI。
  // v0.3.11 重绑发新 keypair：顺序是 DO 注册在先、论坛注册行在后——
  //  - 已有活跃绑定时直接拒绝（须先解绑再重绑，避免重复活跃行）；
  //  - 有过绑定行且本机有旧 keypair 时归档旧 key、生成新 keypair；
  //    无行但有 keypair（之前某次绑定中途失败）时复用旧 key（它可能
  //    已在 DO 里，复用可避免在 DO 里留孤 key）；
  //  - DO 注册失败 → 恢复旧 keypair，中止（论坛一行不动）；
  //  - 论坛注册失败 → 尽力吊销刚注册的 DO 新 key（无任何行引用，
  //    安全），再恢复旧 keypair。旧 DO key 由 registrar 下一轮按
  //    guard 规则自动吊销（旧 keyId 不再有活跃行引用），registrar 不用改。
  apiHandle('api:workspace-bind', async (_e, { challenge } = {}) => {
    const token = sessionToken();
    if (!token) throw codedError('ERR_NEED_LOGIN');
    if (!challenge) throw codedError('ERR_BIND_NO_CHALLENGE');
    let rotated = null;
    let keyInfo = null;
    try {
      const mine = await client.myWorkspaceBindings(token);
      const rows = (mine && mine.bindings) || [];
      const active = rows.find((b) => !b.unlinkedTs);
      if (active) throw codedError('ERR_BIND_ALREADY_BOUND');
      const hasPriorRows = rows.length > 0;
      const hasKeyPair = workspaceBinding.keyPairExists();
      if (workspaceBinding.shouldRotateKeyPair({ hasPriorRows, hasKeyPair })) {
        rotated = workspaceBinding.archiveKeyPair();
        keyInfo = workspaceBinding.generateFreshKeyPair();
      } else {
        keyInfo = workspaceBinding.ensureKeyPair();
      }
      try {
        await doRegistry.registerWorkspaceKey({
          nodeId: keyInfo.nodeId, keyId: keyInfo.keyId, publicKey: keyInfo.publicKey,
        });
      } catch (doErr) {
        throw codedError('ERR_DO_REGISTER_FAILED', doErr.message || doErr);
      }
      const signature = workspaceBinding.signChallenge(
        challenge, workspaceBinding.readPrivateKeyPem());
      let data;
      try {
        data = await client.registerWorkspaceBinding(token, {
          keyId: keyInfo.keyId,
          publicKey: keyInfo.publicKey,
          deviceName: workspaceBinding.sanitizeDeviceName(os.hostname()),
          challenge,
          signature,
        });
      } catch (forumErr) {
        // 论坛注册失败：新 key 只在 DO 里、无任何行引用，尽力吊销回滚。
        try { await doRegistry.revokeWorkspaceKey(keyInfo.nodeId); } catch { /* 回滚吊销失败不掩盖原错 */ }
        throw forumErr;
      }
      if (data && data.binding && data.binding.id) {
        workspaceActiveBindingId = data.binding.id;
        startWorkspaceSeenTimer();
      }
      return data;
    } catch (err) {
      // 回滚只恢复旧 keypair：论坛注册失败时 DO 新 key 已在内联分支尽力
      // 吊销过；论坛行已建后（此处之后无可抛步骤）绝不能再吊 DO key。
      if (rotated) {
        try { workspaceBinding.restoreKeyPair(rotated); } catch { /* 恢复失败不掩盖原错 */ }
      }
      throw err;
    }
  });
  // api:workspace-bindings：本人全部行（含已解绑）；有活跃绑定时起 seen 定时器。
  apiHandle('api:workspace-bindings', async () => {
    const data = await client.myWorkspaceBindings(sessionToken());
    const active = (data && data.bindings || []).find((b) => !b.unlinkedTs);
    if (active) {
      workspaceActiveBindingId = active.id;
      startWorkspaceSeenTimer();
    } else {
      stopWorkspaceSeenTimer();
    }
    return data;
  });
  // api:workspace-unlink：解绑（P0 不做解绑按钮，验收走 API；留接口供后续接线）。
  // 服务端置 unlinkedTs、registryStatus=revoked-pending；DO 吊销由
  // registrar 每小时按 guard 规则自动处理（无活跃行引用时才吊）。
  apiHandle('api:workspace-unlink', async (_e, { id } = {}) => {
    const data = await client.unlinkWorkspaceBinding(sessionToken(), id);
    if (id && workspaceActiveBindingId === id) stopWorkspaceSeenTimer();
    return data;
  });
  // api:workspace-seen：手动打一次 lastSeen（定时器在主进程另跑）。
  apiHandle('api:workspace-seen', (_e, { id } = {}) =>
    client.workspaceSeen(sessionToken(), id));
  // 加群（v0.3.2）：调 /join 自加（服务端按调用方身份加人）；服务端不放行时原样抛错，渲染进程展示原话。
  apiHandle('api:join-group', (_e, { groupId }) =>
    client.joinGroup(sessionToken(), groupId));
  apiHandle('api:upload', (_e, { filePath }) => client.uploadFile(sessionToken(), filePath));
  apiHandle('api:upload-blob', async (_e, { name, base64 }) => {
    // 粘贴图片 / 文件选择器拿到的 File 只有内存数据，没有磁盘路径：
    // 先写进临时文件再走同一套 multipart 上传，传完即删。
    const os = require('os');
    const fs = require('fs');
    const safeName = String(name || 'image.png').replace(/[^A-Za-z0-9._-]/g, '_');
    const tmp = path.join(os.tmpdir(), `chimera-upload-${Date.now()}-${safeName}`);
    fs.writeFileSync(tmp, Buffer.from(base64, 'base64'));
    try {
      return await client.uploadFile(sessionToken(), tmp);
    } finally {
      try { fs.unlinkSync(tmp); } catch { /* 临时文件删失败不阻塞 */ }
    }
  });

  // ---- realtime IPC: typing / receipt 走 WS（SSE 路上返回 false，仍可走 REST） ----
  ipcMain.handle('realtime:typing', (_e, { groupId }) => realtime ? realtime.sendTyping(groupId) : false);
  ipcMain.handle('realtime:receipt', (_e, { groupId, seq, scope }) => {
    if (!realtime) return false;
    if (scope === 'mentions') return realtime.sendReceiptMentions(seq);
    return realtime.sendReceipt(groupId, seq);
  });

  // ---- node IPC: switch + status + log ----
  ipcMain.handle('node:status', () => {
    const st = nodeManager ? nodeManager.status() : { state: 'off', enabled: false };
    // 激活时分配的节点账号名（仅展示；token 不在状态里）。
    st.nodeAccountName = store.state.node.nodeAccountName || null;
    return st;
  });
  ipcMain.handle('node:set-enabled', async (_e, enabled) => {
    const on = !!enabled;
    store.update((s) => { s.node.enabled = on; });
    return nodeManager.setEnabled(on);
  });
  ipcMain.handle('node:sync-now', async () => nodeManager.syncOnce());
  ipcMain.handle('node:headers', (_e, limit) => nodeManager._tailHeaders(limit || 50));
  // ---- 节点激活（v0.3.3）：人类登录态调 /api/node/activate，服务端自动
  // 分配节点账号（owner=本人）；返回的 apiToken 只写进节点 token 文件
  // （0600），绝不写 state.json、不进日志。已绑定过（token 文件已存在）
  // 时不重复激活。运行中的节点管理器立即用上新 token（下次上传即生效）。
  ipcMain.handle('node:activate', async () => {
    const token = sessionToken();
    if (!token) throw codedError('ERR_NEED_LOGIN');
    if (nodeManager && nodeManager._readForumToken()) throw codedError('ERR_NODE_ALREADY_ACTIVATED');
    const data = await client.activateNode(token);
    if (!data || !data.apiToken || !data.account || !data.account.name) throw codedError('ERR_NODE_ACTIVATE_NO_ACCOUNT');
    const fs = require('fs');
    const nodeDir = userDataFile('node');
    fs.mkdirSync(nodeDir, { recursive: true });
    const tokenFile = path.join(nodeDir, 'forum-token');
    fs.writeFileSync(tokenFile, data.apiToken, { mode: 0o600 });
    store.update((s) => {
      s.node.forumTokenFile = tokenFile;
      s.node.nodeAccountName = data.account.name;
    });
    if (nodeManager) nodeManager.config.forumTokenFile = tokenFile;
    return { account: data.account };
  });
}

// 单实例锁（v0.3.10，R3 根因）：第二个实例不得再起第二个节点上传器。
// 锁文件落在 userData 目录下，走查测试应用用独立 --user-data-dir 时各拿
// 各的锁，不影响 CDP 走查；同 profile 的第二个实例直接退出，已有实例聚焦。
const gotSingleInstanceLock = app.requestSingleInstanceLock();
if (!gotSingleInstanceLock) {
  app.quit();
} else {
  app.on('second-instance', () => {
    if (win) {
      if (win.isMinimized()) win.restore();
      win.focus();
    }
  });
  app.whenReady().then(async () => {
  store = new Store(userDataFile('state.json'));
  client = new ForumClient(API_BASE, { resolver });
  // 轻量 asar 自动更新：先看有没有上次暂存的新版，有就换上并 relaunch
  // 一次（上次是被杀掉、退出时没来得及换的情况）；换不上就继续跑旧版。
  updater = new AppUpdater({
    currentVersion: app.getVersion(),
    userDataDir: app.getPath('userData'),
    appAsarPath: app.getAppPath(),
    getBase: () => resolver.getBase(),
  });
  if (updater.applyPendingUpdate()) {
    app.relaunch();
    app.quit();
    return;
  }
  createWindow();
  registerIpc();
  startRealtime();
  updater.startPeriodicCheck(); // 启动 15 秒后查一次，之后每 6 小时查一次
  // 更新已暂存（v0.3.13）：推给渲染进程顶栏提示，不弹窗、不打断。
  updater.on('update-staged', ({ version }) => sendToRenderer('update:staged', { version }));
  await initNode();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
  });
}

// 退出时把暂存的新 asar 换上去（下次启动即新版）；换不上也不阻塞退出。
app.on('before-quit', () => {
  try { if (updater) updater.applyPendingUpdate(); } catch { /* 换不上就算了 */ }
});

app.on('window-all-closed', () => {
  // 退出即停节点：NodeManager 是主进程内服务，随应用退出而结束，
  // 不留后台进程；显式 stop 一次以保证端口释放与日志收尾。
  if (nodeManager && (nodeManager.state === 'running' || nodeManager.state === 'starting')) {
    nodeManager.stop().catch(() => {}).finally(() => app.quit());
    setTimeout(() => app.quit(), 3000).unref?.();
    return;
  }
  if (process.platform !== 'darwin') app.quit();
});
