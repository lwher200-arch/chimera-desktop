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
      forumTokenFile: store.state.node.forumTokenFile,
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
    if (!data || !data.sessionToken) throw new Error('登录失败：服务端未返回会话');
    store.update((s) => {
      s.session = {
        username: data.username || username,
        encToken: encToken(data.sessionToken),
        expiresAt: Date.now() + 7 * 24 * 3600 * 1000,
      };
    });
    startRealtime();
    return { username: data.username || username };
  });

  ipcMain.handle('auth:logout', async () => {
    const token = sessionToken();
    if (token) { try { await client.humanLogout(token); } catch {} }
    store.update((s) => { s.session = null; });
    startRealtime(); // 退回匿名流
    return true;
  });

  ipcMain.handle('api:health', () => client.getHealth());
  ipcMain.handle('api:me', () => client.getMe(sessionToken()));
  ipcMain.handle('api:accounts', () => client.getAccounts());
  ipcMain.handle('api:groups', () => client.getGroups(sessionToken()));
  ipcMain.handle('api:group-messages', (_e, { groupId, since, limit, before_id }) =>
    client.getGroupMessages(groupId, sessionToken(), { since, limit, before_id }));
  ipcMain.handle('api:send-group-message', (_e, { groupId, body, attachments }) =>
    client.sendGroupMessage(groupId, sessionToken(), body, attachments || []));
  ipcMain.handle('api:read', (_e, { groupId, seq }) =>
    client.markGroupRead(sessionToken(), groupId, seq));
  ipcMain.handle('api:dms', () => client.getDms(sessionToken()));
  ipcMain.handle('api:dm-history', (_e, { peer, since, limit, before_id }) =>
    client.getDmHistory(peer, sessionToken(), { since, limit, before_id }));
  ipcMain.handle('api:send-dm', (_e, { peer, body, attachments }) =>
    client.sendDm(peer, sessionToken(), body, attachments || []));
  ipcMain.handle('api:posts', (_e, opts) => client.getPosts(sessionToken(), opts || {}));
  // 翻译（v0.3.1）：单条 + 批量，走 /api/translate（带 session token）。
  ipcMain.handle('api:translate', (_e, { text, target, source }) =>
    client.translate(sessionToken(), text, target, source || null));
  ipcMain.handle('api:translate-batch', (_e, { items, target }) =>
    client.translateBatch(sessionToken(), items, target));
  // 板块常驻（v0.3.1）：直接取全部板块，不再等帖子拉回来。
  ipcMain.handle('api:boards', () => client.getBoards());
  // 社区 / 团队（v0.3.1，只读）。
  ipcMain.handle('api:communities', () => client.getCommunities(sessionToken()));
  ipcMain.handle('api:community', (_e, { id }) => client.getCommunity(sessionToken(), id));
  ipcMain.handle('api:community-ledger', (_e, { id, limit }) =>
    client.getCommunityLedger(sessionToken(), id, { limit }));
  // 加群（v0.3.1）：以当前登录用户名自加；服务端不放行时原样抛错，渲染进程展示原话。
  ipcMain.handle('api:join-group', (_e, { groupId }) =>
    client.joinGroup(sessionToken(), groupId, store.state.session?.username));
  ipcMain.handle('api:upload', (_e, { filePath }) => client.uploadFile(sessionToken(), filePath));
  ipcMain.handle('api:upload-blob', async (_e, { name, base64 }) => {
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
  ipcMain.handle('node:status', () => (nodeManager ? nodeManager.status() : { state: 'off', enabled: false }));
  ipcMain.handle('node:set-enabled', async (_e, enabled) => {
    const on = !!enabled;
    store.update((s) => { s.node.enabled = on; });
    return nodeManager.setEnabled(on);
  });
  ipcMain.handle('node:sync-now', async () => nodeManager.syncOnce());
  ipcMain.handle('node:headers', (_e, limit) => nodeManager._tailHeaders(limit || 50));
}

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
  await initNode();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
});

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
