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

const API_BASE = process.env.CHIMERA_API_BASE || 'https://forum.coldhao.win';

let win = null;
let store = null;
let client = null;
let realtime = null;
let nodeManager = null;

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
function startRealtime() {
  stopRealtime();
  realtime = new RealtimeClient(API_BASE, { token: sessionToken() });
  realtime.on('connect', ({ transport }) => {
    console.log('[realtime] transport:', transport);
    sendToRenderer('sse:connect', { at: new Date().toISOString(), transport });
  });
  realtime.on('disconnect', (err) => sendToRenderer('sse:disconnect', { error: String(err && err.message || err) }));
  realtime.on('truncated', (data) => sendToRenderer('sse:truncated', data));
  realtime.on('event', ({ event, data, seq }) => sendToRenderer('sse:event', { event, data, seq }));
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
  ipcMain.handle('api:posts', (_e, opts) => client.getPosts(opts || {}));
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
  client = new ForumClient(API_BASE);
  createWindow();
  registerIpc();
  startRealtime();
  await initNode();
  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) createWindow();
  });
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
