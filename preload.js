'use strict';
/**
 * preload.js — contextBridge: renderer gets window.chimera only.
 * No nodeIntegration, no direct require from the renderer.
 */
const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('chimera', {
  session: {
    get: () => ipcRenderer.invoke('session:get'),
    login: (username, password) => ipcRenderer.invoke('auth:login', { username, password }),
    logout: () => ipcRenderer.invoke('auth:logout'),
  },
  api: {
    health: () => ipcRenderer.invoke('api:health'),
    leaderboard: (limit) => ipcRenderer.invoke('api:leaderboard', limit),
    me: () => ipcRenderer.invoke('api:me'),
    accounts: () => ipcRenderer.invoke('api:accounts'),
    groups: () => ipcRenderer.invoke('api:groups'),
    groupMessages: (groupId, opts) => ipcRenderer.invoke('api:group-messages', { groupId, ...(opts || {}) }),
    sendGroupMessage: (groupId, body, attachments) =>
      ipcRenderer.invoke('api:send-group-message', { groupId, body, attachments }),
    dms: () => ipcRenderer.invoke('api:dms'),
    dmHistory: (peer, opts) => ipcRenderer.invoke('api:dm-history', { peer, ...(opts || {}) }),
    sendDm: (peer, body, attachments) => ipcRenderer.invoke('api:send-dm', { peer, body, attachments }),
    posts: (opts) => ipcRenderer.invoke('api:posts', opts || {}),
    translate: (text, target, source) => ipcRenderer.invoke('api:translate', { text, target, source }),
    translateBatch: (items, target) => ipcRenderer.invoke('api:translate-batch', { items, target }),
    boards: () => ipcRenderer.invoke('api:boards'),
    communities: () => ipcRenderer.invoke('api:communities'),
    community: (id) => ipcRenderer.invoke('api:community', { id }),
    communityLedger: (id, limit) => ipcRenderer.invoke('api:community-ledger', { id, limit }),
    createCommunity: ({ name, title, description }) => ipcRenderer.invoke('api:create-community', { name, title, description }),
    joinCommunityByCode: (code) => ipcRenderer.invoke('api:join-community-code', { code }),
    communityPosts: (id, opts) => ipcRenderer.invoke('api:community-posts', { id, ...(opts || {}) }),
    createCommunityPost: (id, { title, body }) => ipcRenderer.invoke('api:create-community-post', { id, title, body }),
    createCommunityAnnouncement: (id, body) => ipcRenderer.invoke('api:create-community-announcement', { id, body }),
    deleteCommunityAnnouncement: (id, annId) => ipcRenderer.invoke('api:delete-community-announcement', { id, annId }),
    addCommunityMember: (id, { name, type }) => ipcRenderer.invoke('api:add-community-member', { id, name, type }),
    removeCommunityMember: (id, name) => ipcRenderer.invoke('api:remove-community-member', { id, name }),
    setCommunityMemberRole: (id, name, role) => ipcRenderer.invoke('api:set-community-member-role', { id, name, role }),
    setCommunityGroups: (id, groups) => ipcRenderer.invoke('api:set-community-groups', { id, groups }),
    createPost: ({ board, title, body, replyTo }) => ipcRenderer.invoke('api:create-post', { board, title, body, replyTo }),
    likePost: (id) => ipcRenderer.invoke('api:like-post', { id }),
    pinPost: (id, pinned) => ipcRenderer.invoke('api:pin-post', { id, pinned }),
    deletePost: (id) => ipcRenderer.invoke('api:delete-post', { id }),
    // v0.3.5：全站搜索 / 关注 / 关注动态 / 改自己资料。
    search: (q, limit) => ipcRenderer.invoke('api:search', { q, limit }),
    getFollow: (name) => ipcRenderer.invoke('api:follow', { name }),
    toggleFollow: (name) => ipcRenderer.invoke('api:toggle-follow', { name }),
    followingFeed: (opts) => ipcRenderer.invoke('api:following-feed', opts || {}),
    updateMe: (patch) => ipcRenderer.invoke('api:update-me', { patch }),
    // v0.3.15：通知中心（只做 @提醒）+ 发件箱状态/丢弃（本地操作）。
    notifications: () => ipcRenderer.invoke('api:notifications'),
    markNotificationsRead: () => ipcRenderer.invoke('api:notifications-read'),
    outboxStatus: () => ipcRenderer.invoke('api:outbox-status'),
    outboxDiscard: (clientId) => ipcRenderer.invoke('api:outbox-discard', { clientId }),
    joinGroup: (groupId) => ipcRenderer.invoke('api:join-group', { groupId }),
    // 工作区绑定（融合 P0）：challenge → 签名 → 注册（签名在主进程完成，
    // 渲染进程拿不到私钥）；bindings 只读展示；unlink 留接口，P0 无解绑按钮。
    workspaceChallenge: () => ipcRenderer.invoke('api:workspace-challenge'),
    workspaceBind: (challenge) => ipcRenderer.invoke('api:workspace-bind', { challenge }),
    workspaceBindings: () => ipcRenderer.invoke('api:workspace-bindings'),
    workspaceUnlink: (id) => ipcRenderer.invoke('api:workspace-unlink', { id }),
    workspaceSeen: (id) => ipcRenderer.invoke('api:workspace-seen', { id }),
    read: (groupId, seq) => ipcRenderer.invoke('api:read', { groupId, seq }),
    upload: (filePath) => ipcRenderer.invoke('api:upload', { filePath }),
    uploadBlob: (name, base64) => ipcRenderer.invoke('api:upload-blob', { name, base64 }),
  },
  node: {
    status: () => ipcRenderer.invoke('node:status'),
    setEnabled: (enabled) => ipcRenderer.invoke('node:set-enabled', enabled),
    syncNow: () => ipcRenderer.invoke('node:sync-now'),
    headers: (limit) => ipcRenderer.invoke('node:headers', limit),
    activate: () => ipcRenderer.invoke('node:activate'),
  },
  // macOS 原生菜单跟语言（v0.3.17 修复轮）：启动与切换语言后通知主进程
  // 重建菜单；Linux/Windows 上主进程直接忽略，不出菜单栏。
  app: {
    setLang: (lang) => ipcRenderer.invoke('app:set-lang', lang),
  },
  // 实时（WS 优先）：typing / receipt 走 WS 通道；SSE 回落时返回 false，
  // 调用方仍可走 REST。渲染进程暂未调用，供后续接线用。
  realtime: {
    sendTyping: (groupId) => ipcRenderer.invoke('realtime:typing', { groupId }),
    sendReceipt: (opts) => ipcRenderer.invoke('realtime:receipt', opts || {}),
  },
  onSseEvent: (fn) => ipcRenderer.on('sse:event', (_e, payload) => fn(payload)),
  onSseConnect: (fn) => ipcRenderer.on('sse:connect', (_e, payload) => fn(payload)),
  onSseDisconnect: (fn) => ipcRenderer.on('sse:disconnect', (_e, payload) => fn(payload)),
  onSseTruncated: (fn) => ipcRenderer.on('sse:truncated', (_e, payload) => fn(payload)),
  // 传输状态指示器（v0.2.1）：{state, detail}，state ∈ connecting | ws |
  // sse | disconnected | session-expired。渲染进程只按这个画指示器。
  onRealtimeTransport: (fn) => ipcRenderer.on('realtime:transport', (_e, payload) => fn(payload)),
  // 会话过期（v0.3.13）：主进程 401 判定后推 session:expired，渲染进程回登录页。
  onSessionExpired: (fn) => ipcRenderer.on('session:expired', (_e, payload) => fn(payload)),
  // 更新已暂存（v0.3.13）：{version}，渲染进程顶栏提示。
  onUpdateStaged: (fn) => ipcRenderer.on('update:staged', (_e, payload) => fn(payload)),
  // 发件箱状态推送（v0.3.15）：{pending, failed, total, entries}，每次变动都推。
  onOutboxStatus: (fn) => ipcRenderer.on('outbox:status', (_e, payload) => fn(payload)),
  onNodeState: (fn) => ipcRenderer.on('node:state', (_e, payload) => fn(payload)),
  onNodeLog: (fn) => ipcRenderer.on('node:log', (_e, line) => fn(line)),
  onNodeSync: (fn) => ipcRenderer.on('node:sync', (_e, status) => fn(status)),
});
