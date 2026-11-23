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
    read: (groupId, seq) => ipcRenderer.invoke('api:read', { groupId, seq }),
    upload: (filePath) => ipcRenderer.invoke('api:upload', { filePath }),
    uploadBlob: (name, base64) => ipcRenderer.invoke('api:upload-blob', { name, base64 }),
  },
  node: {
    status: () => ipcRenderer.invoke('node:status'),
    setEnabled: (enabled) => ipcRenderer.invoke('node:set-enabled', enabled),
    syncNow: () => ipcRenderer.invoke('node:sync-now'),
    headers: (limit) => ipcRenderer.invoke('node:headers', limit),
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
  onNodeState: (fn) => ipcRenderer.on('node:state', (_e, payload) => fn(payload)),
  onNodeLog: (fn) => ipcRenderer.on('node:log', (_e, line) => fn(line)),
  onNodeSync: (fn) => ipcRenderer.on('node:sync', (_e, status) => fn(status)),
});
