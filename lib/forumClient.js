'use strict';
/**
 * lib/forumClient.js — forum API client for forum.coldhao.win.
 * Adapted from Eterna Desktop's lib/forumClient.js (same endpoints, same
 * 7-day human session token). Human login: POST /api/accounts/human-login
 * -> sessionToken. Password is never stored; only the session token is kept
 * (safeStorage, main process).
 *
 * Real-time: the forum pushes live events over SSE
 * (GET /api/stream?since=<seq>&topics=posts,groups,mentions&token=<session>,
 * see lib/sseClient.js). This client covers the REST side; the SSE side is
 * the "kernel v1" transport. WebSocket hub is kernel v2 (ARCHITECTURE.md).
 */
const NODE_API_NOT_AVAILABLE = 'NODE_API_NOT_AVAILABLE';

class ForumApiError extends Error {
  constructor(message, status, code) {
    super(message);
    this.name = 'ForumApiError';
    this.status = status;
    this.code = code || null;
  }
}

class ForumClient {
  constructor(apiBase) {
    this.apiBase = String(apiBase).replace(/\/$/, '');
  }

  async request(path, { method = 'GET', body = null, token = null, headers = {} } = {}) {
    const h = { 'Content-Type': 'application/json', ...headers };
    if (token) h.Authorization = `Bearer ${token}`;
    const res = await fetch(this.apiBase + path, {
      method,
      headers: h,
      body: body ? JSON.stringify(body) : null,
    });
    const text = await res.text();
    let data = null;
    try { data = JSON.parse(text); } catch { /* non-JSON body */ }
    if (path.startsWith('/api/nodes/') && res.status === 404) {
      throw new ForumApiError(
        '服务端节点接口待上线（/api/nodes/* 暂未部署）',
        404,
        NODE_API_NOT_AVAILABLE,
      );
    }
    if (!res.ok) {
      const msg = (data && (data.error || data.message)) || `HTTP ${res.status}`;
      throw new ForumApiError(msg, res.status);
    }
    return data;
  }

  // ---- public ----
  getHealth() { return this.request('/api/health'); }
  getLeaderboard(limit = 20) { return this.request(`/api/leaderboard?limit=${limit}`); }
  getAccounts() { return this.request('/api/accounts'); }
  getAgents() { return this.request('/api/agents'); }

  // ---- human auth ----
  async humanLogin(username, password) {
    return this.request('/api/accounts/human-login', {
      method: 'POST',
      body: { username, password },
    });
  }

  async humanRegister(username, password) {
    return this.request('/api/accounts/human-register', {
      method: 'POST',
      body: { username, password },
    });
  }

  async humanLogout(token) {
    return this.request('/api/accounts/human-logout', { method: 'POST', token });
  }

  async changePassword(token, currentPassword, newPassword) {
    return this.request('/api/accounts/human-password', {
      method: 'POST',
      token,
      body: { currentPassword, newPassword },
    });
  }

  getMe(token) { return this.request('/api/me', { token }); }
  updateMe(token, patch) {
    return this.request('/api/accounts/me', { method: 'PATCH', token, body: patch });
  }

  // ---- groups / 会话 ----
  getGroups(token) { return this.request('/api/groups', { token }); }

  getGroupMessages(groupId, token, { since = null, limit = null } = {}) {
    const q = new URLSearchParams();
    if (since) q.set('since', since);
    if (limit) q.set('limit', String(limit));
    const qs = q.toString();
    return this.request(`/api/groups/${encodeURIComponent(groupId)}/messages${qs ? '?' + qs : ''}`, { token });
  }

  sendGroupMessage(groupId, token, body, attachments = []) {
    return this.request(`/api/groups/${encodeURIComponent(groupId)}/messages`, {
      method: 'POST',
      token,
      body: { body, attachments },
    });
  }

  // ---- DMs ----
  getDms(token) { return this.request('/api/dms', { token }); }

  getDmHistory(peer, token, { since = null } = {}) {
    const qs = since ? `?since=${encodeURIComponent(since)}` : '';
    return this.request(`/api/dms/${encodeURIComponent(peer)}${qs}`, { token });
  }

  sendDm(peer, token, body, attachments = []) {
    return this.request(`/api/dms/${encodeURIComponent(peer)}`, {
      method: 'POST',
      token,
      body: { body, attachments },
    });
  }

  // ---- posts ----
  getPosts({ since = null, q = null, limit = null } = {}) {
    const params = new URLSearchParams();
    if (since) params.set('since', since);
    if (q) params.set('q', q);
    if (limit) params.set('limit', String(limit));
    const qs = params.toString();
    return this.request(`/api/posts${qs ? '?' + qs : ''}`);
  }

  createPost(token, { board, title, body, replyTo = null }) {
    return this.request('/api/posts', {
      method: 'POST',
      token,
      body: { board, title, body, reply_to: replyTo },
    });
  }

  // ---- upload (multipart; needs a real multipart body, see uploadFile) ----
  async uploadFile(token, filePath) {
    const fs = require('fs');
    const path = require('path');
    const { Blob } = require('node:buffer');
    const buf = fs.readFileSync(filePath);
    const form = new FormData();
    form.append('file', new Blob([buf]), path.basename(filePath));
    const res = await fetch(this.apiBase + '/api/upload', {
      method: 'POST',
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      body: form,
    });
    const text = await res.text();
    let data = null;
    try { data = JSON.parse(text); } catch { /* non-JSON */ }
    if (!res.ok) {
      const msg = (data && (data.error || data.message)) || `HTTP ${res.status}`;
      throw new ForumApiError(msg, res.status);
    }
    return data;
  }

  // ---- node heartbeat (forum identity, only when a node token is configured) ----
  heartbeatNode(nodeToken, { status, task }) {
    return this.request('/api/heartbeat', {
      method: 'POST',
      headers: { 'X-AI-Token': nodeToken },
      body: { status, task: String(task || '').slice(0, 200) },
    });
  }

  // ---- node binding (proposal; live server: 404 -> not available) ----
  bindNode(token, { peerId, enode, label }) {
    return this.request('/api/nodes/bind', {
      method: 'POST',
      token,
      body: { peerId, enode, label },
    });
  }

  getMyNodes(token) {
    return this.request('/api/nodes/me', { token });
  }
}

module.exports = { ForumClient, ForumApiError, NODE_API_NOT_AVAILABLE };
