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
  /**
   * @param {string} apiBase 主基地址（无 resolver 时就用它）
   * @param {object} opts { resolver } resolver 是 lib/serverResolver.js 的
   *   ServerResolver 单例：有它时每次请求都读 resolver.getBase()（主/备
   *   切换自动跟上），网络层失败时报给 resolver 触发切换。
   */
  constructor(apiBase, opts = {}) {
    this.apiBase = String(apiBase).replace(/\/$/, '');
    this.resolver = opts.resolver || null;
  }

  /** 本次请求用的基地址：有 resolver 就读它（可能已切到备地址）。 */
  _base() { return this.resolver ? this.resolver.getBase() : this.apiBase; }

  async request(path, { method = 'GET', body = null, token = null, headers = {}, timeoutMs = 15000 } = {}) {
    try {
      return await this._doFetch(this._base(), path, { method, body, token, headers, timeoutMs });
    } catch (err) {
      // HTTP 错误（ForumApiError）说明服务端已响应、基地址本身是通的，
      // 不切地址；只有 fetch 本身抛错（网络层失败）才算基地址不可达。
      if (err instanceof ForumApiError) throw err;
      if (!this.resolver) throw err;
      await this.resolver.reportFailure();
      // GET 是幂等的，失败后重试一次（基地址可能已切到另一边；没切
      // 时重试一次也能扛住瞬断）。非 GET 不自动重试，防重复提交。
      if (method === 'GET') {
        return await this._doFetch(this._base(), path, { method, body, token, headers, timeoutMs });
      }
      throw err;
    }
  }

  async _doFetch(base, path, { method = 'GET', body = null, token = null, headers = {}, timeoutMs = 15000 } = {}) {
    const h = { 'Content-Type': 'application/json', ...headers };
    if (token) h.Authorization = `Bearer ${token}`;
    const res = await fetch(base + path, {
      method,
      headers: h,
      body: body ? JSON.stringify(body) : null,
      signal: AbortSignal.timeout(timeoutMs),
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

  getGroupMessages(groupId, token, { since = null, limit = null, before_id = null } = {}) {
    const q = new URLSearchParams();
    if (since) q.set('since', since);
    if (limit) q.set('limit', String(limit));
    if (before_id) q.set('before_id', String(before_id));
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

  getDmHistory(peer, token, { since = null, limit = null, before_id = null } = {}) {
    const q = new URLSearchParams();
    if (since) q.set('since', since);
    if (limit) q.set('limit', String(limit));
    if (before_id) q.set('before_id', String(before_id));
    const qs = q.toString();
    return this.request(`/api/dms/${encodeURIComponent(peer)}${qs ? '?' + qs : ''}`, { token });
  }

  sendDm(peer, token, body, attachments = []) {
    return this.request(`/api/dms/${encodeURIComponent(peer)}`, {
      method: 'POST',
      token,
      body: { body, attachments },
    });
  }

  // ---- 翻译（v0.3.1）：服务端 POST /api/translate 与 /api/translate/batch。
  // 单条 {text, target, source?} -> {ok, translated, cached, same, src}；
  // 批量 {target, items:[{text, source?}]}（1..20 条）-> {ok, results:[{translated?, same?, error?}]}。
  // 限流 120 次上游/60 秒（缓存命中不计）；429 由调用方延迟重试。
  translate(token, text, target, source = null) {
    const body = { text, target };
    if (source) body.source = source;
    return this.request('/api/translate', { method: 'POST', token, body });
  }

  translateBatch(token, items, target) {
    return this.request('/api/translate/batch', {
      method: 'POST',
      token,
      body: { target, items },
      timeoutMs: 30000, // 批量等最慢的上游，放宽到 30 秒
    });
  }

  // ---- 板块（v0.3.1）：GET /api/boards 直接回全部板块，不依赖帖子拉取 ----
  getBoards() { return this.request('/api/boards'); }

  // ---- 社区 / 团队（v0.3.1，只读）：GET /api/communities、详情、账本 ----
  getCommunities(token) { return this.request('/api/communities', { token }); }

  getCommunity(token, id) {
    return this.request(`/api/communities/${encodeURIComponent(id)}`, { token });
  }

  // 团队账本：GET /api/communities/:id/ledger，成员 only；分页 {items, total, has_more}。
  getCommunityLedger(token, id, { limit = null } = {}) {
    const qs = limit ? `?limit=${encodeURIComponent(String(limit))}` : '';
    return this.request(`/api/communities/${encodeURIComponent(id)}/ledger${qs}`, { token });
  }

  // ---- 加群（v0.3.1）：POST /api/groups/:id/members {name}。
  // 注意服务端口径：公开群只有现有成员能拉人，非成员自加会回 403；
  // 私密群须成员或站长。调用方把服务端原话展示出来，不自己编理由。
  joinGroup(token, groupId, name) {
    return this.request(`/api/groups/${encodeURIComponent(groupId)}/members`, {
      method: 'POST',
      token,
      body: { name },
    });
  }

  // ---- posts ----
  getPosts(token, { since = null, q = null, limit = null } = {}) {
    const params = new URLSearchParams();
    if (since) params.set('since', since);
    if (q) params.set('q', q);
    if (limit) params.set('limit', String(limit));
    const qs = params.toString();
    return this.request(`/api/posts${qs ? '?' + qs : ''}`, { token });
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
    // 上传是 POST，不自动重试（防重复提交）；网络层失败时报给 resolver，
    // 让下一次请求用上切过去的基地址。
    try {
      return await this._doUpload(this._base(), token, form);
    } catch (err) {
      if (!(err instanceof ForumApiError) && this.resolver) {
        await this.resolver.reportFailure();
      }
      throw err;
    }
  }

  async _doUpload(base, token, form) {
    const res = await fetch(base + '/api/upload', {
      method: 'POST',
      headers: token ? { Authorization: `Bearer ${token}` } : {},
      body: form,
      signal: AbortSignal.timeout(120000), // 大图上传放宽到 120 秒
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

  // ---- 已读：POST /api/read {group_id, seq}，与 WS receipt 同口径 ----
  markGroupRead(token, groupId, seq) {
    return this.request('/api/read', {
      method: 'POST',
      token,
      body: { group_id: groupId, seq },
    });
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
