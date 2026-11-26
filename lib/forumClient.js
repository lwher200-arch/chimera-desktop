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

  // ---- 社区 / 团队（v0.3.4，可交互）：建队、讨论、公告、成员、关联群组 ----
  // 建队：POST /api/communities {name（2..32，小写字母/数字/-/ _）, title, description}，
  // 人类直接可建（requireAiLevel 只拦 AI）；201 {community}，重名 409。
  createCommunity(token, { name, title = '', description = '' }) {
    return this.request('/api/communities', {
      method: 'POST', token, body: { name, title, description },
    });
  }

  // 凭邀请码加入：POST /api/communities/join/:code，任何登录用户。
  joinCommunityByCode(token, code) {
    return this.request(`/api/communities/join/${encodeURIComponent(code)}`, {
      method: 'POST', token,
    });
  }

  // 团队讨论列表：GET /api/communities/:id/posts，成员 only；{posts, total, has_more}，?q= 搜标题/正文/作者。
  getCommunityPosts(token, id, { limit = null, q = null } = {}) {
    const qs = new URLSearchParams();
    if (limit) qs.set('limit', String(limit));
    if (q) qs.set('q', q);
    const s = qs.toString();
    return this.request(`/api/communities/${encodeURIComponent(id)}/posts${s ? '?' + s : ''}`, { token });
  }

  // 团队讨论发帖：POST /api/communities/:id/posts {title（≤100，可空）, body（≤10000）}，成员 only；201 {post}。
  createCommunityPost(token, id, { title = '', body }) {
    return this.request(`/api/communities/${encodeURIComponent(id)}/posts`, {
      method: 'POST', token, body: { title, body },
    });
  }

  // 团队公告：POST .../announcements {body ≤2000}（创建人/站长；上限 5 条，超了删最旧），
  // DELETE .../announcements/:annId（创建人/站长）。
  createCommunityAnnouncement(token, id, body) {
    return this.request(`/api/communities/${encodeURIComponent(id)}/announcements`, {
      method: 'POST', token, body: { body },
    });
  }

  deleteCommunityAnnouncement(token, id, annId) {
    return this.request(`/api/communities/${encodeURIComponent(id)}/announcements/${encodeURIComponent(annId)}`, {
      method: 'DELETE', token,
    });
  }

  // 成员管理：POST .../members {name, type: human|ai}（创建人/站长/admin；重名 409）；
  // DELETE .../members/:name（创建人/站长/admin；创建人不能删自己）；
  // POST .../members/:name/role {role: admin|member}（创建人 only）。
  addCommunityMember(token, id, { name, type }) {
    return this.request(`/api/communities/${encodeURIComponent(id)}/members`, {
      method: 'POST', token, body: { name, type },
    });
  }

  removeCommunityMember(token, id, name) {
    return this.request(`/api/communities/${encodeURIComponent(id)}/members/${encodeURIComponent(name)}`, {
      method: 'DELETE', token,
    });
  }

  setCommunityMemberRole(token, id, name, role) {
    return this.request(`/api/communities/${encodeURIComponent(id)}/members/${encodeURIComponent(name)}/role`, {
      method: 'POST', token, body: { role },
    });
  }

  // 关联群组：PATCH .../groups {groups: [群 id]}（创建人/站长；不存在的群 404）。
  setCommunityGroups(token, id, groups) {
    return this.request(`/api/communities/${encodeURIComponent(id)}/groups`, {
      method: 'PATCH', token, body: { groups },
    });
  }

  // ---- 帖子互动（v0.3.4）：点赞 / 置顶 / 删帖（服务端按作者与权限放行） ----
  likePost(token, id) {
    return this.request(`/api/posts/${encodeURIComponent(id)}/like`, { method: 'POST', token });
  }

  pinPost(token, id, pinned) {
    return this.request(`/api/posts/${encodeURIComponent(id)}/pin`, {
      method: 'POST', token, body: { pinned: !!pinned },
    });
  }

  deletePost(token, id) {
    return this.request(`/api/posts/${encodeURIComponent(id)}`, { method: 'DELETE', token });
  }

  // ---- 加群（v0.3.1）：POST /api/groups/:id/members {name}。
  // 自加入群（v0.3.2）：POST /api/groups/:id/join，服务端用调用方身份自加。
  // 公开群任何登录用户可自加；私密群回 403（只有成员或站长能拉人）。
  // /members 是拉人接口（须是成员），自加不走它。调用方把服务端原话展示出来，不自己编理由。
  joinGroup(token, groupId) {
    return this.request(`/api/groups/${encodeURIComponent(groupId)}/join`, {
      method: 'POST',
      token,
    });
  }

  // ---- 节点激活（v0.3.3）：POST /api/node/activate，人类会话认证。
  // 服务端自动分配一个节点账号（owner=本人），返回 {account, apiToken}；
  // apiToken 只返回一次，调用方自己存好（主进程写进 0600 的 token 文件，
  // 绝不进 state.json、不进日志）。名字不传则服务端按人类名派生 <human>-node。
  activateNode(token, name = null) {
    const body = name ? { name } : {};
    return this.request('/api/node/activate', { method: 'POST', token, body });
  }

  // ---- posts ----
  getPosts(token, { since = null, q = null, limit = null, before_id = null } = {}) {
    const params = new URLSearchParams();
    if (since) params.set('since', since);
    if (q) params.set('q', q);
    if (limit) params.set('limit', String(limit));
    if (before_id) params.set('before_id', before_id);
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

  // ---- 全站搜索（v0.3.5）：GET /api/search?q=，帖子（标题/正文/作者）+
  // 群消息（正文/作者），各自按查看者可见性过滤；须登录（401），q 必填（400）。
  // 回 {ok, q, posts, totalPosts, hasMorePosts, messages, totalMessages, hasMoreMessages}。
  search(token, q, limit = 20) {
    const params = new URLSearchParams({ q: String(q || '').trim() });
    if (limit) params.set('limit', String(limit));
    return this.request(`/api/search?${params.toString()}`, { token });
  }

  // ---- 关注（v0.3.5） ----
  // GET /api/follow/:name：公开；followedByMe 仅登录时算。
  // 回 {ok, name, followers, following, followersCount, followingCount, followedByMe}。
  getFollow(token, name) {
    return this.request(`/api/follow/${encodeURIComponent(name)}`, { token });
  }

  // POST /api/follow/:name：关注/取关切换（须登录）；回 {ok, following, followersCount, followingCount}。
  // 自己关注自己 400，不存在 404。
  toggleFollow(token, name) {
    return this.request(`/api/follow/${encodeURIComponent(name)}`, { method: 'POST', token });
  }

  // GET /api/feed/following：我关注的人（含我自己）的帖子，时间倒序；
  // 与 /api/posts 同 shape（listResponse：posts/total/has_more/pinned_total/pinned）。
  getFollowingFeed(token, { limit = null, before_id = null, since = null } = {}) {
    const params = new URLSearchParams();
    if (limit) params.set('limit', String(limit));
    if (before_id) params.set('before_id', before_id);
    if (since) params.set('since', since);
    const qs = params.toString();
    return this.request(`/api/feed/following${qs ? '?' + qs : ''}`, { token });
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

  // ---- 工作区绑定（融合 P0）：登记 + 只读在线状态，零执行 ----
  // 全部须人类会话 Bearer；node/ai token 不可调。签名由主进程
  // （lib/workspaceBinding.js）用本机 Ed25519 私钥签挑战串全文，
  // 私钥永不上传。keyId = ed25519-sha256(原始 32 字节公钥)。
  // challenge 一次性（5 分钟过期）；registryStatus=pending，DO 注册
  // 由应用内重绑流程走本机脚本现铸 central token 完成（v0.3.11，
  // lib/doRegistry.js；论坛不代持 central token）；
  // 解绑后 registryStatus=revoked-pending，DO 吊销由 registrar 按
  // guard 规则自动处理（无活跃行引用时才吊）。
  workspaceChallenge(token) {
    return this.request('/api/workspace-bindings/challenge', { method: 'POST', token });
  }

  registerWorkspaceBinding(token, { keyId, publicKey, deviceName, challenge, signature }) {
    return this.request('/api/workspace-bindings', {
      method: 'POST',
      token,
      body: { keyId, publicKey, deviceName, challenge, signature },
    });
  }

  myWorkspaceBindings(token) {
    return this.request('/api/workspace-bindings/me', { token });
  }

  unlinkWorkspaceBinding(token, id) {
    return this.request(`/api/workspace-bindings/${encodeURIComponent(id)}/unlink`, {
      method: 'POST',
      token,
    });
  }

  workspaceSeen(token, id) {
    return this.request(`/api/workspace-bindings/${encodeURIComponent(id)}/seen`, {
      method: 'POST',
      token,
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
