'use strict';
/**
 * test/smoke.js — 冒烟测试（npm test）。
 * 1. live API：ForumClient 调 https://forum.coldhao.win/api/health。
 * 2. live SSE：匿名连 /api/stream，等到 hello 事件（含 seq）即断开。
 * 3. 轻节点：对本地 mock 种子起停 NodeManager——开关、端口开合、
 *    哈希链、干净停止、开关持久化（Store round-trip）全验。
 * 不登录、不发消息、不写任何线上状态。
 */
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const crypto = require('node:crypto');

const { ForumClient } = require('../lib/forumClient');
const { SseClient } = require('../lib/sseClient');
const { WsClient } = require('../lib/wsClient');
const { RealtimeClient } = require('../lib/realtimeClient');
const { nextBackoffMs, INITIAL_BACKOFF_MS, MAX_BACKOFF_MS } = require('../lib/backoff');
const { ServerResolver, PRIMARY_BASE, FALLBACK_BASE } = require('../lib/serverResolver');
const { AppUpdater, compareVersions, sha256Hex } = require('../lib/appUpdater');
const { NodeManager, DEFAULT_L2_RPC, resolveL2Rpc } = require('../lib/nodeManager');
const { Store } = require('../lib/store');
const { buildAppMenuTemplate, applyAppMenu } = require('../lib/appMenu');

const API_BASE = process.env.CHIMERA_API_BASE || 'https://forum.coldhao.win';
let passed = 0;

function ok(name) { passed += 1; console.log(`  ok - ${name}`); }

async function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} 超时 ${ms}ms`)), ms);
  });
  try { return await Promise.race([promise, timeout]); }
  finally { clearTimeout(timer); }
}

// Mock 种子：只提供 /api/health，供 NodeManager 同步循环用。
function startMockSeed() {
  return new Promise((resolve) => {
    const server = http.createServer((_req, res) => {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, posts: 472, accounts: 30, groups: 10, messages: 1676, dms: 7 }));
    });
    server.listen(0, '127.0.0.1', () => resolve(server));
  });
}

// Mock 论坛（仅 §3b 节点上传测试用）：/api/health + /api/node/headers
// （按真实服务端口径验链：401/403/409/400 + ack）+ /api/heartbeat +
// /api/stream（SSE：hello 后发 seq 101 再跳到 104，供 gap 测试）+
// POST /api/groups/:id/join（v0.3.2 自加入群：公开群 200、私密群 403）+
// POST /api/node/activate（v0.3.3 激活即分配：人类 Bearer 201、无 token 401）+
// 社区/团队可交互（v0.3.4）：/api/communities*、/api/posts/:id/like|pin、
// DELETE /api/posts/:id、POST /api/posts（回帖继承 team_id）。
// 只有 token 'test-node-token' 是节点身份；'test-ai-token' 是 AI 账号（403）。
// Bearer 'test-token' = 人类 testhuman（mockteam 创建人）；
// 'test-other-token' = 人类 otherhuman（初始非成员）；
// 'test-stranger-token' = 人类 stranger（永不加入，用于 403 断言）。
// state 暴露出来供测试断言与预置（setNodeState 模拟服务端超前）。
function startMockForum() {
  const state = {
    node: { lastSeq: 0, lastHash: '0'.repeat(64), received: 0, heads: [] },
    uploads: [], // 每次成功上传的 batch（headers 数组）
    heartbeats: 0,
    // 工作区绑定（融合 P0）：bindings 行 + 内存 challenge（5 分钟过期，一次性）。
    workspaceBindings: [], // {id, accountId, keyId, publicKey, deviceName, createdTs, lastSeenTs, unlinkedTs, registryStatus}
    workspaceChallenges: {}, // challenge -> {accountName, expiresAt, used}
    workspaceBindingSeq: 0,
    failNextUpload: 0, // >0 时下几次上传回 500（重试测试用）
    dropNextUploadAck: 0, // >0 时下几次上传存下后不回 ack（丢 ack 测试用，R1/R2）
    // mock 团队：创建人 testhuman，成员 testai；otherhuman/stranger 初始非成员。
    communities: [{
      id: 'com-mock', name: 'mockteam', title: 'Mock 团队', description: 'mock 团队简介',
      creator: 'testhuman', creator_type: 'human', createdTs: '2026-01-01T00:00:00.000Z',
      members: [
        { name: 'testhuman', type: 'human', role: 'creator', joinedTs: '2026-01-01T00:00:00.000Z' },
        { name: 'testai', type: 'ai', role: 'member', joinedTs: '2026-01-01T00:00:00.000Z' },
      ],
      groups: [], announcements: [], inviteCode: 'MOCKCODE',
    }],
    teamPosts: [], // 团队讨论帖（team_id 指向 mock 团队；回帖也进这里）
    // 主帖子页（v0.3.5）：非团队帖；消息供 /api/search 用；账号资料供资料卡用。
    feedPosts: [
      { id: 'p-feed-1', author: 'otherhuman', author_type: 'human', topic: 'general', board: 'general', title: null, body: '欢迎来到奇美拉', team_id: null, group_id: null, reply_to: null, likes: [], pinned: false, ts: '2026-01-02T00:00:00.000Z' },
      { id: 'p-feed-2', author: 'testhuman', author_type: 'human', topic: 'random', board: 'random', title: null, body: '随便聊聊', team_id: null, group_id: null, reply_to: null, likes: [], pinned: false, ts: '2026-01-03T00:00:00.000Z' },
    ],
    feedMessages: [
      { id: 'm-feed-1', author: 'otherhuman', author_type: 'human', group_id: 'grp-public', body: '群里说一句欢迎', ts: '2026-01-02T01:00:00.000Z' },
      { id: 'm-feed-2', author: 'stranger', author_type: 'human', group_id: 'grp-public', body: '路过打个招呼', ts: '2026-01-02T02:00:00.000Z' },
    ],
    accounts: [
      { id: 'acc-testhuman', name: 'testhuman', type: 'human', role: 'member', bio: 'mock 人类', signature: '你好', tags: ['mock'], strengths: ['测试'], weaknesses: [], links: [], points: 10, level: 'L1', levelNum: 1, status: 'active', online: true, stats: { posts: 2, replies: 1, messages: 5, likesReceived: 3, shared: 0 } },
      { id: 'acc-otherhuman', name: 'otherhuman', type: 'human', role: 'member', bio: '另一个 mock 人类', signature: '', tags: [], strengths: [], weaknesses: [], links: [], points: 5, level: 'L1', levelNum: 1, status: 'active', online: false, stats: { posts: 1, replies: 0, messages: 2, likesReceived: 1, shared: 0 } },
      { id: 'acc-stranger', name: 'stranger', type: 'human', role: 'member', bio: '', signature: '', tags: [], strengths: [], weaknesses: [], links: [], points: 0, level: 'L1', levelNum: 1, status: 'active', online: false, stats: { posts: 0, replies: 0, messages: 1, likesReceived: 0, shared: 0 } },
      { id: 'acc-testai', name: 'testai', type: 'ai', role: 'member', bio: 'mock AI', signature: '', tags: ['ai'], strengths: [], weaknesses: [], links: [], points: 20, level: 'L2', levelNum: 2, status: 'active', online: true, stats: { posts: 0, replies: 3, messages: 0, likesReceived: 0, shared: 0 }, model: 'mock-model', isBot: true, verified: null, owner: 'testhuman' },
      { id: 'acc-newai', name: 'newai', type: 'ai', role: 'member', bio: '', signature: '', tags: [], strengths: [], weaknesses: [], links: [], points: 0, level: 'L1', levelNum: 1, status: 'active', online: false, stats: { posts: 0, replies: 0, messages: 0, likesReceived: 0, shared: 0 }, model: 'mock-model', isBot: false, verified: null, owner: 'testhuman' },
    ],
    follows: [], // {follower, follower_type, following, following_type, ts}
    // 通知中心（v0.3.15）：全类型回包（mention/reply/like/follow），
    // unreadCount 按 notifReadTs 算（与真实服务端同一口径）。
    notifications: [
      { id: 'mention:p-feed-1', type: 'mention', ts: '2026-11-25T10:00:00.000Z', actor: { name: 'otherhuman', type: 'human' }, postId: 'p-feed-1', preview: '@testhuman 看这个' },
      { id: 'reply:p-feed-1', type: 'reply', ts: '2026-11-25T11:00:00.000Z', actor: { name: 'otherhuman', type: 'human' }, postId: 'p-feed-1', preview: '回了你的帖子' },
      { id: 'mention-msg:m-feed-1', type: 'mention', ts: '2026-11-25T12:00:00.000Z', actor: { name: 'stranger', type: 'human' }, groupId: 'grp-public', preview: '@testhuman 群里 @ 你' },
      { id: 'like:p-feed-1', type: 'like', ts: '2026-11-25T13:00:00.000Z', actor: { name: 'stranger', type: 'human' }, postId: 'p-feed-1', preview: '', likers: ['stranger'], likesCount: 1 },
    ],
    notifReadTs: null,
  };
  const canonical = (o) => JSON.stringify(o, Object.keys(o).sort());
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    if (url.pathname === '/api/stream') {
      res.writeHead(200, { 'Content-Type': 'text/event-stream', 'Cache-Control': 'no-cache' });
      res.write('retry: 3000\n\n');
      const frame = (id, event, data) => `id: ${id}\nevent: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
      res.write(frame(100, 'hello', { seq: 100, time: new Date().toISOString() }));
      res.write(frame(101, 'group_message', { seq: 101, type: 'group_message', ts: new Date().toISOString(), payload: { group_id: 'g1' } }));
      res.write(frame(104, 'group_message', { seq: 104, type: 'group_message', ts: new Date().toISOString(), payload: { group_id: 'g1' } }));
      req.on('close', () => { try { res.end(); } catch {} });
      return;
    }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      res.setHeader('Content-Type', 'application/json');
      if (url.pathname === '/api/heartbeat') {
        if (req.headers['x-ai-token'] !== 'test-node-token' && req.headers['x-ai-token'] !== 'test-ai-token') {
          res.writeHead(401); res.end(JSON.stringify({ ok: false, error: 'login required' })); return;
        }
        state.heartbeats += 1;
        res.end(JSON.stringify({ ok: true, lastActiveTs: new Date().toISOString(), online: true })); return;
      }
      const joinM = /^\/api\/groups\/([^/]+)\/join$/.exec(url.pathname);
      if (req.method === 'POST' && joinM) {
        if (!req.headers.authorization) { res.writeHead(401); res.end(JSON.stringify({ ok: false, error: 'login required' })); return; }
        if (joinM[1] === 'grp-private') { res.writeHead(403); res.end(JSON.stringify({ ok: false, error: 'private group: only members or the forum owner can add you' })); return; }
        res.end(JSON.stringify({ ok: true, group: { id: joinM[1], name: 'mock-group', members: [{ name: 'me', type: 'human', joinedTs: new Date().toISOString() }] } })); return;
      }
      // POST /api/node/activate（v0.3.3 激活即分配）：人类 Bearer 201；
      // 无 token 401；X-AI-Token（AI/节点）403（与真实服务端同一口径）。
      if (req.method === 'POST' && url.pathname === '/api/node/activate') {
        if (req.headers['x-ai-token']) { res.writeHead(403); res.end(JSON.stringify({ ok: false, error: 'only human accounts can activate a node' })); return; }
        if (!req.headers.authorization) { res.writeHead(401); res.end(JSON.stringify({ ok: false, error: 'login required' })); return; }
        let data = null;
        try { data = JSON.parse(body || '{}'); } catch { res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'invalid JSON' })); return; }
        const name = (data && data.name) || 'testhuman-node';
        if (!/^[a-z0-9][a-z0-9_-]{1,31}$/.test(name)) { res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'AI name must be 2..32 chars' })); return; }
        res.writeHead(201); res.end(JSON.stringify({ ok: true, account: { id: 'acc-mock-node', name, type: 'node', owner: 'testhuman', role: 'member' }, apiToken: 'mock-node-token' })); return;
      }
      if (url.pathname === '/api/node/headers') {
        if (req.method === 'GET') {
          const tok = req.headers['x-ai-token'];
          if (tok) {
            if (tok !== 'test-node-token') { res.writeHead(401); res.end(JSON.stringify({ ok: false, error: 'invalid AI token' })); return; }
            res.end(JSON.stringify({ ok: true, node: 'node:testnode', lastSeq: state.node.lastSeq, lastHash: state.node.lastHash, received: state.node.received, updatedAt: new Date().toISOString() })); return;
          }
          const node = url.searchParams.get('node');
          if (!node) { res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'node query param required without token' })); return; }
          res.end(JSON.stringify({ ok: true, node, lastSeq: state.node.lastSeq, lastHash: state.node.lastHash, received: state.node.received, updatedAt: null })); return;
        }
        // POST：节点身份 + 验链 + ack（与真实服务端同一口径）
        const tok = req.headers['x-ai-token'];
        if (!tok) { res.writeHead(401); res.end(JSON.stringify({ ok: false, error: 'login required' })); return; }
        if (tok === 'test-ai-token') { res.writeHead(403); res.end(JSON.stringify({ ok: false, error: 'only node accounts can upload headers' })); return; }
        if (tok !== 'test-node-token') { res.writeHead(401); res.end(JSON.stringify({ ok: false, error: 'invalid AI token' })); return; }
        if (state.failNextUpload > 0) { state.failNextUpload -= 1; res.writeHead(500); res.end(JSON.stringify({ ok: false, error: 'mock transient 500' })); return; }
        let data = null;
        try { data = JSON.parse(body); } catch { res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'invalid JSON' })); return; }
        const headers = data && data.headers;
        if (!Array.isArray(headers) || headers.length === 0 || headers.length > 100) { res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'headers must be an array of 1..100' })); return; }
        for (const h of headers) {
          if (typeof h !== 'object' || h === null) { res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'header must be an object' })); return; }
          const { hash, ...rest } = h;
          const expect = crypto.createHash('sha256').update(canonical(rest), 'utf8').digest('hex');
          if (expect !== hash) { res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'hash mismatch for seq ' + h.seq })); return; }
        }
        for (let i = 1; i < headers.length; i++) {
          if (headers[i].seq !== headers[i - 1].seq + 1 || headers[i].prevHash !== headers[i - 1].hash) {
            res.writeHead(400); res.end(JSON.stringify({ ok: false, error: 'batch not continuous at index ' + i })); return;
          }
        }
        const first = headers[0];
        const isRebase = first.seq === 1 && first.prevHash === '0'.repeat(64);
        if (state.node.received > 0 && !isRebase) {
          if (first.seq !== state.node.lastSeq + 1 || first.prevHash !== state.node.lastHash) {
            res.writeHead(409); res.end(JSON.stringify({ ok: false, error: 'chain gap', expectedSeq: state.node.lastSeq + 1, expectedPrevHash: state.node.lastHash, lastSeq: state.node.lastSeq })); return;
          }
        }
        if (isRebase) state.node = { lastSeq: 0, lastHash: '0'.repeat(64), received: 0, heads: [] };
        state.node.heads = state.node.heads.concat(headers).slice(-1000);
        state.node.lastSeq = headers[headers.length - 1].seq;
        state.node.lastHash = headers[headers.length - 1].hash;
        state.node.received += headers.length;
        state.uploads.push(headers);
        if (state.dropNextUploadAck > 0) {
          state.dropNextUploadAck -= 1;
          res.destroy(); // 服务端已存下、ack 在半路丢了（R1/R2 的丢 ack 情形）
          return;
        }
        res.end(JSON.stringify({ ok: true, accepted: headers.length, lastSeq: state.node.lastSeq, lastHash: state.node.lastHash, node: 'node:testnode' }));
        return;
      }
      // ---- 社区 / 团队（v0.3.4 可交互，mock 口径与真实服务端一致） ----
      const bearerMe = () => {
        const a = req.headers.authorization;
        if (a === 'Bearer test-token') return { name: 'testhuman', type: 'human' };
        if (a === 'Bearer test-other-token') return { name: 'otherhuman', type: 'human' };
        if (a === 'Bearer test-stranger-token') return { name: 'stranger', type: 'human' };
        return null;
      };
      const commJson = () => { try { return JSON.parse(body || '{}'); } catch { return null; } };
      const comm401 = () => { res.writeHead(401); res.end(JSON.stringify({ ok: false, error: 'login required' })); };
      const commErr = (status, msg) => { res.writeHead(status); res.end(JSON.stringify({ ok: false, error: msg })); };
      const pubComm = (c) => ({ id: c.id, name: c.name, title: c.title, description: c.description, memberCount: c.members.length, groupCount: c.groups.length, creator: c.creator, creator_type: c.creator_type, createdTs: c.createdTs });
      const detailComm = (c) => ({
        ...pubComm(c),
        members: c.members.map((m) => ({ name: m.name, type: m.type, role: m.name === c.creator ? 'creator' : (m.role || 'member') })),
        groups: c.groups.map((id) => ({ id, name: id === 'grp-public' ? 'mock-group' : 'mock-private', kind: 'chat', visibility: id === 'grp-private' ? 'private' : 'public' })),
        announcements: c.announcements, modules_enabled: ['posts', 'ledger'],
      });
      const isMember = (c, me) => !!(me && c.members.some((m) => m.name === me.name && m.type === me.type));
      const isCreator = (c, me) => !!(me && c.creator === me.name);
      const isAdmin = (c, me) => isCreator(c, me) || !!(me && c.members.some((m) => m.name === me.name && m.role === 'admin'));
      const findComm = (m) => (m ? state.communities.find((x) => x.id === decodeURIComponent(m[1])) : null);
      const pubPost = (p) => ({ ...p, likesCount: p.likes.length, likedBy: p.likes.map((l) => l.name), replyCount: [...state.teamPosts, ...state.feedPosts].filter((x) => x.reply_to === p.id && !x.deleted).length });
      // v0.3.5：主帖子页与团队讨论共用一张帖子表（mock 里分两个数组），点赞/置顶/删帖两边都找。
      const findAnyPost = (id) => [...state.teamPosts, ...state.feedPosts].find((x) => x.id === id && !x.deleted);
      // ---- 工作区绑定（融合 P0，mock 口径与契约一致） ----
      // 校验顺序：会话人类 → challenge 有效且未用过（消费掉）→ 签名验过（401）
      // → keyId == ed25519-sha256(publicKey)（400）→ deviceName ≤64（400）
      // → 同 keyId 已有活跃绑定：同账号幂等、他账号 409。
      // 解绑：置 unlinkedTs、registryStatus=revoked-pending（DO 吊销待 Owner
      // 供 setup token，mock 不调 DO）；已解绑再调回 409；解绑后 seen 回 409。
      const wsAccountId = (name) => {
        const a = state.accounts.find((x) => x.name === name);
        return a ? a.id : 'acc-' + name;
      };
      const wsVerifySig = (challenge, publicKeyB64, signatureB64) => {
        try {
          const raw = Buffer.from(String(publicKeyB64 || ''), 'base64');
          if (raw.length !== 32) return false;
          const der = Buffer.concat([Buffer.from('302a300506032b6570032100', 'hex'), raw]);
          const pub = crypto.createPublicKey({ key: der, format: 'der', type: 'spki' });
          return crypto.verify(null, Buffer.from(String(challenge), 'utf8'), pub,
            Buffer.from(String(signatureB64 || ''), 'base64'));
        } catch { return false; }
      };
      const wsKeyId = (publicKeyB64) => {
        const raw = Buffer.from(String(publicKeyB64 || ''), 'base64');
        return 'ed25519-' + crypto.createHash('sha256').update(raw).digest('hex');
      };
      if (req.method === 'POST' && url.pathname === '/api/workspace-bindings/challenge') {
        const me = bearerMe(); if (!me) return comm401();
        const challenge = 'wsbind:' + crypto.randomBytes(32).toString('hex');
        state.workspaceChallenges[challenge] = { accountName: me.name, expiresAt: Date.now() + 5 * 60 * 1000, used: false };
        res.end(JSON.stringify({ ok: true, challenge, expiresAt: new Date(Date.now() + 5 * 60 * 1000).toISOString() })); return;
      }
      if (req.method === 'POST' && url.pathname === '/api/workspace-bindings') {
        const me = bearerMe(); if (!me) return comm401();
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        const rec = state.workspaceChallenges[data.challenge];
        if (!rec || rec.used || Date.now() > rec.expiresAt || rec.accountName !== me.name) return commErr(400, 'challenge invalid or expired');
        rec.used = true; // 一次性：校验通过即消费，重放必 400
        if (!wsVerifySig(data.challenge, data.publicKey, data.signature)) return commErr(401, 'invalid signature');
        if (data.keyId !== wsKeyId(data.publicKey)) return commErr(400, 'keyId does not match publicKey');
        const deviceName = String(data.deviceName || '').replace(/[\u0000-\u001f\u007f]/g, '').trim();
        if (!deviceName || deviceName.length > 64) return commErr(400, 'deviceName must be 1..64 chars');
        const existing = state.workspaceBindings.find((b) => b.keyId === data.keyId && !b.unlinkedTs);
        if (existing) {
          if (existing.accountId !== wsAccountId(me.name)) return commErr(409, 'keyId already bound to another account');
          existing.deviceName = deviceName;
          existing.lastSeenTs = new Date().toISOString();
          res.end(JSON.stringify({ ok: true, binding: existing })); return;
        }
        state.workspaceBindingSeq += 1;
        const row = {
          id: 'wsb-' + state.workspaceBindingSeq,
          accountId: wsAccountId(me.name),
          keyId: data.keyId, publicKey: data.publicKey, deviceName,
          createdTs: new Date().toISOString(), lastSeenTs: new Date().toISOString(),
          unlinkedTs: null, registryStatus: 'pending',
        };
        state.workspaceBindings.push(row);
        res.end(JSON.stringify({ ok: true, binding: row })); return;
      }
      if (req.method === 'GET' && url.pathname === '/api/workspace-bindings/me') {
        const me = bearerMe(); if (!me) return comm401();
        const mine = state.workspaceBindings.filter((b) => b.accountId === wsAccountId(me.name));
        res.end(JSON.stringify({ ok: true, bindings: mine })); return;
      }
      const wsUnlinkM = /^\/api\/workspace-bindings\/([^/]+)\/unlink$/.exec(url.pathname);
      if (req.method === 'POST' && wsUnlinkM) {
        const me = bearerMe(); if (!me) return comm401();
        const row = state.workspaceBindings.find((b) => b.id === decodeURIComponent(wsUnlinkM[1]));
        if (!row) return commErr(404, 'binding not found');
        if (row.accountId !== wsAccountId(me.name) && me.name !== 'coldhao') return commErr(403, 'not your binding');
        if (row.unlinkedTs) return commErr(409, 'binding already unlinked');
        row.unlinkedTs = new Date().toISOString();
        row.registryStatus = 'revoked-pending';
        res.end(JSON.stringify({ ok: true, binding: row })); return;
      }
      const wsSeenM = /^\/api\/workspace-bindings\/([^/]+)\/seen$/.exec(url.pathname);
      if (req.method === 'POST' && wsSeenM) {
        const me = bearerMe(); if (!me) return comm401();
        const row = state.workspaceBindings.find((b) => b.id === decodeURIComponent(wsSeenM[1]));
        if (!row) return commErr(404, 'binding not found');
        if (row.accountId !== wsAccountId(me.name)) return commErr(403, 'not your binding');
        if (row.unlinkedTs) return commErr(409, 'binding already unlinked');
        row.lastSeenTs = new Date().toISOString();
        res.end(JSON.stringify({ ok: true, lastSeenTs: row.lastSeenTs })); return;
      }
      // POST /api/communities（建队）
      if (req.method === 'POST' && url.pathname === '/api/communities') {
        const me = bearerMe(); if (!me) return comm401();
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        const name = String(data.name || '').trim();
        if (!/^[a-z0-9][a-z0-9_-]{1,31}$/.test(name)) return commErr(400, 'team name must be 2..32 chars, lowercase letters/digits/-/ _');
        if (state.communities.some((c) => c.name === name)) return commErr(409, 'team name already taken');
        const c = { id: 'com-' + name, name, title: String(data.title || '').trim().slice(0, 32) || name, description: String(data.description || '').trim().slice(0, 500), creator: me.name, creator_type: me.type, createdTs: new Date().toISOString(), members: [{ name: me.name, type: me.type, role: 'creator', joinedTs: new Date().toISOString() }], groups: [], announcements: [], inviteCode: 'CODE-' + name.toUpperCase() };
        state.communities.push(c);
        res.writeHead(201); res.end(JSON.stringify({ ok: true, community: pubComm(c) })); return;
      }
      // POST /api/communities/join/:code（先于 :id 匹配）
      const commJoinM = /^\/api\/communities\/join\/([^/]+)$/.exec(url.pathname);
      if (req.method === 'POST' && commJoinM) {
        const me = bearerMe(); if (!me) return comm401();
        const c = state.communities.find((x) => x.inviteCode === decodeURIComponent(commJoinM[1]));
        if (!c) return commErr(404, 'invalid invite code');
        if (!isMember(c, me)) c.members.push({ name: me.name, type: me.type, role: 'member', joinedTs: new Date().toISOString() });
        res.end(JSON.stringify({ ok: true, community: pubComm(c) })); return;
      }
      // GET /api/communities
      if (req.method === 'GET' && url.pathname === '/api/communities') {
        res.end(JSON.stringify({ ok: true, communities: state.communities.map(pubComm) })); return;
      }
      const commIdM = /^\/api\/communities\/([^/]+)$/.exec(url.pathname);
      const commPostsM = /^\/api\/communities\/([^/]+)\/posts$/.exec(url.pathname);
      const commAnnM = /^\/api\/communities\/([^/]+)\/announcements$/.exec(url.pathname);
      const commAnnOneM = /^\/api\/communities\/([^/]+)\/announcements\/([^/]+)$/.exec(url.pathname);
      const commMembersM = /^\/api\/communities\/([^/]+)\/members$/.exec(url.pathname);
      const commMemberM = /^\/api\/communities\/([^/]+)\/members\/([^/]+)$/.exec(url.pathname);
      const commRoleM = /^\/api\/communities\/([^/]+)\/members\/([^/]+)\/role$/.exec(url.pathname);
      const commGroupsM = /^\/api\/communities\/([^/]+)\/groups$/.exec(url.pathname);
      // GET /api/communities/:id
      if (req.method === 'GET' && commIdM) {
        const c = findComm(commIdM); if (!c) return commErr(404, 'team not found');
        res.end(JSON.stringify({ ok: true, community: detailComm(c) })); return;
      }
      // GET/POST /api/communities/:id/posts
      if (commPostsM) {
        const c = findComm(commPostsM); if (!c) return commErr(404, 'team not found');
        const me = bearerMe(); if (!me) return comm401();
        if (!isMember(c, me)) return commErr(403, 'only team members can view team discussions');
        if (req.method === 'GET') {
          const q = (url.searchParams.get('q') || '').toLowerCase();
          let posts = state.teamPosts.filter((p) => p.team_id === c.id && !p.deleted);
          if (q) posts = posts.filter((p) => (p.title || '').toLowerCase().includes(q) || (p.body || '').toLowerCase().includes(q) || (p.author || '').toLowerCase().includes(q));
          const items = posts.map(pubPost);
          res.end(JSON.stringify({ ok: true, posts: items, total: items.length, has_more: false })); return;
        }
        if (req.method === 'POST') {
          const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
          const bodyText = String(data.body || '');
          if (!bodyText.trim() || bodyText.length > 10000) return commErr(400, 'body must be 1..10000 chars');
          const p = { id: 'p-mock-' + (state.teamPosts.length + 1), author: me.name, author_type: me.type, topic: 'general', board: 'general', title: String(data.title || '').slice(0, 100), body: bodyText.trim(), team_id: c.id, reply_to: null, likes: [], pinned: false, ts: new Date().toISOString() };
          state.teamPosts.push(p);
          res.writeHead(201); res.end(JSON.stringify({ ok: true, post: pubPost(p) })); return;
        }
      }
      // POST /api/communities/:id/announcements
      if (commAnnM && req.method === 'POST') {
        const c = findComm(commAnnM); if (!c) return commErr(404, 'team not found');
        const me = bearerMe(); if (!me) return comm401();
        if (!isCreator(c, me)) return commErr(403, 'only the team creator or the owner can post announcements');
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        const b = String(data.body || '');
        if (!b || b.length > 2000) return commErr(400, 'announcement body must be 1..2000 chars');
        const ann = { id: 'ann-mock-' + (c.announcements.length + 1), author: me.name, body: b, ts: new Date().toISOString() };
        c.announcements.push(ann); if (c.announcements.length > 5) c.announcements.shift();
        res.writeHead(201); res.end(JSON.stringify({ ok: true, announcement: ann, announcements: c.announcements })); return;
      }
      // DELETE /api/communities/:id/announcements/:annId
      if (commAnnOneM && req.method === 'DELETE') {
        const c = findComm(commAnnOneM); if (!c) return commErr(404, 'team not found');
        const me = bearerMe(); if (!me) return comm401();
        if (!isCreator(c, me)) return commErr(403, 'only the team creator or the owner can delete announcements');
        const idx = c.announcements.findIndex((a) => a.id === decodeURIComponent(commAnnOneM[2]));
        if (idx < 0) return commErr(404, 'announcement not found');
        c.announcements.splice(idx, 1);
        res.end(JSON.stringify({ ok: true, announcements: c.announcements })); return;
      }
      // POST /api/communities/:id/members（拉人）
      if (commMembersM && req.method === 'POST') {
        const c = findComm(commMembersM); if (!c) return commErr(404, 'team not found');
        const me = bearerMe(); if (!me) return comm401();
        if (!isAdmin(c, me)) return commErr(403, 'only the team creator, the owner, or a team admin can add members');
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        const name = String(data.name || '').trim(); const type = data.type;
        if (!name || (type !== 'human' && type !== 'ai')) return commErr(400, 'name and type (human|ai) required');
        if (!['testhuman', 'testai', 'otherhuman', 'stranger', 'newai'].includes(name)) return commErr(404, 'account not found');
        if (c.members.some((m) => m.name === name)) return commErr(409, 'already a member');
        c.members.push({ name, type, role: 'member', joinedTs: new Date().toISOString() });
        res.end(JSON.stringify({ ok: true, members: c.members })); return;
      }
      // DELETE /api/communities/:id/members/:name（移出）
      if (commMemberM && req.method === 'DELETE') {
        const c = findComm(commMemberM); if (!c) return commErr(404, 'team not found');
        const me = bearerMe(); if (!me) return comm401();
        if (!isAdmin(c, me)) return commErr(403, 'only the team creator, the owner, or a team admin can remove members');
        const name = decodeURIComponent(commMemberM[2]);
        if (name === c.creator) return commErr(400, 'the team creator cannot be removed');
        const idx = c.members.findIndex((m) => m.name === name);
        if (idx < 0) return commErr(404, 'not a member');
        c.members.splice(idx, 1);
        res.end(JSON.stringify({ ok: true, members: c.members })); return;
      }
      // POST /api/communities/:id/members/:name/role（改角色）
      if (commRoleM && req.method === 'POST') {
        const c = findComm(commRoleM); if (!c) return commErr(404, 'team not found');
        const me = bearerMe(); if (!me) return comm401();
        if (!isCreator(c, me)) return commErr(403, 'only the team creator can change member roles');
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        const name = decodeURIComponent(commRoleM[2]);
        if (name === c.creator) return commErr(400, 'the team creator role cannot be changed');
        if (data.role !== 'admin' && data.role !== 'member') return commErr(400, 'role must be admin or member');
        const m = c.members.find((x) => x.name === name);
        if (!m) return commErr(404, 'not a member');
        m.role = data.role;
        res.end(JSON.stringify({ ok: true, members: c.members })); return;
      }
      // PATCH /api/communities/:id/groups（关联群组）
      if (commGroupsM && req.method === 'PATCH') {
        const c = findComm(commGroupsM); if (!c) return commErr(404, 'team not found');
        const me = bearerMe(); if (!me) return comm401();
        if (!isCreator(c, me)) return commErr(403, 'only the team creator or the owner can change linked groups');
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        if (!Array.isArray(data.groups)) return commErr(400, 'groups must be an array of group ids');
        for (const gid of data.groups) if (!['grp-public', 'grp-private'].includes(gid)) return commErr(404, 'group not found: ' + String(gid));
        c.groups = data.groups;
        res.end(JSON.stringify({ ok: true, community: detailComm(c) })); return;
      }
      // POST /api/posts/:id/like（点赞/取消，toggle）
      const likeM = /^\/api\/posts\/([^/]+)\/like$/.exec(url.pathname);
      if (req.method === 'POST' && likeM) {
        const me = bearerMe(); if (!me) return comm401();
        const p = findAnyPost(decodeURIComponent(likeM[1]));
        if (!p) return commErr(404, 'post not found');
        const idx = p.likes.findIndex((l) => l.name === me.name);
        if (idx >= 0) p.likes.splice(idx, 1); else p.likes.push({ name: me.name, type: me.type });
        res.end(JSON.stringify({ ok: true, likesCount: p.likes.length, liked: idx < 0 })); return;
      }
      // POST /api/posts/:id/pin（置顶）
      const pinM = /^\/api\/posts\/([^/]+)\/pin$/.exec(url.pathname);
      if (req.method === 'POST' && pinM) {
        const me = bearerMe(); if (!me) return comm401();
        const p = findAnyPost(decodeURIComponent(pinM[1]));
        if (!p) return commErr(404, 'post not found');
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        if (data.pinned !== true && data.pinned !== false) return commErr(400, 'pinned must be true or false');
        if (p.reply_to) return commErr(400, 'only root posts can be pinned');
        const c = p.team_id ? state.communities.find((x) => x.id === p.team_id) : null;
        if (p.author !== me.name && !(c && isAdmin(c, me))) return commErr(403, 'only the author, the owner, or a group manager can pin this post');
        p.pinned = data.pinned;
        res.end(JSON.stringify({ ok: true, post: pubPost(p) })); return;
      }
      // DELETE /api/posts/:id（删帖，作者 only）
      const delPostM = /^\/api\/posts\/([^/]+)$/.exec(url.pathname);
      if (req.method === 'DELETE' && delPostM) {
        const me = bearerMe(); if (!me) return comm401();
        const p = findAnyPost(decodeURIComponent(delPostM[1]));
        if (!p) return commErr(404, 'post not found');
        if (p.author !== me.name) return commErr(403, 'only the author can delete this post');
        p.deleted = true;
        res.end(JSON.stringify({ ok: true })); return;
      }
      // POST /api/posts（v0.3.5：回帖继承 team_id/board；无 reply_to 则是主帖子页发帖，
      // board 校验、title 不存——与真实服务端同口径）。
      if (req.method === 'POST' && url.pathname === '/api/posts') {
        const me = bearerMe(); if (!me) return comm401();
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        const bodyText = String(data.body || '');
        if (!bodyText.trim() || bodyText.length > 10000) return commErr(400, 'body must be 1..10000 chars');
        if (data.reply_to) {
          const target = findAnyPost(String(data.reply_to));
          if (!target) return commErr(400, 'reply target not found');
          const teamId = target.team_id || null;
          if (teamId) {
            const c = state.communities.find((x) => x.id === teamId);
            if (!c || !isMember(c, me)) return commErr(403, 'only team members can reply to team posts');
          }
          const p = { id: 'p-mock-' + (state.teamPosts.length + state.feedPosts.length + 1), author: me.name, author_type: me.type, topic: target.board || 'general', board: target.board || 'general', title: null, body: bodyText.trim(), team_id: teamId, group_id: target.group_id || null, reply_to: String(data.reply_to), likes: [], pinned: false, ts: new Date().toISOString() };
          (teamId ? state.teamPosts : state.feedPosts).push(p);
          res.writeHead(201); res.end(JSON.stringify({ ok: true, post: pubPost(p) })); return;
        }
        const board = data.board ? String(data.board) : 'general';
        if (!['general', 'random', 'tech'].includes(board)) return commErr(400, 'unknown board');
        const p = { id: 'p-mock-' + (state.teamPosts.length + state.feedPosts.length + 1), author: me.name, author_type: me.type, topic: board, board, title: null, body: bodyText.trim(), team_id: null, group_id: null, reply_to: null, likes: [], pinned: false, ts: new Date().toISOString() };
        state.feedPosts.push(p);
        res.writeHead(201); res.end(JSON.stringify({ ok: true, post: pubPost(p) })); return;
      }
      // GET /api/posts（v0.3.5 主帖子页）：须登录；q 搜标题/正文/作者；board 过滤；
      // 置顶与流分离（pinned 在前，stream 按 limit 分页），与真实服务端同 shape。
      if (req.method === 'GET' && url.pathname === '/api/posts') {
        const me = bearerMe(); if (!me) return comm401();
        const q = (url.searchParams.get('q') || '').trim().toLowerCase();
        const board = url.searchParams.get('board') || '';
        const limit = Math.max(1, parseInt(url.searchParams.get('limit') || '30', 10) || 30);
        let posts = state.feedPosts.filter((p) => !p.deleted);
        if (q) posts = posts.filter((p) => ((p.title || '') + ' ' + (p.body || '') + ' ' + (p.author || '')).toLowerCase().includes(q));
        if (board) posts = posts.filter((p) => p.board === board);
        const pinned = posts.filter((p) => p.pinned);
        const stream = posts.filter((p) => !p.pinned);
        const page = stream.slice(0, limit);
        res.end(JSON.stringify({ ok: true, posts: page.map(pubPost), total: stream.length, has_more: stream.length > page.length, pinned_total: pinned.length, pinned: pinned.map(pubPost) })); return;
      }
      // GET /api/search（v0.3.5 全站搜索）：须登录；q 必填；帖子 + 群消息各按 limit 分页。
      if (req.method === 'GET' && url.pathname === '/api/search') {
        const me = bearerMe(); if (!me) return comm401();
        const q = (url.searchParams.get('q') || '').trim();
        if (!q) return commErr(400, 'q is required');
        const needle = q.toLowerCase();
        const limit = Math.max(1, parseInt(url.searchParams.get('limit') || '20', 10) || 20);
        const posts = state.feedPosts.filter((p) => !p.deleted && ((p.title || '') + ' ' + (p.body || '') + ' ' + (p.author || '')).toLowerCase().includes(needle));
        const messages = state.feedMessages.filter((m) => ((m.body || '') + ' ' + (m.author || '')).toLowerCase().includes(needle));
        res.end(JSON.stringify({ ok: true, q, posts: posts.slice(0, limit).map(pubPost), totalPosts: posts.length, hasMorePosts: posts.length > limit, messages: messages.slice(0, limit), totalMessages: messages.length, hasMoreMessages: messages.length > limit })); return;
      }
      // GET /api/accounts（v0.3.5 资料卡）：公开全量；融合 P0：每账号加
      // hasWorkspace（有活跃绑定）、workspaceOnline（活跃绑定且 lastSeen 15 分钟内），
      // 不加 keyId/deviceName/publicKey/指纹。
      if (req.method === 'GET' && url.pathname === '/api/accounts') {
        const now = Date.now();
        const accounts = state.accounts.map((a) => {
          const active = state.workspaceBindings.filter((b) => b.accountId === a.id && !b.unlinkedTs);
          return {
            ...a,
            hasWorkspace: active.length > 0,
            workspaceOnline: active.some((b) => b.lastSeenTs && now - Date.parse(b.lastSeenTs) <= 15 * 60 * 1000),
          };
        });
        res.end(JSON.stringify({ ok: true, accounts })); return;
      }
      // PATCH /api/accounts/me（v0.3.5 改自己资料）：须登录；只接受资料字段。
      if (req.method === 'PATCH' && url.pathname === '/api/accounts/me') {
        const me = bearerMe(); if (!me) return comm401();
        const data = commJson(); if (!data) return commErr(400, 'invalid JSON');
        const mine = state.accounts.find((a) => a.name === me.name);
        if (!mine) return commErr(404, 'account not found');
        if (data.owner !== undefined) return commErr(400, 'owner can only be set by the forum owner');
        for (const k of ['bio', 'signature', 'avatarEmoji', 'avatarColor']) if (data[k] !== undefined) mine[k] = String(data[k]);
        if (data.tags !== undefined) { if (!Array.isArray(data.tags)) return commErr(400, 'tags must be an array'); mine.tags = data.tags.map(String); }
        res.end(JSON.stringify({ ok: true, account: mine })); return;
      }
      // GET/POST /api/follow/:name（v0.3.5 关注）：GET 公开 + followedByMe（登录时）；
      // POST 切换，400 自己、404 不存在。
      const followM = /^\/api\/follow\/([^/]+)$/.exec(url.pathname);
      if (followM) {
        const name = decodeURIComponent(followM[1]);
        const target = state.accounts.find((a) => a.name === name);
        if (!target) return commErr(404, 'account not found');
        if (req.method === 'GET') {
          const me = bearerMe();
          const followers = state.follows.filter((f) => f.following === name).map((f) => ({ name: f.follower, type: f.follower_type }));
          const following = state.follows.filter((f) => f.follower === name).map((f) => ({ name: f.following, type: f.following_type }));
          const followedByMe = !!(me && state.follows.some((f) => f.follower === me.name && f.following === name));
          res.end(JSON.stringify({ ok: true, name, followers, following, followersCount: followers.length, followingCount: following.length, followedByMe })); return;
        }
        if (req.method === 'POST') {
          const me = bearerMe(); if (!me) return comm401();
          if (name === me.name) return commErr(400, 'cannot follow yourself');
          const idx = state.follows.findIndex((f) => f.follower === me.name && f.following === name);
          let following;
          if (idx >= 0) { state.follows.splice(idx, 1); following = false; }
          else { state.follows.push({ follower: me.name, follower_type: me.type, following: name, following_type: target.type, ts: new Date().toISOString() }); following = true; }
          const followersCount = state.follows.filter((f) => f.following === name).length;
          const followingCount = state.follows.filter((f) => f.follower === me.name).length;
          res.end(JSON.stringify({ ok: true, following, followersCount, followingCount })); return;
        }
      }
      // GET /api/feed/following（v0.3.5 关注动态）：我关注的人（含我自己）的帖子，时间倒序。
      if (req.method === 'GET' && url.pathname === '/api/feed/following') {
        const me = bearerMe(); if (!me) return comm401();
        const names = new Set(state.follows.filter((f) => f.follower === me.name).map((f) => f.following));
        names.add(me.name);
        const limit = Math.max(1, parseInt(url.searchParams.get('limit') || '30', 10) || 30);
        const posts = state.feedPosts.filter((p) => !p.deleted && names.has(p.author));
        const page = posts.slice(0, limit);
        res.end(JSON.stringify({ ok: true, posts: page.map(pubPost), total: posts.length, has_more: posts.length > page.length, pinned_total: 0, pinned: [] })); return;
      }
      // GET /api/notifications（v0.3.15 通知中心）：回全类型，unreadCount 按
      // notifReadTs 算（与真实服务端同一口径：全类型、ts 倒序、上限 20）。
      if (req.method === 'GET' && url.pathname === '/api/notifications') {
        const me = bearerMe(); if (!me) return comm401();
        const list = state.notifications.slice().sort((a, b) => Date.parse(b.ts) - Date.parse(a.ts));
        const water = state.notifReadTs ? Date.parse(state.notifReadTs) : 0;
        const unreadCount = list.filter((n) => Date.parse(n.ts) > water).length;
        res.end(JSON.stringify({ ok: true, notifications: list.slice(0, 20), unreadCount })); return;
      }
      // POST /api/notifications/read（v0.3.15）：水位写到当前时间并回 notifReadTs。
      if (req.method === 'POST' && url.pathname === '/api/notifications/read') {
        const me = bearerMe(); if (!me) return comm401();
        state.notifReadTs = new Date().toISOString();
        res.end(JSON.stringify({ ok: true, notifReadTs: state.notifReadTs })); return;
      }
      res.end(JSON.stringify({ ok: true, posts: 472, accounts: 30, groups: 10, messages: 1676, dms: 7 }));
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({
      server,
      state,
      url: `http://127.0.0.1:${server.address().port}`,
      setNodeState(s) { state.node = { ...state.node, ...s }; },
      close() { server.close(); },
    }));
  });
}

// 最小 WS 服务端（仅 §7 测试用）：握手后立刻发一条 hello，之后保持连接。
// 只实现服务端发帧（FIN + opcode 1，小 payload），不解析客户端帧。
function startMockWsServer() {
  return new Promise((resolve) => {
    const sockets = new Set();
    const server = http.createServer((_req, res) => { res.writeHead(404); res.end(); });
    server.on('upgrade', (req, socket) => {
      const key = req.headers['sec-websocket-key'] || '';
      const accept = crypto.createHash('sha1')
        .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', 'utf8').digest('base64');
      socket.write(
        'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
      );
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
      socket.on('error', () => sockets.delete(socket));
      const payload = Buffer.from(JSON.stringify({
        type: 'hello',
        data: { seq: 4200, time: new Date().toISOString() },
      }), 'utf8');
      socket.write(Buffer.concat([Buffer.from([0x81, payload.length]), payload]));
    });
    server.listen(0, '127.0.0.1', () => resolve({
      url: `ws://127.0.0.1:${server.address().port}/ws`,
      close() {
        for (const s of sockets) { try { s.destroy(); } catch {} }
        server.close();
      },
    }));
  });
}

async function main() {
  console.log('== 1. live API ==');
  const client = new ForumClient(API_BASE);
  const health = await withTimeout(client.getHealth(), 15000, 'GET /api/health');
  assert.strictEqual(health.ok, true);
  assert.ok(Number.isFinite(health.posts) && health.posts > 0, 'posts 计数应为正数');
  ok(`GET /api/health -> ok, posts=${health.posts}, groups=${health.groups}`);

  console.log('== 2. live SSE ==');
  const hello = await withTimeout(new Promise((resolve, reject) => {
    const sse = new SseClient(API_BASE, {});
    sse.on('event', ({ event, data }) => {
      if (event === 'hello') { sse.stop(); resolve(data); }
    });
    sse.on('disconnect', reject);
    sse.start();
  }), 20000, 'SSE hello');
  assert.ok(Number.isFinite(hello.seq), 'hello 事件应带 seq');
  ok(`SSE hello 收到，seq=${hello.seq}`);

  console.log('== 3. 轻节点（mock 种子） ==');
  // L2 探针在 §3/§3b/§3c/§5b 里必须保持打不通：v0.3.12 起默认探 het 副本，
  // 沙箱到 het 的 fetch 时快时慢，偶尔成功会多追加一条 l2-head、冲掉
  // 这些用例精确计数的 seq/行数；探针本身由 §17 用本地 stub 覆盖。
  // 127.0.0.1:1 恒拒连，失败是确定性的，不等 8 秒超时。
  process.env.CHIMERA_L2_RPC = 'http://127.0.0.1:1';
  const seed = await startMockSeed();
  const seedUrl = `http://127.0.0.1:${seed.address().port}`;
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-test-'));
  try {
    // 开关持久化：Store round-trip
    const storeFile = path.join(tmp, 'state.json');
    const store = new Store(storeFile);
    assert.strictEqual(store.state.node.enabled, false, '节点开关默认关');
    store.update((s) => { s.node.enabled = true; });
    const store2 = new Store(storeFile);
    assert.strictEqual(store2.state.node.enabled, true, '开关开应持久化');
    ok('节点开关默认关，置开后持久化');

    const logs = [];
    const nm = new NodeManager({
      nodeDir: path.join(tmp, 'node'),
      apiBase: seedUrl,
      config: { enabled: false, port: 0, seeds: [seedUrl] }, // port 0 = 随机空闲端口（仅测试）
      onLog: (l) => logs.push(l),
    });
    // port 0 需要解析实际端口：NodeManager 用 config.port 监听，0 会由系统分配；
    // 但 status() 汇报的是 config.port（0），测试里用 server.address() 核对。
    await nm.start();
    assert.strictEqual(nm.state, 'running');
    const actualPort = nm._server.address().port;
    assert.ok(actualPort > 0, '应监听到真实端口');
    const res = await fetch(`http://127.0.0.1:${actualPort}/health`);
    const body = await res.json();
    assert.strictEqual(body.ok, true);
    assert.strictEqual(body.tier, 'sync-upload');
    ok(`节点启动：/health ok，监听 127.0.0.1:${actualPort}`);

    // 同步一次应写哈希链
    await nm.syncOnce();
    assert.ok(nm.headersCount >= 1, '同步后应有副本头');
    const lines = fs.readFileSync(path.join(tmp, 'node', 'replica', 'headers.jsonl'), 'utf8')
      .trim().split('\n').map((l) => JSON.parse(l));
    assert.ok(lines.length >= 1);
    const last = lines[lines.length - 1];
    const recomputed = crypto.createHash('sha256')
      .update(JSON.stringify(last, Object.keys(last).sort()), 'utf8').digest('hex');
    assert.notStrictEqual(recomputed, last.hash, 'hash 字段本身不参与自哈希（防同义）');
    // 校验链：去掉 hash 后按排序键重算应等于记录的 hash
    const { hash, ...rest } = last;
    const expect = crypto.createHash('sha256')
      .update(JSON.stringify(rest, Object.keys(rest).sort()), 'utf8').digest('hex');
    assert.strictEqual(expect, hash, '副本头哈希链应自洽');
    ok(`哈希链自洽，副本头 ${lines.length} 条`);

    // 干净停止：端口即释放
    await nm.stop();
    assert.strictEqual(nm.state, 'off');
    await assert.rejects(fetch(`http://127.0.0.1:${actualPort}/health`), '停后端口应拒绝连接');
    ok('节点停止：状态 off，端口已释放');

    // setEnabled 开关接口
    await nm.setEnabled(true);
    assert.strictEqual(nm.state, 'running');
    await nm.setEnabled(false);
    assert.strictEqual(nm.state, 'off');
    ok('setEnabled(true/false) 开关接口正常');
  } finally {
    seed.close();
    fs.rmSync(tmp, { recursive: true, force: true });
  }

  console.log('== 3b. 节点上传与自检（mock 论坛） ==');
  const forum = await startMockForum();
  const forumUrl = forum.url;
  const tmp3b = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-upload-'));
  try {
    // token 文件（0600）：节点身份。v0.3.10 (d)：token 必须放在 nodeDir 内
    //（NodeManager 构造即校验，放在外面直接抛——测试 profile 绝不能读
    // live token 的同一道防线）。
    const nodeDir3b = path.join(tmp3b, 'node');
    fs.mkdirSync(nodeDir3b, { recursive: true });
    const tokenFile = path.join(nodeDir3b, 'forum-token');
    fs.writeFileSync(tokenFile, 'test-node-token\n');
    fs.chmodSync(tokenFile, 0o600);

    // (a) 上传流程：启动 1 次同步 + 3 次同步 → 全部上传成功，游标持久化
    const nm = new NodeManager({
      nodeDir: path.join(tmp3b, 'node'),
      apiBase: forumUrl,
      config: { enabled: false, port: 0, seeds: [forumUrl], forumTokenFile: tokenFile },
      onLog: () => {},
    });
    await nm.start();
    await nm.syncOnce();
    await nm.syncOnce();
    await nm.syncOnce();
    assert.ok(nm.lastUploadedSeq >= 3, `同步后应上传 >=3 条，实际 ${nm.lastUploadedSeq}`);
    assert.strictEqual(forum.state.node.lastSeq, nm.lastUploadedSeq, 'mock 服务端 lastSeq 应与客户端游标一致');
    assert.ok(forum.state.heartbeats >= 3, '同步应顺带发心跳');
    const upState = JSON.parse(fs.readFileSync(path.join(tmp3b, 'node', 'upload-state.json'), 'utf8'));
    assert.strictEqual(upState.lastUploadedSeq, nm.lastUploadedSeq, '游标应持久化到 upload-state.json');
    assert.strictEqual(nm.status().upload.lastUploadedSeq, nm.lastUploadedSeq, 'status().upload 应带游标');
    assert.strictEqual(nm.status().tier, 'sync-upload', '档位应为 sync-upload');
    ok(`上传流程：同步全部上传，lastUploadedSeq=${nm.lastUploadedSeq}，心跳 ${forum.state.heartbeats} 次`);
    await nm.stop();

    // (b) 重试/退避：下一次上传先 500，再同步应成功
    const nm2 = new NodeManager({
      nodeDir: path.join(tmp3b, 'node'),
      apiBase: forumUrl,
      config: { enabled: false, port: 0, seeds: [forumUrl], forumTokenFile: tokenFile },
      onLog: () => {},
    });
    await nm2.start();
    forum.state.failNextUpload = 1;
    await nm2.syncOnce();
    assert.strictEqual(nm2.uploadFailures, 1, '500 后失败数应为 1');
    assert.ok(nm2._nextUploadAt > Date.now(), '失败后应设退避门');
    nm2._nextUploadAt = 0; // 测试里直接放行退避（白盒）
    await nm2.syncOnce();
    assert.strictEqual(nm2.uploadFailures, 0, '重试成功后失败数应清零');
    assert.ok(nm2.lastUploadedSeq > 3, '重试后游标应继续前进');
    ok(`重试退避：500 后失败数 1、退避门已设，放行后重试成功，lastUploadedSeq=${nm2.lastUploadedSeq}`);
    await nm2.stop();

    // (c) 链自检：改坏一行 → 启动隔离 + 重起新链
    const headersFile = path.join(tmp3b, 'node', 'replica', 'headers.jsonl');
    const rawLines = fs.readFileSync(headersFile, 'utf8').split('\n').filter(Boolean);
    assert.ok(rawLines.length >= 2, '自检前应至少有 2 条头');
    const bad = JSON.parse(rawLines[0]);
    bad.hash = '0'.repeat(64); // 篡改第一行 hash
    rawLines[0] = JSON.stringify(bad);
    fs.writeFileSync(headersFile, rawLines.join('\n') + '\n');
    let brokenEv = null;
    const nm3 = new NodeManager({
      nodeDir: path.join(tmp3b, 'node'),
      apiBase: forumUrl,
      config: { enabled: false, port: 0, seeds: [forumUrl], forumTokenFile: tokenFile },
      onLog: () => {},
    });
    nm3.on('chain-broken', (e) => { brokenEv = e; });
    assert.strictEqual(nm3._selfCheckChain().ok, false, '改坏的链自检应失败');
    await nm3.start();
    assert.ok(brokenEv && /自哈希不符/.test(brokenEv.error), '启动时应发 chain-broken 事件并报自哈希不符');
    const corrupt = fs.readdirSync(path.join(tmp3b, 'node', 'replica')).filter((f) => f.startsWith('headers.jsonl.corrupt-'));
    assert.strictEqual(corrupt.length, 1, '坏文件应移旁一份');
    assert.strictEqual(nm3.headersCount, 1, '隔离后首次同步应只有新链 1 条头');
    assert.strictEqual(nm3._nextSeq, 2, '新链 seq 应从 1 起');
    assert.strictEqual(nm3.status().upload.chainOk, true, '新链同步成功后 chainOk 应恢复');
    ok('链自检：改坏一行被检出，坏文件移旁，新链从 seq 1 重起');
    await nm3.stop();

    // (d) 409 → 重基：本地链换成与服务端不衔接的 seq 4 头
    const nm4dir = path.join(tmp3b, 'node4');
    fs.mkdirSync(nm4dir, { recursive: true });
    const tokenFile4 = path.join(nm4dir, 'forum-token');
    fs.writeFileSync(tokenFile4, 'test-node-token\n');
    fs.chmodSync(tokenFile4, 0o600);
    const nm4 = new NodeManager({
      nodeDir: nm4dir,
      apiBase: forumUrl,
      config: { enabled: false, port: 0, seeds: [forumUrl], forumTokenFile: tokenFile4 },
      onLog: () => {},
    });
    await nm4.start();
    await nm4.syncOnce(); // seq 1 创世上传（mock 收到重基，lastSeq=1）
    await nm4.stop();
    // 伪造一条与服务端不衔接的 seq 4 头（自哈希合法，但 prevHash 不是服务端 lastHash）
    const forgedBody = { seq: 4, kind: 'community-head', ts: new Date().toISOString(), posts: 999, accounts: 30, groups: 10, messages: 9999, dms: 7, prevHash: 'b'.repeat(64) };
    const forgedHash = crypto.createHash('sha256').update(JSON.stringify(forgedBody, Object.keys(forgedBody).sort()), 'utf8').digest('hex');
    fs.writeFileSync(path.join(nm4dir, 'replica', 'headers.jsonl'), JSON.stringify({ ...forgedBody, hash: forgedHash }) + '\n');
    fs.writeFileSync(path.join(nm4dir, 'upload-state.json'), JSON.stringify({ lastUploadedSeq: 3, lastUploadAt: null, failures: 0, lastRebaseAt: 0 }, null, 2));
    const nm5 = new NodeManager({
      nodeDir: nm4dir,
      apiBase: forumUrl,
      config: { enabled: false, port: 0, seeds: [forumUrl], forumTokenFile: tokenFile4 },
      onLog: () => {},
    });
    await nm5.start(); // 自检：单行链自洽，通过；首次同步追加 seq 5，上传 seq 4-5 → 409 → 重基
    assert.strictEqual(nm5.chainOk, true, '伪造单行链自洽，自检应通过');
    assert.ok(nm5._lastRebaseAt > 0, '409 后应触发本地重基');
    assert.strictEqual(nm5.lastUploadedSeq, 0, '重基后游标应归零');
    await nm5.syncOnce(); // 新链 seq 1 上传（服务端重基接受）
    assert.strictEqual(nm5.lastUploadedSeq, 1, '重基后新链 seq 1 应上传成功');
    assert.strictEqual(forum.state.node.lastSeq, 1, '服务端重基后 lastSeq 应为 1');
    ok('409 重基：不衔接的链触发 409，本地重基后新链上传成功，服务端 lastSeq=1');
    await nm5.stop();

    // (e) SSE 缺口：WS 挂掉回落 SSE，流里 seq 101 → 104 应发 gap 事件
    const rt = new RealtimeClient(forumUrl, { wsUrl: 'wss://127.0.0.1:9/ws' });
    const gapP = new Promise((resolve) => rt.on('gap', resolve));
    rt.start();
    const gap = await withTimeout(gapP, 20000, 'SSE gap 事件');
    assert.strictEqual(gap.expected, 102, '缺口起点应为 102');
    assert.strictEqual(gap.got, 104, '缺口终点应为 104');
    assert.strictEqual(gap.transport, 'sse', '缺口应在 SSE 路上');
    rt.stop();
    ok(`SSE 缺口：seq 101 → 104 触发 gap 事件（expected=${gap.expected}, got=${gap.got}）`);

    // (f) 自加入群（v0.3.2）：公开群 /join 应 200；私密群应 403 且原话透传；无 token 应 401。
    const mockClient = new ForumClient(forumUrl);
    const joined = await withTimeout(mockClient.joinGroup('test-token', 'grp-public'), 15000, 'POST /api/groups/:id/join');
    assert.strictEqual(joined.ok, true, '公开群自加应回 ok');
    assert.strictEqual(joined.group.id, 'grp-public', '回包 group id 应与请求一致');
    ok('自加入群：公开群 /join 回 200');
    await assert.rejects(
      withTimeout(mockClient.joinGroup('test-token', 'grp-private'), 15000, 'POST /api/groups/:id/join 私密群'),
      (e) => e.status === 403 && /private group/.test(e.message),
    );
    ok('自加入群：私密群 /join 回 403，原话透传');
    await assert.rejects(
      withTimeout(mockClient.joinGroup(null, 'grp-public'), 15000, 'POST /api/groups/:id/join 无 token'),
      (e) => e.status === 401,
    );
    ok('自加入群：无 token 回 401');

    // (g) 节点激活即分配（v0.3.3）：人类 Bearer 回 201（节点账号 + token）；无 token 401。
    const activated = await withTimeout(mockClient.activateNode('test-token'), 15000, 'POST /api/node/activate');
    assert.strictEqual(activated.account.name, 'testhuman-node', '派生名应为 testhuman-node');
    assert.strictEqual(activated.account.type, 'node', '应为节点账号');
    assert.strictEqual(activated.account.owner, 'testhuman', 'owner 应为本人');
    assert.strictEqual(activated.account.role, 'member', '节点永为 member');
    assert.ok(typeof activated.apiToken === 'string' && activated.apiToken.length > 0, '应回 apiToken');
    ok('节点激活：人类会话回 201，分配节点账号 testhuman-node');
    await assert.rejects(
      withTimeout(mockClient.activateNode(null), 15000, 'POST /api/node/activate 无 token'),
      (e) => e.status === 401,
    );
    ok('节点激活：无 token 回 401');

    // (h) 社区/团队可交互（v0.3.4）：建队、讨论、公告、成员、关联群组、帖子互动，全走 mock 论坛。
    const TEAM = 'com-mock';
    // 建队：重名 409；新队 201；无 token 401。
    await assert.rejects(
      withTimeout(mockClient.createCommunity('test-token', { name: 'mockteam' }), 15000, 'POST /api/communities 重名'),
      (e) => e.status === 409,
    );
    ok('建队：重名回 409');
    const created = await withTimeout(mockClient.createCommunity('test-token', { name: 'newteam', title: '新队', description: 'mock 新队' }), 15000, 'POST /api/communities');
    assert.strictEqual(created.community.name, 'newteam', '建队应回新队名');
    ok('建队：新队回 201');
    await assert.rejects(
      withTimeout(mockClient.createCommunity(null, { name: 'x' }), 15000, 'POST /api/communities 无 token'),
      (e) => e.status === 401,
    );
    ok('建队：无 token 回 401');
    // 凭邀请码加入：错码 404；正确码 200（otherhuman 加入 mockteam）。
    await assert.rejects(
      withTimeout(mockClient.joinCommunityByCode('test-other-token', 'WRONG'), 15000, 'POST /api/communities/join 错码'),
      (e) => e.status === 404,
    );
    ok('邀请码加入：错码回 404');
    const joinedTeam = await withTimeout(mockClient.joinCommunityByCode('test-other-token', 'MOCKCODE'), 15000, 'POST /api/communities/join');
    assert.strictEqual(joinedTeam.community.id, TEAM, '加入应回 mockteam');
    ok('邀请码加入：正确码回 200');
    // 讨论列表：非成员 403；成员 200 且初始为空。
    await assert.rejects(
      withTimeout(mockClient.getCommunityPosts('test-stranger-token', TEAM), 15000, 'GET 讨论 非成员'),
      (e) => e.status === 403,
    );
    ok('讨论列表：非成员回 403');
    let teamPosts = await withTimeout(mockClient.getCommunityPosts('test-token', TEAM), 15000, 'GET /api/communities/:id/posts');
    assert.strictEqual(teamPosts.total, 0, '初始讨论应为空');
    ok('讨论列表：成员回 200，初始为空');
    // 发帖：空正文 400；非成员 403；成员 201。
    await assert.rejects(
      withTimeout(mockClient.createCommunityPost('test-token', TEAM, { title: '', body: '   ' }), 15000, 'POST 讨论 空正文'),
      (e) => e.status === 400,
    );
    ok('讨论发帖：空正文回 400');
    await assert.rejects(
      withTimeout(mockClient.createCommunityPost('test-stranger-token', TEAM, { title: '', body: 'hi' }), 15000, 'POST 讨论 非成员'),
      (e) => e.status === 403,
    );
    ok('讨论发帖：非成员回 403');
    const firstPost = await withTimeout(mockClient.createCommunityPost('test-token', TEAM, { title: '首帖', body: '大家好' }), 15000, 'POST /api/communities/:id/posts');
    assert.strictEqual(firstPost.post.title, '首帖', '发帖应回标题');
    assert.strictEqual(firstPost.post.team_id, TEAM, '团队帖应标 team_id');
    const postId = firstPost.post.id;
    ok('讨论发帖：成员回 201');
    teamPosts = await withTimeout(mockClient.getCommunityPosts('test-token', TEAM), 15000, 'GET 讨论 发帖后');
    assert.strictEqual(teamPosts.total, 1, '发帖后讨论应有 1 条');
    assert.strictEqual(teamPosts.posts[0].replyCount, 0, '新帖回帖数应为 0');
    ok('讨论列表：发帖后回 1 条');
    // 搜索：按标题搜到，按作者搜不到不存在的。
    teamPosts = await withTimeout(mockClient.getCommunityPosts('test-token', TEAM, { q: '首帖' }), 15000, 'GET 讨论 搜索');
    assert.strictEqual(teamPosts.total, 1, '按标题搜应命中 1 条');
    teamPosts = await withTimeout(mockClient.getCommunityPosts('test-token', TEAM, { q: '不存在' }), 15000, 'GET 讨论 搜索无命中');
    assert.strictEqual(teamPosts.total, 0, '搜不到应回 0 条');
    ok('讨论搜索：标题命中 1 条、无命中 0 条');
    // 回帖：reply_to 继承 team_id；回帖数 +1；置顶回帖 400。
    const reply = await withTimeout(mockClient.createPost('test-token', { board: null, title: '', body: '回一下', replyTo: postId }), 15000, 'POST /api/posts 回帖');
    assert.strictEqual(reply.post.team_id, TEAM, '回帖应继承 team_id');
    const replyId = reply.post.id;
    ok('回帖：reply_to 继承 team_id');
    teamPosts = await withTimeout(mockClient.getCommunityPosts('test-token', TEAM), 15000, 'GET 讨论 回帖后');
    assert.strictEqual(teamPosts.posts.find((p) => p.id === postId).replyCount, 1, '回帖后回帖数应为 1');
    ok('回帖：回帖数 +1');
    await assert.rejects(
      withTimeout(mockClient.pinPost('test-token', replyId, true), 15000, 'POST /api/posts/:id/pin 回帖'),
      (e) => e.status === 400,
    );
    ok('置顶：回帖置顶回 400');
    // 点赞：toggle（点一次 1、再点一次 0）；置顶：作者可置顶、非成员 403。
    let liked = await withTimeout(mockClient.likePost('test-token', postId), 15000, 'POST /api/posts/:id/like');
    assert.strictEqual(liked.likesCount, 1, '点赞后应为 1');
    liked = await withTimeout(mockClient.likePost('test-token', postId), 15000, 'POST /api/posts/:id/like 取消');
    assert.strictEqual(liked.likesCount, 0, '再点一次应取消为 0');
    ok('点赞：toggle 1 → 0');
    const pinned = await withTimeout(mockClient.pinPost('test-token', postId, true), 15000, 'POST /api/posts/:id/pin');
    assert.strictEqual(pinned.post.pinned, true, '置顶后 pinned 应为 true');
    ok('置顶：作者置顶回 200');
    await assert.rejects(
      withTimeout(mockClient.pinPost('test-stranger-token', postId, true), 15000, 'POST /api/posts/:id/pin 非成员'),
      (e) => e.status === 403,
    );
    ok('置顶：非成员回 403');
    // 删帖：非作者 403；作者 200，列表里消失。
    await assert.rejects(
      withTimeout(mockClient.deletePost('test-other-token', postId), 15000, 'DELETE /api/posts/:id 非作者'),
      (e) => e.status === 403,
    );
    ok('删帖：非作者回 403');
    await withTimeout(mockClient.deletePost('test-token', postId), 15000, 'DELETE /api/posts/:id');
    teamPosts = await withTimeout(mockClient.getCommunityPosts('test-token', TEAM), 15000, 'GET 讨论 删帖后');
    assert.strictEqual(teamPosts.posts.filter((p) => p.id === postId).length, 0, '删帖后主帖应不在列表');
    assert.strictEqual(teamPosts.posts.filter((p) => p.id === replyId).length, 1, '删主帖不应连带删回帖');
    ok('删帖：作者删帖回 200，主帖消失、回帖保留');
    // 公告：非创建人 403；创建人 201；删公告：非创建人 403；创建人 200。
    await assert.rejects(
      withTimeout(mockClient.createCommunityAnnouncement('test-other-token', TEAM, 'hi'), 15000, 'POST 公告 非创建人'),
      (e) => e.status === 403,
    );
    ok('公告发布：非创建人回 403');
    const ann = await withTimeout(mockClient.createCommunityAnnouncement('test-token', TEAM, '公告一'), 15000, 'POST /api/communities/:id/announcements');
    assert.strictEqual(ann.announcement.body, '公告一', '公告应回正文');
    const annId = ann.announcement.id;
    ok('公告发布：创建人回 201');
    await assert.rejects(
      withTimeout(mockClient.deleteCommunityAnnouncement('test-other-token', TEAM, annId), 15000, 'DELETE 公告 非创建人'),
      (e) => e.status === 403,
    );
    ok('公告删除：非创建人回 403');
    await withTimeout(mockClient.deleteCommunityAnnouncement('test-token', TEAM, annId), 15000, 'DELETE /api/communities/:id/announcements/:annId');
    ok('公告删除：创建人回 200');
    // 成员：拉人（创建人 200、重名 409、不存在 404、非 admin 403）；改角色（非创建人 403、创建人 200、改创建人 400）；
    // 移出（删创建人 400、移出 200、再移 404）。
    const added = await withTimeout(mockClient.addCommunityMember('test-token', TEAM, { name: 'newai', type: 'ai' }), 15000, 'POST /api/communities/:id/members');
    assert.ok(added.members.some((m) => m.name === 'newai'), '拉人后成员里应有 newai');
    ok('拉人：创建人回 200');
    await assert.rejects(
      withTimeout(mockClient.addCommunityMember('test-token', TEAM, { name: 'newai', type: 'ai' }), 15000, 'POST 拉人 重名'),
      (e) => e.status === 409,
    );
    ok('拉人：重名回 409');
    await assert.rejects(
      withTimeout(mockClient.addCommunityMember('test-token', TEAM, { name: 'ghost', type: 'ai' }), 15000, 'POST 拉人 不存在'),
      (e) => e.status === 404,
    );
    ok('拉人：不存在的账号回 404');
    await assert.rejects(
      withTimeout(mockClient.addCommunityMember('test-other-token', TEAM, { name: 'stranger', type: 'human' }), 15000, 'POST 拉人 非 admin'),
      (e) => e.status === 403,
    );
    ok('拉人：非 admin 回 403');
    await assert.rejects(
      withTimeout(mockClient.setCommunityMemberRole('test-other-token', TEAM, 'testai', 'admin'), 15000, 'POST 改角色 非创建人'),
      (e) => e.status === 403,
    );
    ok('改角色：非创建人回 403');
    const roled = await withTimeout(mockClient.setCommunityMemberRole('test-token', TEAM, 'testai', 'admin'), 15000, 'POST /api/communities/:id/members/:name/role');
    assert.strictEqual(roled.members.find((m) => m.name === 'testai').role, 'admin', '改角色后 testai 应为 admin');
    ok('改角色：创建人回 200，testai 变管理员');
    await assert.rejects(
      withTimeout(mockClient.setCommunityMemberRole('test-token', TEAM, 'testhuman', 'admin'), 15000, 'POST 改角色 创建人'),
      (e) => e.status === 400,
    );
    ok('改角色：改创建人回 400');
    await assert.rejects(
      withTimeout(mockClient.removeCommunityMember('test-token', TEAM, 'testhuman'), 15000, 'DELETE 移出 创建人'),
      (e) => e.status === 400,
    );
    ok('移出：删创建人回 400');
    const removed = await withTimeout(mockClient.removeCommunityMember('test-token', TEAM, 'testai'), 15000, 'DELETE /api/communities/:id/members/:name');
    assert.ok(!removed.members.some((m) => m.name === 'testai'), '移出后成员里不应有 testai');
    ok('移出：创建人回 200');
    await assert.rejects(
      withTimeout(mockClient.removeCommunityMember('test-token', TEAM, 'testai'), 15000, 'DELETE 移出 再移'),
      (e) => e.status === 404,
    );
    ok('移出：再移不在的成员回 404');
    // 关联群组：非创建人 403；不存在的群 404；正常 200。
    await assert.rejects(
      withTimeout(mockClient.setCommunityGroups('test-other-token', TEAM, ['grp-public']), 15000, 'PATCH 关联群组 非创建人'),
      (e) => e.status === 403,
    );
    ok('关联群组：非创建人回 403');
    await assert.rejects(
      withTimeout(mockClient.setCommunityGroups('test-token', TEAM, ['grp-nope']), 15000, 'PATCH 关联群组 不存在的群'),
      (e) => e.status === 404,
    );
    ok('关联群组：不存在的群回 404');
    const linked = await withTimeout(mockClient.setCommunityGroups('test-token', TEAM, ['grp-public']), 15000, 'PATCH /api/communities/:id/groups');
    assert.strictEqual(linked.community.groups.length, 1, '关联后应有 1 个群');
    assert.strictEqual(linked.community.groups[0].id, 'grp-public', '关联的群 id 应为 grp-public');
    ok('关联群组：创建人回 200，挂上 grp-public');

    // (i) 主题（v0.3.4）：深浅变量表与 styles.css 一致；浅色底是亮的、深色底是暗的。
    const { themeVars, applyTheme } = require('../renderer/theme');
    const darkVars = themeVars('dark');
    const lightVars = themeVars('light');
    assert.notStrictEqual(darkVars['--bg0'], lightVars['--bg0'], '深浅底色应不同');
    assert.strictEqual(lightVars['--bg0'], '#f3efe7', '浅色底应为暖纸色');
    assert.strictEqual(darkVars['--text'], '#eef1fa', '深色文字应为亮色');
    assert.strictEqual(lightVars['--text'], '#1d2333', '浅色文字应为深色');
    // 与 styles.css 的变量表逐项对齐（防改一处漏一处）：只比 theme.js 里的键。
    const css = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'styles.css'), 'utf8');
    const cssBlock = (startMarker) => {
      const i = css.indexOf(startMarker);
      assert.ok(i >= 0, `styles.css 应含 ${startMarker}`);
      const j = css.indexOf('}', i);
      const block = css.slice(i, j);
      const out = {};
      for (const m of block.matchAll(/(--[a-z0-9-]+)\s*:\s*([^;]+);/g)) out[m[1]] = m[2].trim();
      return out;
    };
    const cssDark = cssBlock(':root {');
    const cssLight = cssBlock('[data-theme="light"] {');
    for (const [k, v] of Object.entries(darkVars)) assert.strictEqual(cssDark[k], v, `styles.css :root 的 ${k} 应与 theme.js 一致`);
    for (const [k, v] of Object.entries(lightVars)) assert.strictEqual(cssLight[k], v, `styles.css [data-theme="light"] 的 ${k} 应与 theme.js 一致`);
    ok('主题：theme.js 与 styles.css 变量表逐项一致');
    // applyTheme：写 data-theme + 变量（用最小假 document，不依赖 DOM）。
    const styleStore = {};
    const fakeEl = {
      style: { setProperty(k, v) { styleStore[k] = v; } },
      setAttribute(k, v) { this[k] = v; },
    };
    assert.strictEqual(applyTheme({ documentElement: fakeEl }, 'light'), 'light', 'applyTheme 应回 light');
    assert.strictEqual(fakeEl['data-theme'], 'light', '应写 data-theme=light');
    assert.strictEqual(styleStore['--bg0'], '#f3efe7', '应写浅色底变量');
    assert.strictEqual(applyTheme({ documentElement: fakeEl }, 'bogus'), 'dark', '非法主题应回落深色');
    ok('主题：applyTheme 写 data-theme 与变量，非法值回落深色');

    // (j) 帖子页互动 + 全站搜索 + 资料/关注（v0.3.5），全走 mock 论坛。
    // 主帖子页发帖：201、board 落盘、title 不存（服务端只读 body/board/reply_to）。
    const rootPost = await withTimeout(mockClient.createPost('test-token', { board: 'general', title: '应被丢掉的标题', body: '主帖子页第一帖' }), 15000, 'POST /api/posts 发主帖');
    assert.strictEqual(rootPost.post.board, 'general', '发帖应落到 general 板');
    assert.strictEqual(rootPost.post.title, null, '主帖子接口不存 title，应为 null');
    assert.strictEqual(rootPost.post.team_id, null, '主帖不应带 team_id');
    const rootId = rootPost.post.id;
    ok('主帖子页发帖：回 201，board 落盘，title 不存');
    await assert.rejects(
      withTimeout(mockClient.createPost('test-token', { board: 'nope', title: '', body: 'x' }), 15000, 'POST /api/posts 未知板块'),
      (e) => e.status === 400 && /unknown board/.test(e.message),
    );
    ok('主帖子页发帖：未知板块回 400');
    // 列表：q 与 board 过滤；置顶与流分离。
    let feed = await withTimeout(mockClient.getPosts('test-token', { limit: 30 }), 15000, 'GET /api/posts');
    assert.ok(feed.posts.some((p) => p.id === rootId), '列表里应有刚发的主帖');
    assert.ok(feed.posts.some((p) => p.id === 'p-feed-1'), '列表里应有种子帖');
    ok('主帖子页列表：GET /api/posts 回种子帖 + 新帖');
    feed = await withTimeout(mockClient.getPosts('test-token', { q: '第一帖' }), 15000, 'GET /api/posts?q=');
    assert.strictEqual(feed.posts.length, 1, 'q 应只命中一帖');
    assert.strictEqual(feed.posts[0].id, rootId, 'q 应命中刚发的主帖');
    ok('主帖子页搜索：q 只命中匹配帖');
    feed = await withTimeout(mockClient.getPosts('test-token', { q: '第一帖', limit: 30 }), 15000, 'GET /api/posts?q= 带 limit');
    assert.strictEqual(feed.posts.length, 1, '带 limit 的 q 搜索应同样命中');
    ok('主帖子页搜索：q + limit 组合正常');
    // 回帖继承 board；点赞 toggle；置顶作者 200 / 非作者 403；删帖非作者 403 / 作者 200。
    const feedReply = await withTimeout(mockClient.createPost('test-other-token', { board: null, title: '', body: '回主帖', replyTo: rootId }), 15000, 'POST /api/posts 回主帖');
    assert.strictEqual(feedReply.post.board, 'general', '回帖应继承主帖 board');
    assert.strictEqual(feedReply.post.reply_to, rootId, '回帖应带 reply_to');
    ok('主帖子页回帖：board 继承，reply_to 落盘');
    let feedLiked = await withTimeout(mockClient.likePost('test-other-token', rootId), 15000, 'POST /api/posts/:id/like 主帖');
    assert.strictEqual(feedLiked.likesCount, 1, '点赞后应为 1');
    feedLiked = await withTimeout(mockClient.likePost('test-other-token', rootId), 15000, 'POST /api/posts/:id/like 主帖取消');
    assert.strictEqual(feedLiked.likesCount, 0, '取消后应为 0');
    ok('主帖子页点赞：toggle 正常');
    const feedPinned = await withTimeout(mockClient.pinPost('test-token', rootId, true), 15000, 'POST /api/posts/:id/pin 主帖');
    assert.strictEqual(feedPinned.post.pinned, true, '置顶后 pinned 应为真');
    feed = await withTimeout(mockClient.getPosts('test-token', { limit: 30 }), 15000, 'GET /api/posts 置顶后');
    assert.ok((feed.pinned || []).some((p) => p.id === rootId), '置顶帖应在 pinned 区');
    assert.ok(!feed.posts.some((p) => p.id === rootId), '置顶帖不应同时在流里');
    ok('主帖子页置顶：作者回 200，pinned 与流分离');
    await assert.rejects(
      withTimeout(mockClient.pinPost('test-other-token', rootId, true), 15000, 'POST /api/posts/:id/pin 主帖非作者'),
      (e) => e.status === 403,
    );
    ok('主帖子页置顶：非作者回 403');
    await assert.rejects(
      withTimeout(mockClient.deletePost('test-other-token', rootId), 15000, 'DELETE /api/posts/:id 主帖非作者'),
      (e) => e.status === 403,
    );
    ok('主帖子页删帖：非作者回 403');
    await withTimeout(mockClient.deletePost('test-token', rootId), 15000, 'DELETE /api/posts/:id 主帖作者');
    feed = await withTimeout(mockClient.getPosts('test-token', { limit: 30 }), 15000, 'GET /api/posts 删帖后');
    assert.strictEqual(feed.posts.filter((p) => p.id === rootId).length, 0, '删帖后主帖应不在列表');
    ok('主帖子页删帖：作者回 200，主帖消失');
    // 全站搜索：无 q 400；无 token 401；帖子命中与消息命中分开。
    await assert.rejects(
      withTimeout(mockClient.search('test-token', ''), 15000, 'GET /api/search 无 q'),
      (e) => e.status === 400 && /q is required/.test(e.message),
    );
    ok('全站搜索：无 q 回 400');
    await assert.rejects(
      withTimeout(mockClient.search(null, '欢迎'), 15000, 'GET /api/search 无 token'),
      (e) => e.status === 401,
    );
    ok('全站搜索：无 token 回 401');
    const searchPosts = await withTimeout(mockClient.search('test-token', '欢迎来到'), 15000, 'GET /api/search 帖子命中');
    assert.ok(searchPosts.posts.some((p) => p.id === 'p-feed-1'), '应命中种子帖');
    assert.strictEqual(searchPosts.messages.length, 0, '帖子关键词不应命中消息');
    assert.strictEqual(searchPosts.totalPosts, 1, 'totalPosts 应为 1');
    ok('全站搜索：帖子命中，消息为空');
    const searchMsgs = await withTimeout(mockClient.search('test-token', '群里说一句'), 15000, 'GET /api/search 消息命中');
    assert.strictEqual(searchMsgs.posts.length, 0, '消息关键词不应命中帖子');
    assert.ok(searchMsgs.messages.some((m) => m.id === 'm-feed-1'), '应命中群消息');
    assert.strictEqual(searchMsgs.messages[0].group_id, 'grp-public', '消息应带 group_id 供跳转');
    ok('全站搜索：消息命中，带 group_id');
    // 关注：不存在 404；自己 400；切换 + 计数；关注动态只含关注者与自己。
    await assert.rejects(
      withTimeout(mockClient.getFollow('test-token', 'ghost'), 15000, 'GET /api/follow 不存在'),
      (e) => e.status === 404,
    );
    ok('关注查询：不存在的账号回 404');
    await assert.rejects(
      withTimeout(mockClient.toggleFollow('test-token', 'testhuman'), 15000, 'POST /api/follow 自己'),
      (e) => e.status === 400 && /cannot follow yourself/.test(e.message),
    );
    ok('关注切换：关注自己回 400');
    let fol = await withTimeout(mockClient.toggleFollow('test-token', 'otherhuman'), 15000, 'POST /api/follow 关注');
    assert.strictEqual(fol.following, true, '关注后 following 应为真');
    assert.strictEqual(fol.followersCount, 1, '被关注者粉丝应为 1');
    fol = await withTimeout(mockClient.getFollow('test-token', 'otherhuman'), 15000, 'GET /api/follow 关注后');
    assert.strictEqual(fol.followedByMe, true, 'followedByMe 应为真');
    assert.strictEqual(fol.followersCount, 1, '粉丝数应为 1');
    ok('关注：切换 + 计数 + followedByMe 正常');
    let followingFeed = await withTimeout(mockClient.getFollowingFeed('test-token', { limit: 30 }), 15000, 'GET /api/feed/following');
    assert.ok(followingFeed.posts.some((p) => p.author === 'otherhuman'), '关注动态应含被关注者的帖');
    assert.ok(followingFeed.posts.some((p) => p.author === 'testhuman'), '关注动态应含自己的帖');
    assert.ok(!followingFeed.posts.some((p) => p.author === 'stranger'), '关注动态不应含陌生人的帖');
    ok('关注动态：只含关注者与自己的帖');
    fol = await withTimeout(mockClient.toggleFollow('test-token', 'otherhuman'), 15000, 'POST /api/follow 取关');
    assert.strictEqual(fol.following, false, '取关后 following 应为假');
    followingFeed = await withTimeout(mockClient.getFollowingFeed('test-token', { limit: 30 }), 15000, 'GET /api/feed/following 取关后');
    assert.ok(!followingFeed.posts.some((p) => p.author === 'otherhuman'), '取关后动态不应再含对方的帖');
    ok('关注：取关后动态不再含对方');
    // 资料：全量含资料字段；改自己资料落盘；无 token 401。
    const accounts = await withTimeout(mockClient.getAccounts(), 15000, 'GET /api/accounts');
    const meAcc = (accounts.accounts || []).find((a) => a.name === 'testhuman');
    assert.ok(meAcc && meAcc.bio === 'mock 人类', '资料应含 bio');
    assert.ok(Array.isArray(meAcc.tags) && meAcc.tags.includes('mock'), '资料应含 tags');
    assert.strictEqual(meAcc.stats.posts, 2, '资料应含 stats');
    ok('资料：GET /api/accounts 含 bio/tags/stats');
    const updated = await withTimeout(mockClient.updateMe('test-token', { bio: '改过的简介', signature: '新签名', tags: ['a', 'b'], avatarEmoji: '🌊' }), 15000, 'PATCH /api/accounts/me');
    assert.strictEqual(updated.account.bio, '改过的简介', 'bio 应更新');
    assert.strictEqual(updated.account.signature, '新签名', '签名应更新');
    assert.deepStrictEqual(updated.account.tags, ['a', 'b'], 'tags 应更新');
    assert.strictEqual(updated.account.avatarEmoji, '🌊', '头像表情应更新');
    const accounts2 = await withTimeout(mockClient.getAccounts(), 15000, 'GET /api/accounts 改后');
    assert.strictEqual((accounts2.accounts || []).find((a) => a.name === 'testhuman').bio, '改过的简介', '改后全量列表应反映新 bio');
    ok('资料：PATCH /api/accounts/me 落盘，全量列表同步');
    await assert.rejects(
      withTimeout(mockClient.updateMe(null, { bio: 'x' }), 15000, 'PATCH /api/accounts/me 无 token'),
      (e) => e.status === 401,
    );
    ok('资料：无 token 改资料回 401');
    // live 只读探针：GET /api/accounts 公开 200；GET /api/search 未登录 401（与 M3 同口径）。
    const liveAccounts = await withTimeout(client.getAccounts(), 15000, 'live GET /api/accounts');
    assert.ok(Array.isArray(liveAccounts.accounts) && liveAccounts.accounts.length > 0, 'live 账号列表应非空');
    ok(`live 账号列表公开可读，共 ${liveAccounts.accounts.length} 个账号`);
    await assert.rejects(
      withTimeout(client.search(null, 'test'), 15000, 'live GET /api/search 未登录'),
      (e) => e.status === 401,
    );
    ok('live 全站搜索：未登录回 401');
  } finally {
    forum.close();
    fs.rmSync(tmp3b, { recursive: true, force: true });
  }

  console.log('== 3c. 节点上传：丢 ack 与 409 分流（v0.3.10）==');
  const forum3c = await startMockForum();
  const forum3cUrl = forum3c.url;
  const tmp3c = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-ack-'));
  try {
    const nodeDir6 = path.join(tmp3c, 'node');
    fs.mkdirSync(nodeDir6, { recursive: true });
    const tokenFile6 = path.join(nodeDir6, 'forum-token');
    fs.writeFileSync(tokenFile6, 'test-node-token\n');
    fs.chmodSync(tokenFile6, 0o600);
    let rebased = 0;
    const nm6 = new NodeManager({
      nodeDir: nodeDir6,
      apiBase: forum3cUrl,
      config: { enabled: false, port: 0, seeds: [forum3cUrl], forumTokenFile: tokenFile6 },
      onLog: () => {},
    });
    nm6.on('chain-rebased', () => { rebased += 1; });
    await nm6.start(); // seq 1 创世上传
    assert.strictEqual(nm6.lastUploadedSeq, 1, '起步应上传 seq 1');
    await nm6.syncOnce(); // seq 2
    assert.strictEqual(nm6.lastUploadedSeq, 2, '应上传 seq 2');

    // (i) 丢 ack（传输出错）：服务端已存下 seq 3、ack 在半路丢了 → 认领，不重基
    forum3c.state.dropNextUploadAck = 1;
    await nm6.syncOnce(); // 追加 seq 3，POST 存下后 ack 丢 → reconcile 认领
    assert.strictEqual(nm6.lastUploadedSeq, 3, '丢 ack 后应认领服务端 lastSeq=3');
    assert.strictEqual(forum3c.state.node.lastSeq, 3, '服务端应已存下 seq 3');
    assert.strictEqual(nm6.uploadFailures, 0, '认领后失败数应清零，不退避');
    assert.strictEqual(nm6._lastRebaseAt, 0, '丢 ack 绝不能触发重基');
    assert.strictEqual(rebased, 0, '丢 ack 不应发 chain-rebased 事件');
    ok('丢 ack：服务端存下、ack 丢失后认领服务端头继续，不重发、不重基');
    await nm6.syncOnce(); // seq 4 正常上传，证明认领后链继续走
    assert.strictEqual(nm6.lastUploadedSeq, 4, '认领后下一条 seq 4 应正常上传');
    assert.strictEqual(rebased, 0, '认领后续传仍不应重基');
    ok('丢 ack 后续传：seq 4 正常上传，链未断');

    // (j) 409 服务端多 1（重试撞 409）：游标倒回假装 ack 丢了，服务端已超前 → 认领，不重基
    nm6.lastUploadedSeq = 2; // 白盒：假装 seq 3、4 的 ack 都丢了
    nm6._nextUploadAt = 0;
    nm6._saveUploadState();
    await nm6.syncOnce(); // 追加 seq 5，上传 3..5 → 409（expectedSeq=5）→ 认领至 4
    assert.strictEqual(nm6.lastUploadedSeq, 4, '409 后应认领服务端 lastSeq=4');
    assert.strictEqual(nm6._lastRebaseAt, 0, '409 丢 ack 情形绝不能重基');
    assert.strictEqual(rebased, 0, '409 认领不应发 chain-rebased 事件');
    ok('409 丢 ack：服务端多 1 时认领服务端头，不重基');
    await nm6.syncOnce(); // 追加 seq 6，上传 5..6 → 服务端 6
    assert.strictEqual(nm6.lastUploadedSeq, 6, '认领后剩余 seq 5、6 应补传成功');
    assert.strictEqual(forum3c.state.node.lastSeq, 6, '服务端 lastSeq 应追到 6');
    ok('409 认领后续传：剩余 seq 补传成功，服务端追到 6');

    // (k) 409 服务端落后（真分叉）：本地链换成与服务端不衔接的 seq 9 头 → 仍走重基
    const nm7dir = path.join(tmp3c, 'node7');
    fs.mkdirSync(nm7dir, { recursive: true });
    const tokenFile7 = path.join(nm7dir, 'forum-token');
    fs.writeFileSync(tokenFile7, 'test-node-token\n');
    fs.chmodSync(tokenFile7, 0o600);
    const nm7 = new NodeManager({
      nodeDir: nm7dir,
      apiBase: forum3cUrl,
      config: { enabled: false, port: 0, seeds: [forum3cUrl], forumTokenFile: tokenFile7 },
      onLog: () => {},
    });
    await nm7.start(); // seq 1 创世上传（服务端重基接受，lastSeq=1）
    await nm7.stop();
    const forgedBody7 = { seq: 9, kind: 'community-head', ts: new Date().toISOString(), posts: 999, accounts: 30, groups: 10, messages: 9999, dms: 7, prevHash: 'c'.repeat(64) };
    const forgedHash7 = crypto.createHash('sha256').update(JSON.stringify(forgedBody7, Object.keys(forgedBody7).sort()), 'utf8').digest('hex');
    fs.writeFileSync(path.join(nm7dir, 'replica', 'headers.jsonl'), JSON.stringify({ ...forgedBody7, hash: forgedHash7 }) + '\n');
    fs.writeFileSync(path.join(nm7dir, 'upload-state.json'), JSON.stringify({ lastUploadedSeq: 8, lastUploadAt: null, failures: 0, lastRebaseAt: 0 }, null, 2));
    let rebased7 = 0;
    const nm8 = new NodeManager({
      nodeDir: nm7dir,
      apiBase: forum3cUrl,
      config: { enabled: false, port: 0, seeds: [forum3cUrl], forumTokenFile: tokenFile7 },
      onLog: () => {},
    });
    nm8.on('chain-rebased', () => { rebased7 += 1; });
    await nm8.start(); // 自检通过；首次同步追加 seq 10，上传 9..10 → 409（服务端 lastSeq=1 落后）→ 重基
    assert.strictEqual(nm8.chainOk, true, '伪造单行链自洽，自检应通过');
    assert.ok(nm8._lastRebaseAt > 0, '409 服务端落后时应触发本地重基');
    assert.strictEqual(rebased7, 1, '真分叉应发一次 chain-rebased 事件');
    assert.strictEqual(nm8.lastUploadedSeq, 0, '重基后游标应归零');
    await nm8.syncOnce(); // 新链 seq 1 上传（服务端重基接受）
    assert.strictEqual(nm8.lastUploadedSeq, 1, '重基后新链 seq 1 应上传成功');
    assert.strictEqual(forum3c.state.node.lastSeq, 1, '服务端重基后 lastSeq 应为 1');
    ok('409 真分叉：服务端落后时仍走重基，新链上传成功');
    await nm8.stop();
    await nm6.stop();
  } finally {
    forum3c.close();
    fs.rmSync(tmp3c, { recursive: true, force: true });
  }

  console.log('== 11. 工作区绑定（融合 P0）：本机密钥 + mock 论坛五路由 ==');
  const wb = require('../lib/workspaceBinding');
  // 11a. 密钥生成 + key_id 推导 + 签/验 roundtrip（纯本机，不联网）。
  {
    const tmpWb = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-wsbind-'));
    try {
      const first = wb.ensureKeyPair({ dir: tmpWb });
      assert.strictEqual(first.created, true, '首次应新建密钥对');
      assert.ok(/^ed25519-[0-9a-f]{64}$/.test(first.keyId), 'keyId 应为 ed25519- + 64 hex');
      assert.strictEqual(first.fingerprint, first.keyId.slice(8, 16), '指纹应为 keyId 去前缀前 8 位');
      assert.strictEqual(first.nodeId, 'ws-' + first.keyId.slice(8, 20), 'nodeId 应为 ws- + 去前缀前 12 hex');
      const privStat = fs.statSync(path.join(tmpWb, 'workspace-key.pem'));
      assert.strictEqual(privStat.mode & 0o777, 0o600, '私钥文件应为 0600');
      assert.ok(fs.readFileSync(path.join(tmpWb, 'workspace-key.pem'), 'utf8').includes('BEGIN PRIVATE KEY'), '私钥应为 PKCS8 PEM');
      const pubB64 = fs.readFileSync(path.join(tmpWb, 'workspace-key.pub'), 'utf8').trim();
      assert.strictEqual(Buffer.from(pubB64, 'base64').length, 32, '公钥文件应为 base64 原始 32 字节');
      assert.strictEqual(pubB64, first.publicKey, '公钥文件内容应与返回一致');
      assert.strictEqual(wb.keyIdForPublicKey(Buffer.from(pubB64, 'base64')), first.keyId, 'keyId 应由公钥 sha256 推导');
      const second = wb.ensureKeyPair({ dir: tmpWb });
      assert.strictEqual(second.created, false, '再次调用不应轮换');
      assert.strictEqual(second.keyId, first.keyId, '再次调用应读回同一 keyId');
      const challenge = 'wsbind:test-challenge';
      const sig = wb.signChallenge(challenge, wb.readPrivateKeyPem({ dir: tmpWb }));
      assert.ok(wb.verifySignature({ challenge, publicKey: first.publicKey, signature: sig }), '正确挑战的签名应验过');
      assert.strictEqual(wb.verifySignature({ challenge: 'wsbind:other', publicKey: first.publicKey, signature: sig }), false, '换挑战串验签应失败');
      assert.strictEqual(wb.verifySignature({ challenge, publicKey: first.publicKey, signature: Buffer.from('00'.repeat(64), 'hex').toString('base64') }), false, '篡改签名验签应失败');
      assert.strictEqual(wb.isOnline(new Date().toISOString()), true, '刚 seen 应为在线');
      assert.strictEqual(wb.isOnline(new Date(Date.now() - 14 * 60 * 1000).toISOString()), true, '14 分钟前 seen 应为在线');
      assert.strictEqual(wb.isOnline(new Date(Date.now() - 16 * 60 * 1000).toISOString()), false, '16 分钟前 seen 应为离线');
      assert.strictEqual(wb.isOnline(null), false, '无 lastSeen 应为离线');
      assert.strictEqual(wb.sanitizeDeviceName('a'.repeat(100)), 'a'.repeat(64), '设备名应截到 64 字符');
      assert.strictEqual(wb.sanitizeDeviceName('a\x00b'), 'ab', '设备名应去控制字符');
      ok('本机密钥：生成 0600 + keyId 推导 + 签/验 roundtrip + 在线 15 分钟口径');
    } finally {
      fs.rmSync(tmpWb, { recursive: true, force: true });
    }
  }
  // 11b. mock 论坛五路由与状态机：challenge/bind/list/unlink/seen 全分支。
  {
    const forum2 = await startMockForum();
    const tmpWb2 = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-wsbind2-'));
    try {
      const mc = new ForumClient(forum2.url);
      const kp = wb.ensureKeyPair({ dir: tmpWb2 });
      const pem = wb.readPrivateKeyPem({ dir: tmpWb2 });
      const bindAs = async (token, deviceName, challengeOverride = null) => {
        const ch = challengeOverride || (await mc.workspaceChallenge(token)).challenge;
        const sig = wb.signChallenge(ch, pem);
        return mc.registerWorkspaceBinding(token, { keyId: kp.keyId, publicKey: kp.publicKey, deviceName, challenge: ch, signature: sig });
      };
      // 无 token 取 challenge 回 401。
      await assert.rejects(
        withTimeout(mc.workspaceChallenge(null), 15000, 'POST /api/workspace-bindings/challenge 无 token'),
        (e) => e.status === 401,
      );
      ok('工作区 challenge：无 token 回 401');
      // 正常绑定：registryStatus=pending，行字段齐全。
      const bound = await withTimeout(bindAs('test-token', 'test-device'), 15000, 'POST /api/workspace-bindings');
      assert.strictEqual(bound.ok, true, '绑定应回 ok');
      assert.strictEqual(bound.binding.keyId, kp.keyId, '绑定行 keyId 应与本机一致');
      assert.strictEqual(bound.binding.deviceName, 'test-device', '绑定行设备名应与请求一致');
      assert.strictEqual(bound.binding.registryStatus, 'pending', '新绑定 registryStatus 应为 pending');
      assert.strictEqual(bound.binding.unlinkedTs, null, '新绑定 unlinkedTs 应为空');
      const bindingId = bound.binding.id;
      ok(`工作区绑定：正常注册回 pending，id=${bindingId}`);
      // challenge 一次性：同一挑战重放回 400。
      const chReplay = (await mc.workspaceChallenge('test-token')).challenge;
      await withTimeout(bindAs('test-token', 'test-device', chReplay), 15000, 'POST /api/workspace-bindings 首用');
      await assert.rejects(
        withTimeout(bindAs('test-token', 'test-device', chReplay), 15000, 'POST /api/workspace-bindings 重放'),
        (e) => e.status === 400,
      );
      ok('工作区 challenge：一次性，重放回 400');
      // 签名错回 401（新挑战、签错串）。
      {
        const ch = (await mc.workspaceChallenge('test-token')).challenge;
        const badSig = wb.signChallenge('wsbind:wrong-challenge', pem);
        await assert.rejects(
          withTimeout(mc.registerWorkspaceBinding('test-token', { keyId: kp.keyId, publicKey: kp.publicKey, deviceName: 'test-device', challenge: ch, signature: badSig }), 15000, 'POST /api/workspace-bindings 签名错'),
          (e) => e.status === 401,
        );
      }
      ok('工作区绑定：签名错回 401');
      // keyId 与公钥不符回 400（新挑战、签名正确、keyId 换错）。
      {
        const ch = (await mc.workspaceChallenge('test-token')).challenge;
        const sig = wb.signChallenge(ch, pem);
        await assert.rejects(
          withTimeout(mc.registerWorkspaceBinding('test-token', { keyId: 'ed25519-' + '00'.repeat(32), publicKey: kp.publicKey, deviceName: 'test-device', challenge: ch, signature: sig }), 15000, 'POST /api/workspace-bindings keyId 不符'),
          (e) => e.status === 400,
        );
      }
      ok('工作区绑定：keyId 与公钥不符回 400');
      // 他人 keyId 回 409（otherhuman 用同一 keyId 绑）。
      await assert.rejects(
        withTimeout(bindAs('test-other-token', 'other-device'), 15000, 'POST /api/workspace-bindings 他人 keyId'),
        (e) => e.status === 409,
      );
      ok('工作区绑定：他人 keyId 回 409');
      // 同账号幂等：同一 keyId 再绑回原行 id，deviceName 更新。
      const again = await withTimeout(bindAs('test-token', 'test-device-renamed'), 15000, 'POST /api/workspace-bindings 幂等');
      assert.strictEqual(again.binding.id, bindingId, '同账号重绑应回原行 id');
      assert.strictEqual(again.binding.deviceName, 'test-device-renamed', '幂等重绑应更新 deviceName');
      ok('工作区绑定：同账号幂等，回原行 id 并更新设备名');
      // list：本人 1 行活跃；seen 更新 lastSeenTs。
      const mine = await withTimeout(mc.myWorkspaceBindings('test-token'), 15000, 'GET /api/workspace-bindings/me');
      assert.strictEqual(mine.bindings.length, 1, '本人应有 1 行绑定');
      assert.strictEqual(mine.bindings[0].id, bindingId, '列表行 id 应与注册一致');
      const beforeSeen = Date.parse(mine.bindings[0].lastSeenTs);
      const seen = await withTimeout(mc.workspaceSeen('test-token', bindingId), 15000, 'POST /api/workspace-bindings/:id/seen');
      assert.ok(Date.parse(seen.lastSeenTs) >= beforeSeen, 'seen 应更新 lastSeenTs');
      ok('工作区列表/seen：本人 1 行，seen 更新 lastSeenTs');
      // 公开面：testhuman 有绑且在线，otherhuman 无绑；不泄 keyId/deviceName/publicKey。
      const pub = await withTimeout(mc.getAccounts(), 15000, 'GET /api/accounts 工作区公开面');
      const meAcc = (pub.accounts || []).find((a) => a.name === 'testhuman');
      const otherAcc = (pub.accounts || []).find((a) => a.name === 'otherhuman');
      assert.strictEqual(meAcc.hasWorkspace, true, '已绑账号 hasWorkspace 应为真');
      assert.strictEqual(meAcc.workspaceOnline, true, '刚 seen 账号 workspaceOnline 应为真');
      assert.strictEqual(otherAcc.hasWorkspace, false, '未绑账号 hasWorkspace 应为假');
      assert.strictEqual(otherAcc.workspaceOnline, false, '未绑账号 workspaceOnline 应为假');
      for (const a of pub.accounts) {
        assert.ok(!('keyId' in a) && !('deviceName' in a) && !('publicKey' in a), '公开面不应泄 keyId/deviceName/publicKey');
      }
      ok('工作区公开面：只多 hasWorkspace/workspaceOnline 两个布尔值，无凭证泄漏');
      // 解绑：unlinkedTs 已打、registryStatus=revoked-pending；再解 409；解绑后 seen 409。
      const unlinked = await withTimeout(mc.unlinkWorkspaceBinding('test-token', bindingId), 15000, 'POST /api/workspace-bindings/:id/unlink');
      assert.ok(unlinked.binding.unlinkedTs, '解绑应打 unlinkedTs');
      assert.strictEqual(unlinked.binding.registryStatus, 'revoked-pending', '解绑后 registryStatus 应为 revoked-pending（DO 吊销待 Owner）');
      await assert.rejects(
        withTimeout(mc.unlinkWorkspaceBinding('test-token', bindingId), 15000, 'POST /api/workspace-bindings/:id/unlink 再解'),
        (e) => e.status === 409,
      );
      await assert.rejects(
        withTimeout(mc.workspaceSeen('test-token', bindingId), 15000, 'POST /api/workspace-bindings/:id/seen 已解绑'),
        (e) => e.status === 409,
      );
      const mineAfter = await withTimeout(mc.myWorkspaceBindings('test-token'), 15000, 'GET /api/workspace-bindings/me 解绑后');
      assert.strictEqual(mineAfter.bindings.length, 1, '解绑后列表仍保留该行');
      assert.ok(mineAfter.bindings[0].unlinkedTs, '解绑后列表行应带 unlinkedTs');
      const pubAfter = await withTimeout(mc.getAccounts(), 15000, 'GET /api/accounts 解绑后');
      assert.strictEqual((pubAfter.accounts || []).find((a) => a.name === 'testhuman').hasWorkspace, false, '解绑后 hasWorkspace 应为假');
      ok('工作区解绑：registryStatus=revoked-pending，再解 409，解绑后 seen 409，公开面回落');
    } finally {
      forum2.close();
      fs.rmSync(tmpWb2, { recursive: true, force: true });
    }
  }
  // 11c. 渲染器纯函数：指纹前 8 位 + 在线 15 分钟口径（抽 renderer.js 实际函数跑）。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const fpFn = new Function(`${extractFn('workspaceFingerprint')}; return workspaceFingerprint;`)();
    const onlineFn = new Function(`${extractFn('isWorkspaceOnline')}; return isWorkspaceOnline;`)();
    assert.strictEqual(fpFn('ed25519-aa99f1297f02abcdef'), 'aa99f129', '指纹应为去前缀前 8 位');
    assert.strictEqual(fpFn(''), '', '空 keyId 指纹应为空');
    const now = Date.now();
    assert.strictEqual(onlineFn(new Date(now - 60 * 1000).toISOString(), now), true, '1 分钟前应在线');
    assert.strictEqual(onlineFn(new Date(now - 16 * 60 * 1000).toISOString(), now), false, '16 分钟前应离线');
    assert.strictEqual(onlineFn(null, now), false, '无 lastSeen 应离线');
    assert.strictEqual(onlineFn('not-a-date', now), false, '非法时间应离线');
    ok('渲染器纯函数：workspaceFingerprint 前 8 位 + isWorkspaceOnline 15 分钟口径');
  }

  console.log('== 4. live WS（内核 v2，匿名，不写线上状态）==');
  // 4a. WS hello：匿名连 wss，应收到带 seq 的 hello。
  {
    const ws = new WsClient(API_BASE, {});
    const c = await withTimeout(new Promise((resolve, reject) => {
      ws.on('connect', resolve);
      ws.on('disconnect', reject);
      ws.start();
    }), 20000, 'WS hello');
    assert.ok(Number.isFinite(ws.lastSeq), 'hello 后 lastSeq 应为有限 seq');
    assert.strictEqual(ws.everConnected, true);
    ws.stop();
    ok(`WS hello 收到，seq=${c.seq}`);
  }
  // 4b. truncated：since=0 远早于缓冲，服务端应先发 truncated。
  {
    const ws = new WsClient(API_BASE, { since: 0 });
    const data = await withTimeout(new Promise((resolve, reject) => {
      ws.on('truncated', resolve);
      ws.on('disconnect', reject);
      ws.start();
    }), 20000, 'WS truncated');
    assert.strictEqual(data.truncated, true, 'truncated 事件应带 truncated:true');
    assert.ok(Number.isFinite(data.seq), 'truncated 应带最新 seq');
    ws.stop();
    ok(`WS truncated 收到，latest seq=${data.seq}`);
  }
  // 4c. 断线续接：主动断开后应按 lastSeq 自动重连，不丢连接。
  {
    const ws = new WsClient(API_BASE, {});
    let connects = 0;
    const second = new Promise((resolve, reject) => {
      ws.on('connect', () => {
        connects += 1;
        if (connects === 2) resolve(ws.lastSeq);
      });
      ws.on('disconnect', () => { /* 重连中，忽略 */ });
      ws.start();
    });
    await withTimeout(new Promise((resolve, reject) => {
      ws.on('connect', () => resolve());
      setTimeout(() => reject(new Error('首次 hello 未到')), 20000);
    }), 20000, 'WS 首次 hello');
    const seq1 = ws.lastSeq;
    assert.ok(Number.isFinite(seq1));
    ws.dropConnection();
    const seq2 = await withTimeout(second, 25000, 'WS 断线重连');
    assert.ok(Number.isFinite(seq2) && seq2 >= seq1, '重连后 seq 不应倒退');
    ws.stop();
    ok(`WS 断线重连成功，seq ${seq1} -> ${seq2}`);
  }
  // 4d. WS 优先：RealtimeClient 正常应走 ws。
  {
    const rt = new RealtimeClient(API_BASE, {});
    const c = await withTimeout(new Promise((resolve, reject) => {
      rt.on('connect', resolve);
      rt.on('disconnect', reject);
      rt.start();
    }), 20000, 'Realtime WS connect');
    assert.strictEqual(c.transport, 'ws', '正常应优先走 WS');
    assert.strictEqual(rt.transport, 'ws');
    rt.stop();
    ok(`RealtimeClient 优先走 WS，seq=${c.seq}`);
  }
  // 4e. WS 挂掉 → SSE 回落：WS 指到不通的本地端口，应切 SSE 并收到 hello。
  {
    const rt = new RealtimeClient(API_BASE, { wsUrl: 'wss://127.0.0.1:9/ws' });
    const fellBack = new Promise((resolve) => rt.on('fallback', resolve));
    const connected = new Promise((resolve, reject) => {
      rt.on('connect', resolve);
      rt.on('disconnect', () => { /* 回落前的 WS 断开，忽略 */ });
    });
    // hello 监听必须在 start() 之前挂上：SSE hello 紧跟 connect 后面到，
    // 晚挂会漏掉（connect 只表示 HTTP 通了，hello 是流里第一条事件）。
    const helloP = new Promise((resolve) => {
      rt.on('event', (e) => { if (e.event === 'hello') resolve(e); });
    });
    rt.start();
    const fb = await withTimeout(fellBack, 20000, 'WS->SSE 回落信号');
    assert.strictEqual(fb.to, 'sse');
    const c = await withTimeout(connected, 20000, '回落 SSE connect');
    assert.strictEqual(c.transport, 'sse', 'WS 挂掉后应走 SSE');
    const helloEv = await withTimeout(helloP, 20000, '回落 SSE hello');
    assert.ok(Number.isFinite(helloEv.seq), '回落 SSE hello 应带 seq');
    rt.stop();
    ok(`WS 挂掉后回落 SSE，hello seq=${helloEv.seq}`);
  }

  console.log('== 5. 性能优化（v0.2.1）：分块 base64 + 副本头轮转 ==');
  // §3 的 mock 种子已关，这里另起一个供 5b/5c 用。
  const seed5 = await startMockSeed();
  const seed5Url = `http://127.0.0.1:${seed5.address().port}`;
  // 5a. 分块 base64：直接测 renderer.js 里实际发布的函数（从源码抽出来跑，
  // 不是复制一份）。5MB 随机 buffer 应与 Buffer.toString('base64') 逐字节一致；
  // 旧的展开写法在 200KB 就应抛 RangeError（回归锚点，证明修的是真 bug）。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const m = rendererSrc.match(/function arrayBufferToBase64\(buf\) \{[\s\S]*?\n\}/);
    assert.ok(m, 'renderer.js 应包含 arrayBufferToBase64');
    const arrayBufferToBase64 = new Function(`${m[0]}; return arrayBufferToBase64;`)();
    assert.strictEqual(arrayBufferToBase64(new ArrayBuffer(0)), '');
    const small = crypto.randomBytes(3 * 1024);
    assert.strictEqual(arrayBufferToBase64(small.buffer), small.toString('base64'), '3KB 应与 Buffer 口径一致');
    const big = crypto.randomBytes(5 * 1024 * 1024);
    const t0 = Date.now();
    const b64 = arrayBufferToBase64(big.buffer);
    const ms = Date.now() - t0;
    assert.strictEqual(b64, big.toString('base64'), '5MB 分块 base64 应与 Buffer 口径逐字节一致');
    assert.ok(Buffer.from(b64, 'base64').equals(big), '5MB base64 往返应无损');
    assert.throws(() => {
      const b = new Uint8Array(200 * 1024);
      btoa(String.fromCharCode(...b));
    }, RangeError, '旧展开写法在 200KB 应抛 RangeError');
    ok(`分块 base64：5MB 往返一致（${ms}ms），旧写法 200KB 确认抛 RangeError`);
  }
  // 5b. 副本头轮转：上限 5 行 + slack 0，写 8 条后文件只剩最新 5 行，链不断。
  {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-rot-'));
    try {
      const nm = new NodeManager({
        nodeDir: path.join(tmp, 'node'),
        apiBase: seed5Url,
        config: { enabled: false, port: 0, seeds: [seed5Url] },
        maxHeadersLines: 5,
        rotateSlackLines: 0,
        onLog: () => {},
      });
      await nm.start();
      for (let i = 0; i < 7; i++) await nm.syncOnce();
      assert.strictEqual(nm.headersCount, 5, '轮转后计数应为上限 5');
      const lines = fs.readFileSync(path.join(tmp, 'node', 'replica', 'headers.jsonl'), 'utf8')
        .split('\n').filter(Boolean).map((l) => JSON.parse(l));
      assert.strictEqual(lines.length, 5, '文件应只剩最新 5 行');
      for (let i = 1; i < lines.length; i++) {
        assert.strictEqual(lines[i].prevHash, lines[i - 1].hash, `第 ${i} 行 prevHash 应接上一行 hash`);
      }
      assert.strictEqual(nm._prevHash, lines[lines.length - 1].hash, '_prevHash 应为尾行 hash');
      ok('副本头轮转：8 条 → 只留最新 5 行，链内 prevHash 逐行衔接');
      await nm.stop();
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  }
  // 5c. _loadHead 有界读：预填 10000 行合法链，构造即读一次、只解析尾行。
  {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-head-'));
    try {
      const dir = path.join(tmp, 'node', 'replica');
      fs.mkdirSync(dir, { recursive: true });
      const canonical = (o) => JSON.stringify(o, Object.keys(o).sort());
      let prev = '0'.repeat(64);
      const out = [];
      for (let i = 0; i < 10000; i++) {
        const body = { kind: 'community-head', ts: new Date().toISOString(), posts: i, prevHash: prev };
        const hash = crypto.createHash('sha256').update(canonical(body), 'utf8').digest('hex');
        out.push(JSON.stringify({ ...body, hash }));
        prev = hash;
      }
      fs.writeFileSync(path.join(dir, 'headers.jsonl'), out.join('\n') + '\n');
      const t0 = Date.now();
      const nm = new NodeManager({
        nodeDir: path.join(tmp, 'node'),
        apiBase: seed5Url,
        config: { enabled: false, port: 0, seeds: [seed5Url] },
        onLog: () => {},
      });
      const ms = Date.now() - t0;
      assert.strictEqual(nm.headersCount, 10000, '预填 10000 行应数出 10000');
      assert.strictEqual(nm._prevHash, prev, '_prevHash 应为尾行 hash');
      assert.ok(ms < 1000, `10000 行 _loadHead 应在 1 秒内，实际 ${ms}ms`);
      assert.strictEqual(nm._tailHeaders(3).length, 3, '_tailHeaders(3) 应只回最后 3 行');
      assert.strictEqual(nm._tailHeaders(3)[2].hash, prev, '_tailHeaders 尾行应为最新');
      ok(`_loadHead 有界读：10000 行 ${ms}ms，计数与尾 hash 正确`);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  }
  seed5.close();

  console.log('== 6. 消息窗口（v0.2.1+1）：DOM 上限 + 向前翻页 + 贴底判断 ==');
  // 从 renderer.js 源码里抽出窗口相关函数（与 §5a 同一做法：测实际发布的
  // 代码，不是复制品），跑在一个最小假 DOM 上。假 DOM 只实现渲染进程
  // 用到的接口：innerHTML / insertAdjacentHTML / querySelector(.msg、
  // [data-empty]) / querySelectorAll(.msg) / scrollTop / scrollHeight /
  // clientHeight / remove()。每条消息假高 40px，只验滚动补偿的算术。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const MAX_RENDERED = Number((rendererSrc.match(/const MAX_RENDERED = (\d+);/) || [])[1]);
    const PAGE_SIZE = Number((rendererSrc.match(/const PAGE_SIZE = (\d+);/) || [])[1]);
    const NEAR_BOTTOM_PX = Number((rendererSrc.match(/const NEAR_BOTTOM_PX = (\d+);/) || [])[1]);
    assert.strictEqual(MAX_RENDERED, 300, 'DOM 上限应为 300 条');
    assert.strictEqual(PAGE_SIZE, 100, '每页应为 100 条');
    assert.strictEqual(NEAR_BOTTOM_PX, 120, '贴底判定应为 120px');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const escFn = new Function(`${extractFn('esc')}; return esc;`)();
    // v0.3.7：抽出的函数里 chrome 文案走 ui(key)；直接注入真实翻译模块的 ui，
    // 与线上一致（window.ChimeraTranslate 在时 renderer 的 ui 助手也是透传它）。
    const tr = require('../renderer/translate.js');
    // messageHtml 自 v0.3.0 起还用 state.me（判自己的消息）、avatarHtml、fmtTime：
    // 抽出来时一并注入。§6 的假 state 从不设 me（没有一条是自己的消息），
    // 故这里传一个固定的 { me: null }，不随下面 state = freshState() 的重绑而变。
    const avatarClassFn = new Function(`${extractFn('avatarClass')}; return avatarClass;`)();
    const avatarInitialFn = new Function(`${extractFn('avatarInitial')}; return avatarInitial;`)();
    const avatarHtmlFn = new Function('avatarClass', 'avatarInitial', 'esc', `${extractFn('avatarHtml')}; return avatarHtml;`)(avatarClassFn, avatarInitialFn, escFn);
    const fmtTimeFn = new Function(`${extractFn('fmtTime')}; return fmtTime;`)();
    const messageHtmlFn = new Function('esc', 'state', 'avatarHtml', 'fmtTime', 'ui', `${extractFn('messageHtml')}; return messageHtml;`)(escFn, { me: null }, avatarHtmlFn, fmtTimeFn, tr.ui);

    function makeEl(cls, id, parent) {
      return {
        cls, dataset: { id: id || '' }, height: 40, _parent: parent,
        remove() { if (this._parent) this._parent._removeChild(this); },
      };
    }
    function parseChildren(html, parent) {
      const out = [];
      const re = /<div class="msg" data-id="([^"]*)">/g;
      let m;
      while ((m = re.exec(html)) !== null) out.push(makeEl('msg', m[1], parent));
      if (/data-empty="1"/.test(html)) {
        const el = makeEl('muted-empty', '', parent);
        el.height = 20;
        out.push(el);
      }
      return out;
    }
    function makeBox() {
      const box = {
        _children: [],
        _scrollTop: 0,
        clientHeight: 400,
        // 真实浏览器里 scrollTop 会被钳制到 [0, scrollHeight - clientHeight]；
        // 假 DOM 也照此钳制，否则“滚到底”的断言会量到未钳制的原值。
        get scrollTop() { return this._scrollTop; },
        set scrollTop(v) {
          const max = Math.max(0, this.scrollHeight - this.clientHeight);
          this._scrollTop = Math.max(0, Math.min(v, max));
        },
        get scrollHeight() { return this._children.reduce((s, c) => s + (c.height || 0), 0); },
        set innerHTML(html) { this._children = parseChildren(html, this); },
        get innerHTML() { return ''; },
        insertAdjacentHTML(pos, html) {
          const kids = parseChildren(html, this);
          if (pos === 'afterbegin') this._children.unshift(...kids);
          else this._children.push(...kids);
        },
        querySelector(sel) {
          if (sel === '.msg') return this._children.find((c) => c.cls === 'msg') || null;
          if (sel === '[data-empty]') return this._children.find((c) => c.cls === 'muted-empty') || null;
          return null;
        },
        querySelectorAll(sel) {
          if (sel === '.msg') return this._children.filter((c) => c.cls === 'msg');
          return [];
        },
        _removeChild(el) { const i = this._children.indexOf(el); if (i >= 0) this._children.splice(i, 1); },
      };
      return box;
    }
    function makeRowBtn() {
      return {
        _hidden: true,
        classList: { toggle(_cls, force) { this._owner._hidden = !!force; } },
        disabled: false,
        textContent: '',
      };
    }
    const box = makeBox();
    const rowEl = makeRowBtn();
    rowEl.classList._owner = rowEl;
    const btnEl = { disabled: false, textContent: '' };
    const $fake = (id) => ({ chatBox: box, chatOlderRow: rowEl, chatOlderBtn: btnEl }[id] || null);
    const mkMsg = (i) => ({
      id: `m${i}`, author: 'a', ts: new Date(Date.UTC(2026, 10, 23, 0, 0, i)).toISOString(), body: `msg ${i}`,
    });
    const freshState = () => ({
      open: { kind: 'group', id: 'g1', name: 't' },
      lastTs: null, renderedIds: new Set(), hasMoreOlder: false, loadingOlder: false,
    });
    let state = freshState();
    const renderMessages = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'ui',
      `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, tr.ui);
    const appendMessages = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'NEAR_BOTTOM_PX',
      `${extractFn('appendMessages')}; return appendMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, NEAR_BOTTOM_PX);
    const prependMessages = new Function('state', '$', 'messageHtml',
      `${extractFn('prependMessages')}; return prependMessages;`)(state, $fake, messageHtmlFn);
    const syncOlderButton = new Function('state', '$', 'ui',
      `${extractFn('syncOlderButton')}; return syncOlderButton;`)(state, $fake, tr.ui);

    // 6a. 初拉 120 条：全渲染，水位为最后一条。
    renderMessages(Array.from({ length: 120 }, (_, i) => mkMsg(i + 1)));
    assert.strictEqual(box.querySelectorAll('.msg').length, 120, '初拉 120 条应全渲染');
    assert.strictEqual(state.renderedIds.size, 120, 'renderedIds 应记 120 条');
    assert.strictEqual(state.lastTs, mkMsg(120).ts, '水位应为最后一条 ts');
    ok('初拉 120 条全渲染，水位正确');

    // 6b. 初拉 350 条（防御性上限）：只留最新 300 条，hasMoreOlder 为真。
    state = freshState();
    const render2 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'ui',
      `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, tr.ui);
    render2(Array.from({ length: 350 }, (_, i) => mkMsg(i + 1)));
    assert.strictEqual(box.querySelectorAll('.msg').length, 300, '350 条初拉应只渲染 300 条');
    assert.strictEqual(box.querySelector('.msg').dataset.id, 'm51', '应留最新的 300 条（m51 起）');
    assert.strictEqual(state.hasMoreOlder, true, '超上限初拉应标 hasMoreOlder');
    ok('初拉 350 条只留最新 300 条，hasMoreOlder 为真');

    // 6c. 追加超上限：100 条初拉 + 250 条追加 → 留 300 条，最老的 50 条
    // 节点与 id 一起被裁掉，最新一条与水位 intact。
    state = freshState();
    const render3 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'ui',
      `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, tr.ui);
    const append3 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'NEAR_BOTTOM_PX',
      `${extractFn('appendMessages')}; return appendMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, NEAR_BOTTOM_PX);
    render3(Array.from({ length: 100 }, (_, i) => mkMsg(i + 1)));
    box.scrollTop = box.scrollHeight - box.clientHeight; // 贴底
    append3(Array.from({ length: 250 }, (_, i) => mkMsg(101 + i)));
    assert.strictEqual(box.querySelectorAll('.msg').length, 300, '追加后 DOM 应裁到 300 条');
    assert.strictEqual(box.querySelector('.msg').dataset.id, 'm51', '应裁掉最老的 50 条');
    assert.strictEqual(state.renderedIds.size, 300, 'renderedIds 应同步裁到 300');
    assert.ok(!state.renderedIds.has('m1') && state.renderedIds.has('m350'), '被裁 id 应出 Set，最新 id 应在');
    assert.strictEqual(state.lastTs, mkMsg(350).ts, '水位应为最新一条');
    ok('追加超上限：DOM 与 renderedIds 同裁到 300 条，最老 50 条出局');

    // 6d. 贴底判断：滚上去读旧消息时新消息不抢滚动；贴底时才滚到底。
    box.scrollTop = 1000; // 离底 12000-1000-400=10600，远大于 120
    const topBefore = box.scrollTop;
    append3([mkMsg(351)]);
    assert.strictEqual(box.scrollTop, topBefore, '滚上去时追加不应动 scrollTop');
    box.scrollTop = box.scrollHeight - box.clientHeight; // 回到底部
    append3([mkMsg(352)]);
    assert.strictEqual(box.scrollTop, box.scrollHeight - box.clientHeight, '贴底时追加应滚到底');
    ok('贴底判断：上翻时不抢滚动，贴底时滚到底');

    // 6e. 向前追加 2 条：插到顶上，scrollTop 按插入高度补偿，id 补进 Set。
    state = freshState();
    const render5 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'ui',
      `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, tr.ui);
    const prepend5 = new Function('state', '$', 'messageHtml',
      `${extractFn('prependMessages')}; return prependMessages;`)(state, $fake, messageHtmlFn);
    render5(Array.from({ length: 100 }, (_, i) => mkMsg(101 + i))); // m101..m200
    box.scrollTop = 500;
    const hBefore = box.scrollHeight;
    prepend5([mkMsg(99), mkMsg(100)]);
    assert.strictEqual(box.querySelector('.msg').dataset.id, 'm99', '前翻的应插到最顶上');
    assert.strictEqual(box.scrollTop, 500 + (box.scrollHeight - hBefore), 'scrollTop 应按插入高度补偿');
    assert.ok(state.renderedIds.has('m99') && state.renderedIds.has('m100'), '前翻的 id 应进 Set');
    assert.strictEqual(state.lastTs, mkMsg(200).ts, '前翻不应动水位');
    ok('向前追加：插顶、scrollTop 补偿、id 进 Set、水位不动');

    // 6f. loadOlderMessages：假服务端 250 条，按服务端 paginate 口径回页。
    // 第一页翻回 100 条、has_more 仍真；第二页翻回剩下 50 条、has_more 假；
    // 再点不发请求。另：已到 300 上限时直接关按钮、不发请求。
    {
      const serverMsgs = Array.from({ length: 250 }, (_, i) => mkMsg(i + 1));
      let calls = 0;
      const fakeApi = {
        groupMessages: async (_id, opts = {}) => {
          calls += 1;
          let list = serverMsgs;
          if (opts.before_id) {
            const idx = list.findIndex((x) => x.id === opts.before_id);
            list = idx >= 0 ? list.slice(0, idx) : [];
          }
          let hasMore = false;
          if (opts.limit) {
            const limit = Math.max(1, Math.min(100, parseInt(opts.limit, 10) || 20));
            if (list.length > limit) { hasMore = true; list = list.slice(list.length - limit); }
          }
          return { messages: list, total: serverMsgs.length, has_more: hasMore };
        },
        dmHistory: async () => { throw new Error('本节只测群聊'); },
      };
      const fakeWindow = { chimera: { api: fakeApi } };
      state = freshState();
      const render6 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'ui',
        `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, tr.ui);
      const prepend6 = new Function('state', '$', 'messageHtml',
        `${extractFn('prependMessages')}; return prependMessages;`)(state, $fake, messageHtmlFn);
      const sync6 = new Function('state', '$', 'ui',
        `${extractFn('syncOlderButton')}; return syncOlderButton;`)(state, $fake, tr.ui);
      const loadOlder = new Function('state', '$', 'window', 'prependMessages', 'syncOlderButton', 'MAX_RENDERED', 'PAGE_SIZE',
        `${extractFn('loadOlderMessages')}; return loadOlderMessages;`)(state, $fake, fakeWindow, prepend6, sync6, MAX_RENDERED, PAGE_SIZE);
      render6(serverMsgs.slice(-100)); // 初拉最新 100 条 m151..m250
      state.hasMoreOlder = true;
      box.scrollTop = 2000;
      const top6 = box.scrollTop;
      await loadOlder();
      assert.strictEqual(calls, 1, '第一页应发 1 次请求');
      assert.strictEqual(box.querySelectorAll('.msg').length, 200, '第一页应翻回 100 条，共 200 条');
      assert.strictEqual(box.querySelector('.msg').dataset.id, 'm51', '第一页最顶上应为 m51');
      assert.strictEqual(state.hasMoreOlder, true, '还剩 50 条，hasMoreOlder 应仍真');
      assert.ok(box.scrollTop > top6, '前翻后 scrollTop 应补偿上移');
      await loadOlder();
      assert.strictEqual(calls, 2, '第二页应发第 2 次请求');
      assert.strictEqual(box.querySelectorAll('.msg').length, 250, '第二页应翻回剩下 50 条，共 250 条');
      assert.strictEqual(state.hasMoreOlder, false, '翻完应无更早，hasMoreOlder 为假');
      await loadOlder();
      assert.strictEqual(calls, 2, '无更早时再点不应发请求');
      ok('向前翻页：两页翻完 250 条，has_more 收敛，不再发请求');

      // 上限守卫：已渲染 300 条时点翻页，直接关按钮、不发请求。
      state = freshState();
      const render7 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'ui',
        `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, tr.ui);
      const prepend7 = new Function('state', '$', 'messageHtml',
        `${extractFn('prependMessages')}; return prependMessages;`)(state, $fake, messageHtmlFn);
      const sync7 = new Function('state', '$', 'ui',
        `${extractFn('syncOlderButton')}; return syncOlderButton;`)(state, $fake, tr.ui);
      const loadOlder7 = new Function('state', '$', 'window', 'prependMessages', 'syncOlderButton', 'MAX_RENDERED', 'PAGE_SIZE',
        `${extractFn('loadOlderMessages')}; return loadOlderMessages;`)(state, $fake, fakeWindow, prepend7, sync7, MAX_RENDERED, PAGE_SIZE);
      render7(Array.from({ length: 350 }, (_, i) => mkMsg(i + 1))); // 裁到 300 条
      state.hasMoreOlder = true;
      const callsBefore = calls;
      await loadOlder7();
      assert.strictEqual(calls, callsBefore, '已到上限时翻页不应发请求');
      assert.strictEqual(state.hasMoreOlder, false, '已到上限时应关掉翻页');
      assert.strictEqual(rowEl._hidden, true, '已到上限时按钮行应隐藏');
      ok('上限守卫：已渲染 300 条时翻页不发请求、按钮隐藏');
    }

    // 6g. 按钮三态：有更早→显示可点；加载中→禁用“加载中…”；无更早→隐藏。
    state = freshState();
    const syncG = new Function('state', '$', 'ui',
      `${extractFn('syncOlderButton')}; return syncOlderButton;`)(state, $fake, tr.ui);
    state.hasMoreOlder = true;
    syncG();
    assert.strictEqual(rowEl._hidden, false, '有更早时按钮行应显示');
    assert.strictEqual(btnEl.disabled, false, '有更早时按钮应可点');
    assert.strictEqual(btnEl.textContent, '加载更早的消息', '按钮文案应为加载更早的消息');
    state.loadingOlder = true;
    syncG();
    assert.strictEqual(btnEl.disabled, true, '加载中按钮应禁用');
    assert.strictEqual(btnEl.textContent, '加载中…', '加载中文案应为加载中…');
    state.loadingOlder = false;
    state.hasMoreOlder = false;
    syncG();
    assert.strictEqual(rowEl._hidden, true, '无更早时按钮行应隐藏');
    ok('按钮三态：可点 / 加载中禁用 / 无更早隐藏');
  }

  console.log('== 7. 状态指示器与退避（v0.2.1）：退避表 + 会话失效停重试 ==');
  // 7a. 退避表（纯函数，无 live 依赖）：1s 起步翻倍、30s 封顶。
  {
    assert.strictEqual(INITIAL_BACKOFF_MS, 1000, '起步应为 1s');
    assert.strictEqual(MAX_BACKOFF_MS, 30000, '上限应为 30s');
    assert.strictEqual(nextBackoffMs(undefined), 1000, '未传应回 1000');
    assert.strictEqual(nextBackoffMs(0), 1000, '0 应回 1000');
    assert.strictEqual(nextBackoffMs(-5), 1000, '负数应回 1000');
    assert.strictEqual(nextBackoffMs(NaN), 1000, 'NaN 应回 1000');
    let d = INITIAL_BACKOFF_MS;
    const seq = [d];
    for (let i = 0; i < 6; i++) { d = nextBackoffMs(d); seq.push(d); }
    assert.deepStrictEqual(seq, [1000, 2000, 4000, 8000, 16000, 30000, 30000],
      '退避序列应为 1s 起步翻倍、30s 封顶');
    assert.strictEqual(nextBackoffMs(99999), 30000, '超上限应封顶 30000');
    ok('退避表：1s 起步翻倍、30s 封顶，非法输入回 1000');
  }
  // 7b. 会话失效停重试：WS 在 hello 前就失败 + checkSession 抛 401 →
  // session-expired，不回落 SSE、不再重试。
  {
    const states = [];
    const rt = new RealtimeClient(API_BASE, {
      token: 'dead-token-for-test',
      wsUrl: 'wss://127.0.0.1:9/ws',
      checkSession: async () => {
        const e = new Error('HTTP 401');
        e.status = 401;
        throw e;
      },
    });
    let fellBack = false;
    rt.on('fallback', () => { fellBack = true; });
    rt.on('transport', (p) => states.push(p.state));
    const expired = new Promise((resolve, reject) => {
      rt.on('session-expired', resolve);
      setTimeout(() => reject(new Error('session-expired 未在 10s 内触发')), 10000);
    });
    rt.start();
    const ev = await withTimeout(expired, 12000, 'session-expired');
    assert.match(ev.reason, /会话已失效/, 'session-expired 应带原因');
    assert.strictEqual(rt.sessionExpired, true, 'sessionExpired 标志应为真');
    assert.strictEqual(rt._sse, null, '会话失效后不应起 SSE');
    assert.strictEqual(rt._ws, null, '会话失效后 WS 应停掉');
    assert.strictEqual(fellBack, false, '会话失效后不应回落');
    assert.ok(states.includes('connecting'), '应先报 connecting');
    assert.ok(states.includes('session-expired'), 'transport 应报 session-expired');
    await new Promise((r) => setTimeout(r, 2000));
    assert.strictEqual(rt._sse, null, '2 秒后仍不应起 SSE（不再重试）');
    rt.stop();
    ok('会话失效停重试：hello 前失败 + 401 → session-expired，不回落不重试');
  }
  // 7c. 瞬断仍回落：同一死端口，checkSession 通过 → 回落 SSE，不报会话失效。
  {
    const rt = new RealtimeClient(API_BASE, {
      token: 'live-token-stub',
      wsUrl: 'wss://127.0.0.1:9/ws',
      checkSession: async () => {}, // 会话正常
    });
    let expired = false;
    rt.on('session-expired', () => { expired = true; });
    const fellBack = new Promise((resolve, reject) => {
      rt.on('fallback', resolve);
      setTimeout(() => reject(new Error('fallback 未在 10s 内触发')), 10000);
    });
    rt.start();
    const fb = await withTimeout(fellBack, 12000, '瞬断回落');
    assert.strictEqual(fb.to, 'sse', '瞬断应回落 SSE');
    assert.notStrictEqual(rt._sse, null, '回落后 SSE 客户端应已创建');
    assert.strictEqual(expired, false, '会话正常时不应报 session-expired');
    rt.stop();
    ok('瞬断仍回落：checkSession 通过 → 回落 SSE，不报会话失效');
  }
  // 7d. 网络错不算会话失效：checkSession 抛无 status 的错 → 仍回落。
  {
    const rt = new RealtimeClient(API_BASE, {
      token: 'live-token-stub',
      wsUrl: 'wss://127.0.0.1:9/ws',
      checkSession: async () => { throw new Error('fetch failed'); }, // 网络错，无 status
    });
    let expired = false;
    rt.on('session-expired', () => { expired = true; });
    const fellBack = new Promise((resolve, reject) => {
      rt.on('fallback', resolve);
      setTimeout(() => reject(new Error('fallback 未在 10s 内触发')), 10000);
    });
    rt.start();
    await withTimeout(fellBack, 12000, '网络错回落');
    assert.strictEqual(expired, false, '网络错不应报 session-expired');
    rt.stop();
    ok('网络错不算会话失效：checkSession 抛无 status 的错 → 仍回落 SSE');
  }
  // 7e. hello 后会话失效：mock WS 先连上，随后 session-expired、WS 停掉。
  {
    const mock = await startMockWsServer();
    try {
      const states = [];
      const rt = new RealtimeClient(API_BASE, {
        token: 'dead-token-for-test',
        wsUrl: mock.url,
        checkSession: async () => {
          const e = new Error('HTTP 401');
          e.status = 401;
          throw e;
        },
      });
      rt.on('transport', (p) => states.push(p.state));
      const connected = new Promise((resolve, reject) => {
        rt.on('connect', resolve);
        setTimeout(() => reject(new Error('connect 未在 10s 内触发')), 10000);
      });
      const expired = new Promise((resolve, reject) => {
        rt.on('session-expired', resolve);
        setTimeout(() => reject(new Error('session-expired 未在 10s 内触发')), 10000);
      });
      rt.start();
      const c = await withTimeout(connected, 12000, 'mock WS connect');
      assert.strictEqual(c.transport, 'ws', 'mock WS 应先连上');
      const ev = await withTimeout(expired, 12000, 'hello 后 session-expired');
      assert.match(ev.reason, /会话已失效/);
      assert.strictEqual(rt.sessionExpired, true);
      assert.strictEqual(rt._ws, null, '会话失效后 WS 应停掉');
      assert.ok(states.includes('ws'), 'transport 应先报 ws');
      assert.ok(states.includes('session-expired'), 'transport 应随后报 session-expired');
      rt.stop();
      ok('hello 后会话失效：先连上 WS，随后 session-expired、WS 停掉');
    } finally {
      mock.close();
    }
  }
  // 7f. hello 后会话正常：mock WS 连上后保持 WS，不报 session-expired。
  {
    const mock = await startMockWsServer();
    try {
      let expired = false;
      const rt = new RealtimeClient(API_BASE, {
        token: 'live-token-stub',
        wsUrl: mock.url,
        checkSession: async () => {}, // 会话正常
      });
      rt.on('session-expired', () => { expired = true; });
      const connected = new Promise((resolve, reject) => {
        rt.on('connect', resolve);
        setTimeout(() => reject(new Error('connect 未在 10s 内触发')), 10000);
      });
      rt.start();
      const c = await withTimeout(connected, 12000, 'mock WS connect');
      assert.strictEqual(c.transport, 'ws');
      await new Promise((r) => setTimeout(r, 1500));
      assert.strictEqual(expired, false, '会话正常时不应报 session-expired');
      assert.strictEqual(rt.transport, 'ws', '会话正常时应保持 ws');
      assert.strictEqual(rt.sessionExpired, false);
      rt.stop();
      ok('hello 后会话正常：保持 WS，不报 session-expired');
    } finally {
      mock.close();
    }
  }
  // 7g. live /api/me：假 token 应回 401——这是 main.js 里 checkSession
  // 判定会话失效的依据（只读失败，不写线上状态）。
  {
    await assert.rejects(
      client.getMe('bogus-token-for-test'),
      (e) => e && e.status === 401,
      '假 token 调 /api/me 应回 401',
    );
    ok('live /api/me：假 token 回 401（会话失效检测的依据）');
  }
  // 7h. live transport 事件：正常 WS 应先报 connecting 再报 ws。
  {
    const states = [];
    const rt = new RealtimeClient(API_BASE, {});
    rt.on('transport', (p) => states.push(p.state));
    const c = await withTimeout(new Promise((resolve, reject) => {
      rt.on('connect', resolve);
      rt.on('disconnect', reject);
      rt.start();
    }), 20000, 'live WS transport');
    assert.strictEqual(c.transport, 'ws');
    assert.deepStrictEqual(states, ['connecting', 'ws'], 'transport 应先报 connecting 再报 ws');
    rt.stop();
    ok('live transport 事件：connecting → ws');
  }
  // 7i. live 回落 transport 事件：WS 挂掉后应报 connecting 再报 sse。
  {
    const states = [];
    const rt = new RealtimeClient(API_BASE, { wsUrl: 'wss://127.0.0.1:9/ws' });
    rt.on('transport', (p) => states.push(p.state));
    const helloP = new Promise((resolve) => {
      rt.on('event', (e) => { if (e.event === 'hello') resolve(e); });
    });
    const connected = new Promise((resolve, reject) => {
      rt.on('connect', resolve);
      rt.on('disconnect', () => { /* 回落前的 WS 断开，忽略 */ });
    });
    rt.start();
    const c = await withTimeout(connected, 20000, '回落 SSE connect');
    assert.strictEqual(c.transport, 'sse');
    const helloEv = await withTimeout(helloP, 20000, '回落 SSE hello');
    assert.ok(Number.isFinite(helloEv.seq), '回落 SSE hello 应带 seq');
    assert.ok(states.includes('connecting'), '应报 connecting');
    assert.ok(states.includes('sse'), '回落后应报 sse');
    rt.stop();
    ok(`live 回落 transport 事件：connecting → sse，hello seq=${helloEv.seq}`);
  }

  console.log('== 8. 服务端 URL fallback（v0.2.1+1）：主挂切备、恢复切回、不死循环 ==');
  // 可变的 mock health fetch：按基地址前缀决定 /api/health 通不通。
  // state = { primary, fallback }，测试里随时改，模拟主/备恢复与挂掉。
  function mockHealthFetch(state, primary, fallback) {
    return async (url) => {
      const u = String(url);
      if (u.startsWith(primary)) {
        if (state.primary) return { ok: true };
        throw new Error('fetch failed');
      }
      if (u.startsWith(fallback)) {
        if (state.fallback) return { ok: true };
        throw new Error('fetch failed');
      }
      throw new Error(`fetch failed: 未知地址 ${u}`);
    };
  }
  // 既能回 /api/health、又能接 WS（握手后发 hello）的 mock 论坛。
  function startMockForumServer() {
    return new Promise((resolve) => {
      const sockets = new Set();
      const server = http.createServer((req, res) => {
        if (req.url === '/api/health' || req.url.startsWith('/api/health?')) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ ok: true, posts: 1, accounts: 1, groups: 1 }));
          return;
        }
        res.writeHead(404); res.end();
      });
      server.on('upgrade', (req, socket) => {
        const key = req.headers['sec-websocket-key'] || '';
        const accept = crypto.createHash('sha1')
          .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', 'utf8').digest('base64');
        socket.write(
          'HTTP/1.1 101 Switching Protocols\r\n' +
          'Upgrade: websocket\r\n' +
          'Connection: Upgrade\r\n' +
          `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
        );
        sockets.add(socket);
        socket.on('close', () => sockets.delete(socket));
        socket.on('error', () => sockets.delete(socket));
        const payload = Buffer.from(JSON.stringify({
          type: 'hello',
          data: { seq: 7777, time: new Date().toISOString() },
        }), 'utf8');
        socket.write(Buffer.concat([Buffer.from([0x81, payload.length]), payload]));
      });
      server.listen(0, '127.0.0.1', () => resolve({
        base: `http://127.0.0.1:${server.address().port}`,
        close() {
          for (const s of sockets) { try { s.destroy(); } catch {} }
          server.close();
        },
      }));
    });
  }
  // 8a. probe：通的回 true，挂的回 false；默认主/备地址常量正确。
  {
    assert.strictEqual(PRIMARY_BASE, 'https://forum.coldhao.win', '主地址应为公网域名');
    assert.strictEqual(FALLBACK_BASE, 'http://100.79.244.3:8899', '备地址应为尾网 het');
    const state = { primary: true, fallback: false };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
    });
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:9', '初始应在主地址');
    assert.strictEqual(r.label(), '公网', '主地址标签应为公网');
    assert.strictEqual(await r.probe('http://127.0.0.1:9'), true, '通的基地址 probe 应为真');
    assert.strictEqual(await r.probe('http://127.0.0.1:10'), false, '挂的基地址 probe 应为假');
    r.stop();
    ok('probe：通 true / 挂 false，主备常量正确');
  }
  // 8b. reportFailure：主挂 + 备通 → 切到备地址，发 switch 事件。
  {
    const state = { primary: false, fallback: true };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
    });
    const switches = [];
    r.on('switch', (e) => switches.push(e));
    const to = await r.reportFailure();
    assert.strictEqual(to, 'http://127.0.0.1:10', '应切到备地址');
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:10', '切后 getBase 应为备地址');
    assert.strictEqual(r.isFallback, true, '切后 isFallback 应为真');
    assert.strictEqual(r.label(), '尾网', '备地址标签应为尾网');
    assert.strictEqual(switches.length, 1, '应只发一次 switch');
    assert.strictEqual(switches[0].from, 'http://127.0.0.1:9');
    assert.strictEqual(switches[0].to, 'http://127.0.0.1:10');
    r.stop();
    ok('reportFailure：主挂 + 备通 → 切备地址，发一次 switch');
  }
  // 8c. reportFailure：当前基地址还通 → 不切（只是某条路的问题）。
  {
    const state = { primary: true, fallback: true };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
    });
    let switched = false;
    r.on('switch', () => { switched = true; });
    assert.strictEqual(await r.reportFailure(), null, '当前还通时应返回 null');
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:9', '当前还通时不应切');
    assert.strictEqual(switched, false, '当前还通时不应发 switch');
    r.stop();
    ok('reportFailure：当前还通 → 不切');
  }
  // 8d. reportFailure：两边都挂 → 留在当前基地址，不死循环切来切去。
  {
    const state = { primary: false, fallback: false };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
    });
    let switches = 0;
    r.on('switch', () => { switches += 1; });
    assert.strictEqual(await r.reportFailure(), null, '两边都挂时应返回 null');
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:9', '两边都挂时应留在主地址');
    assert.strictEqual(await r.reportFailure(), null, '再报一次仍应返回 null');
    assert.strictEqual(switches, 0, '两边都挂时不应发 switch');
    r.stop();
    ok('reportFailure：两边都挂 → 留在主地址，不切、不循环');
  }
  // 8e. recheck：切到备地址后主地址恢复 → 60 秒探一次，通了切回。
  {
    const state = { primary: false, fallback: true };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
      recheckMs: 100,
    });
    const switches = [];
    r.on('switch', (e) => switches.push(e));
    await r.reportFailure();
    assert.strictEqual(r.isFallback, true, '先应切到备地址');
    state.primary = true; // 主地址恢复
    await new Promise((resolve, reject) => {
      const t0 = Date.now();
      const timer = setInterval(() => {
        if (!r.isFallback) { clearInterval(timer); resolve(); }
        else if (Date.now() - t0 > 5000) { clearInterval(timer); reject(new Error('5 秒内未切回主地址')); }
      }, 50);
    });
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:9', '主地址恢复后应切回');
    assert.strictEqual(switches.length, 2, '应共发两次 switch（去备、回主）');
    assert.strictEqual(switches[1].to, 'http://127.0.0.1:9', '第二次 switch 应指回主地址');
    r.stop();
    ok('recheck：备地址上主恢复 → 切回主地址');
  }
  // 8f. RealtimeClient 经备地址连上 WS：主地址是死端口、备地址是 mock
  // 论坛，WS 首次失败应先切基地址再重连 WS（不直接掉到 SSE），
  // 指示器 detail 应带“尾网”。
  {
    const mock = await startMockForumServer();
    try {
      const r = new ServerResolver({ primary: 'http://127.0.0.1:9', fallback: mock.base });
      const states = [];
      const rt = new RealtimeClient('http://127.0.0.1:9', { resolver: r });
      rt.on('transport', (p) => states.push(p));
      const c = await withTimeout(new Promise((resolve, reject) => {
        rt.on('connect', resolve);
        rt.on('disconnect', () => { /* 切地址前的 WS 断开，忽略 */ });
        rt.start();
      }), 15000, 'Realtime 经备地址连 WS');
      assert.strictEqual(c.transport, 'ws', '备地址通时应走 WS（不是 SSE）');
      assert.strictEqual(rt.transport, 'ws');
      assert.strictEqual(r.isFallback, true, '应已切到备地址');
      assert.strictEqual(r.getBase(), mock.base, 'getBase 应为 mock 论坛地址');
      assert.ok(c.seq === 7777, 'hello seq 应为 mock 的 7777');
      const wsState = states.find((p) => p.state === 'ws');
      assert.ok(wsState, 'transport 应报过 ws');
      assert.ok(String(wsState.detail || '').includes('尾网'), `ws 的 detail 应带“尾网”，实际 ${wsState.detail}`);
      rt.stop();
      r.stop();
      ok('RealtimeClient：主挂切备后经 WS 连上，指示器带“尾网”');
    } finally {
      mock.close();
    }
  }
  // 8g. ForumClient 经 resolver：主地址死端口、备地址 mock 论坛，
  // GET /api/health 应自动切备并重试一次成功；HTTP 错误不切地址。
  {
    const mock = await startMockForumServer();
    try {
      const r = new ServerResolver({ primary: 'http://127.0.0.1:9', fallback: mock.base });
      const client2 = new ForumClient('http://127.0.0.1:9', { resolver: r });
      const health = await withTimeout(client2.getHealth(), 15000, 'ForumClient 经备地址 getHealth');
      assert.strictEqual(health.ok, true, '备地址的 /api/health 应回 ok');
      assert.strictEqual(r.isFallback, true, 'GET 失败后应已切到备地址');
      // HTTP 错误不切：备地址回 404 时应抛 ForumApiError，且不切回主地址。
      const before = r.getBase();
      await assert.rejects(
        client2.request('/api/does-not-exist-xyz'),
        (e) => e && e.status === 404,
        '不存在的路径应回 404',
      );
      assert.strictEqual(r.getBase(), before, 'HTTP 404 不应切基地址');
      r.stop();
      ok('ForumClient：GET 失败切备重试成功，HTTP 错误不切地址');
    } finally {
      mock.close();
    }
  }

  // 8h. 并发 reportFailure 只切一次：两路同时报故障，共用同一轮探测，
  // 只发一条 switch（此前实测会切出第二条 from/to 都是备地址的记录）。
  {
    const state = { primary: false, fallback: true };
    let probes = 0;
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: async (url) => {
        probes += 1;
        // 探测故意慢一点（20ms），让两路调用撞进同一轮。
        await new Promise((resolve) => setTimeout(resolve, 20));
        return mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10')(url);
      },
    });
    const switches = [];
    r.on('switch', (e) => switches.push(e));
    const [a, b] = await Promise.all([r.reportFailure(), r.reportFailure()]);
    assert.strictEqual(a, 'http://127.0.0.1:10', '第一路应切到备地址');
    assert.strictEqual(b, 'http://127.0.0.1:10', '第二路应拿到同一结果');
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:10', '应停在备地址');
    assert.strictEqual(switches.length, 1, '并发两路只应发一条 switch');
    assert.ok(probes <= 2, `并发两路应共用一轮探测，实际探了 ${probes} 次`);
    r.stop();
    ok('并发 reportFailure：两路共用一轮探测，只发一条 switch');
  }

  // 8i. hello 超时直接 reject：TCP 接通但不握手（模拟 SYN 通了、应用层
  // 不回 hello），_connectOnce 应在 ~10 秒 reject，不无限挂住。
  // 这是 lenghao 实测的根因：公网出口抖时 WS 卡在 SYN-SENT，
  // 只靠 close() 等 onclose 会等不到，failover 永远不触发。
  {
    const net = require('node:net');
    const server = net.createServer((socket) => {
      // 接通后什么都不回：不握手、不发 hello、不关。
      socket.on('error', () => {});
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = server.address().port;
    try {
      const ws = new WsClient(`http://127.0.0.1:${port}`, {});
      const t0 = Date.now();
      await assert.rejects(
        ws._connectOnce(),
        // 两种都算过：TCP 已通时 close() 会同步触发 onclose（close 1006）；
        // TCP 卡在 SYN-SENT 时 onclose 不触发，靠定时器直接 reject（hello 超时）。
        // 关键是 ~10 秒一定落定，不无限挂住。
        (e) => /hello 超时|WS 连接失败/.test(e && e.message || ''),
        '_connectOnce 挂起时应在 ~10 秒 reject',
      );
      const ms = Date.now() - t0;
      assert.ok(ms >= 9000 && ms < 15000, `hello 超时应在 ~10 秒触发，实际 ${ms}ms`);
      ws.stop();
      ok(`hello 超时 reject：挂起的 _connectOnce 在 ${ms}ms reject，不无限挂住`);
    } finally {
      server.close();
    }
  }
  // 8j. resolver 已预先切到备地址：RealtimeClient 直接走备地址 WS，
  // 不用先失败再切（REST 先报故障的场景）。
  {
    const mock = await startMockForumServer();
    try {
      const r = new ServerResolver({ primary: 'http://127.0.0.1:9', fallback: mock.base });
      await r.reportFailure(); // 先切到备地址
      assert.strictEqual(r.isFallback, true, '应已预先切到备地址');
      const rt = new RealtimeClient('http://127.0.0.1:9', { resolver: r });
      const c = await withTimeout(new Promise((resolve, reject) => {
        rt.on('connect', resolve);
        rt.on('disconnect', reject);
        rt.start();
      }), 15000, '预先切备后 WS 直连');
      assert.strictEqual(c.transport, 'ws', '预先切备后应直接走 WS');
      assert.strictEqual(rt.transport, 'ws');
      rt.stop();
      r.stop();
      ok('预先切备：RealtimeClient 直接走备地址 WS');
    } finally {
      mock.close();
    }
  }

  console.log('== 9. 轻量 asar 自动更新（v0.2.1+1）：版本号比较、sha 校验、失败不换 ==');
  // mock fetch 的最小 Response：{status, ok, json(), arrayBuffer()}。
  function mockResponse({ status = 200, json = null, bytes = null }) {
    return {
      status,
      ok: status >= 200 && status < 300,
      json: async () => {
        if (json === null) throw new Error('no json');
        return json;
      },
      arrayBuffer: async () => {
        const buf = bytes ? Buffer.from(bytes) : Buffer.alloc(0);
        return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
      },
    };
  }
  // 9a. 版本号比较：纯函数，逐段数字比。
  {
    assert.strictEqual(compareVersions('0.2.1', '0.2.0'), 1, '0.2.1 > 0.2.0');
    assert.strictEqual(compareVersions('0.2.0', '0.2.1'), -1, '0.2.0 < 0.2.1');
    assert.strictEqual(compareVersions('0.2.1', '0.2.1'), 0, '相等应为 0');
    assert.strictEqual(compareVersions('1.0.0', '0.9.9'), 1, '1.0.0 > 0.9.9');
    assert.strictEqual(compareVersions('0.10.0', '0.9.0'), 1, '0.10.0 > 0.9.0（数字比，不是字典序）');
    assert.strictEqual(compareVersions('0.2', '0.2.0'), 0, '0.2 与 0.2.0 相等');
    assert.strictEqual(sha256Hex(Buffer.from('abc')), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad', 'sha256(abc) 应为已知值');
    ok('版本号比较：逐段数字比，0.10.0 > 0.9.0；sha256(abc) 已知值正确');
  }
  // 9b. checkForUpdates：404（未发布清单）→ 无更新不报错；版本旧 → 有更新；
  // 版本不新 → 已是最新；清单格式错 / 网络错 → check-failed。
  {
    const asarBytes = Buffer.from('fake-asar-bytes-v021');
    const asarSha = sha256Hex(asarBytes);
    const manifest = { version: '0.2.1', asarUrl: 'https://example.invalid/app.asar', sha256: asarSha, notes: 'test' };
    // 404：未发布清单
    {
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: '/tmp/nope',
        fetch: async () => mockResponse({ status: 404 }),
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, false, '404 应无更新');
      assert.strictEqual(info.reason, 'no-manifest', '404 原因应为 no-manifest');
    }
    // 有新版
    {
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: '/tmp/nope',
        fetch: async (url) => {
          assert.ok(String(url).endsWith('/desktop-update.json'), '清单应走服务端基地址');
          return mockResponse({ json: manifest });
        },
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, true, '0.2.1 > 0.2.0 应有更新');
      assert.strictEqual(info.version, '0.2.1');
      assert.strictEqual(info.sha256, asarSha);
    }
    // 已是最新
    {
      const up = new AppUpdater({
        currentVersion: '0.2.1', userDataDir: '/tmp/nope',
        fetch: async () => mockResponse({ json: manifest }),
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, false, '已是 0.2.1 应无更新');
      assert.strictEqual(info.reason, 'up-to-date');
    }
    // 清单格式错
    {
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: '/tmp/nope',
        fetch: async () => mockResponse({ json: { version: '0.2.1' } }),
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, false, '格式错的清单应无更新');
      assert.strictEqual(info.reason, 'check-failed');
    }
    // 网络错
    {
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: '/tmp/nope',
        fetch: async () => { throw new Error('fetch failed'); },
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, false, '网络错应无更新');
      assert.strictEqual(info.reason, 'check-failed');
    }
    ok('checkForUpdates：404 无更新、有新版、已是最新、格式错、网络错五态正确');
  }
  // 9c. downloadAndStage：sha 对上才暂存；sha 不符 / 下载失败不留文件。
  {
    const asarBytes = Buffer.from('fake-asar-bytes-v021');
    const asarSha = sha256Hex(asarBytes);
    const info = { version: '0.2.1', asarUrl: 'https://example.invalid/app.asar', sha256: asarSha };
    // 成功：暂存 asar + pending.json，内容与 sha 一致
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-ok-'));
      try {
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp,
          fetch: async () => mockResponse({ bytes: asarBytes }),
        });
        const r = await up.downloadAndStage(info);
        assert.strictEqual(r.ok, true, 'sha 对上应暂存成功');
        assert.strictEqual(r.bytes, asarBytes.length);
        assert.strictEqual(r.sha256, asarSha);
        assert.ok(fs.existsSync(up.pendingAsar), '暂存 asar 应存在');
        assert.ok(fs.existsSync(up.pendingManifest), 'pending.json 应存在');
        assert.strictEqual(sha256Hex(fs.readFileSync(up.pendingAsar)), asarSha, '暂存文件 sha 应与清单一致');
        const meta = JSON.parse(fs.readFileSync(up.pendingManifest, 'utf8'));
        assert.strictEqual(meta.version, '0.2.1');
        assert.strictEqual(meta.sha256, asarSha);
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    // sha 不符：不暂存
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-badsha-'));
      try {
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp,
          fetch: async () => mockResponse({ bytes: Buffer.from('tampered-bytes') }),
        });
        const r = await up.downloadAndStage(info);
        assert.strictEqual(r.ok, false, 'sha 不符应失败');
        assert.match(r.error, /sha256 不符/, '错误应说明 sha256 不符');
        assert.strictEqual(fs.existsSync(up.pendingAsar), false, 'sha 不符不应留暂存 asar');
        assert.strictEqual(fs.existsSync(up.pendingManifest), false, 'sha 不符不应留 pending.json');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    // 下载 HTTP 失败：不暂存
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-dl-'));
      try {
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp,
          fetch: async () => mockResponse({ status: 500 }),
        });
        const r = await up.downloadAndStage(info);
        assert.strictEqual(r.ok, false, '下载 500 应失败');
        assert.strictEqual(fs.existsSync(up.pendingAsar), false, '下载失败不应留暂存');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    ok('downloadAndStage：sha 对上暂存，sha 不符/下载失败不留文件');
  }
  // 9d. applyPendingUpdate：暂存验过才换，旧 asar 备份为 .bak，暂存清掉；
  // 暂存 sha 坏了不换、旧 asar 不动、坏暂存删掉；开发模式（目录）不换。
  {
    // 换成功
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-apply-'));
      try {
        const appDir = path.join(tmp, 'resources');
        fs.mkdirSync(appDir, { recursive: true });
        const asarPath = path.join(appDir, 'app.asar');
        fs.writeFileSync(asarPath, Buffer.from('old-asar-v020'));
        const newBytes = Buffer.from('new-asar-v021');
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp, appAsarPath: asarPath,
        });
        fs.mkdirSync(up.updatesDir, { recursive: true });
        fs.writeFileSync(up.pendingAsar, newBytes);
        fs.writeFileSync(up.pendingManifest, JSON.stringify({ version: '0.2.1', sha256: sha256Hex(newBytes) }));
        assert.strictEqual(up.applyPendingUpdate(), true, '暂存验过应换上');
        assert.strictEqual(fs.readFileSync(asarPath, 'utf8'), 'new-asar-v021', 'app.asar 应为新内容');
        assert.strictEqual(fs.readFileSync(asarPath + '.bak', 'utf8'), 'old-asar-v020', '旧 asar 应备份为 .bak');
        assert.strictEqual(fs.existsSync(up.pendingAsar), false, '换上后暂存 asar 应清掉');
        assert.strictEqual(fs.existsSync(up.pendingManifest), false, '换上后 pending.json 应清掉');
        assert.strictEqual(up.applyPendingUpdate(), false, '无暂存时再换应为 false');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    // 暂存 sha 坏了：不换，旧 asar 不动，坏暂存删掉
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-applybad-'));
      try {
        const appDir = path.join(tmp, 'resources');
        fs.mkdirSync(appDir, { recursive: true });
        const asarPath = path.join(appDir, 'app.asar');
        fs.writeFileSync(asarPath, Buffer.from('old-asar-v020'));
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp, appAsarPath: asarPath,
        });
        fs.mkdirSync(up.updatesDir, { recursive: true });
        fs.writeFileSync(up.pendingAsar, Buffer.from('tampered'));
        fs.writeFileSync(up.pendingManifest, JSON.stringify({ version: '0.2.1', sha256: sha256Hex(Buffer.from('something-else')) }));
        assert.strictEqual(up.applyPendingUpdate(), false, '暂存 sha 坏了不应换');
        assert.strictEqual(fs.readFileSync(asarPath, 'utf8'), 'old-asar-v020', '旧 asar 应原样不动');
        assert.strictEqual(fs.existsSync(asarPath + '.bak'), false, '没换就不应产生 .bak');
        assert.strictEqual(fs.existsSync(up.pendingAsar), false, '坏暂存应被删掉');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    // 开发模式（appAsarPath 是目录）：不换
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-dev-'));
      try {
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp, appAsarPath: tmp,
        });
        fs.mkdirSync(up.updatesDir, { recursive: true });
        fs.writeFileSync(up.pendingAsar, Buffer.from('x'));
        fs.writeFileSync(up.pendingManifest, JSON.stringify({ version: '0.2.1', sha256: sha256Hex(Buffer.from('x')) }));
        assert.strictEqual(up.applyPendingUpdate(), false, '开发模式（目录）不应换 asar');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    ok('applyPendingUpdate：换成功备 .bak，sha 坏不换、开发模式不换');
  }
  // 9e. 全流程（mock）：查清单 → 有新版 → 下载暂存 → 换上，旧版进 .bak。
  {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-flow-'));
    try {
      const appDir = path.join(tmp, 'resources');
      fs.mkdirSync(appDir, { recursive: true });
      const asarPath = path.join(appDir, 'app.asar');
      fs.writeFileSync(asarPath, Buffer.from('old-asar-v020'));
      const newBytes = Buffer.from('new-asar-v021-full-flow');
      const newSha = sha256Hex(newBytes);
      const manifest = { version: '0.2.1', asarUrl: 'https://example.invalid/app.asar', sha256: newSha };
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: tmp, appAsarPath: asarPath,
        getBase: () => 'https://forum.coldhao.win',
        fetch: async (url) => {
          if (String(url).endsWith('/desktop-update.json')) return mockResponse({ json: manifest });
          return mockResponse({ bytes: newBytes });
        },
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, true, '全流程：应发现新版');
      const staged = await up.downloadAndStage(info);
      assert.strictEqual(staged.ok, true, '全流程：应下载暂存成功');
      assert.strictEqual(up.applyPendingUpdate(), true, '全流程：应换上');
      assert.strictEqual(fs.readFileSync(asarPath, 'utf8'), 'new-asar-v021-full-flow', '全流程：app.asar 应为新内容');
      assert.strictEqual(fs.readFileSync(asarPath + '.bak', 'utf8'), 'old-asar-v020', '全流程：旧版应进 .bak');
    } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    ok('全流程：查清单 → 下载暂存 → 换上，旧版进 .bak');
  }
  // 9f. applyPendingUpdate 走 original-fs 操作 asar（Electron 回归）：
  // lenghao 实测：打补丁的 fs 把 app.asar 当归档，copy/rename 报
  // “ENOENT, not found in .../app.asar”，换 asar 失败。注入一个
  // 计数版 fsOrig，断言碰 asar 文件的操作都走了它。
  {
    const fs2 = require('fs');
    const calls = [];
    const countingFs = {
      existsSync: (p) => { calls.push(['existsSync', p]); return fs2.existsSync(p); },
      copyFileSync: (s, d) => { calls.push(['copyFileSync', s, d]); return fs2.copyFileSync(s, d); },
      renameSync: (s, d) => { calls.push(['renameSync', s, d]); return fs2.renameSync(s, d); },
      unlinkSync: (p) => { calls.push(['unlinkSync', p]); return fs2.unlinkSync(p); },
    };
    const dir = fs2.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-fsorig-'));
    const appAsar = path.join(dir, 'app.asar');
    fs2.writeFileSync(appAsar, 'old');
    const up = new AppUpdater({
      currentVersion: '0.2.1', userDataDir: dir, appAsarPath: appAsar,
      fsOrig: countingFs,
    });
    fs2.mkdirSync(up.updatesDir, { recursive: true });
    fs2.writeFileSync(up.pendingAsar, 'new');
    fs2.writeFileSync(up.pendingManifest, JSON.stringify({ version: '0.2.2', sha256: sha256Hex(Buffer.from('new')) }));
    const applied = up.applyPendingUpdate();
    assert.strictEqual(applied, true, '应换上');
    // 碰 asar 文件本身的三步（exists、备份 copy、rename）都走了注入的 fsOrig。
    // 任一路径参数以 .asar / .asar.bak 结尾即算碰 asar 文件。
    const asarOps = calls.filter((c) => c.slice(1).some((a) => String(a).endsWith('.asar') || String(a).endsWith('.asar.bak')));
    assert.ok(asarOps.some(([m]) => m === 'existsSync'), 'existsSync(app.asar) 应走 fsOrig');
    assert.ok(asarOps.some(([m, s, d]) => m === 'copyFileSync' && String(d).endsWith('.asar.bak')), '备份 copy 应走 fsOrig');
    assert.ok(asarOps.some(([m, s, d]) => m === 'renameSync' && String(d).endsWith('.asar')), 'rename 到 app.asar 应走 fsOrig');
    assert.strictEqual(fs2.readFileSync(appAsar, 'utf8'), 'new');
    ok('original-fs：换 asar 时碰 asar 文件的操作都走 fsOrig，不走打补丁的 fs');
  }

  console.log('== 10. 自适应翻译 + 未加入标记 + 板块常驻（v0.3.1）==');
  const tr = require('../renderer/translate.js');
  // 10a. 语言检测与散文抽取（纯函数，与网页版同口径）。
  {
    assert.strictEqual(tr.detectLang('你好世界'), 'zh', '纯中文应为 zh');
    assert.strictEqual(tr.detectLang('Hello world, this is English'), 'en', '纯英文应为 en');
    assert.strictEqual(tr.detectLang('こんにちは世界'), 'ja', '含假名应为 ja');
    assert.strictEqual(tr.detectLang('안녕하세요'), 'ko', '含韩文应为 ko');
    assert.strictEqual(tr.detectLang('12345 !!!'), 'unknown', '无字母应为 unknown');
    assert.strictEqual(tr.detectLang(''), 'unknown', '空串应为 unknown');
    assert.strictEqual(tr.proseOf('```js\nconst a = 1;\n```  hello'), 'hello', '代码块应被剥掉只剩散文');
    assert.strictEqual(tr.proseOf('run `npm test` now'), 'run now', '行内 code 应被剥掉');
    assert.strictEqual(tr.proseOf('```\nonly code\n```'), '', '纯代码块散文应为空');
    ok('语言检测 zh/en/ja/ko/unknown 与 proseOf 剥代码块');
  }
  // 10b. 占位生成：同语言/不明/过短不译，异语言才出 trbox。
  {
    tr._resetForTest();
    assert.strictEqual(tr.placeholder('你好世界'), '', '同语言（中文）不应出翻译框');
    assert.strictEqual(tr.placeholder('12345 !!!'), '', '源语言不明不应出翻译框');
    assert.strictEqual(tr.placeholder('a'), '', '散文过短不应出翻译框');
    const html = tr.placeholder('Hello world, this is an English message.');
    assert.ok(html.includes('class="trbox"'), '英文内容应对中文用户出翻译框');
    assert.ok(html.includes('data-trid="at'), '翻译框应带 trid');
    assert.ok(html.includes('翻译中'), '翻译框应带“翻译中”占位文案');
    ok('占位生成：异语言出 trbox，同语言/不明/过短不出');
  }
  // 10c. 队列：批量 10 条、缓存命中不再调接口、节流自动重试、same 删框。
  {
    // 最小假 DOM：只实现队列用到的接口。
    const makeBox = (id) => ({
      _html: '', _removed: false, _attrs: { 'data-trid': id },
      set innerHTML(v) { this._html = v; },
      get innerHTML() { return this._html; },
      getAttribute(k) { return this._attrs[k] || null; },
      removeAttribute(k) { delete this._attrs[k]; },
      remove() { this._removed = true; },
      closest() { return this; },
      querySelector() { return null; },
    });
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    // 12 条英文 → 两批（10+2），译文全部渲染，切换数据齐全。
    tr._resetForTest();
    const calls = [];
    tr.setBatchFn(async (items, target) => {
      calls.push({ n: items.length, target });
      return items.map((it) => ({ translated: `译：${it.text}`, same: false }));
    });
    const boxes = [];
    const root = { querySelectorAll: () => boxes.filter((b) => !b._removed && b.getAttribute('data-trid')) };
    for (let i = 0; i < 12; i++) {
      const html = tr.placeholder(`English message number ${i} for batching test.`);
      assert.ok(html.includes('data-trid'), `第 ${i} 条应出占位`);
      const id = html.match(/data-trid="(at\d+)"/)[1];
      boxes.push(makeBox(id));
    }
    tr.observeTranslations(root);
    await sleep(600); // 250ms flush + 批量往返
    assert.strictEqual(calls.length, 2, `12 条应分 2 批，实际 ${calls.length} 批`);
    assert.deepStrictEqual(calls.map((c) => c.n), [10, 2], '第一批 10 条、第二批 2 条');
    assert.strictEqual(calls[0].target, 'zh', '批量目标应为 zh');
    assert.ok(boxes.every((b) => b._html.includes('译：English message')), '12 条都应渲染出译文');
    assert.ok(boxes.every((b) => b._html.includes('data-tr-orig') && b._html.includes('data-tr-text')), '译文框应带原文/译文切换数据');
    assert.strictEqual(tr._cacheSize(), 12, '缓存应记 12 条');
    ok('队列批量：12 条分 10+2 两批，全部渲染，切换数据齐全');
    // 缓存命中：同一文本再入队，不再调接口。
    const cacheBox = makeBox(tr.placeholder('English message number 0 for batching test.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [cacheBox] });
    await sleep(500);
    assert.strictEqual(calls.length, 2, '缓存命中不应再调接口');
    assert.ok(cacheBox._html.includes('译：English message number 0'), '缓存命中应直接渲染译文');
    ok('缓存命中：同一文本再入队不再调接口，直接渲染');
    // 节流自动重试：第一次 rate_limited，50ms 后自动重试成功。
    tr._resetForTest();
    tr._setRetryDelayMs(50);
    let tries = 0;
    tr.setBatchFn(async (items) => {
      tries += 1;
      if (tries === 1) return items.map(() => ({ error: 'rate_limited' }));
      return items.map((it) => ({ translated: `译：${it.text}`, same: false }));
    });
    const rlBox = makeBox(tr.placeholder('English message for rate limit retry test.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [rlBox] });
    await sleep(900);
    assert.strictEqual(tries, 2, `节流应自动重试一次，实际调了 ${tries} 次`);
    assert.ok(rlBox._html.includes('译：English message for rate limit'), '重试成功后应渲染译文');
    ok('节流自动重试：rate_limited 后自动再试一次并渲染');
    tr._setRetryDelayMs(65000);
    // same：服务端判同语言（回 translated 原文 + same:true），静默删框只留原文。
    tr._resetForTest();
    tr.setBatchFn(async (items) => items.map((it) => ({ translated: it.text, same: true })));
    const sameBox = makeBox(tr.placeholder('English message that server says same.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [sameBox] });
    await sleep(500);
    assert.strictEqual(sameBox._removed, true, 'same 时翻译框应被删掉');
    ok('same：服务端判同语言时静默删框');
  }
  // 10d. ForumClient 翻译接线（mock 服务端）：请求体与鉴权头、批量回包解析。
  {
    const mock = await new Promise((resolve) => {
      const seen = [];
      const server = http.createServer((req, res) => {
        let body = '';
        req.on('data', (c) => { body += c; });
        req.on('end', () => {
          seen.push({ method: req.method, url: req.url, auth: req.headers.authorization, body });
          res.setHeader('Content-Type', 'application/json');
          if (req.url === '/api/translate') {
            res.end(JSON.stringify({ ok: true, translated: '译文', cached: false, same: false, src: 'en' }));
          } else if (req.url === '/api/translate/batch') {
            const d = JSON.parse(body);
            res.end(JSON.stringify({ ok: true, results: d.items.map((it) => ({ translated: `译：${it.text}` })) }));
          } else { res.writeHead(404); res.end('{}'); }
        });
      });
      server.listen(0, '127.0.0.1', () => resolve({ server, seen, base: `http://127.0.0.1:${server.address().port}` }));
    });
    try {
      const mc = new ForumClient(mock.base);
      const one = await mc.translate('tok-abc', 'Hello', 'zh');
      assert.strictEqual(one.translated, '译文', '单条翻译应回译文');
      assert.strictEqual(mock.seen[0].auth, 'Bearer tok-abc', '单条翻译应带 Bearer token');
      assert.deepStrictEqual(JSON.parse(mock.seen[0].body), { text: 'Hello', target: 'zh' }, '单条请求体应为 {text, target}');
      const batch = await mc.translateBatch('tok-abc', [{ text: 'a' }, { text: 'b' }], 'zh');
      assert.strictEqual(batch.results.length, 2, '批量应回 2 条结果');
      assert.strictEqual(batch.results[1].translated, '译：b', '批量第二条译文应带原文');
      assert.strictEqual(mock.seen[1].auth, 'Bearer tok-abc', '批量翻译应带 Bearer token');
      ok('ForumClient 翻译接线：单条/批量请求体与鉴权头正确');
    } finally { mock.server.close(); }
  }
  // 10e. 未加入判定 + 板块过滤（抽 renderer.js 实际发布的函数跑）。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const isGroupMember = new Function(`${extractFn('isGroupMember')}; return isGroupMember;`)();
    const g = { id: 'grp-test', members: [{ name: 'coldhao', type: 'human' }, { name: 'xilan', type: 'ai' }] };
    assert.strictEqual(isGroupMember(g, { username: 'coldhao' }), true, '成员应判为已加入');
    assert.strictEqual(isGroupMember(g, { username: 'other' }), false, '非成员应判为未加入');
    assert.strictEqual(isGroupMember(g, null), false, '未登录应判为未加入');
    assert.strictEqual(isGroupMember({ id: 'g2', members: [] }, { username: 'coldhao' }), false, '空成员表应判为未加入');
    assert.strictEqual(isGroupMember({ id: 'g3' }, { username: 'coldhao' }), false, '无 members 字段应判为未加入');
    assert.strictEqual(isGroupMember(g, { username: 'xilan' }), false, '同名 AI 不算人类成员');
    const filterPostsByBoard = new Function(`${extractFn('filterPostsByBoard')}; return filterPostsByBoard;`)();
    const posts = [{ board: 'general' }, { board: 'code' }, { board: 'general' }, {}];
    assert.strictEqual(filterPostsByBoard(posts, null).length, 4, '不过滤应回全部');
    assert.strictEqual(filterPostsByBoard(posts, 'general').length, 2, 'general 应滤出 2 篇');
    assert.strictEqual(filterPostsByBoard(posts, 'code').length, 1, 'code 应滤出 1 篇');
    assert.strictEqual(filterPostsByBoard(posts, 'qa').length, 0, '无帖板块应回空');
    assert.deepStrictEqual(filterPostsByBoard(null, 'general'), [], '帖子为空应回空数组');
    ok('isGroupMember 与 filterPostsByBoard 口径正确');
  }
  // 10f. messageHtml 接翻译：有 ChimeraTranslate 时英文消息带 trbox，没有时只留原文。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const escFn = new Function(`${extractFn('esc')}; return esc;`)();
    const avatarClassFn = new Function(`${extractFn('avatarClass')}; return avatarClass;`)();
    const avatarInitialFn = new Function(`${extractFn('avatarInitial')}; return avatarInitial;`)();
    const avatarHtmlFn = new Function('avatarClass', 'avatarInitial', 'esc', `${extractFn('avatarHtml')}; return avatarHtml;`)(avatarClassFn, avatarInitialFn, escFn);
    const fmtTimeFn = new Function(`${extractFn('fmtTime')}; return fmtTime;`)();
    const messageHtmlFn = new Function('esc', 'state', 'avatarHtml', 'fmtTime', 'ui', `${extractFn('messageHtml')}; return messageHtml;`)(escFn, { me: null }, avatarHtmlFn, fmtTimeFn, tr.ui);
    const msg = { id: 'm1', author: 'alice', ts: new Date().toISOString(), body: 'Hello everyone, this is an English message.' };
    const plain = messageHtmlFn(msg);
    assert.ok(!plain.includes('trbox'), '无翻译模块时消息不应带翻译框');
    assert.ok(plain.includes('Hello everyone'), '无翻译模块时原文应在');
    tr._resetForTest();
    globalThis.window = { ChimeraTranslate: tr };
    try {
      const withTr = messageHtmlFn(msg);
      assert.ok(withTr.includes('class="trbox"'), '有翻译模块时英文消息应带翻译框');
      assert.ok(withTr.includes('Hello everyone'), '原文仍应在气泡里');
      const zhMsg = { id: 'm2', author: 'bob', ts: new Date().toISOString(), body: '大家好，这是一条中文消息。' };
      assert.ok(!messageHtmlFn(zhMsg).includes('trbox'), '中文消息对中文用户不应带翻译框');
    } finally { delete globalThis.window; }
    ok('messageHtml 接翻译：英文带 trbox、中文不带、无模块只留原文');
  }
  // 10g. live 板块 / 社区 / 翻译（只读；上游翻译失败时记注不判败）。
  {
    const boards = await withTimeout(client.getBoards(), 15000, 'GET /api/boards');
    assert.strictEqual(boards.ok, true, 'GET /api/boards 应回 ok');
    assert.ok(Array.isArray(boards.boards) && boards.boards.length > 0, '板块列表应非空');
    const ids = boards.boards.map((b) => b.id);
    assert.ok(ids.includes('general') && ids.includes('code'), '板块应含 general 与 code');
    ok(`live GET /api/boards：${boards.boards.length} 个板块`);
    const comms = await withTimeout(client.getCommunities(), 15000, 'GET /api/communities');
    assert.strictEqual(comms.ok, true, 'GET /api/communities 应回 ok');
    const eterna = (comms.communities || []).find((c) => c.name === 'eternatide');
    assert.ok(eterna, '社区列表应有 eternatide');
    // v0.3.18：live 值 5→6（社区 2026-11-27 后新增 1 名成员；只读 live 断言，
    // 人数再变时按 live 值同步，不判产品错）。
    assert.strictEqual(eterna.memberCount, 6, 'eternatide 成员应为 6 人');
    ok(`live GET /api/communities：eternatide 成员 ${eterna.memberCount} 人、下挂 ${eterna.groupCount} 群`);
    // 账本未登录应 401（成员 only 的边界）。
    await assert.rejects(
      client.getCommunityLedger(null, eterna.id),
      (e) => e && e.status === 401,
      '未登录取账本应回 401',
    );
    ok('live GET /api/communities/:id/ledger：未登录回 401（成员 only）');
    // 团队讨论未登录应 401（成员 only 的边界；v0.3.4 新增端点只读探针，不写线上状态）。
    await assert.rejects(
      client.getCommunityPosts(null, eterna.id),
      (e) => e && e.status === 401,
      '未登录取团队讨论应回 401',
    );
    ok('live GET /api/communities/:id/posts：未登录回 401（成员 only）');
    // 翻译 live：M3 安全修复（2026-11-24）后须登录，匿名调用应回 401（不耗上游额度）；
    // 登录态翻译走同一路由（resolvePoster + Bearer 会话），由 mock 测试覆盖请求体与回包口径。
    await assert.rejects(
      client.translate(null, 'Hello, this is a test message for translation.', 'zh'),
      (e) => e && e.status === 401,
      '未登录翻译应回 401',
    );
    ok('live POST /api/translate：未登录回 401（M3 须登录）');
  }

  console.log('== 12. 界面自适应翻译 chrome i18n（v0.3.7）==');
  // 12a. 字典完备性：中英 key 齐全、值非空、%s 个数一致（uiFmt 顺序代入）。
  {
    const zhKeys = Object.keys(tr._uiStrings.zh).sort();
    const enKeys = Object.keys(tr._uiStrings.en).sort();
    assert.deepStrictEqual(enKeys, zhKeys, '中英文 UI 字典 key 应完全一致');
    for (const k of zhKeys) {
      assert.strictEqual(typeof tr._uiStrings.zh[k], 'string', `${k} 中文值应为字符串`);
      assert.ok(tr._uiStrings.zh[k].length > 0, `${k} 中文值不应为空串`);
      assert.strictEqual(typeof tr._uiStrings.en[k], 'string', `${k} 英文值应为字符串`);
      assert.ok(tr._uiStrings.en[k].length > 0, `${k} 英文值不应为空串`);
      const cz = (tr._uiStrings.zh[k].match(/%s/g) || []).length;
      const ce = (tr._uiStrings.en[k].match(/%s/g) || []).length;
      assert.strictEqual(ce, cz, `${k} 中英 %s 个数应一致（zh ${cz} / en ${ce}）`);
    }
    for (const k of ['community.switchTeam', 'node.countItems', 'node.countTimes', 'top.connStatus']) {
      assert.ok(tr._uiStrings.zh[k] && tr._uiStrings.en[k], `字典应含 ${k}`);
    }
    ok(`UI 字典完备：中英各 ${zhKeys.length} 个 key，值非空，%s 个数一致`);
  }
  // 12b. ui / uiFmt：当前语言默认中文、显式语言取对应文案、缺 key 回 key 本身。
  {
    assert.strictEqual(tr.ui('nav.home'), '首页', 'Node 下无 localStorage 默认中文');
    assert.strictEqual(tr.ui('nav.home', 'en'), 'Home', '显式 en 应取英文');
    assert.strictEqual(tr.ui('top.langZh'), '翻译：中文', '语言按钮中文文案');
    assert.strictEqual(tr.ui('top.themeDark', 'en'), '🌙 Dark', '主题按钮英文文案');
    assert.strictEqual(tr.ui('no.such.key'), 'no.such.key', '缺 key 应回 key 本身');
    assert.strictEqual(tr.uiFmt('search.postsSec', 5), '帖子（5）', 'uiFmt 应顺序代入 %s');
    assert.strictEqual(tr.uiFmt('profile.stats', 10, 20, 30, 40), '帖子 10 · 回复 20 · 消息 30 · 获赞 40', 'uiFmt 多参数顺序代入');
    assert.strictEqual(tr.uiFmt('node.countItems', 3), '3 条', 'node.countItems 中文');
    assert.strictEqual(tr.ui('node.countItems', 'en').replace('%s', '3'), '3 items', 'node.countItems 英文模板');
    assert.strictEqual(tr.uiFmt('community.meta', 'coldhao', 5, 2), '创建人 coldhao · 成员 5 人 · 下挂 2 个群', 'community.meta 三参数');
    ok('ui / uiFmt：默认中文、显式语言、缺 key 回 key、%s 顺序代入');
  }
  // 12c. applyUiLang 在假 document 上：文本/placeholder/title 双语切换 + html lang。
  {
    const makeEl = (key) => ({
      _text: '', _attrs: { 'data-i18n': key, 'data-i18n-ph': key, 'data-i18n-title': key },
      get textContent() { return this._text; },
      set textContent(v) { this._text = v; },
      getAttribute(k) { return this._attrs[k] || null; },
      setAttribute(k, v) { this._attrs[k] = v; },
    });
    const textEls = [makeEl('nav.home'), makeEl('side.conv')];
    const phEls = [makeEl('top.searchPh')];
    const titleEls = [makeEl('top.connStatus')];
    const doc = {
      querySelectorAll: (sel) => ({ '[data-i18n]': textEls, '[data-i18n-ph]': phEls, '[data-i18n-title]': titleEls }[sel] || []),
      documentElement: { _attrs: {}, setAttribute(k, v) { this._attrs[k] = v; } },
    };
    tr.applyUiLang(doc, 'en');
    assert.strictEqual(textEls[0].textContent, 'Home', '英文下 nav.home 应为 Home');
    assert.strictEqual(textEls[1].textContent, 'Chats', '英文下 side.conv 应为 Chats');
    assert.strictEqual(phEls[0]._attrs.placeholder, 'Search posts and messages…', '英文下搜索 placeholder 应切换');
    assert.strictEqual(titleEls[0]._attrs.title, 'Realtime connection status', '英文下连接状态 title 应切换');
    assert.strictEqual(doc.documentElement._attrs.lang, 'en', '英文下 html lang 应为 en');
    tr.applyUiLang(doc, 'zh');
    assert.strictEqual(textEls[0].textContent, '首页', '切回中文 nav.home 应为首页');
    assert.strictEqual(phEls[0]._attrs.placeholder, '搜索帖子与消息…', '切回中文 placeholder 应恢复');
    assert.strictEqual(doc.documentElement._attrs.lang, 'zh-CN', '中文下 html lang 应为 zh-CN');
    assert.doesNotThrow(() => tr.applyUiLang(null), 'doc 为 null 不应抛错');
    assert.doesNotThrow(() => tr.applyUiLang({}), '无 querySelectorAll 的 doc 不应抛错');
    ok('applyUiLang：假 document 上文本/placeholder/title 双语切换 + html lang');
  }
  // 12d. renderer.js 的 ui/uiFmt 助手：无模块时回 key 本身，有模块时透传。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const uiFn = new Function(`${extractFn('ui')}; return ui;`)();
    const uiFmtFn = new Function(`${extractFn('uiFmt')}; return uiFmt;`)();
    assert.strictEqual(uiFn('nav.home'), 'nav.home', '无 ChimeraTranslate 时 ui 应回 key 本身');
    assert.strictEqual(uiFmtFn('search.postsSec', 5), 'search.postsSec', '无模块时 uiFmt 应回 key 本身');
    globalThis.window = { ChimeraTranslate: tr };
    try {
      assert.strictEqual(uiFn('nav.home'), '首页', '有模块时 ui 应透传中文文案');
      assert.strictEqual(uiFmtFn('home.points', 209), '209 分', '有模块时 uiFmt 应透传并代入');
    } finally { delete globalThis.window; }
    ok('renderer.js ui/uiFmt：无模块回 key、有模块透传');
  }

  console.log('== 13. 顶栏溢出修复 + 全内容跟随语言（v0.3.8）==');
  // 13a. 顶栏 CSS：1900px 断点收紧（v0.3.16 由 1400 提至 1500，v0.3.18 提至 1900，覆盖 1501–1827）、导航可横向滚动兜底、1000px 规则保留。
  {
    const css = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'styles.css'), 'utf8');
    const m1400 = css.match(/@media \(max-width: 1900px\) \{([\s\S]*?)\n\}/);
    assert.ok(m1400, '应有 1900px 断点');
    assert.ok(m1400[1].includes('.brand .sub { display: none; }'), '1900px 以下应藏品牌副标题');
    assert.ok(m1400[1].includes('#globalSearchInput { width: 140px; }'), '1900px 以下搜索框应收窄到 140px');
    assert.ok(m1400[1].includes('.nav-btn { padding: 7px 6px; }'), '1900px 以下导航按钮应收紧内边距（v0.3.13：8px 时英文 1280 下节点被收进 More，6px 才放得下 7 项）');
    assert.ok(/\.topnav-links \{[^}]*overflow-x: auto/.test(css), '导航容器应可横向滚动兜底');
    assert.ok(css.includes('.topnav-links::-webkit-scrollbar { display: none; }'), '导航滚动条应隐藏');
    assert.ok(css.includes('@media (max-width: 1000px)'), '1000px 断点应保留');
    assert.ok(css.includes('#sseText { display: none; }'), '1000px 以下状态文字仍应隐藏');
    ok('顶栏 CSS：1900px 断点收紧 + 导航横向滚动兜底 + 1000px 规则保留');
  }
  // 13b–13e 共用：假 localStorage（Node 下没有），用完删掉。
  globalThis.localStorage = {
    _s: {},
    getItem(k) { return this._s[k] === undefined ? null : this._s[k]; },
    setItem(k, v) { this._s[k] = String(v); },
  };
  const sleep13 = (ms) => new Promise((r) => setTimeout(r, ms));
  const makeBodyEl = (text, cls) => ({
    _text: text,
    classList: { contains: (c) => c === cls },
    dataset: {},
    get textContent() { return this._text; },
    set textContent(v) { this._text = v; },
  });
  const makeBox13 = (id, bodyEl) => ({
    _html: '', _attrs: { 'data-trid': id },
    previousElementSibling: bodyEl || null,
    dataset: {},
    set innerHTML(v) { this._html = v; },
    get innerHTML() { return this._html; },
    getAttribute(k) { return this._attrs[k] || null; },
    removeAttribute(k) { delete this._attrs[k]; },
    querySelector() { return null; },
  });
  // 13b. 正文换显：英文模式下中文正文整段换成译文，框只剩“显示原文”口子；点两次来回切换。
  {
    tr._resetForTest();
    tr.setViewerLang('en');
    assert.strictEqual(tr.viewerLang(), 'en', '假 localStorage 下应切到英文');
    let calls = 0;
    tr.setBatchFn(async (items) => { calls++; return items.map((it) => ({ translated: 'EN:' + it.text })); });
    const orig = '你好世界这是一个中文帖子正文';
    const html = tr.placeholder(orig);
    assert.ok(html.includes('data-trid="at'), '中文正文对英文用户应出翻译框');
    const id = html.match(/data-trid="(at\d+)"/)[1];
    const bodyEl = makeBodyEl(orig, 'msg-body');
    const box = makeBox13(id, bodyEl);
    tr.observeTranslations({ querySelectorAll: () => [box] });
    tr.flushNow();
    await sleep13(100);
    assert.strictEqual(calls, 1, '应调一次批量接口');
    assert.strictEqual(bodyEl.textContent, 'EN:' + orig, '正文应整段换成译文');
    assert.strictEqual(box.dataset.trSwap, '1', '换显的框应打 trSwap 标记');
    assert.ok(box._html.includes('Show original'), '换显的框应留“显示原文”按钮');
    assert.ok(!box._html.includes('class="tr-text"'), '换显的框不应再有译文 span');
    const btn = {
      _text: 'Show original', dataset: {},
      get textContent() { return this._text; },
      set textContent(v) { this._text = v; },
      getAttribute(k) {
        if (k === 'data-tr-retry') return null;
        if (k === 'data-tr-orig') return encodeURIComponent(orig);
        if (k === 'data-tr-text') return encodeURIComponent('EN:' + orig);
        return null;
      },
      closest(sel) { return sel === '.tr-toggle' ? btn : (sel === '.trbox' ? box : null); },
    };
    tr._handleClick({ target: btn });
    assert.strictEqual(bodyEl.textContent, orig, '点“显示原文”正文应回原文');
    assert.strictEqual(btn.textContent, 'Show translation', '按钮应变“显示翻译”');
    tr._handleClick({ target: btn });
    assert.strictEqual(bodyEl.textContent, 'EN:' + orig, '再点正文应回译文');
    assert.strictEqual(btn.textContent, 'Show original', '按钮应变回“显示原文”');
    ok('正文换显：英文模式中文正文整段换译文，框只剩切换口子，点两次来回');
  }
  // 13c. 带代码的正文不整换：旧框下翻译，正文原样保留（含代码块）。
  {
    tr._resetForTest();
    tr.setViewerLang('en');
    tr.setBatchFn(async (items) => items.map((it) => ({ translated: 'EN:' + it.text })));
    const orig = '```js\nconst a = 1;\n``` 你好世界';
    const html = tr.placeholder(orig);
    assert.ok(html.includes('data-trid="at'), '带代码的中文散文也应出翻译框');
    const id = html.match(/data-trid="(at\d+)"/)[1];
    const bodyEl = makeBodyEl(orig, 'post-excerpt');
    const box = makeBox13(id, bodyEl);
    tr.observeTranslations({ querySelectorAll: () => [box] });
    tr.flushNow();
    await sleep13(100);
    assert.strictEqual(bodyEl.textContent, orig, '带代码的正文不应被整段换掉');
    assert.ok(box._html.includes('class="tr-text"'), '带代码的正文应走旧框下翻译');
    assert.ok(box._html.includes('EN:你好世界'), '旧框应渲染出译文');
    ok('带代码正文不整换：旧框下翻译，正文含代码原样保留');
  }
  // 13d. 缓存命中即时恢复：同一文本第二对正文+框，不再调接口，当场换显。
  {
    tr._resetForTest();
    tr.setViewerLang('en');
    let calls = 0;
    tr.setBatchFn(async (items) => { calls++; return items.map((it) => ({ translated: 'EN:' + it.text })); });
    const orig = '缓存测试正文内容';
    const id1 = tr.placeholder(orig).match(/data-trid="(at\d+)"/)[1];
    const body1 = makeBodyEl(orig, 'msg-body');
    const box1 = makeBox13(id1, body1);
    tr.observeTranslations({ querySelectorAll: () => [box1] });
    tr.flushNow();
    await sleep13(100);
    assert.strictEqual(calls, 1, '第一次应调接口');
    assert.strictEqual(body1.textContent, 'EN:' + orig, '第一次应换显');
    const id2 = tr.placeholder(orig).match(/data-trid="(at\d+)"/)[1];
    const body2 = makeBodyEl(orig, 'msg-body');
    const box2 = makeBox13(id2, body2);
    tr.observeTranslations({ querySelectorAll: () => [box2] });
    tr.flushNow();
    assert.strictEqual(calls, 1, '缓存命中不应再调接口');
    assert.strictEqual(body2.textContent, 'EN:' + orig, '缓存命中应立即换显');
    ok('缓存命中即时恢复：第二对正文不调接口，当场换显');
  }
  // 13e. 名字翻译：群名/版块名走同一批量队列；切回中文恢复原文；再切英文缓存命中不调接口；same 原样保留。
  {
    tr._resetForTest();
    tr.setViewerLang('en');
    let calls = 0;
    tr.setBatchFn(async (items) => { calls++; return items.map((it) => ({ translated: 'EN:' + it.text })); });
    const makeNameEl = (name) => ({
      _text: name, _attrs: { 'data-tr-name': name }, dataset: {},
      get textContent() { return this._text; },
      set textContent(v) { this._text = v; },
      getAttribute(k) { return this._attrs[k] || null; },
    });
    const g1 = makeNameEl('茶馆');
    const g2 = makeNameEl('技术探讨与安全防御');
    const doc = { querySelectorAll: (sel) => (sel === '[data-tr-name]' ? [g1, g2] : []) };
    tr.observeNameTranslations(doc);
    tr.flushNow();
    await sleep13(100);
    assert.strictEqual(calls, 1, '名字应一批调接口');
    assert.strictEqual(g1.textContent, 'EN:茶馆', '群名应换成译文');
    assert.strictEqual(g2.textContent, 'EN:技术探讨与安全防御', '版块名应换成译文');
    assert.strictEqual(g1.dataset.trTarget, 'en', '译过的名字应记目标语言');
    tr.setViewerLang('zh');
    tr.observeNameTranslations(doc);
    assert.strictEqual(g1.textContent, '茶馆', '切回中文群名应恢复原文');
    assert.strictEqual(g2.textContent, '技术探讨与安全防御', '切回中文版块名应恢复原文');
    assert.strictEqual(calls, 1, '恢复原文不应调接口');
    tr.setViewerLang('en');
    tr.observeNameTranslations(doc);
    tr.flushNow();
    assert.strictEqual(calls, 1, '再切英文缓存命中不应调接口');
    assert.strictEqual(g1.textContent, 'EN:茶馆', '再切英文缓存命中应立即恢复译文');
    // same：服务端判同语言，原样保留并记已完成；再跑一次名字 pass 应走缓存，不再调接口。
    tr._resetForTest();
    tr.setViewerLang('en');
    let sameCalls = 0;
    tr.setBatchFn(async (items) => { sameCalls++; return items.map((it) => ({ translated: it.text, same: true })); });
    const g3 = makeNameEl('茶馆');
    const doc3 = { querySelectorAll: (sel) => (sel === '[data-tr-name]' ? [g3] : []) };
    tr.observeNameTranslations(doc3);
    tr.flushNow();
    await sleep13(100);
    assert.strictEqual(sameCalls, 1, 'same 的名字第一次应调接口');
    assert.strictEqual(g3.textContent, '茶馆', 'same 的名字应原样保留');
    assert.strictEqual(g3.dataset.trTarget, 'en', 'same 的名字应记已完成');
    tr.observeNameTranslations(doc3);
    tr.flushNow();
    assert.strictEqual(sameCalls, 1, 'same 的名字再跑名字 pass 不应重复调接口');
    assert.strictEqual(g3.textContent, '茶馆', 'same 的名字再跑一次仍原样保留');
    ok('名字翻译：批量换显、切回恢复原文、缓存命中不调接口、same 原样保留');
    delete globalThis.localStorage;
    tr._resetForTest();
  }
  // 13f. renderer.js 接线：名字打标、搜索消息与公告正文可换显、切换处理器刷队列。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    assert.ok(rendererSrc.includes('<span data-tr-name="${esc(b.name)}">${esc(b.name)}</span>'), '版块列表应给真实版块名打标');
    assert.ok(rendererSrc.includes('<b data-tr-name="${esc(g.name)}">${esc(g.name)}</b>'), '会话列表与社区群组应给群名打标');
    assert.ok(rendererSrc.includes(`$('chatTitle').setAttribute('data-tr-name', name)`), '打开会话时聊天头群名应打标');
    assert.ok(rendererSrc.includes('<span data-tr-name="${esc(boardName)}">${esc(boardName)}</span>'), '帖子面板版块名应打标');
    assert.ok(rendererSrc.includes(`trPlaceholder((m.body || '').slice(0, 200))`), '搜索消息正文应有翻译占位');
    assert.ok(rendererSrc.includes('<div class="post-excerpt">${esc(a.body || \'\')}</div>'), '公告正文应有 post-excerpt 类以便换显');
    assert.ok(rendererSrc.includes('function refreshDataTranslations()'), '应有切换后刷数据翻译的助手');
    assert.ok(rendererSrc.includes('refreshDataTranslations();'), '语言切换处理器末尾应刷数据翻译');
    assert.ok(typeof tr.observeNameTranslations === 'function', 'translate.js 应导出 observeNameTranslations');
    assert.ok(typeof tr.flushNow === 'function', 'translate.js 应导出 flushNow');
    assert.ok(typeof tr._handleClick === 'function', 'translate.js 应导出 _handleClick 供测试');
    ok('renderer.js 接线：名字打标、搜索与公告正文可换显、切换后刷队列');
  }

  console.log('== 14. “全部板块”标签跟随语言（v0.3.9）==');
  // 14a. 字典：中英都有值（根因不在字典缺 key：字典早有 side.allBoards）。
  {
    assert.strictEqual(tr.ui('side.allBoards'), '全部板块', '字典中文应为“全部板块”');
    assert.strictEqual(tr.ui('side.allBoards', 'en'), 'All boards', '字典英文应为 All boards');
    ok('字典：side.allBoards 中英齐全');
  }
  // 14b. 接线：syncChrome 应重画板块列表。根因：板块列表只在 loadBoards/点版块时
  // 重画，语言切换的 syncChrome 只重画了会话列表与右侧栏；真实版块名靠
  // data-tr-name 名字队列翻，唯独“全部板块”这项 chrome 没人重画。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const start = rendererSrc.indexOf('function syncChrome()');
    assert.ok(start >= 0, 'renderer.js 应包含 syncChrome');
    const end = rendererSrc.indexOf('\n}\n', start);
    assert.ok(end > start, '应能取到 syncChrome 函数体');
    const syncChromeSrc = rendererSrc.slice(start, end);
    assert.ok(syncChromeSrc.includes('renderBoardList();'), 'syncChrome 应调用 renderBoardList 重画板块列表');
    assert.ok(syncChromeSrc.includes('renderConvList();'), 'syncChrome 应仍重画会话列表');
    ok('接线：syncChrome 重画板块列表');
  }
  // 14c. 功能：真实 renderBoardList 在假 DOM 上，中英各画一次，首项应跟语言走。
  {
    globalThis.localStorage = {
      _s: {},
      getItem(k) { return this._s[k] === undefined ? null : this._s[k]; },
      setItem(k, v) { this._s[k] = String(v); },
    };
    tr._resetForTest();
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const uiFn = new Function(`${extractFn('ui')}; return ui;`)();
    const escFn = new Function(`${extractFn('esc')}; return esc;`)();
    const renderBoardListFn = new Function(`${extractFn('renderBoardList')}; return renderBoardList;`)();
    const boardEl = {
      _html: '',
      set innerHTML(v) { this._html = v; },
      get innerHTML() { return this._html; },
      querySelectorAll() { return []; },
    };
    const fakeDoc = {
      getElementById: (id) => (id === 'boardList' ? boardEl : null),
      querySelectorAll: () => [],
    };
    globalThis.window = { ChimeraTranslate: tr };
    globalThis.document = fakeDoc;
    globalThis.$ = (id) => fakeDoc.getElementById(id);
    globalThis.state = { boards: [{ id: 'b1', name: '技术探讨' }, { id: 'b2', name: '经验分享' }], postsBoard: null };
    globalThis.ui = uiFn;
    globalThis.esc = escFn;
    try {
      tr.setViewerLang('zh');
      renderBoardListFn();
      assert.ok(boardEl.innerHTML.includes('全部板块'), '中文下首项应为“全部板块”');
      assert.ok(!boardEl.innerHTML.includes('All boards'), '中文下首项不应是英文');
      assert.ok(boardEl.innerHTML.includes('技术探讨'), '中文下真实版块名应原样在');
      tr.setViewerLang('en');
      renderBoardListFn();
      assert.ok(boardEl.innerHTML.includes('All boards'), '英文下首项应为 All boards');
      assert.ok(!boardEl.innerHTML.includes('全部板块'), '英文下首项不应残留中文');
      assert.ok(boardEl.innerHTML.includes('data-tr-name="技术探讨"'), '真实版块名仍应带 data-tr-name 走名字队列');
      ok('renderBoardList：中英各画一次，首项跟语言走，真实版块名仍走名字队列');
    } finally {
      delete globalThis.window;
      delete globalThis.document;
      delete globalThis.$;
      delete globalThis.state;
      delete globalThis.ui;
      delete globalThis.esc;
      delete globalThis.localStorage;
      tr._resetForTest();
    }
  }

  console.log('== 15. 节点链重基根因修复（v0.3.10）==');
  // 15a. 单实例锁（c）：main.js 应在 ready 前请求单实例锁；拿不到锁直接退出；
  // 第二个实例应聚焦已有窗口。锁落在 userData 目录，走查应用用独立
  // --user-data-dir 时各拿各的锁，不影响 CDP 走查。
  {
    const mainSrc = fs.readFileSync(path.join(__dirname, '..', 'main.js'), 'utf8');
    assert.ok(mainSrc.includes('app.requestSingleInstanceLock()'), 'main.js 应调用 requestSingleInstanceLock');
    const lockAt = mainSrc.indexOf('app.requestSingleInstanceLock()');
    const readyAt = mainSrc.indexOf('app.whenReady()');
    assert.ok(lockAt >= 0 && readyAt > lockAt, '单实例锁应在 app.whenReady() 之前请求');
    assert.ok(mainSrc.includes("app.on('second-instance'"), '应监听 second-instance 聚焦已有窗口');
    assert.ok(/if \(!gotSingleInstanceLock\) \{\s*app\.quit\(\);/.test(mainSrc), '拿不到锁时应直接退出');
    ok('单实例锁：ready 前请求、拿不到即退出、second-instance 聚焦已有窗口');
  }
  // 15b. 测试 profile token 隔离 guard（d）：token 文件必须在 nodeDir 内，
  // 否则构造即抛（大声失败）；放在 nodeDir 内应正常工作。
  {
    const tmp15 = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-guard-'));
    try {
      const liveToken = path.join(tmp15, 'live-forum-token');
      fs.writeFileSync(liveToken, 'live-token\n');
      assert.throws(
        () => new NodeManager({
          nodeDir: path.join(tmp15, 'testprofile', 'node'),
          apiBase: 'http://127.0.0.1:9',
          config: { enabled: false, port: 0, forumTokenFile: liveToken },
          onLog: () => {},
        }),
        (e) => e.code === 'ERR_NODE_TOKEN_OUTSIDE_DIR' && /ERR_NODE_TOKEN_OUTSIDE_DIR: .* 不在 .* 下/.test(e.message),
        '测试 profile 读 live token 文件时构造应直接抛错',
      );
      // token 在 nodeDir 内：应正常构造（且 _readForumToken 能读到）
      const nodeDir15 = path.join(tmp15, 'testprofile', 'node');
      fs.mkdirSync(nodeDir15, { recursive: true });
      const ownToken = path.join(nodeDir15, 'forum-token');
      fs.writeFileSync(ownToken, 'own-token\n');
      const nm15 = new NodeManager({
        nodeDir: nodeDir15,
        apiBase: 'http://127.0.0.1:9',
        config: { enabled: false, port: 0, forumTokenFile: ownToken },
        onLog: () => {},
      });
      assert.strictEqual(nm15._readForumToken(), 'own-token', '自有 token 文件应能读到');
      assert.strictEqual(nm15.status().hasForumIdentity, true, '自有 token 应认领身份');
      ok('token 隔离 guard：跨目录 token 构造即抛，自有 token 正常工作');
    } finally {
      fs.rmSync(tmp15, { recursive: true, force: true });
    }
  }
  // 15c. 主进程 token 路径派生（d）：initNode 应按当前 userData 派生 token
  // 路径，绝不读 state.json 里存的老路径（R3：验收 profile 抄了 live
  // state.json，空链 + live token 发 seq-1 创世清链）。
  {
    const mainSrc = fs.readFileSync(path.join(__dirname, '..', 'main.js'), 'utf8');
    const initAt = mainSrc.indexOf('async function initNode()');
    assert.ok(initAt >= 0, 'main.js 应包含 initNode');
    const initEnd = mainSrc.indexOf('\n}\n', initAt);
    const initSrc = mainSrc.slice(initAt, initEnd);
    assert.ok(initSrc.includes("path.join(userDataFile('node'), 'forum-token')"), 'initNode 应按 userData 派生 token 路径');
    assert.ok(!initSrc.includes('store.state.node.forumTokenFile'), 'initNode 不应再读 state.json 里存的 token 路径');
    ok('主进程：token 路径按 userData 派生，不读存量路径');
  }

  console.log('== 16. 重绑发新 keypair（v0.3.11）==');
  // 16a. 轮换三件套：归档 0600 + 新 keypair 指纹变了 + 恢复回旧 key。
  {
    const tmp16 = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-wsrotate-'));
    try {
      const first = wb.ensureKeyPair({ dir: tmp16 });
      assert.strictEqual(wb.keyPairExists({ dir: tmp16 }), true, '生成后 keyPairExists 应为真');
      assert.strictEqual(wb.shouldRotateKeyPair({ hasPriorRows: true, hasKeyPair: true }), true, '有行且有 key 应轮换');
      assert.strictEqual(wb.shouldRotateKeyPair({ hasPriorRows: true, hasKeyPair: false }), false, '有行无 key 不轮换（ensure 会新建）');
      assert.strictEqual(wb.shouldRotateKeyPair({ hasPriorRows: false, hasKeyPair: true }), false, '无行有 key 不轮换（复用，避免 DO 留孤 key）');
      assert.strictEqual(wb.shouldRotateKeyPair({ hasPriorRows: false, hasKeyPair: false }), false, '无行无 key 不轮换');
      const backup = wb.archiveKeyPair({ dir: tmp16 });
      assert.strictEqual(backup.archived, true, '有 key 时归档应成功');
      assert.strictEqual(backup.oldKeyId, first.keyId, '归档的 oldKeyId 应为原 keyId');
      assert.strictEqual(fs.statSync(backup.backupPrivate).mode & 0o777, 0o600, '归档私钥应为 0600');
      assert.ok(fs.existsSync(backup.backupPublic), '归档公钥应存在');
      const fresh = wb.generateFreshKeyPair({ dir: tmp16 });
      assert.strictEqual(fresh.created, true, '新 keypair 应标记 created');
      assert.notStrictEqual(fresh.keyId, first.keyId, '新 keyId 必须与旧的不同');
      assert.notStrictEqual(fresh.fingerprint, first.fingerprint, '新指纹必须与旧的不同');
      assert.notStrictEqual(fresh.nodeId, first.nodeId, '新 nodeId 必须与旧的不同');
      const reread = wb.ensureKeyPair({ dir: tmp16 });
      assert.strictEqual(reread.keyId, fresh.keyId, '轮换后 ensure 应读回新 keyId');
      assert.strictEqual(reread.publicKey, fresh.publicKey, '轮换后公钥文件应与新 key 一致');
      // 恢复：旧 key 回来，归档文件还在（恢复不删归档）。
      const restored = wb.restoreKeyPair(backup, { dir: tmp16 });
      assert.strictEqual(restored.keyId, first.keyId, '恢复后 keyId 应回旧值');
      assert.ok(fs.existsSync(backup.backupPrivate), '恢复后归档私钥应还在');
      // 无 key 时归档回 false；两次生成 key 不同。
      const tmp16b = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-wsrotate-b-'));
      try {
        assert.strictEqual(wb.keyPairExists({ dir: tmp16b }), false, '空目录 keyPairExists 应为假');
        assert.strictEqual(wb.archiveKeyPair({ dir: tmp16b }).archived, false, '无 key 时归档应回 false');
        const g1 = wb.generateFreshKeyPair({ dir: tmp16b });
        const g2 = wb.generateFreshKeyPair({ dir: tmp16b });
        assert.notStrictEqual(g2.keyId, g1.keyId, '两次生成 keyId 应不同');
      } finally {
        fs.rmSync(tmp16b, { recursive: true, force: true });
      }
      ok('轮换：归档 0600 + 新 key 指纹/nodeId 全变 + 恢复回旧 key + 判定表');
    } finally {
      fs.rmSync(tmp16, { recursive: true, force: true });
    }
  }
  // 16b. doRegistry：fake 脚本走通注册/吊销/401 回退/缺脚本四路。
  {
    const dr = require('../lib/doRegistry');
    const tmp16c = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-doreg-'));
    try {
      const fakeRegister = path.join(tmp16c, 'fake-register.py');
      fs.writeFileSync(fakeRegister, [
        '#!/usr/bin/env python3',
        'import sys',
        'node_id = sys.argv[1]',
        'if node_id == "ws-fail409":',
        '    print(\'409 {"ok": false, "error": "conflict"}\')',
        'elif node_id == "ws-fail500":',
        '    print(\'500 {"ok": false, "error": "boom"}\')',
        'else:',
        '    print(\'200 {"ok": true, "key_id": "ed25519-x", "node_id": "\' + node_id + \'", "already_registered": false}\')',
        '',
      ].join('\n'));
      const fakeRevoke = path.join(tmp16c, 'fake-revoke.py');
      fs.writeFileSync(fakeRevoke, [
        '#!/usr/bin/env python3',
        'import sys',
        'node_id = sys.argv[1]',
        'with open(sys.argv[0].replace("fake-revoke.py", "calls.log"), "a") as f:',
        '    f.write(" ".join(sys.argv[1:]) + "\\n")',
        'if node_id == "ws-gone":',
        '    print(\'404 {"ok": false, "code": "NOT_FOUND"}\')',
        'elif node_id == "ws-needs-setup" and "--via" not in sys.argv:',
        '    print(\'401 {"ok": false, "error": "unauthorized"}\')',
        'else:',
        '    print(\'200 {"ok": true, "revoked_at": "2026-11-25T00:00:00Z"}\')',
        '',
      ].join('\n'));
      const reg = await dr.registerWorkspaceKey(
        { nodeId: 'ws-abc', keyId: 'ed25519-x', publicKey: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' },
        { scriptPath: fakeRegister });
      assert.strictEqual(reg.ok, true, 'fake 注册 200 应回 ok');
      assert.strictEqual(reg.node_id, 'ws-abc', 'fake 注册应回原 node_id');
      await assert.rejects(
        dr.registerWorkspaceKey({ nodeId: 'ws-fail409', keyId: 'ed25519-x', publicKey: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' }, { scriptPath: fakeRegister }),
        /DO 注册失败：HTTP 409/,
        'fake 注册 409 应抛错',
      );
      await assert.rejects(
        dr.registerWorkspaceKey({ nodeId: 'ws-abc', keyId: 'ed25519-x', publicKey: 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=' }, { scriptPath: path.join(tmp16c, 'no-such-script.py') }),
        /不在本机/,
        '脚本不存在应报不在本机',
      );
      const rev = await dr.revokeWorkspaceKey('ws-abc', { scriptPath: fakeRevoke });
      assert.strictEqual(rev.revoked, true, 'fake 吊销 200 应回 revoked');
      const gone = await dr.revokeWorkspaceKey('ws-gone', { scriptPath: fakeRevoke });
      assert.strictEqual(gone.alreadyRevoked, true, 'fake 吊销 404 应记 alreadyRevoked（no-op）');
      const callsLog = path.join(tmp16c, 'calls.log');
      if (fs.existsSync(callsLog)) fs.unlinkSync(callsLog);
      const setup = await dr.revokeWorkspaceKey('ws-needs-setup', { scriptPath: fakeRevoke });
      assert.strictEqual(setup.revoked, true, 'central 401 回退 setup 后应吊销成功');
      const calls = fs.readFileSync(callsLog, 'utf8').trim().split('\n');
      assert.strictEqual(calls.length, 2, '401 回退应正好调两次');
      assert.ok(calls[1].includes('--via setup'), '第二次调用应带 --via setup');
      ok('doRegistry：注册 200/409/缺脚本 + 吊销 200/404 + 401 回退 setup');
    } finally {
      fs.rmSync(tmp16c, { recursive: true, force: true });
    }
  }
  // 16c. 主进程接线（c）：bind handler 先查行、已有活跃绑定拒绝、轮换
  // 在先、DO 注册在论坛注册之前、失败恢复旧 key。
  {
    const mainSrc = fs.readFileSync(path.join(__dirname, '..', 'main.js'), 'utf8');
    const hAt = mainSrc.indexOf("apiHandle('api:workspace-bind'");
    assert.ok(hAt >= 0, 'main.js 应包含 api:workspace-bind');
    const hEnd = mainSrc.indexOf("apiHandle('api:workspace-bindings'", hAt);
    const hSrc = mainSrc.slice(hAt, hEnd);
    assert.ok(mainSrc.includes("require('./lib/doRegistry')"), 'main.js 应引入 lib/doRegistry');
    assert.ok(hSrc.includes('myWorkspaceBindings(token)'), 'bind 前应先查本人绑定行');
    assert.ok(hSrc.indexOf('myWorkspaceBindings(token)') < hSrc.indexOf('registerWorkspaceBinding'), '查行应在论坛注册之前');
    assert.ok(hSrc.includes("codedError('ERR_BIND_ALREADY_BOUND')"), '已有活跃绑定时应拒绝重绑');
    assert.ok(hSrc.includes('shouldRotateKeyPair'), '应按轮换规则判定是否发新 keypair');
    assert.ok(hSrc.includes('archiveKeyPair()'), '轮换时应先归档旧 keypair');
    assert.ok(hSrc.includes('generateFreshKeyPair()'), '轮换时应生成新 keypair');
    assert.ok(hSrc.includes('registerWorkspaceKey'), '应调 DO 注册新公钥');
    assert.ok(hSrc.indexOf('registerWorkspaceKey') < hSrc.indexOf('registerWorkspaceBinding'), 'DO 注册应在论坛注册之前');
    assert.ok(hSrc.includes('revokeWorkspaceKey'), '论坛注册失败时应吊销刚注册的 DO 新 key');
    assert.ok(hSrc.includes('restoreKeyPair(rotated)'), '失败时应从归档恢复旧 keypair');
    ok('主进程接线：先查行拒重复绑定、轮换在先、DO 注册先于论坛注册、失败恢复旧 key');
  }

  console.log('== 17. L2 链头探针（v0.3.12：默认 het 副本） ==');
  // Mock L2 RPC：只回 eth_blockNumber 的 result（可中途改值，供回退测试）。
  function startMockL2Rpc(resultHex) {
    let result = resultHex;
    const server = http.createServer((req, res) => {
      let body = '';
      req.on('data', (c) => { body += c; });
      req.on('end', () => {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ jsonrpc: '2.0', id: 1, result }));
      });
    });
    return new Promise((resolve) => {
      server.listen(0, '127.0.0.1', () => resolve({
        server,
        url: `http://127.0.0.1:${server.address().port}`,
        setResult: (r) => { result = r; },
      }));
    });
  }
  function restoreL2Env(prev) {
    if (prev === undefined) delete process.env.CHIMERA_L2_RPC;
    else process.env.CHIMERA_L2_RPC = prev;
  }
  // 17a. 地址解析：未配走默认 het 副本，配了优先用环境变量，配空回落默认。
  {
    const prev = process.env.CHIMERA_L2_RPC;
    try {
      delete process.env.CHIMERA_L2_RPC;
      assert.strictEqual(resolveL2Rpc(), DEFAULT_L2_RPC, '未配时应走默认 het 副本');
      assert.strictEqual(DEFAULT_L2_RPC, 'http://100.79.244.3:9545', '默认地址应为 het 尾网副本 9545');
      process.env.CHIMERA_L2_RPC = 'http://127.0.0.1:9999';
      assert.strictEqual(resolveL2Rpc(), 'http://127.0.0.1:9999', '配了环境变量应优先');
      process.env.CHIMERA_L2_RPC = '   ';
      assert.strictEqual(resolveL2Rpc(), DEFAULT_L2_RPC, '配空应回落默认');
      ok('L2 地址解析：未配走默认 het 副本、配了优先、配空回落');
    } finally { restoreL2Env(prev); }
  }
  // 17b. 探针打通：本地 stub 回块高时 l2Head 应记下高度并追加 l2-head 记录。
  {
    const prev = process.env.CHIMERA_L2_RPC;
    const seed17b = await startMockSeed();
    const rpc = await startMockL2Rpc('0x43113');
    const tmp17b = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-l2-test-'));
    try {
      process.env.CHIMERA_L2_RPC = rpc.url;
      const nm = new NodeManager({
        nodeDir: path.join(tmp17b, 'node'),
        apiBase: `http://127.0.0.1:${seed17b.address().port}`,
        config: { enabled: false, port: 0, seeds: [] },
        onLog: () => {},
      });
      await nm.start();
      await nm.syncOnce();
      assert.ok(nm.l2Head, '探通后 l2Head 应有值');
      assert.strictEqual(nm.l2Head.height, 0x43113, '块高应为 stub 回的 0x43113（274707）');
      const lines = fs.readFileSync(path.join(tmp17b, 'node', 'replica', 'headers.jsonl'), 'utf8')
        .trim().split('\n').map((l) => JSON.parse(l));
      assert.ok(lines.some((h) => h.kind === 'l2-head' && h.height === 0x43113), '应追加一条 l2-head 记录');
      await nm.stop();
      ok('L2 探针打通：块高 274707 已记，l2-head 已追加');
    } finally {
      restoreL2Env(prev);
      rpc.server.close();
      seed17b.close();
      fs.rmSync(tmp17b, { recursive: true, force: true });
    }
  }
  // 17c. RPC 回了非法块高：l2Head 保持 null，不抛错，同步仍成功。
  {
    const prev = process.env.CHIMERA_L2_RPC;
    const seed17c = await startMockSeed();
    const rpc = await startMockL2Rpc('not-a-number');
    const tmp17c = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-l2-test-'));
    try {
      process.env.CHIMERA_L2_RPC = rpc.url;
      const nm = new NodeManager({
        nodeDir: path.join(tmp17c, 'node'),
        apiBase: `http://127.0.0.1:${seed17c.address().port}`,
        config: { enabled: false, port: 0, seeds: [] },
        onLog: () => {},
      });
      await nm.start();
      await nm.syncOnce();
      assert.strictEqual(nm.l2Head, null, '非法块高时 l2Head 应保持 null');
      assert.strictEqual(nm.lastSyncOk, true, '同步本身仍应成功');
      await nm.stop();
      ok('L2 探针容错：非法块高不抛错、l2Head 为 null、同步仍成功');
    } finally {
      restoreL2Env(prev);
      rpc.server.close();
      seed17c.close();
      fs.rmSync(tmp17c, { recursive: true, force: true });
    }
  }
  // 17d. RPC 不可达：l2Head 保持 null，不抛错，同步仍成功。
  {
    const prev = process.env.CHIMERA_L2_RPC;
    const seed17d = await startMockSeed();
    const tmp17d = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-l2-test-'));
    try {
      // 127.0.0.1:1 恒拒连，无需等 8 秒超时。
      process.env.CHIMERA_L2_RPC = 'http://127.0.0.1:1';
      const nm = new NodeManager({
        nodeDir: path.join(tmp17d, 'node'),
        apiBase: `http://127.0.0.1:${seed17d.address().port}`,
        config: { enabled: false, port: 0, seeds: [] },
        onLog: () => {},
      });
      await nm.start();
      await nm.syncOnce();
      assert.strictEqual(nm.l2Head, null, '不可达时 l2Head 应保持 null');
      assert.strictEqual(nm.lastSyncOk, true, '同步本身仍应成功');
      await nm.stop();
      ok('L2 探针容错：RPC 不可达不抛错、l2Head 为 null、同步仍成功');
    } finally {
      restoreL2Env(prev);
      seed17d.close();
      fs.rmSync(tmp17d, { recursive: true, force: true });
    }
  }
  // 17e. 块高回退：告警并保留旧头，不覆盖、不追加。
  {
    const prev = process.env.CHIMERA_L2_RPC;
    const rpc = await startMockL2Rpc('0x100');
    const tmp17e = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-l2-test-'));
    try {
      process.env.CHIMERA_L2_RPC = rpc.url;
      const logs = [];
      const nodeDir17e = path.join(tmp17e, 'node');
      fs.mkdirSync(path.join(nodeDir17e, 'replica'), { recursive: true });
      const nm = new NodeManager({
        nodeDir: nodeDir17e,
        apiBase: 'http://127.0.0.1:1', // 不走同步，只直接调 _syncL2Head
        config: { enabled: false, port: 0, seeds: [] },
        onLog: (l) => logs.push(l),
      });
      await nm._syncL2Head();
      assert.ok(nm.l2Head && nm.l2Head.height === 0x100, '首次应记下高度 256');
      rpc.setResult('0x50');
      await nm._syncL2Head();
      assert.strictEqual(nm.l2Head.height, 0x100, '回退时应保留旧头 256');
      assert.ok(logs.some((l) => /回退/.test(l)), '回退时应记告警日志');
      const l2lines = fs.readFileSync(path.join(nodeDir17e, 'replica', 'headers.jsonl'), 'utf8')
        .trim().split('\n').map((l) => JSON.parse(l)).filter((h) => h.kind === 'l2-head');
      assert.strictEqual(l2lines.length, 1, '回退时不应追加第二条 l2-head');
      ok('L2 高度回退：告警并保留旧头，不追加');
    } finally {
      restoreL2Env(prev);
      rpc.server.close();
      fs.rmSync(tmp17e, { recursive: true, force: true });
    }
  }

  console.log('== 18. v0.3.13 小改进：更多导航 / 401 / 首页统计 / 翻译分流 / i18n / LRU / 防抖 / 更新提示 ==');
  // §13/§14 末尾删掉了假 localStorage；本节多处要切语言，先补回来。
  if (typeof globalThis.localStorage === 'undefined') {
    globalThis.localStorage = {
      _s: {},
      getItem(k) { return this._s[k] === undefined ? null : this._s[k]; },
      setItem(k, v) { this._s[k] = String(v); },
    };
  }
  const rendererSrc18 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
  const mainSrc18 = fs.readFileSync(path.join(__dirname, '..', 'main.js'), 'utf8');
  const preloadSrc18 = fs.readFileSync(path.join(__dirname, '..', 'preload.js'), 'utf8');
  const html18 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'index.html'), 'utf8');
  const css18 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'styles.css'), 'utf8');
  const extractFn18 = (name) => {
    const m = rendererSrc18.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
    assert.ok(m, `renderer.js 应包含 ${name}`);
    return m[0];
  };
  const sleep18 = (ms) => new Promise((r) => setTimeout(r, ms));
  const makeBox18 = (id) => ({
    _html: '', _removed: false, _attrs: { 'data-trid': id },
    set innerHTML(v) { this._html = v; },
    get innerHTML() { return this._html; },
    getAttribute(k) { return this._attrs[k] || null; },
    removeAttribute(k) { delete this._attrs[k]; },
    remove() { this._removed = true; },
    closest() { return this; },
    querySelector() { return null; },
  });
  // 18a. greedyNavFit：纯函数，溢出恒为后缀，More 按钮宽度计入。
  {
    const greedyNavFit = new Function(`${extractFn18('greedyNavFit')}; return greedyNavFit;`)();
    assert.deepStrictEqual(greedyNavFit([60, 60, 60, 60, 60, 60, 60], 500, 70), { visible: [0, 1, 2, 3, 4, 5, 6], overflow: [] }, '全放得下时不应溢出');
    assert.deepStrictEqual(greedyNavFit([60, 60, 60, 60, 60, 60, 60], 400, 70), { visible: [0, 1, 2, 3, 4], overflow: [5, 6] }, '400px 应溢出最后两项（300+70≤400<360+70）');
    // 审计实测：1024px 英文 navScrollW 458 vs clientW 197。
    assert.deepStrictEqual(greedyNavFit([80, 70, 60, 70, 60, 57, 61], 197, 70), { visible: [0], overflow: [1, 2, 3, 4, 5, 6] }, '1024px 英文应只剩第一项，其余进“更多”');
    assert.deepStrictEqual(greedyNavFit([300, 300], 100, 70), { visible: [], overflow: [0, 1] }, '极窄时应全部溢出');
    assert.deepStrictEqual(greedyNavFit([100, 100, 100], 300, 70), { visible: [0, 1, 2], overflow: [] }, '恰好放下时不收 More');
    assert.deepStrictEqual(greedyNavFit([100, 100, 100], 299, 70), { visible: [0, 1], overflow: [2] }, '差 1px 时收最后一项');
    ok('greedyNavFit：全放/溢出一项/审计窄屏/全溢出/恰好放下六态正确');
  }
  // 18b. 更多导航接线：HTML ids、CSS 规则、调用点。
  {
    assert.ok(html18.includes('id="navMoreBtn"'), '应有 navMoreBtn');
    assert.ok(html18.includes('id="navMoreMenu"'), '应有 navMoreMenu');
    assert.ok(html18.includes('id="updatePill"'), '应有 updatePill');
    assert.ok(html18.includes('data-i18n="nav.more"'), '更多按钮应走字典 nav.more');
    assert.ok(html18.includes('data-i18n-title="nav.moreTitle"'), '更多按钮 title 应走字典 nav.moreTitle');
    assert.ok(/\.nav-more-menu \{[^}]*position: absolute/.test(css18), '更多菜单应绝对定位');
    assert.ok(/\.update-pill \{[^}]*border-radius: 999px/.test(css18), '更新 pill 应为圆角');
    assert.ok(/#topbar \{[^}]*position: relative/.test(css18), 'topbar 应为更多菜单的定位锚点');
    const showPaneSrc = extractFn18('showPane');
    assert.ok(showPaneSrc.includes('layoutTopNav()'), 'showPane 末尾应重算导航');
    const syncChromeSrc = extractFn18('syncChrome');
    assert.ok(syncChromeSrc.includes('layoutTopNav()'), 'syncChrome 末尾应重算导航');
    assert.ok(syncChromeSrc.includes('renderHomeStats()'), 'syncChrome 应重画首页统计');
    assert.ok(syncChromeSrc.includes("document.title = ui('top.title')"), 'syncChrome 应设窗口标题');
    assert.ok(rendererSrc18.includes("addEventListener('resize', layoutTopNav)"), 'resize 应重算导航');
    assert.ok(rendererSrc18.includes("addEventListener('load', layoutTopNav)"), 'load 应重算导航');
    assert.ok(rendererSrc18.includes('if (!b.dataset.view) return;'), '导航点击绑定应跳过无 data-view 的“更多”按钮');
    ok('更多导航接线：HTML ids + CSS + showPane/syncChrome/resize/load 调用点齐全');
  }
  // 18c. SseClient 建连 401：错误带上 err.status。
  {
    const prevFetch = globalThis.fetch;
    globalThis.fetch = async () => ({ ok: false, status: 401, body: null });
    try {
      const sse = new SseClient('http://127.0.0.1:1', { token: 'tok-x' });
      let caught = null;
      try { await sse._connectOnce(); } catch (e) { caught = e; }
      assert.ok(caught, '401 建连应抛错');
      assert.strictEqual(caught.status, 401, '建连失败的错误应带 status 401');
      assert.match(caught.message, /401/, '错误信息应含 401');
    } finally { globalThis.fetch = prevFetch; }
    ok('SseClient 建连 401：错误带 err.status=401');
  }
  // 18d. RealtimeClient：WS 挂掉后 SSE 建连 401 → session-expired（不经 checkSession）。
  {
    const prevFetch = globalThis.fetch;
    globalThis.fetch = async (url) => {
      assert.ok(String(url).includes('/api/stream'), 'SSE 应走 /api/stream');
      return { ok: false, status: 401, body: null };
    };
    const rc = new RealtimeClient('http://127.0.0.1:1', {
      token: 'tok-xyz',
      wsUrl: 'ws://127.0.0.1:1',
      checkSession: async () => {}, // 恒通过：证明 401 是 SSE 路自己认出来的
    });
    try {
      const expired = new Promise((resolve) => rc.once('session-expired', resolve));
      const transports = [];
      rc.on('transport', (p) => transports.push(p.state));
      rc.start();
      const ev = await withTimeout(expired, 8000, 'SSE 401 应触发 session-expired');
      assert.strictEqual(rc.sessionExpired, true, 'sessionExpired 标志应为真');
      assert.ok(transports.includes('session-expired'), 'transport 应报 session-expired');
      assert.match(String((ev && ev.reason) || ''), /会话已失效/, '原因应说明会话已失效');
    } finally {
      rc.stop();
      globalThis.fetch = prevFetch;
    }
    ok('RealtimeClient：WS 挂 + SSE 401 → session-expired，不无限重连');
  }

  // 18e. main.js 401 接线：guardApi 包住全部需 token 的 api 处理器。
  {
    assert.ok(mainSrc18.includes('function guardApi(fn)'), '应有 guardApi');
    assert.ok(mainSrc18.includes('function onSessionExpired()'), '应有 onSessionExpired');
    assert.ok(mainSrc18.includes("sendToRenderer('session:expired'"), '过期应推 session:expired');
    assert.ok(mainSrc18.includes('function apiHandle(channel, fn)'), '应有 apiHandle');
    const n = (mainSrc18.match(/apiHandle\('api:/g) || []).length;
    // 41（v0.3.13）+ 4（v0.3.15：notifications/notifications-read/outbox-status/
    // outbox-discard；后两个是本地状态、走同一注册口径，guardApi 只在 401
    // 时触发，对它们是 no-op）。
    assert.strictEqual(n, 45, `需 token 的 api 处理器应有 45 个包进 guardApi，实际 ${n} 个`);
    for (const ch of ['api:health', 'api:leaderboard', 'api:accounts', 'api:boards']) {
      assert.ok(mainSrc18.includes(`ipcMain.handle('${ch}'`), `${ch} 公共接口应保持 plain ipcMain.handle`);
      assert.ok(!mainSrc18.includes(`apiHandle('${ch}'`), `${ch} 不应包 guardApi`);
    }
    assert.ok(mainSrc18.includes("realtime.on('session-expired'"), 'realtime session-expired 应有订阅');
    assert.ok(/realtime\.on\('session-expired'[\s\S]*?onSessionExpired\(\)/.test(mainSrc18), 'session-expired 处理应调 onSessionExpired');
    ok('main.js 401 接线：guardApi + onSessionExpired + 45 个处理器 + 4 个公共接口除外');
  }
  // 18f. resetToLogin：抽函数在假 DOM 上跑；setTransport 遇 session-expired 调它。
  {
    const makeEl18 = () => {
      const el = {
        textContent: '', className: '',
        dataset: {},
        _classes: new Set(),
        classList: {
          add: (c) => el._classes.add(c),
          remove: (c) => el._classes.delete(c),
          toggle: (c, f) => { if (f) el._classes.add(c); else el._classes.delete(c); },
          contains: (c) => el._classes.has(c),
        },
      };
      return el;
    };
    const els = { whoami: makeEl18(), logoutBtn: makeEl18(), whoamiAvatar: makeEl18(), sseDot: makeEl18(), sseText: makeEl18() };
    els.whoami.textContent = 'coldhao';
    els.whoami.dataset.profile = 'coldhao';
    const fakeState = { me: { username: 'coldhao' }, open: { kind: 'group', id: 'g1' }, notif: { items: [{ id: 'mention:p1' }], unread: 3, readTs: '2026-11-25T00:00:00.000Z', mentionEnabled: true, loaded: true }, settings: { acc: {} } };
    let chipCalls = 0;
    let stopNotifCalls = 0;
    let paintBadgeCalls = 0;
    const shown = [];
    const resetToLogin = new Function('state', '$', 'setMeChip', 'show', 'stopNotifTimer', 'paintNotifBadge',
      `${extractFn18('resetToLogin')}; return resetToLogin;`)(fakeState, (id) => els[id], () => { chipCalls += 1; }, (v) => shown.push(v), () => { stopNotifCalls += 1; }, () => { paintBadgeCalls += 1; });
    resetToLogin();
    assert.strictEqual(fakeState.me, null, '应清 me');
    assert.strictEqual(fakeState.open, null, '应清 open');
    assert.strictEqual(stopNotifCalls, 1, '登出应停通知轮询（v0.3.15）');
    assert.strictEqual(paintBadgeCalls, 1, '登出应重画通知徽标（清零，v0.3.15）');
    assert.deepStrictEqual(fakeState.notif, { items: [], unread: 0, readTs: null, mentionEnabled: true, loaded: false }, '登出应清通知状态（v0.3.15）');
    assert.strictEqual(fakeState.settings, null, '登出应清设置页缓存（v0.3.15）');
    assert.strictEqual(els.whoami.textContent, '', '应清 whoami 文本');
    assert.strictEqual(els.whoami.dataset.profile, undefined, '应清 profile 标记');
    assert.strictEqual(chipCalls, 1, '应重画 whoami chip');
    assert.ok(els.logoutBtn._classes.has('hidden'), '应藏退出按钮');
    assert.deepStrictEqual(shown, ['login'], '应切到登录页');
    const TRANSPORT_UI18 = {
      connecting: { cls: 'off', key: 'top.connecting' },
      ws: { cls: 'on', key: 'top.wsConnected' },
      sse: { cls: 'warn', key: 'top.sseFallback' },
      disconnected: { cls: 'bad', key: 'top.disconnected' },
      'session-expired': { cls: 'bad', key: 'top.sessionExpired' },
    };
    let resetCalls = 0;
    const setTransport = new Function('TRANSPORT_UI', 'state', '$', 'ui', 'transportSuffix', 'resetToLogin',
      `${extractFn18('setTransport')}; return setTransport;`)(TRANSPORT_UI18, fakeState, (id) => els[id], (k) => k, () => '', () => { resetCalls += 1; });
    setTransport('ws', null);
    assert.strictEqual(resetCalls, 0, 'ws 不应回登录');
    setTransport('session-expired', null);
    assert.strictEqual(resetCalls, 1, 'session-expired 应回登录');
    ok('resetToLogin：清状态 + 回登录页；setTransport 只在 session-expired 时调它');
  }
  // 18g. renderHomeStats：语言切换只重画，不重拉接口。
  {
    tr.setViewerLang('zh');
    tr._resetForTest();
    let healthCalls = 0;
    let lbCalls = 0;
    const homeBox = {
      _html: '',
      set innerHTML(v) { this._html = v; },
      get innerHTML() { return this._html; },
    };
    const homeBoard = {
      _classes: new Set(),
      classList: {
        remove: (c) => homeBoard._classes.delete(c),
        add: (c) => homeBoard._classes.add(c),
        contains: (c) => homeBoard._classes.has(c),
      },
    };
    const homeBoardList = {
      _html: '',
      set innerHTML(v) { this._html = v; },
      get innerHTML() { return this._html; },
    };
    const fakeWindow = { chimera: { api: {
      health: async () => { healthCalls += 1; return { posts: 10, groups: 2, accounts: 5, messages: 100 }; },
      leaderboard: async () => { lbCalls += 1; return { leaders: [{ name: 'a', points: 209 }] }; },
    } } };
    const fake$ = (id) => ({ homeStats: homeBox, homeBoard, homeBoardList }[id] || null);
    const fakeState18 = { homeHealth: null, homeLeaders: [] };
    const renderHomeStats18 = new Function('state', '$', 'ui', 'uiFmt', 'esc',
      `${extractFn18('renderHomeStats')}; return renderHomeStats;`)(fakeState18, fake$, (k) => tr.ui(k), (...a) => tr.uiFmt(...a), (s) => String(s));
    const loadHomeStats = new Function('state', '$', 'window', 'renderHomeStats',
      `${extractFn18('loadHomeStats')}; return loadHomeStats;`)(fakeState18, fake$, fakeWindow, renderHomeStats18);
    await loadHomeStats();
    assert.strictEqual(healthCalls, 1, '首拉应调一次 health');
    assert.strictEqual(lbCalls, 1, '首拉应调一次 leaderboard');
    assert.ok(homeBox._html.includes('帖子'), '中文下统计标签应为“帖子”');
    assert.ok(homeBox._html.includes('>10<'), '统计值应渲染');
    assert.ok(homeBoardList._html.includes('209 分'), '中文下积分后缀应为“分”');
    tr.setViewerLang('en');
    renderHomeStats18();
    assert.strictEqual(healthCalls, 1, '语言切换不应重拉 health');
    assert.strictEqual(lbCalls, 1, '语言切换不应重拉 leaderboard');
    assert.ok(homeBox._html.includes('Posts'), '英文下统计标签应为 Posts');
    assert.ok(homeBoardList._html.includes('209 pts'), '英文下积分后缀应为 pts');
    tr.setViewerLang('zh');
    ok('renderHomeStats：语言切换只重画不重拉，统计标签与积分后缀跟随语言');
  }
  // 18h. 翻译失败分流：429 自动重试、网络错留重试按钮、名字失败不清 trJobId。
  {
    tr._resetForTest();
    tr.setViewerLang('zh');
    const isRL = tr._isRateLimitError;
    assert.strictEqual(isRL({ status: 429 }), true, 'status 429 应判节流');
    assert.strictEqual(isRL(new Error('HTTP 429')), true, '消息含 429 应判节流');
    assert.strictEqual(isRL(new Error('translate rate limited, try again later')), true, '消息含 rate limited 应判节流');
    assert.strictEqual(isRL(new Error('fetch failed')), false, '网络错不应判节流');
    assert.strictEqual(isRL({ status: 500 }), false, '500 不应判节流');
    assert.strictEqual(isRL(null), false, 'null 不应判节流');
    ok('isRateLimitError：status 429 / 消息 429 / rate limited 三路判真，其余判假');
    // batchFn 抛 status 429：50ms 后自动重试并渲染。
    tr._resetForTest();
    tr._setRetryDelayMs(50);
    let tries = 0;
    tr.setBatchFn(async (items) => {
      tries += 1;
      if (tries === 1) { const e = new Error('SSE connect failed: HTTP 429'); e.status = 429; throw e; }
      return items.map((it) => ({ translated: `译：${it.text}`, same: false }));
    });
    const box429 = makeBox18(tr.placeholder('English message for http 429 retry test.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [box429] });
    await sleep18(1200);
    assert.strictEqual(tries, 2, `HTTP 429 应自动重试一次，实际调了 ${tries} 次`);
    assert.ok(box429._html.includes('译：English message for http 429'), '重试成功后应渲染译文');
    ok('翻译 429：batchFn 抛 status 429 → 自动重试一次并渲染');
    // 消息里只有 429（IPC 丢 status 时）：同样自动重试。
    tr._resetForTest();
    tr._setRetryDelayMs(50);
    let tries2 = 0;
    tr.setBatchFn(async (items) => {
      tries2 += 1;
      if (tries2 === 1) throw new Error('HTTP 429 Too Many Requests');
      return items.map((it) => ({ translated: `译：${it.text}`, same: false }));
    });
    const box429b = makeBox18(tr.placeholder('English message for message only 429 test.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [box429b] });
    await sleep18(1200);
    assert.strictEqual(tries2, 2, `消息 429 应自动重试一次，实际调了 ${tries2} 次`);
    assert.ok(box429b._html.includes('译：English message for message only 429'), '消息 429 重试成功后应渲染译文');
    tr._setRetryDelayMs(65000);
    ok('翻译 429：消息-only 429 同样自动重试');
    // 网络错：留重试按钮，不自动重试。
    tr._resetForTest();
    let netTries = 0;
    tr.setBatchFn(async () => { netTries += 1; throw new Error('fetch failed'); });
    const boxNet = makeBox18(tr.placeholder('English message for network failure test.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [boxNet] });
    await sleep18(600);
    assert.strictEqual(netTries, 1, '网络错不应自动重试');
    assert.ok(boxNet._html.includes('data-tr-retry'), '网络错应留重试按钮');
    ok('翻译网络错：不自动重试，留重试按钮');
    // 名字失败：trJobId 保留，重复 observe 不建新 job。
    tr._resetForTest();
    let nameTries = 0;
    tr.setBatchFn(async (items) => { nameTries += 1; return items.map(() => ({ error: 'translation failed' })); });
    const nameEl = {
      _attrs: { 'data-tr-name': 'English group name for failure test' },
      dataset: {},
      textContent: 'English group name for failure test',
      getAttribute(k) { return this._attrs[k] || null; },
    };
    const nameRoot = { querySelectorAll: () => [nameEl] };
    tr.observeNameTranslations(nameRoot);
    const jobId = nameEl.dataset.trJobId;
    assert.ok(jobId, '名字入队应打 trJobId');
    tr.flushNow();
    await sleep18(400);
    assert.strictEqual(nameTries, 1, '名字应调一次接口');
    assert.strictEqual(nameEl.dataset.trJobId, jobId, '名字失败后 trJobId 应保留');
    assert.strictEqual(nameEl.textContent, 'English group name for failure test', '名字失败应留原文');
    tr.observeNameTranslations(nameRoot);
    tr.flushNow();
    await sleep18(400);
    assert.strictEqual(nameTries, 1, '重复 observe 不应建新 job、不应再调接口');
    assert.strictEqual(nameEl.dataset.trJobId, jobId, '重复 observe 后 trJobId 应不变');
    ok('名字翻译失败：不清 trJobId、不重复建 job，留原文');
  }

  // 18i. 翻译缓存 LRU 上限 500：超了淘汰最旧，命中刷新新近度。
  {
    tr._resetForTest();
    tr.setViewerLang('zh');
    assert.strictEqual(tr._cacheMax(), 500, '缓存上限应为 500');
    const seen = [];
    tr.setBatchFn(async (items) => {
      seen.push(...items.map((it) => it.text));
      return items.map((it) => ({ translated: `译：${it.text}`, same: false }));
    });
    const fillText = (i) => `English cache fill message number ${i} for eviction test.`;
    const boxes = [];
    const root = { querySelectorAll: () => boxes.filter((b) => !b._removed && b.getAttribute('data-trid')) };
    for (let i = 0; i < 505; i++) {
      const html = tr.placeholder(fillText(i));
      const id = html.match(/data-trid="(at\d+)"/)[1];
      boxes.push(makeBox18(id));
    }
    tr.observeTranslations(root);
    await sleep18(1500);
    assert.strictEqual(tr._cacheSize(), 500, '填 505 条后缓存应为 500 条');
    assert.strictEqual(seen.length, 505, `应调接口译 505 条，实际 ${seen.length} 条`);
    // 最旧 5 条已被淘汰：再入队应调接口；最新一条应命中。
    const before0 = seen.length;
    const box0 = makeBox18(tr.placeholder(fillText(0)).match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [box0] });
    await sleep18(600);
    assert.ok(seen.length > before0, '第 0 条被淘汰后应重新调接口');
    assert.ok(box0._html.includes('译：English cache fill message number 0'), '重译后应渲染');
    const before504 = seen.length;
    const box504 = makeBox18(tr.placeholder(fillText(504)).match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [box504] });
    await sleep18(600);
    assert.strictEqual(seen.length, before504, '第 504 条命中缓存不应再调接口');
    assert.ok(box504._html.includes('译：English cache fill message number 504'), '命中应直接渲染');
    // 新近度：命中第 10 条刷新它，再填 5 条新项，淘汰的是最旧的 6–9、11，
    // 而不是被刷新过的 10。
    const box10 = makeBox18(tr.placeholder(fillText(10)).match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [box10] });
    await sleep18(600);
    assert.strictEqual(seen.length, before504, '第 10 条命中不应调接口');
    const moreBoxes = [];
    for (let i = 505; i < 510; i++) {
      const id = tr.placeholder(fillText(i)).match(/data-trid="(at\d+)"/)[1];
      moreBoxes.push(makeBox18(id));
    }
    tr.observeTranslations({ querySelectorAll: () => moreBoxes.filter((b) => !b._removed && b.getAttribute('data-trid')) });
    await sleep18(900);
    assert.strictEqual(tr._cacheSize(), 500, '再填 5 条后缓存仍应为 500 条');
    const before10 = seen.length;
    const box10b = makeBox18(tr.placeholder(fillText(10)).match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [box10b] });
    await sleep18(600);
    assert.strictEqual(seen.length, before10, '被刷新过的第 10 条应仍在缓存里');
    const before6 = seen.length;
    const box6 = makeBox18(tr.placeholder(fillText(6)).match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [box6] });
    await sleep18(600);
    assert.ok(seen.length > before6, '未被刷新过的第 6 条应已被淘汰');
    ok('翻译缓存 LRU：上限 500、最旧淘汰、命中刷新新近度');
  }
  // 18j. debounce：连敲只跑一次，cancel 能拦住；会话搜索已接防抖。
  {
    const debounce = new Function(`${extractFn18('debounce')}; return debounce;`)();
    let runs = 0;
    let lastArg = null;
    const fn = debounce((x) => { runs += 1; lastArg = x; }, 100);
    fn('a'); fn('b'); fn('c');
    await sleep18(200);
    assert.strictEqual(runs, 1, '连敲 3 次应只跑一次');
    assert.strictEqual(lastArg, 'c', '应取最后一次参数');
    let runs2 = 0;
    const fn2 = debounce(() => { runs2 += 1; }, 100);
    fn2();
    fn2.cancel();
    await sleep18(200);
    assert.strictEqual(runs2, 0, 'cancel 后不应跑');
    assert.ok(rendererSrc18.includes("$('convSearch').addEventListener('input', debounce(renderConvList, 150))"), '会话搜索 input 应接 150ms 防抖');
    ok('debounce：连敲合并一次、取末参、cancel 拦截；convSearch 已接线');
  }
  // 18k. errText：8 个错误码双语映射 + 细节代入 + 未知原样透传。
  {
    const errText = new Function('ui', 'uiFmt', `${extractFn18('errText')}; return errText;`)((k) => tr.ui(k), (...a) => tr.uiFmt(...a));
    tr.setViewerLang('zh');
    assert.strictEqual(errText(new Error('ERR_LOGIN_NO_SESSION')), '服务端未返回会话', '登录无会话中文');
    assert.strictEqual(errText(new Error('ERR_NEED_LOGIN')), '请先登录人类账号', '需登录中文');
    assert.strictEqual(errText(new Error('ERR_BIND_NO_CHALLENGE')), '缺少挑战串', '绑定缺挑战串中文');
    assert.strictEqual(errText(new Error('ERR_BIND_ALREADY_BOUND')), '已绑定工作区，请先解绑再重绑', '已绑定中文');
    assert.strictEqual(errText(new Error('ERR_DO_REGISTER_FAILED: boom')), '注册公钥到信任层失败：boom', 'DO 注册失败中文应代入细节');
    assert.strictEqual(errText(new Error('ERR_NODE_ALREADY_ACTIVATED')), '节点已激活过，无需重复激活', '已激活中文');
    assert.strictEqual(errText(new Error('ERR_NODE_ACTIVATE_NO_ACCOUNT')), '服务端未返回节点账号', '激活无账号中文');
    assert.strictEqual(errText(new Error('ERR_NODE_TOKEN_OUTSIDE_DIR: /a 不在 /b 下')), '节点 token 文件不在节点数据目录内，拒绝启动：/a 不在 /b 下', 'token 越目录中文应代入细节');
    tr.setViewerLang('en');
    assert.strictEqual(errText(new Error('ERR_LOGIN_NO_SESSION')), 'The server did not return a session', '登录无会话英文');
    assert.strictEqual(errText('ERR_NEED_LOGIN'), 'Please log in with a human account first', '字符串入参也应翻译');
    assert.strictEqual(errText(new Error('ERR_DO_REGISTER_FAILED: boom')), 'Failed to register the public key with the trust layer: boom', 'DO 注册失败英文应代入细节');
    assert.strictEqual(errText(new Error('ERR_NODE_TOKEN_OUTSIDE_DIR: /a not under /b')), 'The node token file is outside the node data directory; refusing to start: /a not under /b', 'token 越目录英文应代入细节');
    assert.strictEqual(errText(new Error('ERR_SOMETHING_ELSE: x')), 'ERR_SOMETHING_ELSE: x', '未知错误码应原样透传');
    assert.strictEqual(errText(new Error('plain server message')), 'plain server message', '无码服务端原话应原样透传');
    assert.strictEqual(errText(null), '', 'null 应为空串');
    tr.setViewerLang('zh');
    ok('errText：8 码双语映射 + 细节代入 + 未知/原话透传');
  }
  // 18l. fmtUptime / nodeStateText / profileRoleText / profileStatusText 双语表。
  {
    const ui18 = (k) => tr.ui(k);
    const uiFmt18 = (...a) => tr.uiFmt(...a);
    const fmtUptime = new Function('uiFmt', `${extractFn18('fmtUptime')}; return fmtUptime;`)(uiFmt18);
    const nodeStateText = new Function('ui', `${extractFn18('nodeStateText')}; return nodeStateText;`)(ui18);
    const profileRoleText = new Function('ui', `${extractFn18('profileRoleText')}; return profileRoleText;`)(ui18);
    const profileStatusText = new Function('ui', `${extractFn18('profileStatusText')}; return profileStatusText;`)(ui18);
    tr.setViewerLang('zh');
    assert.strictEqual(fmtUptime(0), '0 秒', '0 秒');
    assert.strictEqual(fmtUptime(45), '45 秒', '45 秒');
    assert.strictEqual(fmtUptime(60), '1 分 0 秒', '60 秒应为 1 分 0 秒');
    assert.strictEqual(fmtUptime(3661), '1 小时 1 分', '3661 秒应为 1 小时 1 分');
    assert.strictEqual(fmtUptime(7320), '2 小时 2 分', '7320 秒应为 2 小时 2 分');
    assert.strictEqual(nodeStateText('running'), '运行中', 'running 中文');
    assert.strictEqual(nodeStateText('starting'), '启动中', 'starting 中文');
    assert.strictEqual(nodeStateText('stopping'), '停止中', 'stopping 中文');
    assert.strictEqual(nodeStateText('error'), '出错', 'error 中文');
    assert.strictEqual(nodeStateText('off'), '已停靠', 'off 中文');
    assert.strictEqual(nodeStateText('weird'), 'weird', '未知状态原样');
    assert.strictEqual(profileRoleText('admin'), '管理员', 'admin 中文');
    assert.strictEqual(profileRoleText('creator'), '创建人', 'creator 中文');
    assert.strictEqual(profileRoleText('member'), '成员', 'member 中文');
    assert.strictEqual(profileRoleText(undefined), '成员', '缺 role 默认为成员');
    assert.strictEqual(profileRoleText('superuser'), 'superuser', '未知 role 原样');
    assert.strictEqual(profileStatusText('suspended'), '已停用', 'suspended 中文');
    assert.strictEqual(profileStatusText('retired'), '已退役', 'retired 中文');
    assert.strictEqual(profileStatusText('active'), 'active', 'active 原样');
    tr.setViewerLang('en');
    assert.strictEqual(fmtUptime(3661), '1h 1m', '3661 秒英文');
    assert.strictEqual(fmtUptime(45), '45s', '45 秒英文');
    assert.strictEqual(nodeStateText('running'), 'Running', 'running 英文');
    assert.strictEqual(nodeStateText('off'), 'Docked', 'off 英文');
    assert.strictEqual(profileRoleText('admin'), 'Admin', 'admin 英文');
    assert.strictEqual(profileStatusText('suspended'), 'Suspended', 'suspended 英文');
    tr.setViewerLang('zh');
    ok('fmtUptime / nodeStateText / profileRoleText / profileStatusText：双语表 + 未知原样');
  }
  // 18m. update-staged：checkAndStage 暂存成功即发事件；三端接线齐全。
  {
    const asarBytes = Buffer.from('fake-asar-bytes-v0313');
    const asarSha = sha256Hex(asarBytes);
    const manifest = { version: '0.3.13', asarUrl: 'https://example.invalid/app.asar', sha256: asarSha, notes: 'test' };
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-staged-'));
    try {
      const up = new AppUpdater({
        currentVersion: '0.3.12', userDataDir: tmp,
        fetch: async (url) => (String(url).endsWith('/desktop-update.json')
          ? mockResponse({ json: manifest })
          : mockResponse({ bytes: asarBytes })),
      });
      const staged = new Promise((resolve) => up.once('update-staged', resolve));
      const info = await up.checkAndStage();
      const ev = await withTimeout(staged, 3000, 'checkAndStage 应发 update-staged');
      assert.strictEqual(ev.version, '0.3.13', '事件应带新版本号');
      assert.strictEqual(info.staged.ok, true, '暂存应成功');
      assert.ok(fs.existsSync(up.pendingAsar), '暂存 asar 应存在');
    } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    assert.ok(mainSrc18.includes("updater.on('update-staged'"), 'main 应订阅 update-staged');
    assert.ok(mainSrc18.includes("sendToRenderer('update:staged'"), 'main 应转发 update:staged');
    assert.ok(preloadSrc18.includes('onUpdateStaged'), 'preload 应暴露 onUpdateStaged');
    assert.ok(rendererSrc18.includes('window.chimera.onUpdateStaged'), 'renderer 应订阅 onUpdateStaged');
    assert.ok(rendererSrc18.includes("$('updatePill')"), 'renderer 应操作 updatePill');
    ok('update-staged：checkAndStage 发事件带版本 + main/preload/renderer 三端接线');
  }
  // 18n. ForumClient 翻译 429：单条与批量都应以 status 429 抛错；main 归一化接线。
  {
    const mock = await new Promise((resolve) => {
      const server = http.createServer((req, res) => {
        let body = '';
        req.on('data', (c) => { body += c; });
        req.on('end', () => {
          res.setHeader('Content-Type', 'application/json');
          res.writeHead(429);
          res.end(JSON.stringify({ ok: false, error: 'translate rate limited, try again later' }));
        });
      });
      server.listen(0, '127.0.0.1', () => resolve({ server, base: `http://127.0.0.1:${server.address().port}` }));
    });
    try {
      const mc = new ForumClient(mock.base);
      await assert.rejects(mc.translate('tok-abc', 'Hello', 'zh'), (e) => e.status === 429, '单条翻译 429 应抛 status 429');
      await assert.rejects(mc.translateBatch('tok-abc', [{ text: 'Hello' }], 'zh'), (e) => e.status === 429, '批量翻译 429 应抛 status 429');
    } finally { mock.server.close(); }
    assert.ok(mainSrc18.includes('err.status === 429'), 'main 翻译处理器应识别 429');
    assert.ok(mainSrc18.includes("results: (items || []).map(() => ({ error: 'rate_limited' }))"), 'main 应把批量 429 归一化为 per-item rate_limited');
    ok('ForumClient 翻译 429：单条/批量均抛 status 429；main 归一化接线齐全');
  }

  console.log('== 19. v0.3.14：帖子分页 + 增量追加 / 翻译视口内入队 ==');
  // 19a. 帖子合并纯函数：更早插置顶后、新帖追加末尾、去重、游标、水位。
  {
    const mergeOlderFeed = new Function(`${extractFn18('mergeOlderFeed')}; return mergeOlderFeed;`)();
    const mergeNewerFeed = new Function(`${extractFn18('mergeNewerFeed')}; return mergeNewerFeed;`)();
    const feedCursorOf = new Function(`${extractFn18('feedCursorOf')}; return feedCursorOf;`)();
    const feedNewestTsOf = new Function(`${extractFn18('feedNewestTsOf')}; return feedNewestTsOf;`)();
    const p = (id, ts, pinned) => ({ id, ts, pinned: !!pinned, board: 'general' });
    assert.deepStrictEqual(
      mergeOlderFeed([p('a', 't', true), p('b', 't'), p('c', 't')], [p('x', 't'), p('y', 't')]).map((x) => x.id),
      ['a', 'x', 'y', 'b', 'c'], '更早一页应插到置顶之后、现有流之前');
    assert.deepStrictEqual(
      mergeOlderFeed([p('a', 't', true), p('b', 't')], [p('b', 't'), p('z', 't')]).map((x) => x.id),
      ['a', 'z', 'b'], '重复 id 去重后保留旧卡原位，新页卡插到现有流之前');
    assert.deepStrictEqual(mergeOlderFeed([p('a', 't')], []).map((x) => x.id), ['a'], '空页不应改动');
    assert.deepStrictEqual(
      mergeNewerFeed([p('a', 't', true), p('b', 't')], [p('d', 't'), p('b', 't')]).map((x) => x.id),
      ['a', 'b', 'd'], '新帖应追加到末尾并去重');
    assert.strictEqual(feedCursorOf([p('a', 't'), p('b', 't'), p('c', 't')]), 'a', '游标应是最老一条（首位，正序列表的第一条）');
    assert.strictEqual(feedCursorOf([]), null, '空页游标为 null');
    assert.strictEqual(feedNewestTsOf([p('a', '2026-11-26T01:00'), p('b', '2026-11-26T03:00'), p('c', '2026-11-26T02:00')]), '2026-11-26T03:00', '水位应取最大 ts');
    ok('帖子合并纯函数：更早插置顶后/新帖追加末尾/去重/游标取首位/水位取最大');
  }
  // 19b. ForumClient：getPosts 透传 before_id；getFollowingFeed 透传 since/before_id。
  {
    const mock = await new Promise((resolve) => {
      const seen = [];
      const server = http.createServer((req, res) => {
        seen.push(req.url);
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ ok: true, posts: [], pinned: [], has_more: false }));
      });
      server.listen(0, '127.0.0.1', () => resolve({ server, base: `http://127.0.0.1:${server.address().port}`, seen }));
    });
    try {
      const mc = new ForumClient(mock.base);
      await mc.getPosts('tok-x', { limit: 30, before_id: 'pmw-oldest' });
      await mc.getFollowingFeed('tok-x', { since: '2026-11-26T02:00:00Z' });
      await mc.getFollowingFeed('tok-x', { limit: 30, before_id: 'pmw-oldest2' });
      assert.ok(mock.seen[0].includes('before_id=pmw-oldest') && mock.seen[0].includes('limit=30'), `getPosts 应透传 before_id，实际 ${mock.seen[0]}`);
      assert.ok(decodeURIComponent(mock.seen[1]).includes('since=2026-11-26T02:00:00Z'), `getFollowingFeed 应透传 since，实际 ${mock.seen[1]}`);
      assert.ok(mock.seen[2].includes('before_id=pmw-oldest2'), `getFollowingFeed 应透传 before_id，实际 ${mock.seen[2]}`);
    } finally { mock.server.close(); }
    ok('ForumClient：getPosts 透传 before_id；getFollowingFeed 透传 since/before_id');
  }
  // 19c. 帖子分页接线：翻页/拉新/SSE/按钮/字典/main 透传齐全。
  {
    assert.ok(rendererSrc18.includes('function loadOlderPosts()'), '应有 loadOlderPosts');
    assert.ok(rendererSrc18.includes('before_id: state.feedCursor'), '翻页应用 feedCursor 当 before_id');
    assert.ok(rendererSrc18.includes('function fetchNewFeedPosts()'), '应有 fetchNewFeedPosts');
    assert.ok(rendererSrc18.includes('since: state.feedNewestTs'), '拉新应用 feedNewestTs 当 since');
    const sseIdx = rendererSrc18.indexOf("event === 'post_created'");
    assert.ok(sseIdx >= 0, '应有 post_created 事件分支');
    const sseWin = rendererSrc18.slice(sseIdx, sseIdx + 300);
    assert.ok(sseWin.includes('fetchNewFeedPosts()'), 'SSE 新帖应走增量拉新');
    assert.ok(!sseWin.includes('loadPosts()'), 'SSE 新帖不应再全量 loadPosts');
    assert.ok(html18.includes('id="feedOlderRow"'), '应有 feedOlderRow');
    assert.ok(html18.includes('id="feedOlderBtn"'), '应有 feedOlderBtn');
    assert.ok(html18.includes('data-i18n="posts.loadOlder"'), '翻页按钮应走字典 posts.loadOlder');
    assert.ok(rendererSrc18.includes('function syncFeedOlderButton()'), '应有 syncFeedOlderButton');
    assert.ok(rendererSrc18.includes("addEventListener('click', loadOlderPosts)"), '翻页按钮应绑 loadOlderPosts');
    assert.ok(tr._uiStrings.zh['posts.loadOlder'] && tr._uiStrings.en['posts.loadOlder'], '字典中英应含 posts.loadOlder');
    assert.ok(tr._uiStrings.zh['posts.loading'] && tr._uiStrings.en['posts.loading'], '字典中英应含 posts.loading');
    assert.ok(mainSrc18.includes("apiHandle('api:following-feed', (_e, { limit, before_id, since })"), 'main following-feed 应透传 since');
    ok('帖子分页接线：翻页/拉新/SSE/按钮/字典/main 透传齐全');
  }
  // 19d. insertFeedCards（假 DOM）：更早插锚点前、新帖追加末尾、清掉空态、只扫新卡。
  {
    const prevDoc19d = globalThis.document;
    globalThis.document = {};
    try {
      const calls19d = [];
      let observed19d = 0;
      let emptyRemoved19d = false;
      const anchor19d = { insertAdjacentHTML(pos, html) { calls19d.push(['anchor', pos, html]); } };
      const fakeBox19d = {
        insertAdjacentHTML(pos, html) { calls19d.push(['box', pos, html]); },
        querySelector(sel) {
          if (sel === '.empty-note') return { remove() { emptyRemoved19d = true; } };
          if (sel === '[data-post="b"]') return anchor19d;
          return null;
        },
      };
      const fakeWin19d = { ChimeraTranslate: { observeTranslations: () => { observed19d += 1; }, observeNameTranslations: () => {} } };
      const insertFeedCards = new Function('postCardHtml', 'state', '$', 'window', 'isForumOwner',
        `${extractFn18('insertFeedCards')}; return insertFeedCards;`)(
        (p) => `<div class="post-card" data-post="${p.id}">${p.id}</div>`,
        { me: { username: 'coldhao' } },
        () => fakeBox19d,
        fakeWin19d,
        () => true);
      insertFeedCards([{ id: 'x' }, { id: 'y' }], 'b');
      assert.ok(calls19d.some((c) => c[0] === 'anchor' && c[1] === 'beforebegin' && c[2].includes('data-post="x"') && c[2].includes('data-post="y"')), '更早一页应插到第一张流卡之前');
      assert.strictEqual(emptyRemoved19d, true, '插卡时应清掉空态提示');
      assert.strictEqual(observed19d, 1, '插卡后应只给新卡入队翻译');
      insertFeedCards([{ id: 'z' }], null);
      assert.ok(calls19d.some((c) => c[0] === 'box' && c[1] === 'beforeend' && c[2].includes('data-post="z"')), '新帖应追加到列表末尾');
      assert.strictEqual(observed19d, 2, '每次插卡都应入队一次翻译扫描');
      ok('insertFeedCards：更早插锚点前/新帖追加末尾/清空态/只扫新卡');
    } finally {
      if (prevDoc19d === undefined) delete globalThis.document; else globalThis.document = prevDoc19d;
    }
  }
  // 19e. 视口内入队：屏外不入队、进视口才入队；缓存/429/名字/重试语义不变。
  {
    const sleep19 = sleep18;
    const prevIO19 = globalThis.IntersectionObserver;
    const prevDoc19 = globalThis.document;
    // 假 IntersectionObserver：记录观察目标，测试手动触发进视口。
    const observers19 = [];
    class FakeIO19 {
      constructor(cb) { this.cb = cb; this.targets = new Set(); observers19.push(this); }
      observe(t) { this.targets.add(t); }
      unobserve(t) { this.targets.delete(t); }
      disconnect() { this.targets.clear(); }
      trigger(ids) {
        const entries = [...this.targets].filter((t) => ids.includes(t.getAttribute('data-trid'))).map((t) => ({ isIntersecting: true, target: t }));
        if (entries.length) this.cb(entries);
      }
    }
    try {
      // e1. 无 IO 时回落立即入队（旧行为）。
      if ('IntersectionObserver' in globalThis) delete globalThis.IntersectionObserver;
      if (typeof globalThis.document === 'undefined') globalThis.document = { contains: () => true };
      tr._resetForTest();
      tr.setViewerLang('zh');
      let callsE1 = 0;
      tr.setBatchFn(async (items) => { callsE1 += 1; return items.map((it) => ({ translated: `译：${it.text}`, same: false })); });
      const boxE1 = makeBox18(tr.placeholder('English viewport fallback immediate test.').match(/data-trid="(at\d+)"/)[1]);
      tr.observeTranslations({ querySelectorAll: () => [boxE1] });
      await sleep19(600);
      assert.strictEqual(callsE1, 1, '无 IO 时应立即入队并调接口');
      assert.ok(boxE1._html.includes('译：English viewport fallback'), '无 IO 时应渲染译文');
      ok('视口回落：无 IntersectionObserver 时立即入队（旧行为）');
      // e2-e3. 有 IO：屏外不入队、进视口才入队；缓存命中不调接口。
      globalThis.IntersectionObserver = FakeIO19;
      tr._resetForTest();
      let calls = 0;
      tr.setBatchFn(async (items) => { calls += 1; return items.map((it) => ({ translated: `译：${it.text}`, same: false })); });
      const boxes = [];
      const root = { querySelectorAll: () => boxes.filter((b) => !b._removed && b.getAttribute('data-trid')) };
      for (let i = 0; i < 3; i++) {
        boxes.push(makeBox18(tr.placeholder(`English viewport message number ${i} for test.`).match(/data-trid="(at\d+)"/)[1]));
      }
      tr.observeTranslations(root);
      await sleep19(600);
      assert.strictEqual(calls, 0, '屏外占位不应入队、不应调接口');
      assert.strictEqual(tr._pendingViewport(), 3, '3 个占位应都在待观察');
      observers19[observers19.length - 1].trigger([boxes[0].getAttribute('data-trid')]);
      await sleep19(600);
      assert.strictEqual(calls, 1, '进视口 1 个应只调一次接口');
      assert.ok(boxes[0]._html.includes('译：English viewport message number 0'), '进视口的应渲染译文');
      assert.strictEqual(tr._pendingViewport(), 2, '剩下 2 个仍待观察');
      assert.ok(!boxes[1]._html.includes('译：'), '未进视口的不应渲染');
      observers19[observers19.length - 1].trigger([boxes[1].getAttribute('data-trid'), boxes[2].getAttribute('data-trid')]);
      await sleep19(600);
      assert.strictEqual(calls, 2, '剩下 2 个进视口应再调一次（同一批）');
      assert.strictEqual(tr._pendingViewport(), 0, '全部进视口后待观察应清零');
      ok('视口入队：屏外不入队、进视口才入队、逐个触发');
      const cacheBox = makeBox18(tr.placeholder('English viewport message number 0 for test.').match(/data-trid="(at\d+)"/)[1]);
      tr.observeTranslations({ querySelectorAll: () => [cacheBox] });
      assert.strictEqual(tr._pendingViewport(), 1, '缓存文本的占位也应先观察');
      observers19[observers19.length - 1].trigger([cacheBox.getAttribute('data-trid')]);
      await sleep19(500);
      assert.strictEqual(calls, 2, '缓存命中不应再调接口');
      assert.ok(cacheBox._html.includes('译：English viewport message number 0'), '缓存命中进视口应直接渲染');
      ok('视口入队 + 缓存：缓存命中不调接口，进视口即渲染');
      // e4. 429 分流仍走：进视口后 rate_limited，50ms 后自动重试成功。
      tr._resetForTest();
      tr._setRetryDelayMs(50);
      let tries = 0;
      tr.setBatchFn(async (items) => {
        tries += 1;
        if (tries === 1) return items.map(() => ({ error: 'rate_limited' }));
        return items.map((it) => ({ translated: `译：${it.text}`, same: false }));
      });
      const rlBox = makeBox18(tr.placeholder('English viewport rate limit retry test.').match(/data-trid="(at\d+)"/)[1]);
      tr.observeTranslations({ querySelectorAll: () => [rlBox] });
      assert.strictEqual(tr._pendingViewport(), 1, '429 测试占位应先观察');
      observers19[observers19.length - 1].trigger([rlBox.getAttribute('data-trid')]);
      await sleep19(900);
      assert.strictEqual(tries, 2, `节流应自动重试一次，实际调了 ${tries} 次`);
      assert.ok(rlBox._html.includes('译：English viewport rate limit'), '重试成功后应渲染译文');
      tr._setRetryDelayMs(65000);
      ok('视口入队 + 429 分流：rate_limited 后自动重试并渲染');
      // e5. 脱离文档的待观察占位应被扫掉。
      tr._resetForTest();
      tr.setBatchFn(async (items) => items.map((it) => ({ translated: `译：${it.text}`, same: false })));
      const staleBox = makeBox18(tr.placeholder('English viewport stale box test.').match(/data-trid="(at\d+)"/)[1]);
      tr.observeTranslations({ querySelectorAll: () => [staleBox] });
      assert.strictEqual(tr._pendingViewport(), 1, '应先观察 1 个');
      globalThis.document = { contains: (el) => el !== staleBox };
      tr.observeTranslations({ querySelectorAll: () => [] });
      assert.strictEqual(tr._pendingViewport(), 0, '脱离文档的占位应被扫掉');
      const freshBox = makeBox18(tr.placeholder('English viewport stale box test.').match(/data-trid="(at\d+)"/)[1]);
      assert.notStrictEqual(freshBox.getAttribute('data-trid'), staleBox.getAttribute('data-trid'), '重建占位应是新 id');
      globalThis.document = { contains: () => true };
      ok('视口扫除：脱离文档的占位清 texts、不积压');
      // e6. 名字类不走视口：有 IO 也立即入队。
      tr._resetForTest();
      let nameCalls = 0;
      tr.setBatchFn(async (items) => { nameCalls += 1; return items.map((it) => ({ translated: `译名：${it.text}`, same: false })); });
      tr.setViewerLang('en');
      const nameEl = {
        _text: '技术探讨',
        dataset: {},
        getAttribute: () => '技术探讨',
        set textContent(v) { this._text = v; },
        get textContent() { return this._text; },
      };
      tr.observeNameTranslations({ querySelectorAll: () => [nameEl] });
      await sleep19(600);
      assert.strictEqual(nameCalls, 1, '名字类应立即入队、不等视口');
      assert.strictEqual(tr._pendingViewport(), 0, '名字类不应进视口观察');
      tr.setViewerLang('zh');
      ok('名字类不受视口影响：仍立即入队');
      // e7. 重试按钮仍立即入队：不经视口。
      tr._resetForTest();
      let retryCalls = 0;
      tr.setBatchFn(async () => { retryCalls += 1; throw new Error('network down'); });
      const rBox = makeBox18(tr.placeholder('English viewport retry button test.').match(/data-trid="(at\d+)"/)[1]);
      tr.observeTranslations({ querySelectorAll: () => [rBox] });
      observers19[observers19.length - 1].trigger([rBox.getAttribute('data-trid')]);
      await sleep19(600);
      assert.strictEqual(retryCalls, 1, '首次应调一次接口');
      assert.ok(rBox._html.includes('data-tr-retry'), '网络失败应留重试按钮');
      tr.setBatchFn(async (items) => { retryCalls += 1; return items.map((it) => ({ translated: `译：${it.text}`, same: false })); });
      const retryId = rBox._html.match(/data-tr-retry="(at\d+)"/)[1];
      const retryBtn = {
        getAttribute: (k) => (k === 'data-tr-retry' ? retryId : null),
        set disabled(v) { this._disabled = v; },
        set textContent(v) { this._text = v; },
        closest: () => rBox,
      };
      tr._handleClick({ target: { closest: (sel) => (sel === '.tr-toggle' ? retryBtn : null) } });
      await sleep19(600);
      assert.strictEqual(retryCalls, 2, '点重试应立即再调接口');
      assert.ok(rBox._html.includes('译：English viewport retry'), '重试成功后应渲染译文');
      ok('重试按钮不受视口影响：点击立即再入队');
    } finally {
      if (prevIO19 === undefined) delete globalThis.IntersectionObserver; else globalThis.IntersectionObserver = prevIO19;
      if (prevDoc19 === undefined) delete globalThis.document; else globalThis.document = prevDoc19;
      tr._resetForTest();
      tr._setRetryDelayMs(65000);
      tr.setViewerLang('zh');
    }
  }
  // 19f. fetchNewFeedPosts：增量追加末尾、去重、水位推进；raw 为空才全量重拉。
  {
    const mergeNewerFeed19 = new Function(`${extractFn18('mergeNewerFeed')}; return mergeNewerFeed;`)();
    const feedNewestTsOf19 = new Function(`${extractFn18('feedNewestTsOf')}; return feedNewestTsOf;`)();
    const filterPostsByBoard19 = new Function(`${extractFn18('filterPostsByBoard')}; return filterPostsByBoard;`)();
    const fetchNewFeedPosts = (st, fetchStub, insertStub, loadStub) => new Function('state', 'fetchFeedPage', 'filterPostsByBoard', 'feedNewestTsOf', 'mergeNewerFeed', 'insertFeedCards', 'loadPosts',
      `${extractFn18('fetchNewFeedPosts')}; return fetchNewFeedPosts;`)(st, fetchStub, filterPostsByBoard19, feedNewestTsOf19, mergeNewerFeed19, insertStub, loadStub);
    const p19 = (id, ts, board) => ({ id, ts, board: board || 'general' });
    // 新帖追加末尾并去重，水位推进；重复卡不重复插。
    {
      const st = { postsFeed: 'all', postsBoard: null, feedPosts: [p19('a', '2026-11-26T01:00:00Z')], feedNewestTs: '2026-11-26T01:00:00Z' };
      let inserted = null;
      let reloaded = 0;
      await fetchNewFeedPosts(st,
        async (opts) => {
          assert.strictEqual(opts.since, '2026-11-26T01:00:00Z', '拉新应用水位当 since');
          return { posts: [p19('b', '2026-11-26T02:00:00Z'), p19('a', '2026-11-26T01:00:00Z')] };
        },
        (posts) => { inserted = posts; },
        async () => { reloaded += 1; })();
      assert.deepStrictEqual(st.feedPosts.map((x) => x.id), ['a', 'b'], '新帖应追加到末尾');
      assert.deepStrictEqual(inserted.map((x) => x.id), ['b'], '重复卡不应重复插入 DOM');
      assert.strictEqual(st.feedNewestTs, '2026-11-26T02:00:00Z', '水位应推进到最新帖');
      assert.strictEqual(reloaded, 0, '有新帖时不应全量重拉');
      ok('fetchNewFeedPosts：增量追加末尾、去重、水位推进');
    }
    // raw 为空：全量重拉一次（防同毫秒撞 ts 漏帖）。
    {
      const st = { postsFeed: 'all', postsBoard: null, feedPosts: [p19('a', '2026-11-26T01:00:00Z')], feedNewestTs: '2026-11-26T01:00:00Z' };
      let reloaded = 0;
      await fetchNewFeedPosts(st, async () => ({ posts: [] }),
        () => { throw new Error('不应插卡'); },
        async () => { reloaded += 1; })();
      assert.strictEqual(reloaded, 1, 'raw 为空时应全量重拉一次');
      ok('fetchNewFeedPosts：raw 为空时全量重拉');
    }
    // raw 有数但全被板块过滤：不插卡、不重拉，水位照常推进。
    {
      const st = { postsFeed: 'all', postsBoard: 'general', feedPosts: [p19('a', '2026-11-26T01:00:00Z', 'general')], feedNewestTs: '2026-11-26T01:00:00Z' };
      let reloaded = 0;
      await fetchNewFeedPosts(st, async () => ({ posts: [p19('b', '2026-11-26T02:00:00Z', 'other')] }),
        () => { throw new Error('不应插卡'); },
        async () => { reloaded += 1; })();
      assert.strictEqual(reloaded, 0, '全被板块过滤时不应全量重拉');
      assert.strictEqual(st.feedNewestTs, '2026-11-26T02:00:00Z', '水位应照常推进（过滤前取）');
      ok('fetchNewFeedPosts：全被板块过滤时不插卡、不重拉、水位推进');
    }
  }
  // 19g. loadOlderPosts：整页被滤自动续拉、游标跟最后一页、5 页上限。
  {
    const mergeOlderFeed19 = new Function(`${extractFn18('mergeOlderFeed')}; return mergeOlderFeed;`)();
    const feedCursorOf19 = new Function(`${extractFn18('feedCursorOf')}; return feedCursorOf;`)();
    const filterPostsByBoard19 = new Function(`${extractFn18('filterPostsByBoard')}; return filterPostsByBoard;`)();
    const loadOlderPosts = (st, fetchStub, insertStub) => new Function('state', 'fetchFeedPage', 'filterPostsByBoard', 'feedCursorOf', 'mergeOlderFeed', 'insertFeedCards', 'syncFeedOlderButton', 'FEED_PAGE_SIZE', 'FEED_MAX_AUTO_PAGES',
      `${extractFn18('loadOlderPosts')}; return loadOlderPosts;`)(st, fetchStub, filterPostsByBoard19, feedCursorOf19, mergeOlderFeed19, insertStub, () => {}, 30, 5);
    const p19 = (id, ts, board) => ({ id, ts, board: board || 'general' });
    // 页 1 全被板块过滤、页 2 有可见卡：自动续拉 2 次，游标走到页 2 最老一条。
    {
      const pages = [
        { posts: [p19('o1', 't', 'other'), p19('o2', 't', 'other')], has_more: true },
        { posts: [p19('o3', 't', 'general'), p19('o4', 't', 'general')], has_more: true },
      ];
      let fetchCount = 0;
      const fakeFetch = async (opts) => {
        fetchCount += 1;
        assert.strictEqual(opts.before_id, fetchCount === 1 ? 'c0' : 'o1', `第 ${fetchCount} 次应以正确游标拉`);
        assert.strictEqual(opts.limit, 30, '每页应拉 30 条');
        return pages[fetchCount - 1];
      };
      const st = { feedLoadingOlder: false, feedHasMore: true, feedCursor: 'c0', postsBoard: 'general', feedPosts: [p19('n1', 't', 'general')] };
      let inserted = null;
      let anchor = null;
      await loadOlderPosts(st, fakeFetch,
        (posts, beforeId) => { inserted = posts; anchor = beforeId; })();
      assert.strictEqual(fetchCount, 2, '整页被滤掉时应自动续拉');
      assert.deepStrictEqual(inserted.map((x) => x.id), ['o3', 'o4'], '只应插入可见卡');
      assert.strictEqual(st.feedCursor, 'o3', '游标应走到最后一页最老一条');
      assert.strictEqual(st.feedHasMore, true, 'has_more 应跟最后一页');
      assert.deepStrictEqual(st.feedPosts.map((x) => x.id), ['o3', 'o4', 'n1'], '更早页应插到现有流之前');
      assert.strictEqual(anchor, 'n1', 'DOM 插入锚点应是第一张现有流卡');
      assert.strictEqual(st.feedLoadingOlder, false, '翻页结束应清 loading');
      ok('loadOlderPosts：整页被滤自动续拉、游标跟进、锚点正确');
    }
    // 5 页全被滤掉：停在 5 次，不无限拉。
    {
      let fetchCount = 0;
      const fakeFetch = async () => { fetchCount += 1; return { posts: [p19('z' + fetchCount, 't', 'other')], has_more: true }; };
      const st = { feedLoadingOlder: false, feedHasMore: true, feedCursor: 'c0', postsBoard: 'general', feedPosts: [] };
      let inserted = null;
      await loadOlderPosts(st, fakeFetch,
        (posts) => { inserted = posts; })();
      assert.strictEqual(fetchCount, 5, '最多自动续拉 5 页');
      assert.strictEqual(inserted, null, '全被滤掉时不应插卡');
      assert.strictEqual(st.feedHasMore, true, 'has_more 仍真时按钮应保留');
      ok('loadOlderPosts：5 页上限，不无限拉');
    }
    // 回归（v0.3.14 走查真 bug）：游标取错最新一条时，再拉一页 30 条里 29 条
    // 与已显示重叠、只剩 1 张新卡。游标=最老一条时，再拉一页应得 30 张全新卡。
    {
      const shown = Array.from({ length: 30 }, (_, i) => p19('n' + String(i + 1).padStart(2, '0'), 't', 'general'));
      const fresh = Array.from({ length: 30 }, (_, i) => p19('o' + String(i + 1).padStart(2, '0'), 't', 'general'));
      let fetchCount = 0;
      let seenBeforeId = null;
      const fakeFetch = async (opts) => {
        fetchCount += 1;
        seenBeforeId = opts.before_id;
        assert.strictEqual(opts.limit, 30, '每页应拉 30 条');
        return { posts: fresh, has_more: true };
      };
      const st = { feedLoadingOlder: false, feedHasMore: true, feedCursor: 'n01', postsBoard: 'general', feedPosts: shown.slice() };
      let inserted = null;
      let anchor = null;
      await loadOlderPosts(st, fakeFetch,
        (posts, beforeId) => { inserted = posts; anchor = beforeId; })();
      assert.strictEqual(fetchCount, 1, '一页可见卡只需拉一次');
      assert.strictEqual(seenBeforeId, 'n01', '应以已显示最老一条当游标');
      assert.deepStrictEqual(inserted.map((x) => x.id), fresh.map((x) => x.id), '应插入整页 30 张新卡');
      const shownIds = new Set(shown.map((x) => x.id));
      assert.strictEqual(inserted.filter((x) => shownIds.has(x.id)).length, 0, '新一页与已显示零重叠');
      assert.strictEqual(st.feedPosts.length, 60, '合并后应有 60 张卡');
      assert.strictEqual(st.feedCursor, 'o01', '游标应走到新页最老一条');
      assert.strictEqual(anchor, 'n01', 'DOM 插入锚点应是第一张现有流卡');
      ok('loadOlderPosts 回归：游标=最老时再拉一页得 30 张全新卡、零重叠');
    }
  }

  console.log('== 20. v0.3.15：通知中心（@提醒）/ 只读设置 / 离线发件箱 / 草稿 ==');
  const rendererSrc20 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
  const mainSrc20 = fs.readFileSync(path.join(__dirname, '..', 'main.js'), 'utf8');
  const preloadSrc20 = fs.readFileSync(path.join(__dirname, '..', 'preload.js'), 'utf8');
  const html20 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'index.html'), 'utf8');
  const css20 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'styles.css'), 'utf8');
  const outboxSrc20 = fs.readFileSync(path.join(__dirname, '..', 'lib', 'outbox.js'), 'utf8');
  const extractFn20 = (name) => {
    const m = rendererSrc20.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
    assert.ok(m, `renderer.js 应包含 ${name}`);
    return m[0];
  };
  const { Outbox, createClientId, isNetworkError } = require('../lib/outbox');
  const { ForumApiError } = require('../lib/forumClient');
  tr.setViewerLang('zh');
  const ui20 = (k) => tr.ui(k);
  const uiFmt20 = (...a) => tr.uiFmt(...a);
  const esc20 = new Function(`${extractFn20('esc')}; return esc;`)();
  // 假 localStorage（带 key/length，供 countDrafts 用）；经 win 参数注入，不动全局。
  const fakeLS20 = {
    _m: new Map(),
    getItem(k) { return this._m.has(k) ? this._m.get(k) : null; },
    setItem(k, v) { this._m.set(k, String(v)); },
    removeItem(k) { this._m.delete(k); },
    key(i) { const ks = [...this._m.keys()]; return i < ks.length ? ks[i] : null; },
    get length() { return this._m.size; },
  };
  const fakeWin20 = { localStorage: fakeLS20 };
  // 20a. 通知纯函数：@过滤 / 未读计数（水位边界）/ 动词 / 跳转目标。
  {
    const filterMentionNotifications = new Function(`${extractFn20('filterMentionNotifications')}; return filterMentionNotifications;`)();
    const notifUnreadCount = new Function(`${extractFn20('notifUnreadCount')}; return notifUnreadCount;`)();
    const notifVerb = new Function('ui', `${extractFn20('notifVerb')}; return notifVerb;`)(ui20);
    const notifTarget = new Function(`${extractFn20('notifTarget')}; return notifTarget;`)();
    const mixed = [
      { id: 'mention:p1', type: 'mention', ts: '2026-11-25T10:00:00.000Z', actor: { name: 'a', type: 'human' }, postId: 'p1', preview: '@你' },
      { id: 'reply:p1', type: 'reply', ts: '2026-11-25T11:00:00.000Z', actor: { name: 'b', type: 'human' }, postId: 'p1' },
      { id: 'mention-msg:m1', type: 'mention', ts: '2026-11-25T12:00:00.000Z', actor: { name: 'c', type: 'human' }, groupId: 'g1' },
      { id: 'like:p1', type: 'like', ts: '2026-11-25T13:00:00.000Z', actor: { name: 'd', type: 'human' }, postId: 'p1' },
      { id: 'follow:x', type: 'follow', ts: '2026-11-25T14:00:00.000Z', actor: { name: 'e', type: 'human' } },
    ];
    const mentions = filterMentionNotifications(mixed);
    assert.deepStrictEqual(mentions.map((x) => x.id), ['mention:p1', 'mention-msg:m1'], '@过滤应只留 mention（含帖子 @ 与群消息 @）');
    assert.deepStrictEqual(filterMentionNotifications(null), [], '空列表应回空数组');
    // 未读计数：水位为空（首跑）全计未读；水位在中间只计晚于水位的；
    // ts 等于水位不算未读；时间戳坏的计未读；水位本身坏的全计未读。
    assert.strictEqual(notifUnreadCount(mentions, null), 2, '首跑无水位应全计未读');
    assert.strictEqual(notifUnreadCount(mentions, '2026-11-25T11:00:00.000Z'), 1, '水位 11:00 时只有 12:00 的 @ 未读');
    assert.strictEqual(notifUnreadCount(mentions, '2026-11-25T12:00:00.000Z'), 0, 'ts 等于水位不算未读（已读过）');
    assert.strictEqual(notifUnreadCount(mentions, '2026-11-26T00:00:00.000Z'), 0, '水位晚于全部应为 0');
    assert.strictEqual(notifUnreadCount([{ type: 'mention', ts: 'not-a-date' }], '2026-11-25T11:00:00.000Z'), 1, '时间戳坏的 @ 应计未读');
    assert.strictEqual(notifUnreadCount(mentions, 'not-a-date'), 2, '水位坏时应全计未读');
    assert.strictEqual(notifVerb({ type: 'mention' }), '@ 了你', '@动词应与网页版同口径');
    assert.strictEqual(notifVerb({ type: 'like' }), '', '非 @ 类型不动词');
    assert.deepStrictEqual(notifTarget(mixed[0]), { kind: 'post', id: 'p1' }, '有 postId 应跳帖子');
    assert.deepStrictEqual(notifTarget(mixed[2]), { kind: 'group', id: 'g1' }, '有 groupId 应进群聊');
    assert.deepStrictEqual(notifTarget(mixed[4]), { kind: 'profile', name: 'e' }, '无 postId/groupId 应看 @人资料卡');
    assert.deepStrictEqual(notifTarget(null), { kind: 'profile', name: '' }, '空条目应回空资料卡目标');
    ok('通知纯函数：@过滤只留 mention / 未读水位边界 / 动词 / 跳转目标');
  }
  // 20j. 帖子目标 @提醒（v0.3.15 修复轮）：mock 一条帖子 @，列表应渲染出
  // post 目标，跳转判定应是帖子页。走查 live 数据里 20 条 @ 全是群聊提醒、
  // 没有帖子目标条目可点，故跳转用单元测试覆盖；驱动遇无帖子目标时跳过。
  {
    const uiFn = new Function(`${extractFn20('ui')}; return ui;`)();
    const escFn = new Function(`${extractFn20('esc')}; return esc;`)();
    const avatarClassFn = new Function(`${extractFn20('avatarClass')}; return avatarClass;`)();
    const avatarInitialFn = new Function(`${extractFn20('avatarInitial')}; return avatarInitial;`)();
    const avatarHtmlFn = new Function(`${extractFn20('avatarHtml')}; return avatarHtml;`)();
    const fmtTimeFn = new Function(`${extractFn20('fmtTime')}; return fmtTime;`)();
    const notifTargetFn = new Function(`${extractFn20('notifTarget')}; return notifTarget;`)();
    const notifVerbFn = new Function('ui', `${extractFn20('notifVerb')}; return notifVerb;`)(uiFn);
    const notifJumpTargetFn = new Function(`${extractFn20('notifJumpTarget')}; return notifJumpTarget;`)();
    const renderNotifListFn = new Function(`${extractFn20('renderNotifList')}; return renderNotifList;`)();
    const boxEl = {
      _html: '',
      set innerHTML(v) { this._html = v; },
      get innerHTML() { return this._html; },
    };
    globalThis.window = { ChimeraTranslate: tr };
    globalThis.document = {
      getElementById: (id) => (id === 'notifList' ? boxEl : null),
      querySelectorAll: () => [],
    };
    globalThis.$ = (id) => globalThis.document.getElementById(id);
    globalThis.ui = uiFn;
    globalThis.esc = escFn;
    globalThis.avatarClass = avatarClassFn;
    globalThis.avatarInitial = avatarInitialFn;
    globalThis.avatarHtml = avatarHtmlFn;
    globalThis.fmtTime = fmtTimeFn;
    globalThis.notifTarget = notifTargetFn;
    globalThis.notifVerb = notifVerbFn;
    globalThis.state = {
      notif: {
        items: [{ id: 'mention:p1', type: 'mention', ts: '2026-11-25T10:00:00.000Z', actor: { name: 'a', type: 'human' }, postId: 'p1', preview: '@你看这个帖子' }],
        loaded: true,
        readTs: null,
      },
    };
    try {
      tr.setViewerLang('zh');
      renderNotifListFn();
      assert.ok(boxEl.innerHTML.includes('data-target-kind="post"'), '帖子 @ 应渲染为 post 目标');
      assert.ok(boxEl.innerHTML.includes('data-target-id="p1"'), '帖子 @ 应带帖子 id');
      assert.ok(boxEl.innerHTML.includes('@ 了你'), '帖子 @ 仍应带 @动词');
      assert.deepStrictEqual(notifJumpTargetFn('post', 'p1', ''), { pane: 'posts' }, 'post 目标应跳帖子页');
      assert.deepStrictEqual(notifJumpTargetFn('group', 'g1', ''), { conv: ['group', 'g1'] }, '群聊目标应进群');
      assert.deepStrictEqual(notifJumpTargetFn('profile', '', 'e'), { profile: 'e' }, '资料卡目标应看 @人');
      assert.strictEqual(notifJumpTargetFn('group', '', ''), null, '群聊无 id 应无跳转');
      assert.strictEqual(notifJumpTargetFn('profile', '', ''), null, '资料卡无名字应无跳转');
      ok('帖子 @ 渲染 post 目标 + 跳转判定帖子页/群聊/资料卡');
    } finally {
      delete globalThis.window;
      delete globalThis.document;
      delete globalThis.$;
      delete globalThis.ui;
      delete globalThis.esc;
      delete globalThis.avatarClass;
      delete globalThis.avatarInitial;
      delete globalThis.avatarHtml;
      delete globalThis.fmtTime;
      delete globalThis.notifTarget;
      delete globalThis.notifVerb;
      delete globalThis.state;
      tr._resetForTest();
    }
  }
  // 20k. 顶栏收紧带子（v0.3.15 修复轮，v0.3.16 上提到 1500px，v0.3.18 上提到 1900px）：1280 英文下铃铛/齿轮各占 46px，
  // 导航容器只剩 345px、7 项共需 433px，差 88px。1101–1900px 带子收回约
  // 120px（右侧 ghost 按钮 5×12 + 搜索框 40 + 两侧 gap 20），容器回到约
  // 465px。带子从 1101px 起，1024px 的规则原样不动。
  // v0.3.17 修复轮带子又收紧一次（ghost 6px、搜索 90px、右侧 gap 4px，
  // 见 §23），以下规则断言按当前 CSS；算术仍是 v0.3.15 那轮的快照。
  // v0.3.18 补一条 ID 级覆盖：#themeToggle/#langToggle 自 v0.3.1/v0.3.4 起就有
  // padding: 7px 10px 的 ID 规则，特异度压过带子的 .btn.ghost 类规则，所以
  // v0.3.17 的 ghost 收紧只对铃铛/设置/退出三钮生效（实收 12px 不是 20px，
  // 正是 §23 理论 48px、实收 41px 差的那 7px）；带子内须有同级 ID 规则把它俩也收到 6px。
  {
    const bandAt = css20.indexOf('@media (min-width: 1101px) and (max-width: 1900px)');
    assert.ok(bandAt >= 0, '应有 1101–1900px 顶栏收紧带子');
    const bandEnd = css20.indexOf('}', css20.indexOf('#globalSearchInput { width: 90px; }', bandAt)) + 1;
    const band = css20.slice(bandAt, bandEnd);
    assert.ok(band.includes('.top-right .btn.ghost { padding: 7px 6px; }'), '带子内右侧 ghost 按钮应收紧到 6px 内边距');
    assert.ok(band.includes('#themeToggle, #langToggle { padding: 7px 6px; }'), '带子内主题/语言按钮应有 ID 级覆盖收到 6px 内边距（ID 规则压过类规则，见注释）');
    assert.ok(band.includes('#globalSearchInput { width: 90px; }'), '带子内搜索框应收到 90px');
    assert.ok(band.includes('#topbar { gap: 6px; }'), '带子内顶栏 gap 应收到 6px');
    assert.ok(band.includes('.top-right { gap: 4px; }'), '带子内右侧 gap 应收到 4px');
    const base1400 = css20.slice(css20.indexOf('@media (max-width: 1900px)'), bandAt);
    assert.ok(base1400.includes('#globalSearchInput { width: 140px; }'), '1900 断点仍应是搜索框 140px（1024 不动）');
    assert.ok(base1400.includes('#topbar { gap: 8px; }'), '1900 断点仍应是顶栏 gap 8px（1024 不动）');
    // 算术（探针 2026-11-27 真机实测）：7 项宽 54+51+50+58+89+50+81=433；
    // 收回 = ghost 5×12 + 搜索 40 + gap 20 = 120；345+120=465 ≥ 433。
    const navNeed1280en = 54 + 51 + 50 + 58 + 89 + 50 + 81;
    const container1280en = 345;
    const recovered = 5 * 12 + 40 + 20;
    assert.strictEqual(navNeed1280en, 433, '1280 英文 7 项共需 433px（探针实测）');
    assert.ok(container1280en + recovered >= navNeed1280en, `收紧后容器应放得下 7 项（${container1280en}+${recovered}≥${navNeed1280en}）`);
    ok('顶栏收紧带子：规则齐全、1024 原样、1280 英文算术放得下');
  }
  // 20b. ForumClient 通知接口（mock 论坛）：全类型回包 + 标记已读清零 + 未登录 401。
  {
    const mock20 = await startMockForum();
    try {
      const mc = new ForumClient(mock20.url);
      const list = await mc.getNotifications('test-token');
      assert.strictEqual(list.notifications.length, 4, '应回 4 条通知（全类型）');
      assert.strictEqual(list.unreadCount, 4, '未读应为 4（水位未设）');
      assert.strictEqual(list.notifications[0].type, 'like', '应按 ts 倒序（最新在前）');
      const read = await mc.markNotificationsRead('test-token');
      assert.ok(read.notifReadTs, '标记已读应回 notifReadTs');
      const list2 = await mc.getNotifications('test-token');
      assert.strictEqual(list2.unreadCount, 0, '标记已读后未读应为 0');
      await assert.rejects(mc.getNotifications(null), (e) => e && e.status === 401, '未登录取通知应回 401');
      await assert.rejects(mc.markNotificationsRead(null), (e) => e && e.status === 401, '未登录标记已读应回 401');
    } finally { mock20.close(); }
    ok('ForumClient 通知接口：全类型回包 + 标记已读清零 + 未登录 401');
  }
  // 20c. Outbox：入箱去重 + FIFO 串行 + 网络错停发 + HTTP 错标 dead 续发 + 丢弃规则 + 单航班。
  {
    const makeStore = () => ({ state: { outbox: [] }, saves: 0, save() { this.saves += 1; } });
    // 入箱去重 + FIFO 成功全发 + status 计数。
    {
      const store = makeStore();
      const sent = [];
      const ob = new Outbox({ store, send: async (e) => { sent.push(e.clientId); }, onChange: () => {} });
      const a = ob.enqueue({ clientId: 'ob-a', kind: 'dm', target: { peer: 'd1' }, body: 'a' });
      ob.enqueue({ clientId: 'ob-b', kind: 'group-message', target: { groupId: 'g1' }, body: 'b' });
      const dup = ob.enqueue({ clientId: 'ob-a', kind: 'dm', target: { peer: 'd1' }, body: 'a-again' });
      assert.strictEqual(dup, a, '同 clientId 重复入箱应回原条目');
      assert.strictEqual(ob.entries.length, 2, '重复入箱不应多占一条');
      assert.deepStrictEqual(ob.status().pending, 2, 'status.pending 应为 2');
      const r = await ob.flush();
      assert.deepStrictEqual(sent, ['ob-a', 'ob-b'], 'flush 应按入箱顺序串行发');
      assert.strictEqual(r.flushed, 2, '应发 2 条');
      assert.strictEqual(ob.entries.length, 0, '成功即删条目，不留重发');
      assert.ok(store.saves > 0, '变动应落盘');
      ok('Outbox：入箱去重 + FIFO 串行 + 成功即删 + status 计数');
    }
    // 网络错停发（后一条不碰）+ HTTP 错标 dead 续发下一条；dead 不再自动重试。
    {
      const store = makeStore();
      const sent = [];
      const ob = new Outbox({
        store,
        send: async (e) => {
          sent.push(e.clientId);
          if (e.clientId === 'ob-net') throw new Error('ECONNREFUSED');
          if (e.clientId === 'ob-http') throw new ForumApiError('HTTP 500', 500);
        },
        onChange: () => {},
      });
      ob.enqueue({ clientId: 'ob-http', kind: 'post', target: {}, body: 'x' });
      ob.enqueue({ clientId: 'ob-ok', kind: 'post', target: {}, body: 'y' });
      ob.enqueue({ clientId: 'ob-net', kind: 'post', target: {}, body: 'z' });
      ob.enqueue({ clientId: 'ob-after', kind: 'post', target: {}, body: 'w' });
      const r = await ob.flush();
      assert.deepStrictEqual(sent, ['ob-http', 'ob-ok', 'ob-net'], '网络错应停发，后一条不应尝试');
      assert.strictEqual(r.flushed, 1, '只应发成功 1 条');
      const byId = Object.fromEntries(ob.entries.map((e) => [e.clientId, e]));
      assert.strictEqual(byId['ob-http'].dead, true, 'HTTP 错应标 dead（永久失败，不再自动重试）');
      assert.match(byId['ob-http'].lastError, /500/, 'dead 条目应记 lastError');
      assert.strictEqual(byId['ob-net'].dead, false, '网络错不应标 dead');
      assert.strictEqual(byId['ob-net'].attempts, 1, '网络错条目 attempts 应 +1');
      assert.strictEqual(byId['ob-after'].attempts, 0, '停发后一条 attempts 应为 0');
      assert.strictEqual(ob.status().failed, 1, 'failed 应为 1');
      sent.length = 0;
      const r2 = await ob.flush();
      assert.deepStrictEqual(sent, ['ob-net'], 'dead 条目不应再自动重试');
      assert.strictEqual(r2.flushed, 0, '网络仍错时 flushed 应为 0');
      ok('Outbox：网络错停发 + HTTP 错标 dead 续发 + dead 不再自动重试');
    }
    // 丢弃规则：发送中拒删，删后不在，删不存在回 false。
    {
      const store = makeStore();
      let release = null;
      const gate = new Promise((resolve) => { release = resolve; });
      const ob = new Outbox({ store, send: async () => { await gate; }, onChange: () => {} });
      ob.enqueue({ clientId: 'ob-g', kind: 'dm', target: {}, body: 'g' });
      const flushing = ob.flush();
      await new Promise((r) => setTimeout(r, 20)); // 等 send 挂进 gate
      assert.strictEqual(ob.discard('ob-g'), false, '发送中的条目应拒删');
      assert.strictEqual(ob.entries.length, 1, '拒删后条目应还在');
      release();
      await flushing;
      assert.strictEqual(ob.entries.length, 0, '发送成功后条目已删');
      assert.strictEqual(ob.discard('ob-g'), false, '删不存在的条目应回 false');
      ob.enqueue({ clientId: 'ob-h', kind: 'dm', target: {}, body: 'h' });
      assert.strictEqual(ob.discard('ob-h'), true, '待发条目应能丢弃');
      assert.strictEqual(ob.entries.length, 0, '丢弃后条目应没');
      ok('Outbox 丢弃：发送中拒删 / 不存在回 false / 待发可删');
    }
    // 单航班 + clientId + isNetworkError。
    {
      const store = makeStore();
      let calls = 0;
      const ob = new Outbox({
        store,
        send: async () => { calls += 1; await new Promise((r) => setTimeout(r, 30)); },
        onChange: () => {},
      });
      ob.enqueue({ clientId: 'ob-s', kind: 'dm', target: {}, body: 's' });
      const p1 = ob.flush();
      const p2 = await ob.flush();
      assert.strictEqual(p2.skipped, true, '并发 flush 应直接跳过（单航班）');
      await p1;
      assert.strictEqual(calls, 1, '单航班下 send 只应跑一次');
      assert.ok(/^ob-[0-9a-f-]{36}$/.test(createClientId()), 'clientId 应为 ob- + UUID');
      assert.notStrictEqual(createClientId(), createClientId(), 'clientId 应唯一');
      assert.strictEqual(isNetworkError(new ForumApiError('HTTP 500', 500)), false, 'HTTP 错不是网络错');
      assert.strictEqual(isNetworkError(new Error('ECONNREFUSED')), true, '无响应的错是网络错');
      ok('Outbox 单航班 + clientId 唯一 + isNetworkError 分流');
    }
  }
  // 20d. Store：notif 水位 + outbox 条目持久化；旧文件缺键时补默认值。
  {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-store20-'));
    const file = path.join(tmpDir, 'state.json');
    try {
      const st1 = new Store(file);
      assert.deepStrictEqual(st1.state.outbox, [], '新 store 的 outbox 应为空数组');
      assert.strictEqual(st1.state.notif.readTs, null, '新 store 的 notif.readTs 应为 null');
      assert.strictEqual(st1.state.notif.mentionEnabled, true, '新 store 的 mentionEnabled 默认开');
      st1.state.notif.readTs = '2026-11-26T10:00:00.000Z';
      st1.state.notif.mentionEnabled = false;
      st1.state.outbox.push({ clientId: 'ob-x1', kind: 'dm', target: { peer: 'd1' }, body: 'hello', title: '', attachments: [], createdAt: '2026-11-26T10:00:00.000Z', attempts: 0, dead: false, lastError: null });
      st1.save();
      const st2 = new Store(file);
      assert.strictEqual(st2.state.notif.readTs, '2026-11-26T10:00:00.000Z', 'readTs 应持久化');
      assert.strictEqual(st2.state.notif.mentionEnabled, false, 'mentionEnabled 应持久化');
      assert.strictEqual(st2.state.outbox.length, 1, '发件箱条目应持久化');
      assert.strictEqual(st2.state.outbox[0].clientId, 'ob-x1', '条目 clientId 应保留');
      fs.writeFileSync(file, JSON.stringify({ version: 1 }));
      const st3 = new Store(file);
      assert.deepStrictEqual(st3.state.outbox, [], '旧文件缺 outbox 时应补空数组');
      assert.strictEqual(st3.state.notif.readTs, null, '旧文件缺 notif 时应补默认值');
    } finally { fs.rmSync(tmpDir, { recursive: true, force: true }); }
    ok('Store：notif 水位 + outbox 条目持久化，旧文件缺键补默认值');
  }
  // 20e. 草稿：按作用域存取、空值清键、跨作用域隔离、计数只数非空草稿、无 storage 不抛错。
  {
    const draftFns = new Function('window', `
      ${extractFn20('draftKey')};
      ${extractFn20('draftStorage')};
      ${extractFn20('loadDraft')};
      ${extractFn20('saveDraft')};
      ${extractFn20('clearDraft')};
      ${extractFn20('countDrafts')};
      return { draftKey, loadDraft, saveDraft, clearDraft, countDrafts };
    `)(fakeWin20);
    const { draftKey, loadDraft, saveDraft, clearDraft, countDrafts } = draftFns;
    assert.strictEqual(draftKey('conv:group:g1'), 'chimera_draft:conv:group:g1', '草稿键应带 chimera_draft: 前缀');
    assert.strictEqual(loadDraft('conv:group:g1', fakeWin20), '', '没存过应回空串');
    saveDraft('conv:group:g1', '还没发', fakeWin20);
    saveDraft('conv:dm:d1', '私信草稿', fakeWin20);
    saveDraft('post:general', '帖子草稿', fakeWin20);
    assert.strictEqual(loadDraft('conv:group:g1', fakeWin20), '还没发', '群聊草稿应按作用域存取');
    assert.strictEqual(loadDraft('conv:dm:d1', fakeWin20), '私信草稿', '私信草稿应与群聊隔离');
    assert.strictEqual(countDrafts(fakeWin20), 3, '应数出 3 个非空草稿');
    saveDraft('conv:group:g1', '', fakeWin20);
    assert.strictEqual(loadDraft('conv:group:g1', fakeWin20), '', '空值应清键');
    assert.strictEqual(countDrafts(fakeWin20), 2, '清键后计数应减');
    clearDraft('post:general', fakeWin20);
    assert.strictEqual(loadDraft('post:general', fakeWin20), '', 'clearDraft 应清键');
    assert.strictEqual(loadDraft('conv:dm:d1', { localStorage: null }), '', '无 storage 时 loadDraft 应回空串不抛错');
    assert.strictEqual(countDrafts({ localStorage: null }), 0, '无 storage 时 countDrafts 应为 0');
    saveDraft('x', 'y', { localStorage: null }); // 无 storage 时 saveDraft 不应抛错
    const draftKeyForOpen = new Function('state', `${extractFn20('draftKeyForOpen')}; return draftKeyForOpen;`)({ open: { kind: 'group', id: 'g1' } });
    assert.strictEqual(draftKeyForOpen(), 'conv:group:g1', '会话草稿键应按 kind:id 分作用域');
    const draftKeyForOpenNone = new Function('state', `${extractFn20('draftKeyForOpen')}; return draftKeyForOpen;`)({ open: null });
    assert.strictEqual(draftKeyForOpenNone(), null, '未打开会话时草稿键应为 null');
    ok('草稿：按作用域存取隔离 / 空值清键 / 计数只数非空 / 无 storage 不抛错');
  }
  // 20f. outboxStatusText：空箱无文案、待发计数、flash 前缀、失败条目带丢弃按钮与预览截断。
  {
    const outboxStatusText = new Function('ui', 'uiFmt', 'esc', `${extractFn20('outboxStatusText')}; return outboxStatusText;`)(ui20, uiFmt20, esc20);
    assert.strictEqual(outboxStatusText({ pending: 0, failed: 0, total: 0, entries: [] }, false), '', '空箱应无状态行');
    assert.strictEqual(outboxStatusText({ pending: 1, failed: 0, total: 1, entries: [{ clientId: 'ob-1', kind: 'dm', body: 'hi', dead: false }] }, false), '待发送 1 条', '待发 1 条应显示计数');
    assert.strictEqual(outboxStatusText({ pending: 2, failed: 0, total: 2, entries: [] }, true), '已存入发件箱，联网后自动发送 · 待发送 2 条', 'flash 应当前缀');
    const failedText = outboxStatusText({ pending: 0, failed: 1, total: 1, entries: [{ clientId: 'ob-9', kind: 'post', body: '这条没发出去', dead: true, lastError: 'HTTP 500' }] }, false);
    assert.ok(failedText.includes('发送失败 1 条'), '失败应显示计数');
    assert.ok(failedText.includes('data-discard-outbox="ob-9"'), '失败条目应带本地丢弃按钮');
    assert.ok(failedText.includes('这条没发出去'), '失败条目应显示正文预览');
    assert.ok(failedText.includes('HTTP 500'), '失败条目应显示 lastError');
    const longBody = '一'.repeat(40);
    const truncText = outboxStatusText({ pending: 0, failed: 1, total: 1, entries: [{ clientId: 'ob-8', kind: 'post', body: longBody, dead: true, lastError: null }] }, false);
    assert.ok(!truncText.includes(longBody), '预览应截到 30 字');
    assert.ok(truncText.includes('一'.repeat(30)), '预览应保留前 30 字');
    ok('outboxStatusText：空箱无文案 / 待发计数 / flash 前缀 / 失败带丢弃 + 预览截断');
  }
  // 20g. 接线：HTML 入口/面板/状态行、CSS、main/preload IPC、渲染层调用点齐全。
  {
    for (const id of ['notifBell', 'notifBadge', 'settingsBtn', 'pane-notif', 'pane-settings', 'notifMarkReadBtn', 'notifRefresh', 'notifList', 'settingsRefresh', 'settingsBody', 'chatOutboxLine', 'feedOutboxLine']) {
      assert.ok(html20.includes(`id="${id}"`), `index.html 应含 ${id}`);
    }
    assert.ok(html20.includes('v0.3.15'), '侧栏脚应标 v0.3.15');
    for (const cls of ['.notif-bell {', '.notif-badge {', '.notif-item {', '.outbox-line {', '.settings-grid {']) {
      assert.ok(css20.includes(cls), `styles.css 应含 ${cls}`);
    }
    for (const api of ['notifications: () =>', 'markNotificationsRead: () =>', 'outboxStatus: () =>', 'outboxDiscard: (clientId)', 'onOutboxStatus: (fn)']) {
      assert.ok(preloadSrc20.includes(api), `preload 应暴露 ${api}`);
    }
    assert.ok(preloadSrc20.includes('onUpdateStaged: (fn)'), 'preload 的 onUpdateStaged 不应被顶掉（v0.3.15 回归守卫）');
    for (const ipc of ["api:notifications", "api:notifications-read", "api:outbox-status", "api:outbox-discard"]) {
      assert.ok(mainSrc20.includes(ipc), `main 应注册 ${ipc}`);
    }
    assert.ok(mainSrc20.includes('function sendOrQueue('), 'main 应有 sendOrQueue（网络错才入箱）');
    assert.ok(mainSrc20.includes('initOutbox()'), 'main 应在 whenReady 里初始化发件箱');
    assert.ok(mainSrc20.includes('armOutboxFlushTimer'), 'main 应有发件箱 60 秒定时器');
    assert.ok(mainSrc20.includes('outbox.flush()'), '重连时应触发发件箱 flush');
    for (const fn of ['function loadNotifications()', 'function renderNotifList()', 'function loadSettings()', 'function renderSettings()', 'function refreshOutboxStatus()', 'function renderOutboxLine()', 'function queueFlash()']) {
      assert.ok(rendererSrc20.includes(fn), `renderer 应含 ${fn}`);
    }
    assert.ok(rendererSrc20.includes("showPane('notif')"), '铃铛应开通知面板');
    assert.ok(rendererSrc20.includes("showPane('settings')"), '齿轮应开设置面板');
    assert.ok(rendererSrc20.includes('60 * 1000'), '通知轮询应为 60 秒');
    assert.ok((rendererSrc20.match(/r\.queued/g) || []).length >= 4, '四条发送路径都应处理 queued 回包');
    assert.ok((rendererSrc20.match(/queueFlash\(\);/g) || []).length >= 4, '四条发送路径都应闪一下入箱提示');
    ok('接线：HTML 入口/面板/状态行 + CSS + main/preload IPC + 渲染层调用点齐全');
  }
  // 20h. 字典：本轮 43 个新 key 中英齐全、%s 个数一致（§12 已验全局 key 集相等）。
  {
    const keys20 = [
      'top.notifTitle', 'top.settingsTitle',
      'notif.title', 'notif.mentionOnly', 'notif.empty', 'notif.markRead', 'notif.refresh', 'notif.verbMention', 'notif.loadFail',
      'settings.title', 'settings.readonlyNote', 'settings.refresh', 'settings.loadFail', 'settings.account', 'settings.workspace', 'settings.notif', 'settings.outbox', 'settings.session', 'settings.name', 'settings.type', 'settings.level', 'settings.points', 'settings.bound', 'settings.unbound', 'settings.online', 'settings.offline', 'settings.mention', 'settings.mentionOn', 'settings.mentionOff', 'settings.bindState', 'settings.onlineState', 'settings.device', 'settings.fingerprint', 'settings.noAccount', 'settings.expires', 'settings.pending', 'settings.failed', 'settings.drafts',
      'outbox.queued', 'outbox.pending', 'outbox.failed', 'outbox.discard', 'outbox.failedTitle',
    ];
    assert.strictEqual(keys20.length, 43, '本轮新 key 应为 43 个');
    for (const k of keys20) {
      assert.ok(typeof tr._uiStrings.zh[k] === 'string' && tr._uiStrings.zh[k].length > 0, `中文字典应含 ${k}`);
      assert.ok(typeof tr._uiStrings.en[k] === 'string' && tr._uiStrings.en[k].length > 0, `英文字典应含 ${k}`);
      const cz = (tr._uiStrings.zh[k].match(/%s/g) || []).length;
      const ce = (tr._uiStrings.en[k].match(/%s/g) || []).length;
      assert.strictEqual(ce, cz, `${k} 中英 %s 个数应一致`);
    }
    ok(`字典：本轮 ${keys20.length} 个新 key 中英齐全、%s 个数一致`);
  }
  // 20i. 设置页只读：loadSettings 只调只读接口；渲染层无任何写操作入口；发件箱不碰 localStorage。
  {
    const loadSettingsSrc = extractFn20('loadSettings');
    for (const api of ['api.accounts', 'api.workspaceBindings', 'api.outboxStatus', 'session.get']) {
      assert.ok(loadSettingsSrc.includes(api), `loadSettings 应调只读接口 ${api}`);
    }
    for (const w of ['rotate', 'revoke', 'workspaceUnlink', 'updateMe', 'token/rotate']) {
      assert.ok(!loadSettingsSrc.includes(w), `loadSettings 不应含写操作 ${w}`);
    }
    assert.ok(!rendererSrc20.includes('workspace-unlink'), '渲染层不应调解绑');
    assert.ok(!rendererSrc20.includes('token/rotate'), '渲染层不应调换 token');
    assert.ok(!mainSrc20.includes("'api:settings"), 'main 不应有 settings 写 IPC');
    assert.ok(!outboxSrc20.includes('localStorage.'), '发件箱（已提交消息）不应读写 localStorage（草稿才用）');
    ok('设置页只读：只调只读接口、无写操作入口、发件箱与草稿存储分离');
  }
  // 20j. 面板与语言切换：showPane 开通知/设置面板并拉取；syncChrome 只重画不重拉；mention 事件实时刷徽标；草稿存取接线。
  {
    const showPaneSrc = extractFn20('showPane');
    assert.ok(showPaneSrc.includes("'pane-notif'"), 'showPane 应切通知面板');
    assert.ok(showPaneSrc.includes("'pane-settings'"), 'showPane 应切设置面板');
    assert.ok(showPaneSrc.includes('loadNotifications()'), '开通知面板应拉通知');
    assert.ok(showPaneSrc.includes('loadSettings()'), '开设置面板应拉设置');
    const syncChromeSrc = extractFn20('syncChrome');
    assert.ok(syncChromeSrc.includes('renderNotifList()'), 'syncChrome 应重画通知列表');
    assert.ok(syncChromeSrc.includes('renderSettings()'), 'syncChrome 应重画设置页');
    assert.ok(syncChromeSrc.includes('renderOutboxLine()'), 'syncChrome 应重画发件箱状态行');
    assert.ok(!syncChromeSrc.includes('loadNotifications()'), '语言切换不应重拉通知（只重画）');
    assert.ok(!syncChromeSrc.includes('loadSettings()'), '语言切换不应重拉设置（只重画）');
    assert.ok(rendererSrc20.includes('loadNotifications().catch(() => {})'), 'mention 事件到时应实时刷通知徽标');
    assert.ok(rendererSrc20.includes('loadDraft(draftKeyForOpen())'), '切会话应恢复该会话草稿');
    assert.ok(rendererSrc20.includes('clearDraft(draftKeyForOpen())'), '发送后应清会话草稿');
    assert.ok(rendererSrc20.includes("clearDraft('post:'"), '发帖后应清帖子草稿');
    // 回归：首开帖子页时框是空的，空串不能清键，否则跨重启的帖子草稿
    // 还没恢复就被删掉（v0.3.15 走查前自查发现）。
    assert.ok(showPaneSrc.includes("if (curPostDraft) saveDraft('post:'"), '切帖子页时框空不应存草稿（空串会误删未恢复的草稿）');
    ok('面板与语言切换：showPane 拉取 / syncChrome 只重画 / mention 实时刷徽标 / 草稿存取接线');
  }

  console.log('== 21. v0.3.16：1440px 英文顶栏溢出修复 ==');
  // 21a. 1440 英文顶栏（v0.3.16）：副标题藏到 1900px、收紧带子上提到 1900px；
  // 修前沙箱探针：容器 145px、7 项需 533px（12px 内边距）、副标题 260px 可见，
  // 只有首页可见；修后容器 586px、7 项需 449px（6px 内边距），余 137px。
  // 1280/1024 的规则与沙箱实测数与修前完全一致（回归）。
  // v0.3.17 修复轮带子又收紧一次（ghost 6px、搜索 90px、右侧 gap 4px，
  // 见 §23），沙箱 1440 英文容器 586→618px。
  // v0.3.18 带子上提至 1900px + 主题/语言按钮 ID 级覆盖收到 6px（见 §20k/§25），
  // 沙箱 1440 英文容器 601→617px（修前复测 601，§21 旧值 618 高出 17px，见 §23 注释），以下按当前数断言。
  {
    const m1500 = css20.match(/@media \(max-width: 1900px\) \{([\s\S]*?)\n\}/);
    assert.ok(m1500, '应有 1900px 断点（副标题藏到 1900px，覆盖 1440）');
    assert.ok(m1500[1].includes('.brand .sub { display: none; }'), '1900px 以下应藏品牌副标题');
    assert.ok(m1500[1].includes('.nav-btn { padding: 7px 6px; }'), '1900px 以下导航按钮应为 6px 内边距');
    assert.ok(m1500[1].includes('#globalSearchInput { width: 140px; }'), '1900px 以下搜索框应为 140px');
    const bandAt21 = css20.indexOf('@media (min-width: 1101px) and (max-width: 1900px)');
    assert.ok(bandAt21 >= 0, '收紧带子应为 1101–1900px（覆盖 1440，1024 不进带子）');
    const bandEnd21 = css20.indexOf('}', css20.indexOf('#globalSearchInput { width: 90px; }', bandAt21)) + 1;
    const band21 = css20.slice(bandAt21, bandEnd21);
    assert.ok(band21.includes('.top-right .btn.ghost { padding: 7px 6px; }'), '带子内右侧 ghost 按钮应收紧到 6px 内边距');
    assert.ok(band21.includes('#globalSearchInput { width: 90px; }'), '带子内搜索框应收到 90px');
    assert.ok(band21.includes('#topbar { gap: 6px; }'), '带子内顶栏 gap 应收到 6px');
    assert.ok(band21.includes('.top-right { gap: 4px; }'), '带子内右侧 gap 应收到 4px');
    // 算术（沙箱 xvfb Electron 真机探针 2026-11-27，登录态）：
    // 修前 1440 英文：品牌 362（含副标题 260）+ 右侧 877 + 两侧 gap 24 +
    // 顶栏 padding 32 = 1295，容器 1440-1295=145；7 项 65+61+59+68+101+60+91
    // =505，+7×4 gap=533，缺 388px。
    // 修后（v0.3.16）：副标题藏（-270，含 gap）、搜索 190→100（-90）、ghost 收紧
    // （347→311，-36）、gap 12/10→6（-44）、导航内边距 12→6（7 项 -84）；
    // 容器 145+441=586，7 项 53+49+47+56+89+48+79=421，+7×4 gap=449，余 137px。
    // v0.3.17 再收紧：ghost 8→6（-20）、搜索 100→90（-10）、右侧 gap 6→4
    // （-18），容器 586→618px，余 169px（沙箱探针实测，见 §23）。
    // v0.3.18 再收紧：主题/语言按钮 ID 覆盖 10→6（-16），容器 601→617px，
    // 余 168px（沙箱探针实测，见 §25；§21 旧值 618 高出 17px，见 §23 注释）。
    const navNeed1440en = 53 + 49 + 47 + 56 + 89 + 48 + 79 + 7 * 4;
    const container1440en = 617;
    assert.strictEqual(navNeed1440en, 449, '1440 英文 7 项共需 449px（6px 内边距，探针实测）');
    assert.ok(container1440en >= navNeed1440en, `修后 1440 英文容器应放得下 7 项（${container1440en}≥${navNeed1440en}，余 ${container1440en - navNeed1440en}px）`);
    // greedyNavFit 在 1440 英文输入下不应溢出（More 隐藏）。
    const greedyNavFit21 = new Function(`${extractFn20('greedyNavFit')}; return greedyNavFit;`)();
    assert.deepStrictEqual(
      greedyNavFit21([57, 53, 51, 60, 93, 52, 83], 617, 70),
      { visible: [0, 1, 2, 3, 4, 5, 6], overflow: [] },
      '1440 英文下 greedyNavFit 应 7 项全可见、More 隐藏');
    // 回归：1280 仍在带子内（验收线不受影响）、1024 仍在带子外、
    // 1000px 规则保留；1280/1024 的沙箱实测容器与修前一致（426/74 英文）。
    assert.ok(1101 <= 1280 && 1280 <= 1900, '带子应仍覆盖 1280（1280 验收线不受影响）');
    assert.ok(1024 < 1101, '带子下限 1101 应把 1024 排除在外（1024 More 行为不动）');
    assert.ok(css20.includes('@media (max-width: 1000px)'), '1000px 断点应保留');
    assert.ok(css20.includes('#sseText { display: none; }'), '1000px 以下状态文字仍应隐藏');
    ok('1440 英文顶栏：副标题藏到 1900px、带子上提、容器 617≥449、More 隐藏、1280/1024 规则未变');
  }

  console.log('== 22. v0.3.17：品牌名跟随语言（奇美拉 / Chimera）==');
  // 22a. 字典：brand.name 中英齐全；key 集仍完全一致（§12a 已全量断言，
  // 这里单点断言新 key 本身 + 加 key 后 key 集仍对齐）。
  {
    assert.strictEqual(tr._uiStrings.zh['brand.name'], '奇美拉', '中文品牌名应为 奇美拉');
    assert.strictEqual(tr._uiStrings.en['brand.name'], 'Chimera', '英文品牌名应为 Chimera');
    const zhKeys22 = Object.keys(tr._uiStrings.zh).sort();
    const enKeys22 = Object.keys(tr._uiStrings.en).sort();
    assert.deepStrictEqual(enKeys22, zhKeys22, '加 brand.name 后中英 key 集仍应完全一致');
    ok('字典 brand.name：中文 奇美拉 / 英文 Chimera，key 集仍一致');
  }
  // 22b. applyUiLang：品牌名文本 + logo alt 双语切换（假 document）。
  {
    const makeEl22 = (key) => ({
      _text: '', _attrs: { 'data-i18n': key, 'data-i18n-alt': key },
      get textContent() { return this._text; },
      set textContent(v) { this._text = v; },
      getAttribute(k) { return this._attrs[k] || null; },
      setAttribute(k, v) { this._attrs[k] = v; },
    });
    const textEls22 = [makeEl22('brand.name')];
    const altEls22 = [makeEl22('brand.name')];
    const doc22 = {
      querySelectorAll: (sel) => ({ '[data-i18n]': textEls22, '[data-i18n-alt]': altEls22 }[sel] || []),
      documentElement: { _attrs: {}, setAttribute(k, v) { this._attrs[k] = v; } },
    };
    tr.applyUiLang(doc22, 'en');
    assert.strictEqual(textEls22[0].textContent, 'Chimera', '英文下品牌名文本应为 Chimera');
    assert.strictEqual(altEls22[0]._attrs.alt, 'Chimera', '英文下品牌 logo alt 应为 Chimera');
    tr.applyUiLang(doc22, 'zh');
    assert.strictEqual(textEls22[0].textContent, '奇美拉', '切回中文品牌名文本应为 奇美拉');
    assert.strictEqual(altEls22[0]._attrs.alt, '奇美拉', '切回中文品牌 logo alt 应为 奇美拉');
    assert.doesNotThrow(() => tr.applyUiLang(null), 'doc 为 null 不应抛错');
    ok('applyUiLang：品牌名文本 + alt 双语切换');
  }
  // 22c. index.html 静态接线：顶栏品牌名、首页大标题、三个品牌 logo alt 全走 brand.name；
  // 登录标题早已是 data-i18n="login.title"（英文 Log in to Chimera），页脚跟版本号。
  {
    const html22 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'index.html'), 'utf8');
    assert.ok(html22.includes('<span class="brand-name" data-i18n="brand.name">'), '顶栏品牌名应走 data-i18n="brand.name"');
    assert.ok(html22.includes('<h1 data-i18n="brand.name">'), '首页大标题应走 data-i18n="brand.name"');
    assert.strictEqual((html22.match(/data-i18n-alt="brand\.name"/g) || []).length, 3, '三个品牌 logo img（顶栏/登录页/空会话）都应有 data-i18n-alt="brand.name"');
    assert.ok(html22.includes('<h1 data-i18n="login.title">'), '登录标题应仍走 data-i18n="login.title"');
    assert.ok(html22.includes('v0.3.18'), '页脚版本应为 v0.3.18');
    ok('index.html：顶栏品牌名 + 首页大标题 + 3 处 logo alt 全走 brand.name');
  }
  // 22d. renderer.js 动态空会话：alt 走 ui('brand.name')，随渲染时语言；
  // 数据内容不动：帖子正文里的“欢迎来到奇美拉”仍是数据，不进字典。
  {
    const rendererSrc22 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    assert.ok(rendererSrc22.includes(`alt="' + ui('brand.name') + '"`), '动态空会话 logo alt 应走 ui(\'brand.name\')');
    assert.ok(!rendererSrc22.includes('alt="奇美拉"'), 'renderer.js 不应再有写死的中文品牌 alt');
    const pkg22 = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));
    assert.strictEqual(pkg22.version, '0.3.18', 'package.json 版本应为 0.3.18');
    ok('renderer.js 动态 alt + package.json 0.3.18；数据内容未动');
  }

  console.log('== 23. v0.3.17 修复轮：1280px 英文顶栏溢出（品牌名变宽顶掉 Node）==');
  // 回归：v0.3.17 把品牌名改成跟语言（Chimera 比 奇美拉宽约 17px），
  // lenghao 1280 英文容器 421px、7 项需 433px，差 12px，Node 被顶进 More
  // （走查 Driver A 101/104，3 失败全是这一项；1440 英文 584/433 不受影响）。
  // 修法（CSS-only，带子内）：ghost 内边距 8px→6px（5 个 × 4px = 20px）、
  // 搜索框 100px→90px（10px）、右侧 gap 6px→4px（9 个间隙 × 2px = 18px），
  // 理论共收 48px。但 lenghao 重探针实测修后容器 462px（实收 41px，理论值
  // 多算 7px——三项里有一处没抠满，逐项对不上；以实测为准），余 29px。
  // v0.3.18 补记：7px 差额已定位——#themeToggle/#langToggle 自 v0.3.1/v0.3.4 起
  // 有 padding: 7px 10px 的 ID 规则，特异度压过带子的 .btn.ghost 类规则，v0.3.17
  // 的 ghost 收紧只对铃铛/设置/退出三钮生效（实收 12px 不是 20px）；本轮在带子内
  // 补同级 ID 覆盖收到 6px，补回 16px。另：v0.3.18 重探针测得同款修前代码沙箱
  // 1280 英文容器为 441px（§23 旧值 458 高出 17px，应是当时探针的右侧状态差；
  // 以本次重探针为准），补回 16px 后为 457px，与旧值差 1px，取实测 457；中文
  // 1280 修后为 544px（旧值 562 高出 18px，同上），修前中文未单独重测，544-16
  // 推得修前约 528px，不作断言依据。
  {
    // 回归锁定：修前 lenghao 1280 英文确实放不下（421 < 433，差 12px）。
    const container1280enBefore = 421;
    const navNeed1280enLenghao = 54 + 51 + 50 + 58 + 89 + 50 + 81;
    assert.strictEqual(navNeed1280enLenghao, 433, 'lenghao 1280 英文 7 项共需 433px（走查探针实测）');
    assert.ok(container1280enBefore < navNeed1280enLenghao, `修前容器应放不下 7 项（${container1280enBefore}<${navNeed1280enLenghao}，差 ${navNeed1280enLenghao - container1280enBefore}px，Node 进 More）`);
    // 修后实测：lenghao 容器 462px（修前 421 + 实收 41）≥ 433，余 29px。
    const container1280enAfter = 462; // lenghao 重探针实测（v0317-probe2.log），修后代码
    assert.strictEqual(container1280enAfter, 462, '修后 lenghao 容器应为 462px（重探针实测）');
    assert.ok(container1280enAfter >= navNeed1280enLenghao, `修后容器应放得下 7 项（${container1280enAfter}≥${navNeed1280enLenghao}，余 ${container1280enAfter - navNeed1280enLenghao}px）`);
    // 修后实测（沙箱 xvfb Electron 真机探针，登录态，修后代码）：
    // 1280 英文容器 457px、7 项需 449px（沙箱字体更宽），余 8px，7/7、
    // More 隐藏；1280 中文 544px、需 334px，余 210px（v0.3.18 重探针实测，见节首注释）。
    const container1280enSandbox = 457;
    const navNeed1280enSandbox = 57 + 53 + 51 + 60 + 93 + 52 + 83;
    assert.strictEqual(navNeed1280enSandbox, 449, '沙箱 1280 英文 7 项共需 449px（探针实测）');
    assert.ok(container1280enSandbox >= navNeed1280enSandbox, `沙箱修后容器应放得下 7 项（${container1280enSandbox}≥${navNeed1280enSandbox}，余 ${container1280enSandbox - navNeed1280enSandbox}px）`);
    const container1280zhSandbox = 544;
    const navNeed1280zhSandbox = 44 + 44 + 44 + 44 + 44 + 44 + 70;
    assert.strictEqual(navNeed1280zhSandbox, 334, '沙箱 1280 中文 7 项共需 334px（探针实测）');
    assert.ok(container1280zhSandbox >= navNeed1280zhSandbox, '沙箱 1280 中文容器应放得下 7 项');
    // greedyNavFit：修后两套宽度在各自容器下都不应溢出（More 隐藏）。
    const greedyNavFit23 = new Function(`${extractFn20('greedyNavFit')}; return greedyNavFit;`)();
    assert.deepStrictEqual(
      greedyNavFit23([57, 53, 51, 60, 93, 52, 83], 457, 61),
      { visible: [0, 1, 2, 3, 4, 5, 6], overflow: [] },
      '沙箱 1280 英文下 greedyNavFit 应 7 项全可见、More 隐藏');
    assert.deepStrictEqual(
      greedyNavFit23([54, 51, 50, 58, 89, 50, 81], 462, 60),
      { visible: [0, 1, 2, 3, 4, 5, 6], overflow: [] },
      'lenghao 1280 英文下 greedyNavFit 应 7 项全可见、More 隐藏');
    // 回归：1024 仍在带子外（带子下限 1101 不变），1000px 规则保留。
    assert.ok(css20.includes('@media (min-width: 1101px) and (max-width: 1900px)'), '带子仍应为 1101–1900px');
    assert.ok(css20.includes('@media (max-width: 1000px)'), '1000px 断点应保留');
    ok('1280 英文顶栏修复：带子理论收 48px、实收 41px、lenghao 462≥433、沙箱 457≥449、More 隐藏');
  }

  console.log('== 24. v0.3.17 修复轮：macOS 原生菜单本地化 ==');
  // 背景：应用从没建过菜单，macOS 上一直是 Electron 默认英文菜单
  // （File / Edit / View / Window / Help，Minimize / Zoom / Close 全英文），
  // 而应用内早已是中文。修法：lib/appMenu.js 按字典 menu.* 键逐项写死
  // label（role 只管行为），语言切换时重建；只在 darwin 上建菜单，
  // Windows/Linux 不设菜单、本就不显示菜单栏，行为不动。
  // 原生 macOS 菜单栏在 lenghao Linux 走查里看不到，不进 CDP 断言；
  // Mac 实机效果待发版后在 Mac 上验。
  {
    // 24a. 字典：24 个 menu.* 键中英齐全、值非空；带 %s 的三项中英各一个 %s。
    const menuKeys = Object.keys(tr._uiStrings.zh).filter((k) => k.startsWith('menu.')).sort();
    assert.strictEqual(menuKeys.length, 24, '字典应有 24 个 menu.* 键');
    for (const k of menuKeys) {
      assert.ok(tr._uiStrings.zh[k] && tr._uiStrings.zh[k].length > 0, `中文 menu 键 ${k} 应非空`);
      assert.ok(tr._uiStrings.en[k] && tr._uiStrings.en[k].length > 0, `英文 menu 键 ${k} 应非空`);
    }
    for (const k of ['menu.appAbout', 'menu.appHide', 'menu.appQuit']) {
      const pct = (s) => (s.match(/%s/g) || []).length;
      assert.strictEqual(pct(tr._uiStrings.zh[k]), 1, `中文 ${k} 应含 1 个 %s（品牌名）`);
      assert.strictEqual(pct(tr._uiStrings.en[k]), 1, `英文 ${k} 应含 1 个 %s（品牌名）`);
    }
    const zhKeys24 = Object.keys(tr._uiStrings.zh).sort();
    const enKeys24 = Object.keys(tr._uiStrings.en).sort();
    assert.deepStrictEqual(enKeys24, zhKeys24, '加 menu.* 后中英 key 集仍应完全一致');
    ok('字典 menu.*：24 键中英齐全、%s 个数一致、key 集仍一致');
  }
  // 24b. 模板纯函数：中文 6 个顶级菜单 + 应用菜单 About/Hide/Quit 走品牌名。
  {
    const zhTpl = buildAppMenuTemplate('zh', tr._uiStrings.zh);
    assert.deepStrictEqual(
      zhTpl.map((m) => m.label),
      ['奇美拉', '文件', '编辑', '视图', '窗口', '帮助'],
      '中文模板顶级菜单应为 奇美拉/文件/编辑/视图/窗口/帮助');
    const zhApp = zhTpl[0].submenu.filter((i) => i.label);
    assert.deepStrictEqual(
      zhApp.map((i) => i.label),
      ['关于奇美拉', '隐藏奇美拉', '隐藏其他', '全部显示', '退出奇美拉'],
      '中文应用菜单应为 关于/隐藏/隐藏其他/全部显示/退出奇美拉');
    assert.strictEqual(zhTpl[0].submenu[0].role, 'about', '关于项应走 about 行为');
    assert.strictEqual(zhTpl[0].submenu[zhTpl[0].submenu.length - 1].role, 'quit', '退出项应走 quit 行为');
    const zhWindow = zhTpl[4].submenu.filter((i) => i.label);
    assert.deepStrictEqual(
      zhWindow.map((i) => i.label),
      ['最小化', '缩放', '关闭', '前置全部窗口'],
      '中文窗口菜单应为 最小化/缩放/关闭/前置全部窗口');
    assert.deepStrictEqual(
      zhWindow.map((i) => i.role),
      ['minimize', 'zoom', 'close', 'front'],
      '窗口菜单项应走 minimize/zoom/close/front 行为');
    const zhEdit = zhTpl[2].submenu.filter((i) => i.label);
    assert.deepStrictEqual(
      zhEdit.map((i) => i.role),
      ['undo', 'redo', 'cut', 'copy', 'paste', 'selectAll'],
      '编辑菜单应走标准 undo/redo/cut/copy/paste/selectAll 行为');
    assert.strictEqual(typeof zhTpl[5].submenu[0].click, 'function', '帮助菜单的论坛项应有点击动作');
    ok('中文菜单模板：6 菜单、应用菜单走品牌名、窗口/编辑 role 齐全');
  }
  // 24c. 英文模板：同一结构，文案全英文；品牌名跟语言（Chimera）。
  {
    const enTpl = buildAppMenuTemplate('en', tr._uiStrings.en);
    assert.deepStrictEqual(
      enTpl.map((m) => m.label),
      ['Chimera', 'File', 'Edit', 'View', 'Window', 'Help'],
      '英文模板顶级菜单应为 Chimera/File/Edit/View/Window/Help');
    const enApp = enTpl[0].submenu.filter((i) => i.label);
    assert.deepStrictEqual(
      enApp.map((i) => i.label),
      ['About Chimera', 'Hide Chimera', 'Hide Others', 'Show All', 'Quit Chimera'],
      '英文应用菜单应为 About/Hide/Hide Others/Show All/Quit Chimera');
    const enWindow = enTpl[4].submenu.filter((i) => i.label);
    assert.deepStrictEqual(
      enWindow.map((i) => i.label),
      ['Minimize', 'Zoom', 'Close', 'Bring All to Front'],
      '英文窗口菜单应为 Minimize/Zoom/Close/Bring All to Front');
    ok('英文菜单模板：6 菜单、品牌名 Chimera、窗口项全英文');
  }
  // 24d. 平台口径 + 接线：非 darwin 上 applyAppMenu 返回 false 且绝不建菜单；
  // main/preload/renderer 三处接线齐全（字符串断言，走既有语言切换链路）。
  {
    assert.strictEqual(applyAppMenu('zh', tr._uiStrings.zh), false, 'Linux 上 applyAppMenu 应返回 false（不建菜单）');
    assert.strictEqual(applyAppMenu('en', tr._uiStrings.en), false, 'Linux 上英文 applyAppMenu 也应返回 false');
    const appMenuSrc = fs.readFileSync(path.join(__dirname, '..', 'lib', 'appMenu.js'), 'utf8');
    assert.ok(appMenuSrc.includes("process.platform !== 'darwin'"), 'appMenu 应有 darwin 门禁');
    const mainSrc24 = fs.readFileSync(path.join(__dirname, '..', 'main.js'), 'utf8');
    assert.ok(mainSrc24.includes("ipcMain.handle('app:set-lang'"), 'main.js 应有 app:set-lang 处理');
    assert.ok(mainSrc24.includes('applyAppMenu('), 'main.js 应调用 applyAppMenu');
    const preloadSrc24 = fs.readFileSync(path.join(__dirname, '..', 'preload.js'), 'utf8');
    assert.ok(preloadSrc24.includes("setLang: (lang) => ipcRenderer.invoke('app:set-lang', lang)"), 'preload 应暴露 app.setLang');
    const rendererSrc24 = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    assert.ok(rendererSrc24.includes('window.chimera.app.setLang(next)'), '语言切换时渲染进程应通知主进程重建菜单');
    assert.ok(rendererSrc24.includes('window.chimera.app.setLang(state.lang)'), '启动时渲染进程应按存的语言通知主进程一次');
    ok('平台口径：非 darwin 不建菜单；main/preload/renderer 接线齐全');
  }

  console.log('== 25. v0.3.18：1501–1827px 英文顶栏溢出修复（v0.3.16 遗留缺口）==');
  // 背景：v0.3.16 把副标题隐藏与收紧带子上提到 1500px，只覆盖到 1440；
  // 1501 以上副标题回来、带子脱出，英文导航又放不下，要到 1920 才够宽。
  // 修前实测（沙箱 xvfb Electron 真机探针，登录态模拟，英文，修前代码）：
  // 1501 容器 184/需 533（差 349，菜单里 6 项）、1600 容器 283/需 533
  // （差 250）、1700 容器 383/需 533（差 150）、1827 容器 510/需 533
  // （差 23，菜单里 team/node）、1920 容器 603/需 533（余 70，7/7）。
  // 不收紧时容器 = 宽 − 1317，1850 起才自然放得下。
  // 修法（CSS-only）：副标题隐藏与收紧带子上限 1500→1900px（悬崖落在自然
  // 放得下的位置，1901 容器 584/需 533，余 51px）；带子内补主题/语言按钮
  // 的 ID 级覆盖收到 6px（见 §20k），1280 沙箱容器 441→457px。
  // 25a. CSS：1900px 断点 + 1101–1900px 带子 + ID 覆盖 + 1024/1000px 不动。
  {
    const m1900 = css20.match(/@media \(max-width: 1900px\) \{([\s\S]*?)\n\}/);
    assert.ok(m1900, '应有 1900px 断点');
    assert.ok(m1900[1].includes('.brand .sub { display: none; }'), '1900px 以下应藏品牌副标题');
    const bandAt25 = css20.indexOf('@media (min-width: 1101px) and (max-width: 1900px)');
    assert.ok(bandAt25 >= 0, '收紧带子应为 1101–1900px（覆盖 1501–1827，1024 不进带子）');
    const bandEnd25 = css20.indexOf('}', css20.indexOf('#globalSearchInput { width: 90px; }', bandAt25)) + 1;
    const band25 = css20.slice(bandAt25, bandEnd25);
    assert.ok(band25.includes('#themeToggle, #langToggle { padding: 7px 6px; }'), '带子内应有主题/语言按钮 ID 级覆盖（收到 6px）');
    assert.ok(band25.includes('.top-right .btn.ghost { padding: 7px 6px; }'), '带子内右侧 ghost 按钮应为 6px 内边距');
    assert.ok(1101 <= 1501 && 1827 <= 1900, '带子应覆盖 1501–1827（缺口全在带子内）');
    assert.ok(1024 < 1101, '带子下限 1101 应把 1024 排除在外（1024 More 行为不动）');
    assert.ok(css20.includes('@media (max-width: 1000px)'), '1000px 断点应保留');
    ok('CSS：1900px 断点 + 1101–1900px 带子 + ID 覆盖 + 1024/1000px 不动');
  }
  // 25b. 修后算术（沙箱 xvfb Electron 真机探针，登录态模拟，英文，修后代码）：
  // 带子内 7 项需 449px（6px 内边距）；1501 容器 678（余 229）、1600 容器 777
  // （余 328）、1700 容器 877（余 428）、1827 容器 1004（余 555）、1850 容器
  // 1027（余 578）、1900 容器 1077（余 628），全部 7/7、More 隐藏；中文对照
  // 1501 容器 765/需 334、1827 容器 1091/需 334，同样 7/7。
  {
    const rows25 = [
      [1501, 678, 449], [1600, 777, 449], [1700, 877, 449],
      [1827, 1004, 449], [1850, 1027, 449], [1900, 1077, 449],
    ];
    for (const [w, container, need] of rows25) {
      assert.ok(container >= need, `${w}px 英文修后容器应放得下 7 项（${container}≥${need}，余 ${container - need}px）`);
    }
    const container1501zh = 765;
    const navNeed1501zh = 44 + 44 + 44 + 44 + 44 + 44 + 70;
    assert.strictEqual(navNeed1501zh, 334, '1501 中文 7 项共需 334px（探针实测）');
    assert.ok(container1501zh >= navNeed1501zh, `1501 中文修后容器应放得下 7 项（${container1501zh}≥${navNeed1501zh}）`);
    const container1827zh = 1091;
    assert.ok(container1827zh >= navNeed1501zh, `1827 中文修后容器应放得下 7 项（${container1827zh}≥${navNeed1501zh}）`);
    ok('修后算术：1501–1900 英文容器 678–1077≥449、中英对照 7/7');
  }
  // 25c. greedyNavFit：修后 1501（带子内）与 1901（带子外、自然放得下）都不应溢出；
  // 回归锁定修前 1827 确实溢出（容器 510 < 需 533，team/node 进菜单）。
  {
    const greedyNavFit25 = new Function(`${extractFn20('greedyNavFit')}; return greedyNavFit;`)();
    assert.deepStrictEqual(
      greedyNavFit25([57, 53, 51, 60, 93, 52, 83], 678, 61),
      { visible: [0, 1, 2, 3, 4, 5, 6], overflow: [] },
      '修后 1501 英文下 greedyNavFit 应 7 项全可见、More 隐藏');
    assert.deepStrictEqual(
      greedyNavFit25([69, 65, 63, 72, 105, 64, 95], 584, 77),
      { visible: [0, 1, 2, 3, 4, 5, 6], overflow: [] },
      '修后 1901 英文下 greedyNavFit 应 7 项全可见、More 隐藏（带子外自然放得下）');
    assert.deepStrictEqual(
      greedyNavFit25([69, 65, 63, 72, 105, 64, 95], 510, 77),
      { visible: [0, 1, 2, 3, 4], overflow: [5, 6] },
      '修前 1827 英文下 greedyNavFit 应溢出 team/node（容器 510 < 需 533，回归锁定）');
    assert.deepStrictEqual(
      greedyNavFit25([57, 53, 51, 60, 93, 52, 83], 51, 61),
      { visible: [], overflow: [0, 1, 2, 3, 4, 5, 6] },
      '1024 英文下 greedyNavFit 应 7 项全进菜单（带子外，既定行为）');
    ok('greedyNavFit：修后 1501/1901 全可见、修前 1827 溢出锁定、1024 全进菜单');
  }
  // 25d. 悬崖与回归：1901（带子外）容器 584/需 533，余 51px；1920 容器 603/需 533
  // 与修前一致（余 70px，不退化）；1024 仍在带子外。
  {
    const container1901en = 584;
    const navNeed1901en = 69 + 65 + 63 + 72 + 105 + 64 + 95;
    assert.strictEqual(navNeed1901en, 533, '1901 英文 7 项共需 533px（12px 内边距，探针实测）');
    assert.ok(container1901en >= navNeed1901en, `1901 英文容器应放得下 7 项（${container1901en}≥${navNeed1901en}，余 ${container1901en - navNeed1901en}px，悬崖无缺口）`);
    const container1920en = 603;
    assert.ok(container1920en >= navNeed1901en, `1920 英文容器应与修前一致放得下 7 项（${container1920en}≥${navNeed1901en}，余 ${container1920en - navNeed1901en}px，不退化）`);
    assert.ok(!css20.includes('@media (min-width: 1101px) and (max-width: 1500px)'), '旧 1500px 带子不应再有');
    ok('悬崖与回归：1901 余 51px、1920 与修前一致 603≥533、旧 1500 带子已无');
  }

  console.log(`\nPASS: ${passed} 项全过`);
}

main().catch((err) => {
  console.error(`\nFAIL: ${err.message}`);
  process.exit(1);
});
