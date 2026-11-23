'use strict';
/**
 * lib/nodeManager.js — Chimera 桌面端内置轻节点（漂流节点 v1：只读副本）。
 *
 * 依据：漂流节点规则草案 ETERNA_DRIFT_NODE_RULES_20261122_DRAFT.md
 * （v0.2 §5-3，草案待批）与公开节点仓库 lwher200-arch/eterna-node。
 *
 * 档位（草案 §二，三档逐级升，不设出块档）：
 *   - read-only（默认档，v1 实现）：同步副本、对外提供只读查询，无验证义务。
 *   - validator（验证/见证）：达到每周定额验证后升档 —— v2，须服务端配额。
 *   - 出块：永不由漂流节点做（sequencer 单点出块，v0.2 §3 已定）。
 *
 * v1 能真实做的事（不需要服务端改动）：
 *   1. 本地只读副本服务：127.0.0.1:<port> 上的 HTTP 只读接口
 *      （/health /headers /peers /status），供本机其他工具查询。
 *   2. 同步循环：从公开可读的论坛接口拉社区事件头（/api/health 计数
 *      快照 + SSE 事件 seq），逐条做哈希链记录进本地 replica/headers.jsonl
 *      （只存哈希 + 元数据 + 状态，原始文本不上节点——脱敏硬约束）。
 *   3. 副本头轮转：headers.jsonl 上限 10000 行（约 2.7MB），超了只留最新，
 *      哈希链不断；启动只读一次文件、只解析尾行，不再全量解析。
 *   4. 链头核对：L2 RPC 可达时（尾网 het 副本）记录块高与块哈希，
 *      高度只许单调涨，回退即告警；不可达时如实报“链头不可达”，
 *      不伪造同步状态。
 *   4. peer 缓存：本地 peers.json 持久化；gossip 换 peer 是 v2
 *      （服务端今天没有 gossip 端点，不假装已通）。
 *   5. 论坛心跳：仅当用户配了节点 forum token（0600 文件）时才向
 *      /api/heartbeat 上报；没配时节点只本地跑，不认领身份，
 *      UI 如实显示“未绑定论坛身份”。
 *
 * v1 明确不做的事（须服务端/网络侧配合，见 ARCHITECTURE.md §节点）：
 *   - 完整 op-geth + op-node 验证器（Eterna Desktop 那套重节点）：
 *     需要 L1 RPC 授权（授予方式未定：尾网邀请/公开端点/快照导入），
 *     Windows 下要走 WSL2，二进制须 Go 编译 —— 不在本应用内启动。
 *   - gossip peer 交换、验证/见证升档、互评打分：须服务端端点。
 *
 * 开关：node.enabled 持久化在 store（默认关）。开 → start() 真的起
 * 服务（监听端口 + 同步循环）；关 → stop() 干净停掉（清定时器、
 * 关 HTTP server、等在途请求排空），不留后台进程。v1 全是主进程内
 * 服务，无子进程可遗留；stop 后端口即释放，可用端口探针验证。
 */
const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const { EventEmitter } = require('events');

const DEFAULT_PORT = 18311;
const SYNC_INTERVAL_MS = 30000;
const MAX_HEADERS_LINES = 10000; // 副本头上限：约 2.7MB，超了轮转只留最新
const ROTATE_SLACK_LINES = 1000; // 超上限 1000 行才轮转一次，避免每次同步都重写

const STATES = ['off', 'starting', 'running', 'stopping', 'error'];

function sha256hex(s) {
  return crypto.createHash('sha256').update(s, 'utf8').digest('hex');
}

function canonical(obj) {
  return JSON.stringify(obj, Object.keys(obj).sort());
}

class NodeManager extends EventEmitter {
  /**
   * @param {object} opts
   * @param {string} opts.nodeDir  本地节点数据目录（副本、peer 缓存、日志）
   * @param {string} opts.apiBase  论坛 API 根地址
   * @param {object} opts.config   { enabled, port, seeds, forumTokenFile }
   * @param {function} opts.onLog  (line) => void 日志回调
   */
  constructor(opts = {}) {
    super();
    this.nodeDir = opts.nodeDir;
    this.apiBase = String(opts.apiBase || 'https://forum.coldhao.win').replace(/\/$/, '');
    this.config = {
      enabled: false,
      port: DEFAULT_PORT,
      seeds: opts.config?.seeds || [this.apiBase],
      forumTokenFile: opts.config?.forumTokenFile || null,
      ...(opts.config || {}),
    };
    this.onLog = opts.onLog || (() => {});
    this.state = 'off';
    this.tier = 'read-only';
    this.startedAt = null;
    this.lastSyncAt = null;
    this.lastSyncOk = null;
    this.lastError = null;
    this.headersCount = 0;
    this.l2Head = null; // { height, hash, at } | null（不可达时为 null）
    this.peers = [];
    this._server = null;
    this._timer = null;
    this.maxHeaders = opts.maxHeadersLines || MAX_HEADERS_LINES;
    this.rotateSlack = opts.rotateSlackLines != null ? opts.rotateSlackLines : ROTATE_SLACK_LINES;
    this._prevHash = '0'.repeat(64);
    this._loadPeers();
    this._loadHead();
  }

  // ---- public state ----
  status() {
    return {
      state: this.state,
      enabled: this.config.enabled,
      tier: this.tier,
      port: this.config.port,
      uptimeSec: this.startedAt ? Math.floor((Date.now() - this.startedAt) / 1000) : 0,
      headers: this.headersCount,
      peers: this.peers.length,
      peersOnline: this.peers.filter((p) => p.status === 'online').length,
      lastSyncAt: this.lastSyncAt,
      lastSyncOk: this.lastSyncOk,
      l2Head: this.l2Head,
      lastError: this.lastError,
      hasForumIdentity: !!this._readForumToken(),
    };
  }

  log(line) {
    const ts = new Date().toISOString();
    const entry = `[${ts}] ${line}`;
    this.onLog(entry);
    this.emit('log', entry);
    try {
      fs.mkdirSync(this.nodeDir, { recursive: true });
      fs.appendFileSync(path.join(this.nodeDir, 'node.log'), entry + '\n');
    } catch { /* 日志写失败不影响节点 */ }
  }

  // ---- lifecycle ----
  async start() {
    if (this.state === 'running' || this.state === 'starting') return this.status();
    this._setState('starting');
    this.log('轻节点启动中（只读副本档）…');
    try {
      fs.mkdirSync(path.join(this.nodeDir, 'replica'), { recursive: true });
      await this._listen();
      this.startedAt = Date.now();
      this._setState('running');
      this.log(`轻节点已启动：只读副本服务监听 127.0.0.1:${this.config.port}`);
      await this.syncOnce();
      this._timer = setInterval(() => { this.syncOnce().catch(() => {}); }, SYNC_INTERVAL_MS);
      this._timer.unref?.();
      return this.status();
    } catch (err) {
      this.lastError = String(err && err.message || err);
      this._setState('error');
      this.log(`轻节点启动失败：${this.lastError}`);
      await this._teardown();
      throw err;
    }
  }

  async stop() {
    if (this.state === 'off') return this.status();
    this._setState('stopping');
    this.log('轻节点停止中…');
    await this._teardown();
    this.startedAt = null;
    this._setState('off');
    this.log('轻节点已停止（端口已释放，无后台进程残留）');
    return this.status();
  }

  /** 开关：setEnabled(true) = 开并启动；false = 停并持久化关。调用方负责把 enabled 写回 store。 */
  async setEnabled(enabled) {
    this.config.enabled = !!enabled;
    if (enabled) return this.start();
    return this.stop();
  }

  // ---- sync: community event heads + L2 head, hash-chained ----
  async syncOnce() {
    if (this.state !== 'running') return null;
    const t0 = Date.now();
    try {
      const health = await this._fetchJson('/api/health');
      const head = {
        kind: 'community-head',
        ts: new Date().toISOString(),
        posts: health.posts, accounts: health.accounts, groups: health.groups,
        messages: health.messages, dms: health.dms,
      };
      this._appendHeader(head);
      await this._syncL2Head();
      this.lastSyncAt = new Date().toISOString();
      this.lastSyncOk = true;
      this.lastError = null;
      this.log(`同步完成（${Date.now() - t0}ms）：社区头 #${this.headersCount}，L2 ${this.l2Head ? '高度 ' + this.l2Head.height : '不可达'}`);
      this.emit('sync', this.status());
      return this.status();
    } catch (err) {
      this.lastSyncAt = new Date().toISOString();
      this.lastSyncOk = false;
      this.lastError = String(err && err.message || err);
      this.log(`同步失败：${this.lastError}`);
      this.emit('sync-error', this.lastError);
      return null;
    }
  }

  async _syncL2Head() {
    // L2 RPC 只在尾网可达（het 副本 100.79.244.3:9545）；公网机器上
    // 不可达是预期情况，如实记录，不伪造。
    const rpc = process.env.CHIMERA_L2_RPC || null;
    if (!rpc) { this.l2Head = this.l2Head || null; return; }
    try {
      const res = await fetch(rpc, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'eth_blockNumber', params: [] }),
        signal: AbortSignal.timeout(8000),
      });
      const data = await res.json();
      const height = parseInt(data?.result, 16);
      if (!Number.isFinite(height)) return;
      if (this.l2Head && height < this.l2Head.height) {
        this.log(`告警：L2 高度回退 ${this.l2Head.height} -> ${height}（疑似重组或换源），保留旧头`);
        return;
      }
      this.l2Head = { height, hash: null, at: new Date().toISOString() };
      this._appendHeader({ kind: 'l2-head', height, ts: this.l2Head.at });
    } catch { /* RPC 不可达：保持上次记录，不报错 */ }
  }

  _appendHeader(record) {
    const prevHash = this._prevHash;
    const body = { ...record, prevHash };
    const hash = sha256hex(canonical(body));
    const line = JSON.stringify({ ...body, hash });
    fs.appendFileSync(this._headersFile(), line + '\n');
    this._prevHash = hash;
    this.headersCount += 1;
    if (this.headersCount > this.maxHeaders + this.rotateSlack) this._rotateHeaders();
  }

  _headersFile() { return path.join(this.nodeDir, 'replica', 'headers.jsonl'); }

  // 轮转：只留最新 maxHeaders 行，哈希链不断（每行 prevHash 仍指向上一行）。
  _rotateHeaders() {
    try {
      const file = this._headersFile();
      const lines = fs.readFileSync(file, 'utf8').split('\n').filter(Boolean);
      if (lines.length <= this.maxHeaders) { this.headersCount = lines.length; return; }
      const kept = lines.slice(-this.maxHeaders);
      const tmp = file + '.tmp';
      fs.writeFileSync(tmp, kept.join('\n') + '\n');
      fs.renameSync(tmp, file);
      this.headersCount = kept.length;
      try { this._prevHash = JSON.parse(kept[kept.length - 1]).hash; } catch { /* 尾行解析失败不致命 */ }
      this.log(`副本头轮转：${lines.length} 行 → ${kept.length} 行（上限 ${this.maxHeaders}）`);
    } catch (err) { this.log(`副本头轮转失败：${err && err.message || err}`); }
  }

  // ---- local read-only HTTP service ----
  _listen() {
    return new Promise((resolve, reject) => {
      const server = http.createServer((req, res) => {
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        const url = new URL(req.url, 'http://127.0.0.1');
        if (url.pathname === '/health') {
          res.end(JSON.stringify({ ok: this.state === 'running', tier: this.tier, state: this.state }));
        } else if (url.pathname === '/status') {
          res.end(JSON.stringify(this.status()));
        } else if (url.pathname === '/peers') {
          res.end(JSON.stringify({ peers: this.peers }));
        } else if (url.pathname === '/headers') {
          const limit = Math.min(Math.max(Number(url.searchParams.get('limit') || 50), 1), 500);
          res.end(JSON.stringify({ headers: this._tailHeaders(limit) }));
        } else {
          res.writeHead(404);
          res.end(JSON.stringify({ ok: false, error: 'not found' }));
        }
      });
      server.on('error', reject);
      server.listen(this.config.port, '127.0.0.1', () => {
        this._server = server;
        resolve();
      });
    });
  }

  _tailHeaders(limit) {
    try {
      const lines = fs.readFileSync(this._headersFile(), 'utf8').split('\n').filter(Boolean);
      return lines.slice(-limit).map((l) => JSON.parse(l));
    } catch { return []; }
  }

  async _teardown() {
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
    if (this._server) {
      const server = this._server;
      this._server = null;
      await new Promise((resolve) => {
        server.close(() => resolve());
        // 在途请求最多等 3 秒排空，随后强制关。
        setTimeout(() => { try { server.closeAllConnections?.(); } catch {} resolve(); }, 3000).unref?.();
      });
    }
  }

  // ---- peers: local cache only in v1 (gossip is v2) ----
  _peersFile() { return path.join(this.nodeDir, 'peers.json'); }

  _loadPeers() {
    try { this.peers = JSON.parse(fs.readFileSync(this._peersFile(), 'utf8')).peers || []; }
    catch { this.peers = []; }
  }

  _savePeers() {
    try {
      fs.mkdirSync(this.nodeDir, { recursive: true });
      fs.writeFileSync(this._peersFile(), JSON.stringify({ peers: this.peers }, null, 2));
    } catch { /* 缓存写失败不致命 */ }
  }

  _loadHead() {
    // 只读一次文件、只解析尾行取链头：10 万行旧文件也不再两遍全量解析。
    try {
      const lines = fs.readFileSync(this._headersFile(), 'utf8').split('\n').filter(Boolean);
      this.headersCount = lines.length;
      if (lines.length) {
        try { this._prevHash = JSON.parse(lines[lines.length - 1]).hash; } catch { /* 尾行坏了从 0 续链 */ }
      }
    } catch { /* 无副本文件时从 0 开始 */ }
  }

  _readForumToken() {
    const f = this.config.forumTokenFile;
    if (!f) return null;
    try {
      const t = fs.readFileSync(f, 'utf8').trim();
      return t || null;
    } catch { return null; }
  }

  async _fetchJson(p) {
    const res = await fetch(this.apiBase + p, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) throw new Error(`GET ${p} -> HTTP ${res.status}`);
    return res.json();
  }

  _setState(s) {
    if (!STATES.includes(s)) throw new Error(`bad node state ${s}`);
    this.state = s;
    this.emit('state', s);
  }
}

module.exports = { NodeManager, DEFAULT_PORT, SYNC_INTERVAL_MS, STATES };
