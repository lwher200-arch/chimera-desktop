'use strict';
/**
 * lib/nodeManager.js — Chimera 桌面端内置轻节点（漂流节点 v2：同步 + 上传）。
 *
 * 依据：漂流节点规则草案 ETERNA_DRIFT_NODE_RULES_20261122_DRAFT.md
 * （v0.2 §5-3，草案待批）与公开节点仓库 lwher200-arch/eterna-node。
 *
 * 档位（草案 §二，三档逐级升，不设出块档）：
 *   - sync-upload（当前档，v2 实现）：同步副本 + 把副本头传回论坛，
 *     对外提供只读查询，无验证义务。
 *   - validator（验证/见证）：达到每周定额验证后升档 —— 须服务端配额。
 *   - 出块：永不由漂流节点做（sequencer 单点出块，v0.2 §3 已定）。
 *
 * v2 能真实做的事：
 *   1. 本地只读副本服务：127.0.0.1:<port> 上的 HTTP 只读接口
 *      （/health /headers /peers /status），供本机其他工具查询。
 *   2. 同步循环：从公开可读的论坛接口拉社区事件头（/api/health 计数
 *      快照 + SSE 事件 seq），逐条做哈希链记录进本地 replica/headers.jsonl
 *      （只存哈希 + 元数据 + 状态，原始文本不上节点——脱敏硬约束）。
 *      每条头带持久单调 seq（从文件尾行续接，轮转不断号）。
 *   3. 上传：每次同步后把 lastUploadedSeq 之后的新头 POST 到论坛
 *      /api/node/headers（X-AI-Token 节点身份，0600 文件）；服务端验链
 *      连续性与自哈希后回 ack（accepted / lastSeq）。失败记数并指数
 *      退避（30s 起，10 分钟封顶）。v0.3.10：传输出错或 409 后先问服务端
 *      实际收到了多少（_reconcileWithServer）：ack 丢了但服务端已存下时
 *      认领服务端头继续，不重发、不重基；只有真分叉才走重基（见
 *      _handleUploadConflict，冷却逻辑保留）。token 文件必须在 nodeDir
 *      内（构造即校验，测试 profile 绝不能读 live token）。
 *   4. 链自检：启动时全量验本地链（逐行 prevHash 衔接 + 自哈希重算 +
 *      seq 连续）；断了就告警、把坏文件移旁（headers.jsonl.corrupt-<ts>）、
 *      从创世重起新链并记 lastRebaseAt，下次上传即向服务端重基。
 *   5. 副本头轮转：headers.jsonl 上限 10000 行（约 2.7MB），超了只留最新，
 *      哈希链不断；启动只读一次文件、只解析尾行，不再全量解析。
 *   6. 链头核对：L2 RPC 可达时（尾网 het 副本）记录块高与块哈希，
 *      高度只许单调涨，回退即告警；不可达时如实报“链头不可达”，
 *      不伪造同步状态。
 *   7. peer 缓存：本地 peers.json 持久化；gossip 换 peer 是 v2
 *      （服务端今天没有 gossip 端点，不假装已通）。
 *   8. 论坛心跳：仅当用户配了节点 forum token（0600 文件）时才向
 *      /api/heartbeat 上报；没配时节点只本地跑，不认领身份，
 *      UI 如实显示“未绑定论坛身份”。
 *   9. SSE 缺口断点续拉：主进程收到 RealtimeClient 的 'gap' 事件时
 *      立即调 syncOnce() 重拉一次（main.js 接线），不等待 30 秒周期。
 *
 * v2 明确不做的事（须服务端/网络侧配合，见 ARCHITECTURE.md §节点）：
 *   - 完整 op-geth + op-node 验证器（Eterna Desktop 那套重节点）：
 *     需要 L1 RPC 授权（授予方式未定：尾网邀请/公开端点/快照导入），
 *     Windows 下要走 WSL2，二进制须 Go 编译 —— 不在本应用内启动。
 *   - gossip peer 交换、验证/见证升档、互评打分：须服务端端点。
 *
 * 开关：node.enabled 持久化在 store（默认关）。开 → start() 真的起
 * 服务（监听端口 + 同步循环）；关 → stop() 干净停掉（清定时器、
 * 关 HTTP server、等在途请求排空），不留后台进程。v2 全是主进程内
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
const UPLOAD_BATCH_MAX = 100; // 每批最多上传 100 条头（服务端同上限）
const UPLOAD_BACKOFF_BASE_MS = 30000; // 上传失败退避起点 30 秒
const UPLOAD_BACKOFF_MAX_MS = 600000; // 上传失败退避封顶 10 分钟
const REBASE_COOLDOWN_MS = 600000; // 409 重基冷却 10 分钟，防重基循环

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
    // v0.3.10 (d) token 隔离 guard：token 文件必须在节点数据目录内。
    // 测试 profile 绝不能读 live token（R3：v0.3.6 验收 profile 抄了 live
    // state.json，空链 + live token 发 seq-1 创世，服务端按设计清链）。
    // 主进程已改为按 userData 派生 token 路径；这里是最后一道显性防线：
    // 配错了就大声失败（构造即抛），而不是悄悄用别人的 token 上传。
    if (this.config.forumTokenFile && this.nodeDir) {
      const tokenAbs = path.resolve(this.config.forumTokenFile);
      const dirAbs = path.resolve(this.nodeDir);
      if (tokenAbs !== dirAbs && !tokenAbs.startsWith(dirAbs + path.sep)) {
        throw new Error(
          `节点 token 文件不在节点数据目录内，拒绝启动：${this.config.forumTokenFile} 不在 ${this.nodeDir} 下`
          + '（测试 profile 绝不能读 live token）',
        );
      }
    }
    this.onLog = opts.onLog || (() => {});
    this.state = 'off';
    this.tier = 'sync-upload';
    this.startedAt = null;
    this.lastSyncAt = null;
    this.lastSyncOk = null;
    this.lastError = null;
    this.headersCount = 0;
    this.l2Head = null; // { height, hash, at } | null（不可达时为 null）
    this.peers = [];
    // 上传游标（持久化在 nodeDir/upload-state.json，轮转与重启都不丢）：
    // lastUploadedSeq = 服务端已 ack 的最大 seq；lastUploadAt = 最近成功
    // 上传时间；uploadFailures = 连续失败数（退避用）；chainOk = 启动自检结果。
    this.lastUploadedSeq = 0;
    this.lastUploadAt = null;
    this.uploadFailures = 0;
    this.chainOk = true;
    this._nextSeq = 1; // 下一条头的 seq：从文件尾行续接（_loadHead 里定）
    this._nextUploadAt = 0; // 下次允许上传的时间戳（退避门）
    this._lastRebaseAt = 0; // 最近一次本地重基时间（409 冷却用）
    this._uploading = false; // 上传在途锁：同步周期 30 秒，不叠加上传
    this._server = null;
    this._timer = null;
    this.maxHeaders = opts.maxHeadersLines || MAX_HEADERS_LINES;
    this.rotateSlack = opts.rotateSlackLines != null ? opts.rotateSlackLines : ROTATE_SLACK_LINES;
    this._prevHash = '0'.repeat(64);
    this._loadPeers();
    this._loadHead();
    this._loadUploadState();
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
      upload: {
        lastUploadedSeq: this.lastUploadedSeq,
        lastUploadAt: this.lastUploadAt,
        failures: this.uploadFailures,
        pending: Math.max(0, (this._nextSeq - 1) - this.lastUploadedSeq),
        chainOk: this.chainOk,
      },
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
    this.log('轻节点启动中（同步 + 上传）…');
    try {
      fs.mkdirSync(path.join(this.nodeDir, 'replica'), { recursive: true });
      // 启动链自检：断了就告警、坏文件移旁、从创世重起新链（下次上传即重基）。
      const check = this._selfCheckChain();
      if (!check.ok) {
        this.chainOk = false;
        this._quarantineChain(check.error);
        this.emit('chain-broken', { error: check.error, checked: check.checked });
      } else {
        this.chainOk = true;
        this.log(`链自检通过：${check.checked} 条头，哈希链连续`);
      }
      await this._listen();
      this.startedAt = Date.now();
      this._setState('running');
      this.log(`轻节点已启动：同步 + 上传服务监听 127.0.0.1:${this.config.port}`);
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
      // 同步成功后顺手上传与心跳：都是 best-effort，失败不拖垮本次同步。
      await this._uploadPending().catch(() => {});
      await this._heartbeat().catch(() => {});
      this.lastSyncAt = new Date().toISOString();
      this.lastSyncOk = true;
      this.lastError = null;
      this.chainOk = true; // 同步能正常追加 = 当前链头是连续的（启动自检的异常已由隔离/重基修复）
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
    // seq 进哈希输入：顺序绑进链里，服务端验链时 seq 与哈希互相印证。
    const body = { ...record, seq: this._nextSeq, prevHash };
    const hash = sha256hex(canonical(body));
    const line = JSON.stringify({ ...body, hash });
    fs.appendFileSync(this._headersFile(), line + '\n');
    this._prevHash = hash;
    this._nextSeq += 1;
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

  // ---- 上传游标持久化 ----
  _uploadStateFile() { return path.join(this.nodeDir, 'upload-state.json'); }

  _loadUploadState() {
    try {
      const s = JSON.parse(fs.readFileSync(this._uploadStateFile(), 'utf8'));
      if (Number.isInteger(s.lastUploadedSeq) && s.lastUploadedSeq >= 0) this.lastUploadedSeq = s.lastUploadedSeq;
      if (typeof s.lastUploadAt === 'string') this.lastUploadAt = s.lastUploadAt;
      if (Number.isInteger(s.failures) && s.failures >= 0) this.uploadFailures = s.failures;
      if (Number.isInteger(s.lastRebaseAt) && s.lastRebaseAt > 0) this._lastRebaseAt = s.lastRebaseAt;
    } catch { /* 无游标文件时从 0 开始上传 */ }
  }

  _saveUploadState() {
    try {
      fs.mkdirSync(this.nodeDir, { recursive: true });
      fs.writeFileSync(this._uploadStateFile(), JSON.stringify({
        lastUploadedSeq: this.lastUploadedSeq,
        lastUploadAt: this.lastUploadAt,
        failures: this.uploadFailures,
        lastRebaseAt: this._lastRebaseAt,
      }, null, 2));
    } catch { /* 游标写失败不致命：下次按旧游标重传，服务端按 seq 去重（409/重基兜底） */ }
  }

  // ---- 链自检：启动时全量验本地链 ----
  // 逐行验：prevHash 衔接上一行 hash、自哈希重算、seq（有则）连续 +1。
  // 注意：轮转后文件首行是链中段，其 prevHash 指向已删的旧行，不要求为全 0。
  // 返回 { ok, checked, error? }；ok=false 时调用方负责隔离 + 重基。
  _selfCheckChain() {
    let lines = [];
    try {
      lines = fs.readFileSync(this._headersFile(), 'utf8').split('\n').filter(Boolean);
    } catch { return { ok: true, checked: 0 }; } // 无文件 = 空链，通过
    let prevHash = null;
    let prevSeq = null;
    for (let i = 0; i < lines.length; i++) {
      let h = null;
      try { h = JSON.parse(lines[i]); } catch { return { ok: false, checked: i, error: `第 ${i + 1} 行不是合法 JSON` }; }
      if (typeof h.hash !== 'string' || typeof h.prevHash !== 'string') {
        return { ok: false, checked: i, error: `第 ${i + 1} 行缺 hash/prevHash` };
      }
      if (prevHash !== null && h.prevHash !== prevHash) {
        return { ok: false, checked: i, error: `第 ${i + 1} 行 prevHash 与上一行 hash 断开` };
      }
      const { hash, ...rest } = h;
      if (sha256hex(canonical(rest)) !== hash) {
        return { ok: false, checked: i, error: `第 ${i + 1} 行自哈希不符（内容被改过）` };
      }
      if (Number.isInteger(h.seq)) {
        if (prevSeq !== null && h.seq !== prevSeq + 1) {
          return { ok: false, checked: i, error: `第 ${i + 1} 行 seq 不连续（${prevSeq} -> ${h.seq}）` };
        }
        prevSeq = h.seq;
      }
      prevHash = h.hash;
    }
    return { ok: true, checked: lines.length };
  }

  // 隔离坏链：文件移旁（不删，留证据），计数与链头归零，下次上传即重基。
  _quarantineChain(error) {
    try {
      const bad = `${this._headersFile()}.corrupt-${Date.now()}`;
      fs.renameSync(this._headersFile(), bad);
      this.log(`告警：本地链自检失败（${error}），坏文件已移旁 ${path.basename(bad)}，从创世重起新链`);
    } catch (err) {
      this.log(`告警：本地链自检失败（${error}），隔离坏文件失败：${err && err.message || err}`);
    }
    this.headersCount = 0;
    this._prevHash = '0'.repeat(64);
    this._nextSeq = 1;
    this.lastUploadedSeq = 0;
    this._lastRebaseAt = Date.now();
    this._saveUploadState();
  }

  // ---- 上传：把 lastUploadedSeq 之后的新头推到论坛 ----
  async _uploadPending() {
    if (this.state !== 'running' || this._uploading) return null;
    const token = this._readForumToken();
    if (!token) return null; // 未绑定论坛身份：只本地跑，不上传
    if (Date.now() < this._nextUploadAt) return null; // 退避中
    const pending = this._readPendingHeaders();
    if (!pending.length) return null;
    this._uploading = true;
    try {
      const { status, data } = await this._postNodeJson('/api/node/headers', { headers: pending }, token);
      if (status === 200 && data && data.ok) {
        this.lastUploadedSeq = data.lastSeq;
        this.lastUploadAt = new Date().toISOString();
        this.uploadFailures = 0;
        this._nextUploadAt = 0;
        this._saveUploadState();
        this.log(`上传完成：${data.accepted} 条头已 ack，服务端 lastSeq=${data.lastSeq}`);
        this.emit('upload', this.status());
        return this.status();
      }
      if (status === 409) {
        await this._handleUploadConflict(token, pending);
        return null;
      }
      throw new Error(`POST /api/node/headers -> HTTP ${status}: ${(data && (data.error || data.message)) || ''}`);
    } catch (err) {
      // v0.3.10 (a) 传输出错：先问服务端实际收到了多少——ack 丢了但服务端
      // 已存下时认领服务端头继续，不重发、不重基；真分叉时才走 409 处理。
      let rec = 'unknown';
      try { rec = await this._reconcileWithServer(token, pending); } catch { rec = 'unknown'; }
      if (rec === 'adopted') return null;
      if (rec === 'fork') { await this._handleUploadConflict(token, pending); return null; }
      this.uploadFailures += 1;
      const wait = Math.min(UPLOAD_BACKOFF_BASE_MS * (2 ** (this.uploadFailures - 1)), UPLOAD_BACKOFF_MAX_MS);
      this._nextUploadAt = Date.now() + wait;
      this._saveUploadState();
      this.log(`上传失败（第 ${this.uploadFailures} 次，${Math.round(wait / 1000)} 秒后重试）：${err && err.message || err}`);
      this.emit('upload-error', String(err && err.message || err));
      return null;
    } finally {
      this._uploading = false;
    }
  }

  // 读出待上传的头：seq > lastUploadedSeq，按 seq 升序，最多一批。
  _readPendingHeaders() {
    try {
      const lines = fs.readFileSync(this._headersFile(), 'utf8').split('\n').filter(Boolean);
      const out = [];
      for (const line of lines) {
        let h = null;
        try { h = JSON.parse(line); } catch { continue; }
        if (!Number.isInteger(h.seq) || h.seq <= this.lastUploadedSeq) continue;
        out.push(h);
        if (out.length >= UPLOAD_BATCH_MAX) break;
      }
      return out;
    } catch { return []; }
  }

  // 按 seq 找本地头（409/丢 ack 认领时核对 hash 用；只在异常路径上扫文件）。
  _headerBySeq(seq) {
    try {
      const lines = fs.readFileSync(this._headersFile(), 'utf8').split('\n').filter(Boolean);
      for (const line of lines) {
        let h = null;
        try { h = JSON.parse(line); } catch { continue; }
        if (h.seq === seq) return h;
      }
    } catch { /* 无文件时返回 null */ }
    return null;
  }

  // 认领服务端头（v0.3.10 (a)/(b)）：传输出错或 409 后，先问服务端实际收到了多少。
  // 返回 'adopted'（服务端已收下本批起点，核对 hash 后认领游标，不重发、不重基）、
  // 'fork'（服务端同 seq 头的 hash 与本地不符，真分叉）、'unknown'（服务端没收到
  // 这批 / 拉不到状态）。方向判断以 GET /api/node/headers 的 lastSeq 为准——它与
  // 409 体里的 expectedSeq（= lastSeq+1）同义，GET 是权威状态，不依赖体解析。
  async _reconcileWithServer(token, pending) {
    if (!pending || !pending.length) return 'unknown';
    let server = null;
    try { server = await this._getServerState(token); } catch { return 'unknown'; }
    if (!server || !Number.isInteger(server.lastSeq)) return 'unknown';
    if (server.lastSeq < pending[0].seq) return 'unknown'; // 服务端没收到这批
    const local = this._headerBySeq(server.lastSeq);
    if (local && local.hash !== server.lastHash) return 'fork';
    this.lastUploadedSeq = server.lastSeq;
    this.lastUploadAt = new Date().toISOString();
    this.uploadFailures = 0;
    this._nextUploadAt = 0;
    this._saveUploadState();
    this.log(`上传确认：服务端已收下至 seq=${server.lastSeq}（ack 丢失，认领服务端头继续，不重发、不重基）`);
    this.emit('upload', this.status());
    return 'adopted';
  }

  // 409 按方向分流（v0.3.10 (b)）：先拉服务端状态。
  // 服务端 lastSeq 已覆盖本批起点 = 丢 ack：核对 hash 后认领，不重基。
  // 否则（服务端落后 / 同 seq hash 不符）= 真分叉，走现有重基路径（冷却逻辑保留）。
  async _handleUploadConflict(token, pending) {
    const now = Date.now();
    let server = null;
    try { server = await this._getServerState(token); } catch (err) {
      this.log(`上传 409 后拉服务端状态失败：${err && err.message || err}，本次只退避`);
      this.uploadFailures += 1;
      this._nextUploadAt = now + UPLOAD_BACKOFF_BASE_MS;
      this._saveUploadState();
      return;
    }
    const firstSeq = pending && pending.length ? pending[0].seq : null;
    if (server && Number.isInteger(server.lastSeq) && firstSeq != null && server.lastSeq >= firstSeq) {
      const local = this._headerBySeq(server.lastSeq);
      if (!local || local.hash === server.lastHash) {
        this.lastUploadedSeq = server.lastSeq;
        this.lastUploadAt = new Date().toISOString();
        this.uploadFailures = 0;
        this._nextUploadAt = 0;
        this._saveUploadState();
        this.log(`上传 409：服务端已收下至 seq=${server.lastSeq}（丢 ack），认领服务端头继续，不重基`);
        this.emit('upload', this.status());
        return;
      }
      this.log(`上传 409：服务端 seq=${server.lastSeq} 的头哈希与本地不符（真分叉），走重基`);
    }
    if (now - this._lastRebaseAt < REBASE_COOLDOWN_MS) {
      this.uploadFailures += 1;
      this._nextUploadAt = now + UPLOAD_BACKOFF_BASE_MS;
      this._saveUploadState();
      this.log('上传 409（服务端链已超前），重基冷却期内，只退避不重基');
      return;
    }
    this.log(`上传 409：服务端 lastSeq=${server.lastSeq}，本地 lastUploadedSeq=${this.lastUploadedSeq}，本地重基`);
    this._rebaseLocalChain(`服务端 lastSeq=${server.lastSeq} 超前`);
  }

  // 本地重基：删当前链文件（不同于自检隔离：重基是主动放弃旧链，不留 corrupt 副本，
  // 旧链头已在服务端有存档），seq 从 1、prevHash 全 0、游标归零。
  _rebaseLocalChain(reason) {
    try { fs.unlinkSync(this._headersFile()); } catch { /* 无文件时不用删 */ }
    this.headersCount = 0;
    this._prevHash = '0'.repeat(64);
    this._nextSeq = 1;
    this.lastUploadedSeq = 0;
    this.uploadFailures = 0;
    this._nextUploadAt = 0;
    this._lastRebaseAt = Date.now();
    this.chainOk = true;
    this._saveUploadState();
    this.log(`本地链已重基（${reason}）：旧链丢弃，下次同步起新链上传`);
    this.emit('chain-rebased', { reason });
  }

  async _postNodeJson(p, body, token) {
    const res = await fetch(this.apiBase + p, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-AI-Token': token },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15000),
    });
    let data = null;
    try { data = await res.json(); } catch { /* 非 JSON 体 */ }
    return { status: res.status, data };
  }

  async _getServerState(token) {
    const res = await fetch(this.apiBase + '/api/node/headers', {
      headers: { 'X-AI-Token': token },
      signal: AbortSignal.timeout(10000),
    });
    let data = null;
    try { data = await res.json(); } catch { /* 非 JSON 体 */ }
    if (!res.ok) throw new Error(`GET /api/node/headers -> HTTP ${res.status}`);
    return data;
  }

  // ---- 论坛心跳（v1 文档已写、一直未接线）：配了 token 才报，best-effort ----
  async _heartbeat() {
    const token = this._readForumToken();
    if (!token) return null;
    try {
      const { status } = await this._postNodeJson('/api/heartbeat', {
        status: 'working',
        task: `node sync-upload: ${this.headersCount} heads, uploaded ${this.lastUploadedSeq}`,
      }, token);
      if (status !== 200) this.log(`心跳回 HTTP ${status}（不影响同步）`);
      return status;
    } catch (err) {
      this.log(`心跳失败（不影响同步）：${err && err.message || err}`);
      return null;
    }
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
        try {
          const tail = JSON.parse(lines[lines.length - 1]);
          this._prevHash = tail.hash;
          // seq 从尾行续接；v0.3.0 之前的旧文件没有 seq，从 1 开始编新号。
          this._nextSeq = Number.isInteger(tail.seq) && tail.seq >= 1 ? tail.seq + 1 : 1;
        } catch { /* 尾行坏了从 0 续链 */ }
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
