'use strict';
/**
 * lib/realtimeClient.js — 实时传输：WS 优先，SSE 回落。
 *
 * 策略（单向回落，一个 start() 会话内不切回）：
 *  - 启动先连 WS（lib/wsClient.js）。收到 hello 即 WS 可用，mode='ws'。
 *  - WS 在首次 hello 之前就失败（握手失败、hello 超时、连不上）→ 立刻停 WS、
 *    起 SSE（lib/sseClient.js），lastSeq 原样带过去，mode='sse'。
 *  - WS 连上后断开：WsClient 自己按退避重连；30 秒内重连不上 → 切 SSE。
 *  - 一旦切到 SSE，本会话不再回 WS（SSE 本身完整可用）；下次 start()
 *    （登录/登出/重启）重新先试 WS。
 *
 * 对外事件口径与 SseClient 一致：'connect' ({transport, seq}) /
 *   'disconnect' (err) / 'truncated' (data) / 'event' ({event, data, seq})。
 *   'connect' 的 payload 带 transport（'ws' | 'sse'），便于主进程打日志与排障。
 *   另有 'gap' 事件（v0.3.1 节点上传）：流内事件 seq 出现缺口
 *   （seq > lastSeq + 1）时发 {expected, got, transport}，主进程据此
 *   触发节点 syncOnce() 即时重拉（断点续拉），不等待 30 秒同步周期。
 * 另有 'transport' 事件（v0.2.1 状态指示器用）：每次传输状态变化都发
 *   {state, detail}，state ∈ 'connecting' | 'ws' | 'sse' |
 *   'disconnected' | 'session-expired'，渲染进程只按这个画指示器。
 * 会话失效（v0.2.1）：token 已死时 WS 仍会以匿名身份连上、或在 hello
 *   前就失败，两种都不能无限重试。构造时可传 opts.checkSession（async
 *   函数，调一次已登录的 REST，如 GET /api/me）：只有它抛 401/403 才算
 *   会话失效——网络错、超时、500 一律不算，仍按瞬断走回落/重连。
 *   会话失效是终态：停 WS、不回落 SSE、不再重试，发 'session-expired'
 *   + transport 'session-expired'，等用户重新登录（新建 RealtimeClient）。
 *   不传 checkSession 时保持老行为（直接回落 SSE），测试可注入假函数。
 *
 * 事件 data 归一化：SSE 的 data 是整条 ev（{seq, type, ts, actor, payload}），
 *   WS 的 data 本来就是 payload。两路统一剥到 payload 层再往上交，
 *   这样渲染进程收到的 {event, data} 在两路下形状一致——
 *   group_message 的 data.group_id 能对上（SSE 路原来 data 是整条 ev，
 *   data.group_id 恒为 undefined，打开的群聊收不到增量刷新；归一化后修好，
 *   渲染进程本身不用改）。
 *   hello / unread / truncated / presence / typing / receipt / pong 的 data
 *   没有 payload 字段，原样透传。
 *
 * sendTyping / sendReceipt 只在 WS 路上走；SSE 没有对应通道时返回 false，
 *   调用方仍可走 REST（POST /api/read）。
 */
const { EventEmitter } = require('events');
const { WsClient } = require('./wsClient');
const { SseClient } = require('./sseClient');

const WS_RECONNECT_WINDOW_MS = 30000; // WS 断开后 30 秒重连不上就切 SSE

function normalizeEventData(raw) {
  if (raw && typeof raw === 'object' && raw.payload && typeof raw.payload === 'object') {
    return raw.payload;
  }
  return raw;
}

class RealtimeClient extends EventEmitter {
  /**
   * @param {string} apiBase e.g. https://forum.coldhao.win
   * @param {object} opts { token, topics, since, wsUrl, checkSession, resolver }
   *   wsUrl 仅供测试覆盖（模拟 WS 挂掉走回落）。
   *   checkSession 仅供主进程注入（async () => await client.getMe(token)）；
   *   不传时会话失效检测关闭，保持老行为。
   *   resolver 是 ServerResolver 单例：有它时 WS/SSE 每次建连都读
   *   resolver.getBase()，WS 在 hello 前失败会先试切基地址（主挂了
   *   切尾网），切成功就重连 WS，不切才回落 SSE；transport 事件的
   *   detail 里带当前基地址标签（公网/尾网），指示器能看出走哪条路。
   */
  constructor(apiBase, opts = {}) {
    super();
    this.apiBase = String(apiBase).replace(/\/$/, '');
    this.token = opts.token || null;
    this.topics = opts.topics || null;
    this.since = opts.since != null ? opts.since : null;
    this.wsUrl = opts.wsUrl || null;
    this.checkSession = typeof opts.checkSession === 'function' ? opts.checkSession : null;
    this.resolver = opts.resolver || null;
    this.transport = null; // 'ws' | 'sse' | null（null = 还没连上任何一路）
    this.lastSeq = this.since;
    this._stopped = false;
    this._ws = null;
    this._sse = null;
    this._wsFallbackTimer = null;
    this._sessionDeadFlag = false; // 会话失效是终态，见 _failSession
    this._sessionChecked = false; // 每 start() 只在首次 hello 后验一次会话
  }

  /** 会话是否已判定失效（终态，等重新登录）。 */
  get sessionExpired() { return this._sessionDeadFlag; }

  setToken(token) {
    this.token = token || null;
    if (this._ws) this._ws.setToken(this.token);
    if (this._sse) this._sse.setToken(this.token);
  }

  // transport 事件的 detail 里带基地址标签（公网/尾网），指示器能
  // 看出当前走哪条路；没 resolver 时不带。
  _serverSuffix() {
    return this.resolver ? `（${this.resolver.label()}）` : '';
  }

  start() {
    this.stop();
    this._stopped = false;
    this._sessionDeadFlag = false;
    this._sessionChecked = false;
    this.emit('transport', { state: 'connecting', detail: `正在连接 WS…${this._serverSuffix()}` });
    this._startWs();
  }

  stop() {
    this._stopped = true;
    this._clearWsFallbackTimer();
    if (this._ws) { try { this._ws.stop(); } catch {} this._ws = null; }
    if (this._sse) { try { this._sse.stop(); } catch {} this._sse = null; }
    this.transport = null;
  }

  sendTyping(groupId) {
    if (this.transport === 'ws' && this._ws) return this._ws.sendTyping(groupId);
    return false;
  }

  sendReceipt(groupId, seq) {
    if (this.transport === 'ws' && this._ws) return this._ws.sendReceipt(groupId, seq);
    return false;
  }

  sendReceiptMentions(seq) {
    if (this.transport === 'ws' && this._ws) return this._ws.sendReceiptMentions(seq);
    return false;
  }

  // ---- WS 路 ----
  _startWs() {
    const ws = new WsClient(this.apiBase, {
      token: this.token,
      topics: this.topics || undefined,
      since: this.lastSeq,
      wsUrl: this.wsUrl || undefined,
      getApiBase: this.resolver ? () => this.resolver.getBase() : undefined,
    });
    this._ws = ws;
    ws.on('connect', ({ seq }) => {
      if (this._ws !== ws || this._stopped) return;
      this._clearWsFallbackTimer();
      this.transport = 'ws';
      if (Number.isFinite(seq)) this.lastSeq = seq;
      this.emit('connect', { transport: 'ws', seq: this.lastSeq });
      this.emit('transport', { state: 'ws', detail: `WS 已连接${this._serverSuffix()}` });
      this._checkSessionOnce(); // 首次 hello 后验一次会话（见该方法注释）
    });
    ws.on('disconnect', async (err) => {
      if (this._ws !== ws || this._stopped) return;
      if (!ws.everConnected) {
        // 先停掉旧 WS：它在等基地址探测时还会按退避再发起一次连接，
        // 两次失败会触发两次 failover 判定。停掉后循环在睡完这次退避
        // 即退出，不再建连（见 WsClient._loop 的 while 条件）。
        try { ws.stop(); } catch { /* 已停 */ }
        // 首个 hello 之前就失败：先验会话——会话已死就不重试、不回落，
        // 直接报会话失效；否则先看基地址是不是已被别的路切走了
        // （REST 先报了故障）：切走了就直接在新基地址上重连 WS；
        // 没切走才试切一次，切成功重连 WS，切不成功（另一边也不通，
        // 或只是 WS 本身的问题）才按老路立刻回落 SSE。
        const dead = await this._sessionDead();
        if (this._ws !== ws || this._stopped) return; // 等待期间已被停掉
        if (dead) {
          this._failSession('WS 首次连接失败且会话已失效');
          return;
        }
        if (this.resolver) {
          const triedBase = ws.lastBase || this.apiBase;
          if (this.resolver.getBase() !== triedBase) {
            // 基地址已被别的路切走：直接在新基地址上重连 WS。
            this.emit('transport', { state: 'connecting', detail: `已切到${this.resolver.label()}，正在重连 WS…` });
            this._startWs();
            return;
          }
          const switched = await this.resolver.reportFailure();
          if (this._ws !== ws || this._stopped) return;
          if (switched) {
            this.emit('transport', { state: 'connecting', detail: `已切到${this.resolver.label()}，正在重连 WS…` });
            this._startWs();
            return;
          }
        }
        this._fallbackToSse(new Error(`WS 首次连接失败：${err && err.message || err}`));
        return;
      }
      // 连上后断开：先让 WsClient 自己按退避重连，30 秒还连不上再切 SSE。
      this.emit('disconnect', err);
      this.emit('transport', { state: 'disconnected', detail: `WS 已断开，重连中…${this._serverSuffix()}` });
      this._armWsFallbackTimer();
    });
    ws.on('truncated', (data) => {
      if (this._ws !== ws || this._stopped) return;
      if (data && Number.isFinite(data.seq)) this.lastSeq = data.seq;
      this.emit('truncated', data);
    });
    ws.on('event', ({ event, data, seq }) => {
      if (this._ws !== ws || this._stopped) return;
      if (Number.isFinite(seq)) {
        if (this.lastSeq != null && seq > this.lastSeq + 1) {
          this.emit('gap', { expected: this.lastSeq + 1, got: seq, transport: 'ws' });
        }
        this.lastSeq = seq;
      }
      this.emit('event', { event, data: normalizeEventData(data), seq: this.lastSeq });
    });
    ws.start();
  }

  // ---- 会话失效（终态） ----
  // 会话是不是死了：只有 401/403 算。网络错、超时、500 一律不算，
  // 否则一次网络抖动就会把用户踢下线。没 token 或没注入 checkSession
  // 时永远返回 false（匿名流没有会话可死）。
  async _sessionDead() {
    if (!this.token || typeof this.checkSession !== 'function') return false;
    try {
      await this.checkSession();
      return false;
    } catch (err) {
      return err && (err.status === 401 || err.status === 403);
    }
  }

  // 首次 hello 后验一次会话：token 已死时 WS 仍会以匿名身份连上，
  // 指示器会误报“已连接”。只验一次（每 start() 一次），不阻塞 connect。
  async _checkSessionOnce() {
    if (this._sessionChecked || this._stopped) return;
    this._sessionChecked = true;
    if (!this.token || !this.checkSession) return;
    if (await this._sessionDead()) this._failSession('会话已失效（服务端拒绝了当前 token）');
  }

  // 会话失效是终态：停掉 WS 与 SSE、不回落、不再重试；指示器报会话失效，
  // 等用户重新登录（auth:login 会建新的 RealtimeClient 重新 start）。
  _failSession(reason) {
    if (this._stopped) return;
    this._clearWsFallbackTimer();
    if (this._ws) { try { this._ws.stop(); } catch {} this._ws = null; }
    if (this._sse) { try { this._sse.stop(); } catch {} this._sse = null; }
    this.transport = null;
    this._sessionDeadFlag = true;
    const detail = String(reason);
    this.emit('session-expired', { reason: detail });
    this.emit('transport', { state: 'session-expired', detail });
  }

  _armWsFallbackTimer() {
    this._clearWsFallbackTimer();
    this._wsFallbackTimer = setTimeout(() => {
      if (this._stopped || this.transport !== 'ws') return;
      this._fallbackToSse(new Error('WS 断开 30 秒未重连上，切 SSE'));
    }, WS_RECONNECT_WINDOW_MS);
  }

  _clearWsFallbackTimer() {
    if (this._wsFallbackTimer) { clearTimeout(this._wsFallbackTimer); this._wsFallbackTimer = null; }
  }

  // ---- SSE 回落 ----
  _fallbackToSse(reason) {
    if (this._stopped) return;
    this._clearWsFallbackTimer();
    if (this._ws) { try { this._ws.stop(); } catch {} this._ws = null; }
    if (this._sse) return; // 已在 SSE 路上
    const sse = new SseClient(this.apiBase, {
      token: this.token,
      topics: this.topics || undefined,
      since: this.lastSeq,
      getApiBase: this.resolver ? () => this.resolver.getBase() : undefined,
    });
    this._sse = sse;
    sse.on('connect', () => {
      if (this._sse !== sse || this._stopped) return;
      this.transport = 'sse';
      this.emit('connect', { transport: 'sse', seq: this.lastSeq });
      this.emit('transport', { state: 'sse', detail: `SSE 回落${this._serverSuffix()}` });
    });
    sse.on('disconnect', (err) => {
      if (this._sse !== sse || this._stopped) return;
      // SSE 断了也可能是基地址挂了：报给 resolver（不 await，SseClient
      // 自己按退避重连；下一次建连读到的就是切过去的新基地址）。
      if (this.resolver) this.resolver.reportFailure().catch(() => {});
      this.emit('disconnect', err);
      this.emit('transport', { state: 'disconnected', detail: `SSE 已断开，重连中…${this._serverSuffix()}` });
    });
    sse.on('truncated', (data) => {
      if (this._sse !== sse || this._stopped) return;
      this.emit('truncated', data);
    });
    sse.on('event', ({ event, data, seq }) => {
      if (this._sse !== sse || this._stopped) return;
      if (Number.isFinite(seq)) {
        if (this.lastSeq != null && seq > this.lastSeq + 1) {
          this.emit('gap', { expected: this.lastSeq + 1, got: seq, transport: 'sse' });
        }
        this.lastSeq = seq;
      }
      this.emit('event', { event, data: normalizeEventData(data), seq: this.lastSeq });
    });
    // 先报一声“正在回落”，再等 SSE 自己的 connect；不断线感知不中断。
    this.emit('fallback', { from: 'ws', to: 'sse', reason: String(reason && reason.message || reason) });
    this.emit('transport', { state: 'connecting', detail: `WS 不可用，正在回落 SSE…${this._serverSuffix()}` });
    sse.start();
  }
}

module.exports = { RealtimeClient, normalizeEventData };
