'use strict';
/**
 * lib/wsClient.js — forum WebSocket stream client (kernel v2 transport).
 *
 * 端点（server.js 内核 v2，live 已验）：
 *   wss://forum.coldhao.win/ws?since=<seq>&topics=posts,groups,mentions&token=<session>
 * - since 是 *seq*（与 SSE 的 ?since= 同口径）。不带 since 只收直播。
 * - token 无效按匿名处理，公开流照常走；浏览器 WebSocket 不能带自定义头，
 *   所以只认 query 参数。
 * - 服务端→客户端：
 *     持久事件（与 SSE 同名同序）：{seq, type, ts, actor, data}
 *       post_created / reply_created / group_message / mention。
 *       直播的 group_message 的 data 额外带 body 与 attachments（省一次拉取）；
 *       断线补发的 group_message 没有 body——收到无 body 的要按 SSE 口径增量拉取。
 *     临时消息（不进缓冲、不持久化）：hello / unread / truncated /
 *       presence / typing / receipt / pong。
 *       hello 是 {type:'hello', data:{seq, time}}（seq 在 data 里，不在顶层）；
 *       truncated 是 {type:'truncated', data:{truncated, earliest, latest, seq}}。
 * - 客户端→服务端：{type:'ping'} → {type:'pong'}；
 *   {type:'typing', group_id}（服务端每客户端 3 秒限 1 次，不回显给自己）；
 *   {type:'receipt', group_id, seq} 或 {type:'receipt', scope:'mentions', seq}
 *   → ack + 全量 unread。都需要登录态，匿名发了服务端直接忽略。
 * - 服务端每 30 秒发 WS ping 帧，90 秒内无任何帧（含 pong）即断开。
 *   Node 内置 WebSocket（undici）收到 ping 帧会自动回 pong，不用应用层处理；
 *   长时间不断开即是自动回 pong 生效的证据（见 test/ws-live.js 的 100 秒保活）。
 *
 * 重连：断开后按 lastSeq 续接（?since=lastSeq），服务端补发缓冲事件；
 *   seq 太旧先发 truncated，调用方须全量重拉。退避 1s 起步、翻倍、30s 封顶
 *   （lib/backoff.js：nextBackoffMs）；连上后重置回 1s。
 * 与 SseClient 同一套事件口径：'connect' / 'disconnect' / 'truncated' /
 *   'event' ({event, data, seq})，便于上层（lib/realtimeClient.js）无缝切换。
 */
const { EventEmitter } = require('events');
const { nextBackoffMs, INITIAL_BACKOFF_MS, MAX_BACKOFF_MS } = require('./backoff');

const DEFAULT_TOPICS = 'posts,groups,mentions';
const HELLO_TIMEOUT_MS = 10000; // 握手通了但 hello 迟迟不来，算本次连接失败

function wsUrlFor(apiBase) {
  const u = new URL(String(apiBase).replace(/\/$/, ''));
  u.protocol = u.protocol === 'https:' ? 'wss:' : 'ws:';
  u.pathname = '/ws';
  u.search = '';
  u.hash = '';
  return u.toString();
}

class WsClient extends EventEmitter {
  /**
   * @param {string} apiBase e.g. https://forum.coldhao.win
   * @param {object} opts { token, topics, since, wsUrl, getApiBase }
   *   wsUrl 仅供测试覆盖（模拟 WS 挂掉走回落）；正常不传，由 apiBase 推导。
   *   getApiBase 是 ServerResolver 的读口（() => resolver.getBase()）：
   *   有它时每次建连都重新推导 wsUrl，基地址切到备地址后下一次重连
   *   自然跟上，不用重建客户端。
   */
  constructor(apiBase, opts = {}) {
    super();
    this.apiBase = String(apiBase).replace(/\/$/, '');
    this._wsUrlOverride = opts.wsUrl || null;
    this.wsUrl = opts.wsUrl || wsUrlFor(apiBase);
    this.getApiBase = typeof opts.getApiBase === 'function' ? opts.getApiBase : null;
    this.token = opts.token || null;
    this.topics = opts.topics || DEFAULT_TOPICS;
    this.lastSeq = opts.since != null ? opts.since : null;
    this._stopped = false;
    this._running = false;
    this._ws = null;
    this._helloTimer = null;
    this._helloReceived = false;
    this._backoffMs = INITIAL_BACKOFF_MS;
    this.connected = false;
    this.everConnected = false;
    this.lastBase = null; // 最近一次建连用的基地址（供上层判断基地址是否已切走）
  }

  setToken(token) { this.token = token || null; }

  start() {
    if (this._running) return;
    this._stopped = false;
    this._running = true;
    this._loop();
  }

  stop() {
    this._stopped = true;
    this._running = false;
    this._clearHelloTimer();
    this._clearSleepTimer();
    if (this._ws) { try { this._ws.close(); } catch { /* 已关 */ } }
    this._ws = null;
    this.connected = false;
  }

  /** 模拟断线（测试用）：只关 socket 不停循环，下一次循环按 backoff 重连。 */
  dropConnection() {
    if (this._ws) { try { this._ws.close(); } catch { /* 已关 */ } }
  }

  sendPing() { return this._send({ type: 'ping' }); }
  sendTyping(groupId) {
    if (!groupId) return false;
    return this._send({ type: 'typing', group_id: String(groupId) });
  }
  sendReceipt(groupId, seq) {
    const n = Math.floor(Number(seq));
    if (!groupId || !Number.isFinite(n) || n < 0) return false;
    return this._send({ type: 'receipt', group_id: String(groupId), seq: n });
  }
  sendReceiptMentions(seq) {
    const n = Math.floor(Number(seq));
    if (!Number.isFinite(n) || n < 0) return false;
    return this._send({ type: 'receipt', scope: 'mentions', seq: n });
  }

  _send(obj) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return false;
    try { this._ws.send(JSON.stringify(obj)); return true; } catch { return false; }
  }

  async _loop() {
    while (!this._stopped) {
      try {
        await this._connectOnce();
        // 已连上后正常断开：按退避重连。
        if (!this._stopped) await this._sleep(this._backoffMs);
      } catch (err) {
        if (this._stopped) break;
        this.connected = false;
        this.emit('disconnect', err);
        // 先按当前延迟睡，再翻倍：第一次失败只等 1s（INITIAL），
        // 随后 2s、4s、8s、16s、30s 封顶（nextBackoffMs）。
        await this._sleep(this._backoffMs);
        this._backoffMs = nextBackoffMs(this._backoffMs);
      }
    }
    this._running = false;
  }

  _connectOnce() {
    return new Promise((resolve, reject) => {
      // 每次建连都重新取基地址：resolver 切到备地址后，下一次重连
      // 就走新地址（显式 wsUrl 覆盖只用于测试，不参与 fallback）。
      // lastBase 记下这次用的基地址，供 RealtimeClient 判断“基地址
      // 是不是已被别的路切走了”。
      const base = this._wsUrlOverride
        ? null
        : (this.getApiBase ? this.getApiBase() : this.apiBase);
      const wsUrl = this._wsUrlOverride || wsUrlFor(base);
      this.lastBase = base;
      const url = new URL(wsUrl);
      url.searchParams.set('topics', this.topics);
      if (this.lastSeq != null) url.searchParams.set('since', String(this.lastSeq));
      if (this.token) url.searchParams.set('token', this.token);
      let ws;
      try {
        ws = new WebSocket(url.toString());
      } catch (err) { reject(err); return; }
      this._ws = ws;
      this._helloReceived = false;
      this._helloTimer = setTimeout(() => {
        if (this._helloReceived || this._stopped) return;
        try { ws.close(); } catch { /* 已关 */ }
        // close 事件随后触发，helloReceived 仍为 false，走 reject 分支。
        // 但 TCP 卡在 SYN-SENT 时 close 可能迟迟不触发 onclose：
        // 这里直接 reject，保证 _connectOnce 在 HELLO_TIMEOUT_MS 内
        // 一定落定，不会无限挂住（onclose 随后若再触发，重复 reject
        // 会被 Promise 忽略）。
        reject(new Error('WS hello 超时（10 秒内未收到 hello）'));
      }, HELLO_TIMEOUT_MS);
      ws.onopen = () => { /* 等 hello 再算连上 */ };
      ws.onmessage = (ev) => {
        let msg = null;
        try { msg = JSON.parse(String(ev.data)); } catch { return; }
        this._handleMessage(msg);
      };
      ws.onerror = () => { /* close 随后触发，统一在 onclose 里处理 */ };
      ws.onclose = (ev) => {
        this._clearHelloTimer();
        if (this._ws === ws) this._ws = null;
        if (this._stopped) { resolve(); return; }
        if (!this._helloReceived) {
          reject(new Error(`WS 连接失败（close ${ev && ev.code != null ? ev.code : ''}）`));
          return;
        }
        this.connected = false;
        this.emit('disconnect', new Error(`WS 断开（close ${ev && ev.code != null ? ev.code : ''}）`));
        resolve();
      };
    });
  }

  _handleMessage(msg) {
    if (!msg || typeof msg !== 'object') return;
    const type = msg.type;
    if (!type) return;
    // seq 推进：持久事件与带顶层 seq 的临时消息用顶层 seq；
    // hello 与 truncated 的 seq 在 data 里。
    if (Number.isFinite(msg.seq)) this.lastSeq = msg.seq;
    else if ((type === 'hello' || type === 'truncated') && msg.data && Number.isFinite(msg.data.seq)) {
      this.lastSeq = msg.data.seq;
    }
    const data = msg.data !== undefined ? msg.data : null;
    if (type === 'hello' && !this._helloReceived) {
      this._helloReceived = true;
      this._clearHelloTimer();
      this.connected = true;
      this.everConnected = true;
      this._backoffMs = INITIAL_BACKOFF_MS;
      this.emit('connect', { seq: this.lastSeq });
    }
    if (type === 'truncated') {
      this.emit('truncated', data);
      return;
    }
    this.emit('event', { event: type, data, seq: this.lastSeq });
    this.emit(type, data);
  }

  _clearHelloTimer() {
    if (this._helloTimer) { clearTimeout(this._helloTimer); this._helloTimer = null; }
  }

  _clearSleepTimer() {
    if (this._sleepTimer) { clearTimeout(this._sleepTimer); this._sleepTimer = null; }
  }

  // 可打断的 sleep：stop() 时清掉定时器，循环直接退出，不拖进程退出。
  _sleep(ms) {
    return new Promise((r) => {
      this._clearSleepTimer();
      this._sleepTimer = setTimeout(() => { this._sleepTimer = null; r(); }, ms);
    });
  }
}

module.exports = { WsClient, DEFAULT_TOPICS, wsUrlFor, INITIAL_BACKOFF_MS, MAX_BACKOFF_MS };
