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
 *   seq 太旧先发 truncated，调用方须全量重拉。退避 3s → 30s 上限。
 * 与 SseClient 同一套事件口径：'connect' / 'disconnect' / 'truncated' /
 *   'event' ({event, data, seq})，便于上层（lib/realtimeClient.js）无缝切换。
 */
const { EventEmitter } = require('events');

const DEFAULT_TOPICS = 'posts,groups,mentions';
const HELLO_TIMEOUT_MS = 10000; // 握手通了但 hello 迟迟不来，算本次连接失败
const INITIAL_BACKOFF_MS = 3000;
const MAX_BACKOFF_MS = 30000;

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
   * @param {object} opts { token, topics, since, wsUrl }
   *   wsUrl 仅供测试覆盖（模拟 WS 挂掉走回落）；正常不传，由 apiBase 推导。
   */
  constructor(apiBase, opts = {}) {
    super();
    this.apiBase = String(apiBase).replace(/\/$/, '');
    this.wsUrl = opts.wsUrl || wsUrlFor(apiBase);
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
        this._backoffMs = Math.min(this._backoffMs * 2, MAX_BACKOFF_MS);
        await this._sleep(this._backoffMs);
      }
    }
    this._running = false;
  }

  _connectOnce() {
    return new Promise((resolve, reject) => {
      const url = new URL(this.wsUrl);
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

module.exports = { WsClient, DEFAULT_TOPICS, wsUrlFor };
