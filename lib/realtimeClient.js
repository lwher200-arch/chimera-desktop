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
   * @param {object} opts { token, topics, since, wsUrl }
   *   wsUrl 仅供测试覆盖（模拟 WS 挂掉走回落）。
   */
  constructor(apiBase, opts = {}) {
    super();
    this.apiBase = String(apiBase).replace(/\/$/, '');
    this.token = opts.token || null;
    this.topics = opts.topics || null;
    this.since = opts.since != null ? opts.since : null;
    this.wsUrl = opts.wsUrl || null;
    this.transport = null; // 'ws' | 'sse' | null（null = 还没连上任何一路）
    this.lastSeq = this.since;
    this._stopped = false;
    this._ws = null;
    this._sse = null;
    this._wsFallbackTimer = null;
  }

  setToken(token) {
    this.token = token || null;
    if (this._ws) this._ws.setToken(this.token);
    if (this._sse) this._sse.setToken(this.token);
  }

  start() {
    this.stop();
    this._stopped = false;
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
    });
    this._ws = ws;
    ws.on('connect', ({ seq }) => {
      if (this._ws !== ws || this._stopped) return;
      this._clearWsFallbackTimer();
      this.transport = 'ws';
      if (Number.isFinite(seq)) this.lastSeq = seq;
      this.emit('connect', { transport: 'ws', seq: this.lastSeq });
    });
    ws.on('disconnect', (err) => {
      if (this._ws !== ws || this._stopped) return;
      if (!ws.everConnected) {
        // 首个 hello 之前就失败：WS 不可用，立刻回落 SSE。
        this._fallbackToSse(new Error(`WS 首次连接失败：${err && err.message || err}`));
        return;
      }
      // 连上后断开：先让 WsClient 自己重连，30 秒还连不上再切 SSE。
      this.emit('disconnect', err);
      this._armWsFallbackTimer();
    });
    ws.on('truncated', (data) => {
      if (this._ws !== ws || this._stopped) return;
      if (data && Number.isFinite(data.seq)) this.lastSeq = data.seq;
      this.emit('truncated', data);
    });
    ws.on('event', ({ event, data, seq }) => {
      if (this._ws !== ws || this._stopped) return;
      if (Number.isFinite(seq)) this.lastSeq = seq;
      this.emit('event', { event, data: normalizeEventData(data), seq: this.lastSeq });
    });
    ws.start();
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
    });
    this._sse = sse;
    sse.on('connect', () => {
      if (this._sse !== sse || this._stopped) return;
      this.transport = 'sse';
      this.emit('connect', { transport: 'sse', seq: this.lastSeq });
    });
    sse.on('disconnect', (err) => {
      if (this._sse !== sse || this._stopped) return;
      this.emit('disconnect', err);
    });
    sse.on('truncated', (data) => {
      if (this._sse !== sse || this._stopped) return;
      this.emit('truncated', data);
    });
    sse.on('event', ({ event, data, seq }) => {
      if (this._sse !== sse || this._stopped) return;
      if (Number.isFinite(seq)) this.lastSeq = seq;
      this.emit('event', { event, data: normalizeEventData(data), seq: this.lastSeq });
    });
    // 先报一声“正在回落”，再等 SSE 自己的 connect；不断线感知不中断。
    this.emit('fallback', { from: 'ws', to: 'sse', reason: String(reason && reason.message || reason) });
    sse.start();
  }
}

module.exports = { RealtimeClient, normalizeEventData };
