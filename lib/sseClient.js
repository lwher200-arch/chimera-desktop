'use strict';
/**
 * lib/sseClient.js — forum SSE stream client (kernel v1 transport).
 *
 * Endpoint (server.js ~L2321, verified against the live copy):
 *   GET /api/stream?since=<seq>&topics=posts,groups,mentions&token=<session>
 * - since is a *seq* number (NOT the ISO timestamp used by ?since= on
 *   /api/posts and /api/groups/:id/messages). Omit since to live-tail only.
 * - Invalid token is treated as anonymous; the public stream still flows.
 * - Server sends `retry: 3000`, a `hello` event with the current seq, an
 *   `unread` event on connect (logged-in only), a `truncated` event when
 *   `since` is older than the retained buffer, and a `: ping` comment
 *   every 20s.
 * - Event payloads seen live: group_message {id, group_id, author,
 *   author_type, ts, shared_post_id} — NOTE: no body. The client must
 *   fetch the new message via GET /api/groups/:id/messages?since=<ISO>.
 *
 * Reconnect: on drop, resume from the last seen seq (server replays the
 * buffer when it still holds it, else sends `truncated` and the client
 * falls back to a full reload). Backoff 1s -> 30s max (lib/backoff.js).
 */
const { EventEmitter } = require('events');
const { nextBackoffMs, INITIAL_BACKOFF_MS } = require('./backoff');

const DEFAULT_TOPICS = 'posts,groups,mentions';

class SseClient extends EventEmitter {
  /**
   * @param {string} apiBase e.g. https://forum.coldhao.win
   * @param {object} opts { token, topics, onEvent }
   */
  constructor(apiBase, opts = {}) {
    super();
    this.apiBase = String(apiBase).replace(/\/$/, '');
    this.token = opts.token || null;
    this.topics = opts.topics || DEFAULT_TOPICS;
    this.lastSeq = opts.since != null ? opts.since : null;
    this._stopped = false;
    this._running = false;
    this._abort = null;
    this._backoffMs = INITIAL_BACKOFF_MS;
    this.connected = false;
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
    if (this._abort) { try { this._abort.abort(); } catch { /* already done */ } }
    this._abort = null;
    this.connected = false;
  }

  async _loop() {
    while (!this._stopped) {
      try {
        await this._connectOnce();
        // Clean close (server ended the stream): reconnect after backoff.
        if (!this._stopped) await this._sleep(this._backoffMs);
      } catch (err) {
        if (this._stopped) break;
        this.connected = false;
        this.emit('disconnect', err);
        // 先按当前延迟睡，再翻倍（与 WsClient 同一口径，见 lib/backoff.js）。
        await this._sleep(this._backoffMs);
        this._backoffMs = nextBackoffMs(this._backoffMs);
      }
    }
    this._running = false;
  }

  async _connectOnce() {
    const params = new URLSearchParams({ topics: this.topics });
    if (this.lastSeq != null) params.set('since', String(this.lastSeq));
    if (this.token) params.set('token', this.token);
    this._abort = new AbortController();
    const res = await fetch(`${this.apiBase}/api/stream?${params.toString()}`, {
      headers: { Accept: 'text/event-stream' },
      signal: this._abort.signal,
    });
    if (!res.ok || !res.body) {
      throw new Error(`SSE connect failed: HTTP ${res.status}`);
    }
    this.connected = true;
    this._backoffMs = INITIAL_BACKOFF_MS;
    this.emit('connect');
    const reader = res.body.getReader();
    const decoder = new TextDecoder('utf-8');
    let buf = '';
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf('\n\n')) !== -1) {
          const raw = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          this._handleBlock(raw);
        }
        if (this._stopped) break;
      }
    } finally {
      // cancel() 返回 promise：流已因 abort 出错时它会 reject，
      // 必须接住，否则成未处理 rejection（实测 AbortError 顶层抛出）。
      try { await reader.cancel().catch(() => {}); } catch { /* already closed */ }
    }
  }

  _handleBlock(raw) {
    let event = 'message';
    let id = null;
    const dataLines = [];
    for (const line of raw.split('\n')) {
      if (line.startsWith(':')) continue; // comment / ping
      if (line.startsWith('event:')) event = line.slice(6).trim();
      else if (line.startsWith('data:')) dataLines.push(line.slice(5).replace(/^ /, ''));
      else if (line.startsWith('id:')) id = line.slice(3).trim();
      else if (line.startsWith('retry:')) { /* server retry hint, keep our own backoff */ }
    }
    if (id !== null) {
      const n = Number(id);
      if (Number.isFinite(n)) this.lastSeq = n;
    }
    if (!dataLines.length) return;
    let data = null;
    try { data = JSON.parse(dataLines.join('\n')); } catch { data = dataLines.join('\n'); }
    if (event === 'truncated') {
      // Our seq is older than the retained buffer: the caller must reload.
      this.emit('truncated', data);
      return;
    }
    this.emit('event', { event, data, seq: this.lastSeq });
    this.emit(event, data);
  }

  _sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }
}

module.exports = { SseClient, DEFAULT_TOPICS };
