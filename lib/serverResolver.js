'use strict';
/**
 * lib/serverResolver.js — 服务端 URL 解析与 fallback。
 *
 * 问题：桌面端只认一个基地址（https://forum.coldhao.win，公网域名）。
 * lenghao 的公网出口一抖（2026-11-23 19:47 起，github.com 仍 000），
 * 桌面端就全断，尽管论坛本身健康、尾网 http://100.79.244.3:8899
 * 502ms 可达。论坛页面（REST）与实时流（WS/SSE）都走这个基地址，
 * 所以 fallback 必须放在基地址这一层，而不是每路各做一套。
 *
 * 候选：
 *  - 主：https://forum.coldhao.win（公网；可用 CHIMERA_API_BASE 覆盖）
 *  - 备：http://100.79.244.3:8899（尾网 het；只有尾网内的机器能连上）
 *
 * 规则（防抖、防死循环）：
 *  - 平时只用主地址，不探测、不切换。
 *  - 只有“当前基地址被证实挂了”才切：reportFailure() 先探当前基地址
 *    的 /api/health，还通就说明只是某条路（WS/某次请求）的问题，
 *    不切；当前基地址确实不通、且另一边探通了，才切过去。
 *  - 另一边也不通时留在当前基地址上，继续按退避重试，不来回切。
 *    公网用户没有尾网地址，备地址永远探不通，就永远留在主地址上。
 *  - 切到备地址后，每 60 秒探一次主地址，通了就切回（只在备地址上
 *    跑这个定时器；切回主地址后即停）。已建立的连接不受影响——
 *    切回只影响新发起的请求/连接，WS 不会因此被掐断重连。
 *  - 切换只发一次 'switch' 事件（{from, to}），主进程打日志；
 *    各路客户端每次建连都读 getBase()，自然跟上新地址。
 *  - 探测本身带超时（默认 5s），挂掉的主机快速失败，不拖住调用方。
 */
const { EventEmitter } = require('events');

const PRIMARY_BASE = 'https://forum.coldhao.win';
const FALLBACK_BASE = 'http://100.79.244.3:8899';
const PROBE_TIMEOUT_MS = 5000;
const RECHECK_MS = 60000; // 在备地址上时，多久探一次主地址看它恢复没有

class ServerResolver extends EventEmitter {
  /**
   * @param {object} opts { primary, fallback, probeTimeoutMs, recheckMs, fetch }
   *   fetch 仅供测试注入；正常用全局 fetch。
   */
  constructor(opts = {}) {
    super();
    this.primary = String(opts.primary || process.env.CHIMERA_API_BASE || PRIMARY_BASE).replace(/\/$/, '');
    this.fallback = String(opts.fallback || FALLBACK_BASE).replace(/\/$/, '');
    this.current = this.primary;
    this.probeTimeoutMs = opts.probeTimeoutMs || PROBE_TIMEOUT_MS;
    this.recheckMs = opts.recheckMs || RECHECK_MS;
    this._fetch = opts.fetch || fetch;
    this._recheckTimer = null;
    this._reportPromise = null; // 进行中的 reportFailure：并发调用共用同一轮探测
  }

  /** 当前基地址（各路客户端每次建连都读这个）。 */
  getBase() { return this.current; }

  /** 当前是不是备地址（尾网）。 */
  get isFallback() { return this.current !== this.primary; }

  /** 给人看的标签：'公网' | '尾网'，指示器与日志用。 */
  label() { return this.isFallback ? '尾网' : '公网'; }

  /** 探一个基地址：GET /api/health，超时算不通。 */
  async probe(base) {
    try {
      const res = await this._fetch(String(base).replace(/\/$/, '') + '/api/health', {
        signal: AbortSignal.timeout(this.probeTimeoutMs),
      });
      return res.ok;
    } catch { return false; }
  }

  /**
   * 当前基地址出故障时调用。先探当前基地址——还通就不切（只是某条路
   * 的问题）；确实不通、且另一边探通了，才切过去并返回新基地址；
   * 另一边也不通时返回 null（留在当前基地址上继续按退避重试）。
   * 并发调用共用同一轮探测（_reportPromise）：否则两路同时报故障
   * 会各探各的、先后切出两条 switch 记录（第二条 from/to 都是备地址，
   * 看着像在原地切）。_doReportFailure 切之前会再确认 current 还是
   * 报故障时的那个基地址，已被别人切走就直接返回。
   */
  reportFailure() {
    if (this._reportPromise) return this._reportPromise;
    this._reportPromise = this._doReportFailure();
    const p = this._reportPromise;
    const clear = () => { if (this._reportPromise === p) this._reportPromise = null; };
    p.then(clear, clear);
    return p;
  }

  async _doReportFailure() {
    const failedBase = this.current;
    if (await this.probe(failedBase)) return null; // 当前还通，不切
    // 探测期间可能已被另一路切走：只在还停在故障基地址上时才切。
    if (this.current !== failedBase) return this.current;
    const other = failedBase === this.primary ? this.fallback : this.primary;
    if (other === failedBase) return null; // 只有一个候选，无处可切
    if (!(await this.probe(other))) return null; // 另一边也不通，留着
    if (this.current !== failedBase) return this.current; // 探测期间已被切走
    const from = this.current;
    this.current = other;
    this.emit('switch', { from, to: other });
    if (other === this.fallback) this._startRecheck();
    else this._stopRecheck();
    return other;
  }

  // 在备地址上时定期探主地址，通了就切回（只影响新请求/新连接）。
  _startRecheck() {
    if (this._recheckTimer) return;
    this._recheckTimer = setInterval(async () => {
      if (this.current !== this.fallback) { this._stopRecheck(); return; }
      if (await this.probe(this.primary)) {
        const from = this.current;
        this.current = this.primary;
        this.emit('switch', { from, to: this.primary });
        this._stopRecheck();
      }
    }, this.recheckMs);
    if (typeof this._recheckTimer.unref === 'function') this._recheckTimer.unref();
  }

  _stopRecheck() {
    if (this._recheckTimer) { clearInterval(this._recheckTimer); this._recheckTimer = null; }
  }

  stop() { this._stopRecheck(); }
}

module.exports = { ServerResolver, PRIMARY_BASE, FALLBACK_BASE, PROBE_TIMEOUT_MS, RECHECK_MS };
