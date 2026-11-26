'use strict';
/**
 * lib/outbox.js — 离线发件箱（v0.3.15）：纯桌面端本地逻辑。
 *
 * 规则（用户 2026-11-26 定“断网自动发”）：
 *  - 只有“网络层失败（无 HTTP 响应）”的发送才入箱。HTTP 错（含 500）
 *    原样抛给用户——POST 可能已落库，重发会重复。
 *  - 每条入箱消息带客户端唯一 id（ob- + randomUUID）；发送成功即删条目，
 *    删了就不会重发；同 clientId 重复入箱直接丢弃。
 *  - 发送中（_inFlight）防并发重发：flush 单航班 + 逐条串行 + 发送中
 *    的条目跳过；discard 也不删发送中的条目。
 *  - flush 逐条按序发：网络错即停（后面的也发不出去，留待下次触发）；
 *    HTTP 错是永久失败，标 dead 继续下一条（dead 条目不再自动重试，
 *    用户可在界面上丢弃）。
 *  - 触发点（主进程）：实时流重连成功时 + 有待发条目时的 60 秒定时器。
 *  - 草稿与发件箱是两回事：草稿是没发出去的文字（渲染进程 localStorage，
 *    关应用不自动发），发件箱是已提交待发送的消息（这里）。别混在一起。
 */
const crypto = require('node:crypto');
const { ForumApiError } = require('./forumClient');

function createClientId() {
  return 'ob-' + crypto.randomUUID();
}

// 网络层失败 = 不是 ForumApiError（服务端已响应、带 status 的都是 HTTP 错）。
function isNetworkError(err) {
  return !(err instanceof ForumApiError);
}

class Outbox {
  /**
   * @param {object} opts { store, send, onChange }
   *   store：lib/store 的 Store 实例（state.outbox 持久化）。
   *   send：async (entry) => 实际发送（ForumClient 调法）；成功 resolve，失败 throw。
   *   onChange：(status) => 每次变动后回调（主进程推给渲染进程）。
   */
  constructor(opts = {}) {
    this.store = opts.store || null;
    this.send = opts.send || null;
    this.onChange = opts.onChange || null;
    this._flushing = false;
    this._inFlight = new Set();
  }

  get entries() {
    return this.store ? (this.store.state.outbox || []) : [];
  }

  status() {
    const list = this.entries;
    return {
      pending: list.filter((e) => !e.dead).length,
      failed: list.filter((e) => e.dead).length,
      total: list.length,
      entries: list.map((e) => ({ ...e })),
    };
  }

  _save() {
    if (this.store) this.store.save();
  }

  _emit() {
    if (this.onChange) {
      try { this.onChange(this.status()); } catch { /* 推送失败不挡发件箱本身 */ }
    }
  }

  enqueue(entry) {
    const item = {
      clientId: entry.clientId || createClientId(),
      kind: entry.kind,
      target: entry.target || {},
      body: entry.body || '',
      title: entry.title || '',
      attachments: Array.isArray(entry.attachments) ? entry.attachments : [],
      createdAt: entry.createdAt || new Date().toISOString(),
      attempts: 0,
      dead: false,
      lastError: null,
    };
    const list = this.entries;
    const dup = list.find((e) => e.clientId === item.clientId);
    if (dup) return dup;
    list.push(item);
    this._save();
    this._emit();
    return item;
  }

  discard(clientId) {
    if (this._inFlight.has(clientId)) return false;
    const list = this.entries;
    const idx = list.findIndex((e) => e.clientId === clientId);
    if (idx < 0) return false;
    list.splice(idx, 1);
    this._save();
    this._emit();
    return true;
  }

  async flush() {
    if (this._flushing || !this.send) return { flushed: 0, kept: this.entries.length, skipped: true };
    this._flushing = true;
    let flushed = 0;
    try {
      for (const entry of this.entries.slice()) {
        if (entry.dead) continue;
        if (this._inFlight.has(entry.clientId)) continue;
        this._inFlight.add(entry.clientId);
        try {
          entry.attempts += 1;
          await this.send(entry);
          const list = this.entries;
          const idx = list.findIndex((e) => e.clientId === entry.clientId);
          if (idx >= 0) list.splice(idx, 1);
          flushed += 1;
          this._save();
        } catch (err) {
          if (err instanceof ForumApiError) {
            entry.dead = true;
            entry.lastError = err.message || `HTTP ${err.status}`;
            this._save();
          } else {
            this._save();
            break;
          }
        } finally {
          this._inFlight.delete(entry.clientId);
        }
      }
    } finally {
      this._flushing = false;
      this._emit();
    }
    return { flushed, kept: this.entries.length, skipped: false };
  }
}

module.exports = { Outbox, createClientId, isNetworkError };
