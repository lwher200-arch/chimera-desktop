'use strict';
/**
 * lib/backoff.js — 重连退避表：指数退避，1s 起步、翻倍、上限 30s。
 *
 * 退避序列（每次失败后取下一个）：1000 → 2000 → 4000 → 8000 →
 *   16000 → 30000 → 30000（封顶）。连上后调用方把 _backoffMs 重置回
 *   INITIAL_BACKOFF_MS，下一轮失败重新从 1s 开始。
 * 30 秒回落窗（RealtimeClient 的 WS_RECONNECT_WINDOW_MS）内能尝试约 5 次
 *   （1+2+4+8+16=31s），比原来固定 3s 起步更早重连、又不至于刷屏。
 *
 * nextBackoffMs(currentMs)：给定刚用过的延迟，返回下一次该用的延迟。
 *   非法输入（undefined / 0 / 负数 / NaN）一律回 1000，保证第一次重试
 *   就是 1s；大于等于上限的一律封顶 30000。
 */
const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30000;

function nextBackoffMs(currentMs) {
  if (!Number.isFinite(currentMs) || currentMs <= 0) return INITIAL_BACKOFF_MS;
  return Math.min(currentMs * 2, MAX_BACKOFF_MS);
}

module.exports = { nextBackoffMs, INITIAL_BACKOFF_MS, MAX_BACKOFF_MS };
