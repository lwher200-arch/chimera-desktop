'use strict';
/**
 * lib/store.js — local JSON state in the user-data dir.
 * NEVER holds: passwords, node forum tokens, or any secret in plaintext.
 * Secrets live only as safeStorage-encrypted blobs (main process).
 */
const fs = require('fs');
const path = require('path');

const DEFAULT_STATE = {
  version: 1,
  session: null, // { username, encToken, expiresAt } — encToken is a safeStorage blob
  node: {
    enabled: false, // 节点开关：默认关，用户自由选择开/关，持久化
    port: 18311,
    seeds: [], // 引导发现源（默认论坛公网地址）；gossip peer 交换是 v2
    forumTokenFile: null, // 节点论坛身份 token 文件（0600），未配时节点只本地跑
    nodeAccountName: null, // 激活时服务端分配的节点账号名（仅展示用；token 只在 forum-token 文件里）
  },
  prefs: {
    lastView: 'conv',
    window: null, // { width, height } 上次窗口大小
  },
  notif: {
    readTs: null, // 上次全部已读的服务端水位（POST /api/notifications/read 回的 notifReadTs）；@未读 = ts > readTs 的 @条目数
    mentionEnabled: true, // @提醒开关（本地偏好，默认开；网页版 forum_notif_mention 同口径）
  },
  outbox: [], // 离线发件箱 [{clientId, kind, target, body, title, attachments, createdAt, attempts, dead, lastError}]，见 lib/outbox.js
};

class Store {
  constructor(filePath) {
    this.filePath = filePath;
    this.state = load(filePath);
  }

  save() {
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
    const tmp = this.filePath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(this.state, null, 2));
    fs.renameSync(tmp, this.filePath);
  }

  update(fn) {
    fn(this.state);
    this.save();
  }
}

function load(filePath) {
  try {
    const raw = JSON.parse(fs.readFileSync(filePath, 'utf8'));
    return {
      ...structuredClone(DEFAULT_STATE),
      ...raw,
      node: { ...DEFAULT_STATE.node, ...(raw.node || {}) },
      prefs: { ...DEFAULT_STATE.prefs, ...(raw.prefs || {}) },
      notif: { ...DEFAULT_STATE.notif, ...(raw.notif || {}) },
      outbox: Array.isArray(raw.outbox) ? raw.outbox : [],
    };
  } catch {
    return structuredClone(DEFAULT_STATE);
  }
}

module.exports = { Store, DEFAULT_STATE };
