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
  },
  prefs: {
    lastView: 'conv',
    window: null, // { width, height } 上次窗口大小
  },
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
    };
  } catch {
    return structuredClone(DEFAULT_STATE);
  }
}

module.exports = { Store, DEFAULT_STATE };
