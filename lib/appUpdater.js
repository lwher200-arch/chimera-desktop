'use strict';
/**
 * lib/appUpdater.js — 轻量 asar 自动更新（无感更新），不用 electron-updater。
 *
 * 流程：
 *  1. 查：启动后延迟查一次，之后每 6 小时查一次。查的是论坛上的版本
 *     清单 `{serverBase}/desktop-update.json`，走 ServerResolver 的
 *     getBase()——与功能 1 共用同一解析：公网断了也能经尾网查到清单。
 *     清单格式：{ version, asarUrl, sha256, notes? }。
 *     清单不存在（404）= 还没发布过更新，静默当“无更新”，不报错。
 *  2. 下：清单版本比当前版本新时，后台下载 asarUrl 的 asar，算 sha256，
 *     与清单里的 sha256 对不上就丢弃、不换、不崩；下载/校验失败也一样。
 *  3. 换：校验通过后暂存到 userData/updates/（app.asar.pending +
 *     pending.json）。换 asar 的时机是“下次启动”与“本次退出”：
 *       - 退出时（before-quit）：把暂存的 asar 换上去（先把当前 asar
 *         备份成 app.asar.bak，再移入新 asar），下次启动即新版。
 *       - 启动时：若上次是被杀掉的、退出时没来得及换，启动时再换一次，
 *         换上后 relaunch 一次即新版；换不上（校验失败/没权限）就
 *         继续跑旧版，不崩。
 *     换 asar 前必备份旧 asar（app.asar.bak，与 asar 同目录）；回滚
 *     就是把 .bak 拷回 app.asar 再重启。
 *  4. 注意：开发模式（npm start，app.getAppPath() 是目录不是 .asar）
 *     不换 asar，只查、只下、只暂存。
 *
 * 版本来源（发版时要做的事，详见 README §自动更新）：
 *  每次发版除打 zip/AppImage 外，还要把本次的 app.asar 传到 GitHub
 *  Release 当附件，并把论坛 public 目录下的 desktop-update.json
 *  更新为 {version, asarUrl（指到该 Release 附件）, sha256}。
 *  桌面端只认清单，不直接猜 Release——清单没更新，桌面端就不知道。
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { EventEmitter } = require('events');

// 操作 asar 文件本身必须用 original-fs：Electron 打过补丁的 fs 会把
// “app.asar”当成归档、去找里面的文件（空文件名），copy/rename 都会报
// “ENOENT, not found in .../app.asar”。original-fs 是 Electron 内置
// 的未打补丁 fs（主进程可用）；普通 Node（测试）里没有，退回 fs。
let fsOrig = fs;
try { fsOrig = require('original-fs'); } catch { /* 普通 Node：没有 original-fs */ }

const MANIFEST_PATH = '/desktop-update.json';
const CHECK_INTERVAL_MS = 6 * 3600 * 1000; // 每 6 小时查一次
const STARTUP_CHECK_DELAY_MS = 15000; // 启动 15 秒后再查，不堵启动
const MANIFEST_TIMEOUT_MS = 10000;
const DOWNLOAD_TIMEOUT_MS = 120000;

function compareVersions(a, b) {
  const pa = String(a || '0').split('.').map((x) => parseInt(x, 10) || 0);
  const pb = String(b || '0').split('.').map((x) => parseInt(x, 10) || 0);
  const len = Math.max(pa.length, pb.length);
  for (let i = 0; i < len; i++) {
    const d = (pa[i] || 0) - (pb[i] || 0);
    if (d !== 0) return d;
  }
  return 0;
}

function sha256Hex(buf) {
  return crypto.createHash('sha256').update(buf).digest('hex');
}

class AppUpdater extends EventEmitter {
  /**
   * @param {object} opts {
   *   currentVersion, userDataDir, appAsarPath, getBase, fetch,
   *   manifestPath, checkIntervalMs, startupCheckDelayMs }
   *   appAsarPath：app.getAppPath()，只有以 .asar 结尾时才换 asar；
   *   开发模式传目录或不传，换 asar 会自动跳过。
   *   getBase：() => resolver.getBase()，清单走这个基地址。
   *   fetch：仅供测试注入；正常用全局 fetch。
   */
  constructor(opts = {}) {
    super();
    this.currentVersion = opts.currentVersion || '0.0.0';
    this.userDataDir = opts.userDataDir;
    this.appAsarPath = opts.appAsarPath || null;
    this.getBase = typeof opts.getBase === 'function' ? opts.getBase : () => 'https://forum.coldhao.win';
    this._fetch = opts.fetch || fetch;
    this.manifestPath = opts.manifestPath || MANIFEST_PATH;
    this._fsOrig = opts.fsOrig || fsOrig; // 操作 asar 文件本身用的 fs（可注入测试）
    this.checkIntervalMs = opts.checkIntervalMs || CHECK_INTERVAL_MS;
    this.startupCheckDelayMs = opts.startupCheckDelayMs != null ? opts.startupCheckDelayMs : STARTUP_CHECK_DELAY_MS;
    this._timer = null;
    this._checking = false;
    this._started = false;
  }

  get updatesDir() { return path.join(this.userDataDir, 'updates'); }
  get pendingAsar() { return path.join(this.updatesDir, 'app.asar.pending'); }
  get pendingManifest() { return path.join(this.updatesDir, 'pending.json'); }

  /** 读远端清单。404 = 未发布更新，返回 null；其它失败抛错。 */
  async fetchManifest() {
    const base = String(this.getBase()).replace(/\/$/, '');
    const res = await this._fetch(base + this.manifestPath, {
      signal: AbortSignal.timeout(MANIFEST_TIMEOUT_MS),
    });
    if (res.status === 404) return null;
    if (!res.ok) throw new Error(`manifest HTTP ${res.status}`);
    const m = await res.json();
    if (!m || typeof m.version !== 'string' || typeof m.asarUrl !== 'string' || typeof m.sha256 !== 'string') {
      throw new Error('manifest 格式不对（缺 version / asarUrl / sha256）');
    }
    return m;
  }

  /** 查一次：{available, version, asarUrl, sha256, notes, reason, error}。 */
  async checkForUpdates() {
    if (this._checking) return { available: false, reason: 'checking' };
    this._checking = true;
    try {
      const m = await this.fetchManifest();
      if (!m) return { available: false, reason: 'no-manifest' };
      if (compareVersions(m.version, this.currentVersion) <= 0) {
        return { available: false, reason: 'up-to-date', version: m.version };
      }
      return { available: true, version: m.version, asarUrl: m.asarUrl, sha256: m.sha256, notes: m.notes || '' };
    } catch (err) {
      return { available: false, reason: 'check-failed', error: String((err && err.message) || err) };
    } finally {
      this._checking = false;
    }
  }

  /**
   * 后台下载 + 校验 + 暂存。成功返回 {ok:true, version, bytes, sha256}；
   * 任何一步失败返回 {ok:false, error}，不换、不崩、不留坏文件。
   */
  async downloadAndStage(info) {
    try {
      const res = await this._fetch(info.asarUrl, { signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS) });
      if (!res.ok) return { ok: false, error: `下载 HTTP ${res.status}` };
      const buf = Buffer.from(await res.arrayBuffer());
      const actual = sha256Hex(buf);
      if (actual.toLowerCase() !== String(info.sha256).toLowerCase()) {
        return { ok: false, error: `sha256 不符（期望 ${info.sha256}，实际 ${actual}）` };
      }
      fs.mkdirSync(this.updatesDir, { recursive: true });
      fs.writeFileSync(this.pendingAsar, buf);
      fs.writeFileSync(this.pendingManifest, JSON.stringify({
        version: info.version, sha256: info.sha256, stagedAt: new Date().toISOString(),
      }, null, 2));
      return { ok: true, version: info.version, bytes: buf.length, sha256: actual };
    } catch (err) {
      return { ok: false, error: String((err && err.message) || err) };
    }
  }

  /** 有没有已暂存、待换的更新。 */
  hasPendingUpdate() {
    try {
      return fs.existsSync(this.pendingAsar) && fs.existsSync(this.pendingManifest);
    } catch { return false; }
  }

  /**
   * 把暂存的 asar 换上去：再验一次 sha256 → 备份当前 asar 为 .bak →
   * 移入新 asar → 清掉暂存。返回 true 表示换上了（调用方在启动时
   * 应 relaunch，在退出时不用做任何事，下次启动即新版）。
   * 以下情况返回 false 且不换：开发模式（无 .asar 可换）、无暂存、
   * 暂存校验失败（删掉坏暂存）、当前 asar 不存在、没写权限。
   */
  applyPendingUpdate() {
    try {
      if (!this.appAsarPath || !this.appAsarPath.endsWith('.asar')) return false;
      if (!this.hasPendingUpdate()) return false;
      const meta = JSON.parse(fs.readFileSync(this.pendingManifest, 'utf8'));
      const buf = fs.readFileSync(this.pendingAsar);
      if (sha256Hex(buf).toLowerCase() !== String(meta.sha256 || '').toLowerCase()) {
        try { fs.unlinkSync(this.pendingAsar); } catch { /* 删不掉就算了 */ }
        try { fs.unlinkSync(this.pendingManifest); } catch { /* 删不掉就算了 */ }
        console.error('[updater] 暂存 asar 的 sha256 不符，已丢弃，不换。');
        return false;
      }
      // 下面几步碰 asar 文件本身，一律走 _fsOrig（见文件头注释）：
      // 打补丁的 fs 会把 app.asar 当归档处理，copy/rename 必报 ENOENT。
      if (!this._fsOrig.existsSync(this.appAsarPath)) return false;
      this._fsOrig.copyFileSync(this.appAsarPath, this.appAsarPath + '.bak'); // 备份旧 asar
      try {
        this._fsOrig.renameSync(this.pendingAsar, this.appAsarPath);
      } catch (err) {
        if (err && err.code === 'EXDEV') {
          // 跨盘符 rename 不行，改走拷 + 删。
          this._fsOrig.copyFileSync(this.pendingAsar, this.appAsarPath);
          this._fsOrig.unlinkSync(this.pendingAsar);
        } else { throw err; }
      }
      try { fs.unlinkSync(this.pendingManifest); } catch { /* 清不掉就算了 */ }
      console.log(`[updater] 已换上新 asar（${meta.version}），旧版已备份为 app.asar.bak。`);
      return true;
    } catch (err) {
      console.error('[updater] 换 asar 失败（继续跑旧版）：', (err && err.message) || err);
      return false;
    }
  }

  /** 查一次，有新版就后台下载暂存；失败只记日志，不抛。 */
  async checkAndStage() {
    const info = await this.checkForUpdates();
    if (!info.available) {
      if (info.reason && info.reason !== 'up-to-date' && info.reason !== 'no-manifest') {
        console.log('[updater] 检查更新失败：', info.reason, info.error || '');
      }
      return info;
    }
    const staged = await this.downloadAndStage(info);
    if (staged.ok) {
      console.log(`[updater] 新版 ${info.version} 已下载暂存（${staged.bytes} 字节），下次启动/退出时换上。`);
      this.emit('update-staged', { version: info.version, bytes: staged.bytes, sha256: staged.sha256 });
    } else {
      console.log('[updater] 新版下载/校验失败，不换：', staged.error);
    }
    return { ...info, staged };
  }

  /** 启动后延迟查一次，之后每隔 checkIntervalMs 查一次。 */
  startPeriodicCheck() {
    if (this._started) return;
    this._started = true;
    setTimeout(() => { this.checkAndStage().catch(() => {}); }, this.startupCheckDelayMs);
    this._timer = setInterval(() => { this.checkAndStage().catch(() => {}); }, this.checkIntervalMs);
    if (typeof this._timer.unref === 'function') this._timer.unref();
  }

  stopPeriodicCheck() {
    this._started = false;
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
  }
}

module.exports = { AppUpdater, compareVersions, sha256Hex, MANIFEST_PATH, CHECK_INTERVAL_MS, STARTUP_CHECK_DELAY_MS };
