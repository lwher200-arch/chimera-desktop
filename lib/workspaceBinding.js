'use strict';
/**
 * lib/workspaceBinding.js — 工作区绑定本机密钥（融合 P0）。
 *
 * 口径（与融合 P0 契约一致，论坛侧各自算出后互相核对）：
 *  - Ed25519 密钥对，本机生成（若无）。私钥只存本机 0600 文件，
 *    永不上传、永不进日志/git/聊天。
 *  - 私钥：~/.config/chimera-desktop/workspace/workspace-key.pem（PKCS8 PEM，0600）
 *  - 公钥：同目录 workspace-key.pub（base64 原始 32 字节，公开材料）
 *  - key_id = 'ed25519-' + hex(sha256(原始 32 字节公钥))
 *  - node_id（注册 DO 时用）= 'ws-' + key_id 去掉 'ed25519-' 前缀后的前 12 个 hex 字符
 *  - 签名：Ed25519 签 UTF-8 挑战串全文，传输 base64。
 *    论坛验签：公钥先拼 SPKI DER 前缀 302a300506032b6570032100 + 原始 32 字节，
 *    再 crypto.verify(null, challengeBytes, publicKey, sigBytes)。
 *  - 在线口径：lastSeenTs 在 15 分钟内为在线。
 *
 * 本模块是纯 Node 模块（主进程用）；渲染器 DOM 部分不在此。
 * 私钥/签名原文绝不打印：本模块不写任何日志。
 */
const crypto = require('node:crypto');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// SPKI DER 前缀（Ed25519）：302a300506032b6570032100 + 32 字节原始公钥。
const SPKI_ED25519_PREFIX = Buffer.from('302a300506032b6570032100', 'hex');
const ONLINE_WINDOW_MS = 15 * 60 * 1000;

/** 工作区密钥目录。测试可传 overrides.dir；否则默认 ~/.config/chimera-desktop/workspace。 */
function workspaceDir(overrides = {}) {
  if (overrides.dir) return String(overrides.dir);
  if (process.env.CHIMERA_WORKSPACE_DIR) return String(process.env.CHIMERA_WORKSPACE_DIR);
  return path.join(os.homedir(), '.config', 'chimera-desktop', 'workspace');
}

function keyPaths(overrides = {}) {
  const dir = workspaceDir(overrides);
  return {
    dir,
    privateKey: path.join(dir, 'workspace-key.pem'),
    publicKey: path.join(dir, 'workspace-key.pub'),
  };
}

/** 原始 32 字节公钥 -> key_id。 */
function keyIdForPublicKey(rawPublicKey) {
  const raw = Buffer.isBuffer(rawPublicKey) ? rawPublicKey : Buffer.from(rawPublicKey);
  if (raw.length !== 32) throw new Error('公钥必须是 32 字节');
  return 'ed25519-' + crypto.createHash('sha256').update(raw).digest('hex');
}

/** key_id -> 指纹前 8 位（去掉 'ed25519-' 前缀后的前 8 个 hex 字符）。 */
function fingerprintForKeyId(keyId) {
  return String(keyId || '').replace(/^ed25519-/, '').slice(0, 8);
}

/** key_id -> DO node_id：'ws-' + 去前缀后的前 12 个 hex 字符。 */
function nodeIdForKeyId(keyId) {
  return 'ws-' + String(keyId || '').replace(/^ed25519-/, '').slice(0, 12);
}

/** base64 公钥 -> 原始 32 字节；非法时抛错。 */
function publicKeyFromBase64(publicKeyB64) {
  const raw = Buffer.from(String(publicKeyB64 || ''), 'base64');
  if (raw.length !== 32) throw new Error('公钥 base64 解码后必须是 32 字节');
  return raw;
}

/** 原始 32 字节公钥 -> SPKI DER 公钥对象（验签用）。 */
function publicKeyObjectFromRaw(raw) {
  const der = Buffer.concat([SPKI_ED25519_PREFIX, Buffer.from(raw)]);
  return crypto.createPublicKey({ key: der, format: 'der', type: 'spki' });
}

/** 验签：challenge（UTF-8 串）+ base64 公钥 + base64 签名。 */
function verifySignature({ challenge, publicKey, signature }) {
  const raw = publicKeyFromBase64(publicKey);
  const pub = publicKeyObjectFromRaw(raw);
  const sig = Buffer.from(String(signature || ''), 'base64');
  return crypto.verify(null, Buffer.from(String(challenge), 'utf8'), pub, sig);
}

/** 设备名清洗：去控制字符、截 64 字符；空则回落 os.hostname()。 */
function sanitizeDeviceName(name) {
  const cleaned = String(name || '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 64);
  return cleaned || os.hostname();
}

/** lastSeenTs 在 15 分钟内为在线。nowMs 仅测试注入。 */
function isOnline(lastSeenTs, nowMs = Date.now()) {
  if (!lastSeenTs) return false;
  const t = Date.parse(lastSeenTs);
  if (!Number.isFinite(t)) return false;
  return nowMs - t <= ONLINE_WINDOW_MS && nowMs - t >= 0;
}

/**
 * 确保本机密钥对存在并返回 { keyId, publicKey, fingerprint, nodeId, created }。
 * 已存在时直接读回（不轮换）；私钥文件权限不是 0600 时修正为 0600。
 * 只有公钥文件缺失时才从私钥重新派生公钥写回。
 */
function ensureKeyPair(overrides = {}) {
  const paths = keyPaths(overrides);
  fs.mkdirSync(paths.dir, { recursive: true, mode: 0o700 });
  try { fs.chmodSync(paths.dir, 0o700); } catch { /* 目录权限修正失败不阻塞 */ }

  let privateKeyObj;
  let created = false;
  if (fs.existsSync(paths.privateKey)) {
    const pem = fs.readFileSync(paths.privateKey, 'utf8');
    privateKeyObj = crypto.createPrivateKey(pem);
    try { fs.chmodSync(paths.privateKey, 0o600); } catch { /* 修正失败不阻塞 */ }
  } else {
    const { privateKey, publicKey } = crypto.generateKeyPairSync('ed25519');
    privateKeyObj = privateKey;
    const pem = privateKey.export({ type: 'pkcs8', format: 'pem' });
    fs.writeFileSync(paths.privateKey, pem, { mode: 0o600 });
    try { fs.chmodSync(paths.privateKey, 0o600); } catch { /* 已按 mode 写，修正失败不阻塞 */ }
    const rawPub = publicKey.export({ type: 'spki', format: 'der' }).subarray(-32);
    fs.writeFileSync(paths.publicKey, Buffer.from(rawPub).toString('base64') + '\n', { mode: 0o644 });
    created = true;
    const publicKeyB64 = Buffer.from(rawPub).toString('base64');
    const keyId = keyIdForPublicKey(rawPub);
    return { keyId, publicKey: publicKeyB64, fingerprint: fingerprintForKeyId(keyId), nodeId: nodeIdForKeyId(keyId), created, paths };
  }

  // 已有私钥：派生公钥；公钥文件缺失或内容不符时写回正确值。
  const rawPub = Buffer.from(crypto.createPublicKey(privateKeyObj).export({ type: 'spki', format: 'der' }).subarray(-32));
  const publicKeyB64 = rawPub.toString('base64');
  if (!fs.existsSync(paths.publicKey) || fs.readFileSync(paths.publicKey, 'utf8').trim() !== publicKeyB64) {
    fs.writeFileSync(paths.publicKey, publicKeyB64 + '\n', { mode: 0o644 });
  }
  const keyId = keyIdForPublicKey(rawPub);
  return { keyId, publicKey: publicKeyB64, fingerprint: fingerprintForKeyId(keyId), nodeId: nodeIdForKeyId(keyId), created, paths };
}

/** 签 UTF-8 挑战串全文，返回 base64 签名。privateKeyPem 不进日志。 */
function signChallenge(challenge, privateKeyPem) {
  const key = typeof privateKeyPem === 'string'
    ? crypto.createPrivateKey(privateKeyPem)
    : privateKeyPem;
  return crypto.sign(null, Buffer.from(String(challenge), 'utf8'), key).toString('base64');
}

/** 读私钥 PEM（主进程内部用，不返回给渲染进程、不进日志）。 */
function readPrivateKeyPem(overrides = {}) {
  return fs.readFileSync(keyPaths(overrides).privateKey, 'utf8');
}

/** 本机是否已有密钥对（私钥文件在）。 */
function keyPairExists(overrides = {}) {
  return fs.existsSync(keyPaths(overrides).privateKey);
}

/** 归档时间戳：UTC ISO 去冒号/点 + pid，文件名安全、同一毫秒不撞。 */
function archiveStamp() {
  return new Date().toISOString().replace(/[:.]/g, '-') + '-' + process.pid;
}

/**
 * 归档现有密钥对（v0.3.11 重绑用）：pem+pub 各拷一份带时间戳的备份
 * （pem 0600），原文件不动。无密钥对时回 { archived: false }。
 * 归档文件永不自动删——新绑定 live 验通之前绝不能丢旧 key。
 */
function archiveKeyPair(overrides = {}) {
  const paths = keyPaths(overrides);
  if (!fs.existsSync(paths.privateKey)) return { archived: false, paths };
  const stamp = archiveStamp();
  const backupPrivate = paths.privateKey + '.bak-' + stamp;
  const backupPublic = paths.publicKey + '.bak-' + stamp;
  fs.copyFileSync(paths.privateKey, backupPrivate);
  try { fs.chmodSync(backupPrivate, 0o600); } catch { /* 0600 修正失败不阻塞 */ }
  if (fs.existsSync(paths.publicKey)) fs.copyFileSync(paths.publicKey, backupPublic);
  const pem = fs.readFileSync(paths.privateKey, 'utf8');
  const oldKeyId = keyIdForPublicKey(
    Buffer.from(crypto.createPublicKey(crypto.createPrivateKey(pem)).export({ type: 'spki', format: 'der' }).subarray(-32)));
  return { archived: true, oldKeyId, oldFingerprint: fingerprintForKeyId(oldKeyId), backupPrivate, backupPublic, paths };
}

/** 原子写一对文件：先写同目录临时文件（fsync）再 rename 覆盖。 */
function writeKeyFilesAtomic(paths, pemText, pubB64) {
  const tmpPriv = paths.privateKey + '.tmp-' + process.pid;
  const tmpPub = paths.publicKey + '.tmp-' + process.pid;
  try {
    fs.writeFileSync(tmpPriv, pemText, { mode: 0o600 });
    const fd = fs.openSync(tmpPriv, 'r+');
    try { fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
    try { fs.chmodSync(tmpPriv, 0o600); } catch { /* 已按 mode 写 */ }
    fs.writeFileSync(tmpPub, pubB64 + '\n', { mode: 0o644 });
    fs.renameSync(tmpPriv, paths.privateKey);
    fs.renameSync(tmpPub, paths.publicKey);
  } catch (err) {
    try { fs.unlinkSync(tmpPriv); } catch { /* 无临时文件可删 */ }
    try { fs.unlinkSync(tmpPub); } catch { /* 无临时文件可删 */ }
    throw err;
  }
}

/**
 * 生成一把全新 Ed25519 密钥对并原子写盘（v0.3.11 重绑用）。
 * 返回与 ensureKeyPair 同形的结果（created 恒为 true）。
 * 写盘中途失败时临时文件已清、原文件不动（rename 之前不碰原文件）。
 */
function generateFreshKeyPair(overrides = {}) {
  const paths = keyPaths(overrides);
  fs.mkdirSync(paths.dir, { recursive: true, mode: 0o700 });
  try { fs.chmodSync(paths.dir, 0o700); } catch { /* 目录权限修正失败不阻塞 */ }
  const { privateKey, publicKey } = crypto.generateKeyPairSync('ed25519');
  const pem = privateKey.export({ type: 'pkcs8', format: 'pem' });
  const rawPub = publicKey.export({ type: 'spki', format: 'der' }).subarray(-32);
  const publicKeyB64 = Buffer.from(rawPub).toString('base64');
  writeKeyFilesAtomic(paths, pem, publicKeyB64);
  const keyId = keyIdForPublicKey(rawPub);
  return { keyId, publicKey: publicKeyB64, fingerprint: fingerprintForKeyId(keyId), nodeId: nodeIdForKeyId(keyId), created: true, paths };
}

/**
 * 从归档恢复密钥对（重绑失败回滚用）：把备份的 pem+pub 原子拷回，
 * 恢复后校验 keyId 必须等于备份时的 oldKeyId，不符即抛错。
 */
function restoreKeyPair(backup, overrides = {}) {
  if (!backup || !backup.archived) throw new Error('没有可恢复的归档密钥对');
  const paths = keyPaths(overrides);
  const pem = fs.readFileSync(backup.backupPrivate, 'utf8');
  let pubB64 = null;
  try { pubB64 = fs.readFileSync(backup.backupPublic, 'utf8').trim(); } catch { /* 公钥备份缺失时从私钥派生 */ }
  if (!pubB64) {
    pubB64 = Buffer.from(crypto.createPublicKey(crypto.createPrivateKey(pem)).export({ type: 'spki', format: 'der' }).subarray(-32)).toString('base64');
  }
  writeKeyFilesAtomic(paths, pem, pubB64);
  const restored = ensureKeyPair(overrides);
  if (restored.keyId !== backup.oldKeyId) {
    throw new Error(`恢复后的 keyId 与归档不符：${restored.keyId} != ${backup.oldKeyId}`);
  }
  return restored;
}

/**
 * 重绑是否轮换密钥对（v0.3.11 规则，纯判定、便于单测）：
 * 只有“有过绑定行（哪怕已解绑）且本机有旧 keypair”时才轮换。
 * 无行但有 keypair（之前某次绑定中途失败）时不轮换、复用旧 key
 * （它可能已在 DO 里，复用可避免在 DO 里留孤 key）。
 */
function shouldRotateKeyPair({ hasPriorRows, hasKeyPair }) {
  return !!hasPriorRows && !!hasKeyPair;
}

module.exports = {
  SPKI_ED25519_PREFIX,
  ONLINE_WINDOW_MS,
  workspaceDir,
  keyPaths,
  keyIdForPublicKey,
  fingerprintForKeyId,
  nodeIdForKeyId,
  publicKeyFromBase64,
  publicKeyObjectFromRaw,
  verifySignature,
  sanitizeDeviceName,
  isOnline,
  ensureKeyPair,
  keyPairExists,
  archiveKeyPair,
  generateFreshKeyPair,
  restoreKeyPair,
  shouldRotateKeyPair,
  signChallenge,
  readPrivateKeyPem,
};
