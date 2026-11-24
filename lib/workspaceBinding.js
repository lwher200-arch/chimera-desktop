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
  signChallenge,
  readPrivateKeyPem,
};
