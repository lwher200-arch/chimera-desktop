'use strict';
/**
 * lib/doRegistry.js — NodeRegistry DO 注册/吊销（v0.3.11，重绑发新 keypair 用）。
 *
 * 背景（grounding，勿凭空假设）：
 *  - v0.3.6 之前应用内绑定只做论坛侧；DO 注册是 lenghao 上手工跑
 *    /home/lenghao/register-workspace-key.py（现铸 central token + POST
 *    /registry/register）。本模块把同一条路接进应用内重绑流程，不重
 *    实现铸 token、不编新接口。
 *  - central token 只能 lenghao 本地现铸（central 私钥只在 lenghao
 *    0600 文件里），论坛不能代持；所以 DO 注册只限本机。脚本不在
 *    （非 lenghao 机器）时注册失败，绑定整体中止、旧 keypair 恢复。
 *  - 两个脚本都不打印 token：register 脚本只打印 "<status> <registry
 *    回包>"；revoke helper 只打印回包。本模块同样不记任何 token 值。
 *  - register 是 upsert：同一 node_id 重注册会轮换 key 并清 revoked_at，
 *    回 already_registered=true；限流 10 次/分钟全局、3 次/小时每节点。
 *  - revoke helper 对不存在的节点打印 404 且 exit 0：必须从 stdout
 *    首字段解析状态码，不能看 exit 码（registrar 已踩过这个坑）。
 *
 * 本模块是纯 Node 模块（主进程用）；测试可传 overrides.scriptPath
 * 指向 fake 脚本，或用环境变量 CHIMERA_REGISTER_KEY_SCRIPT /
 * CHIMERA_REVOKE_KEY_SCRIPT 覆盖。
 */
const { execFile } = require('node:child_process');

const DEFAULT_REGISTER_SCRIPT = '/home/lenghao/register-workspace-key.py';
const DEFAULT_REVOKE_SCRIPT = '/home/lenghao/revoke-workspace-key.py';
const SCRIPT_TIMEOUT_MS = 60 * 1000;

/** execFile 的 Promise 包装；超时/启动失败都抛 Error。 */
function runScript(scriptPath, args, { timeoutMs = SCRIPT_TIMEOUT_MS } = {}) {
  const fs = require('node:fs');
  if (!fs.existsSync(scriptPath)) {
    return Promise.reject(new Error(`注册脚本不在本机：${scriptPath}（DO 注册只限 lenghao 本机）`));
  }
  return new Promise((resolve, reject) => {
    execFile('python3', [scriptPath, ...args], { timeout: timeoutMs, maxBuffer: 1024 * 1024 }, (err, stdout, stderr) => {
      if (err && err.code === 'ENOENT') {
        reject(new Error(`python3 不可用，无法跑注册脚本：${scriptPath}`));
        return;
      }
      if (err && err.killed) {
        reject(new Error(`注册脚本超时（${timeoutMs}ms）：${scriptPath}`));
        return;
      }
      // revoke helper 对 404 也 exit 0；register 脚本非 200 时 urlopen 会抛
      // （exit 非 0）但 stdout 里仍有状态行。两种情况都以后面的解析为准：
      // 只要 stdout 有 "<status> {...}" 就按状态码走；否则才按启动失败算。
      const out = String(stdout || '').trim();
      const firstLine = out.split('\n')[0] || '';
      const m = firstLine.match(/^(\d{3})\s+(.*)$/s);
      if (m) {
        resolve({ status: Number(m[1]), bodyText: m[2], stderr: String(stderr || '') });
        return;
      }
      if (err) {
        reject(new Error(`注册脚本执行失败：${err.message}${stderr ? `（${String(stderr).trim().slice(0, 200)}）` : ''}`));
        return;
      }
      reject(new Error(`注册脚本输出无法解析：${out.slice(0, 200)}`));
    });
  });
}

/** 解析 "<status> {json}" 里的 JSON 体；解析失败时原样带回文本。 */
function parseBody(bodyText) {
  try { return JSON.parse(bodyText); } catch { return { raw: bodyText }; }
}

/**
 * 把一把公钥注册进 DO。成功回 registry 回包（含 already_registered）。
 * 脚本不在 / 非 200 / ok:false 时抛错（调用方负责回滚）。
 */
async function registerWorkspaceKey({ nodeId, keyId, publicKey } = {}, overrides = {}) {
  if (!nodeId || !keyId || !publicKey) throw new Error('DO 注册缺少 nodeId/keyId/publicKey');
  const script = overrides.scriptPath
    || process.env.CHIMERA_REGISTER_KEY_SCRIPT
    || DEFAULT_REGISTER_SCRIPT;
  const { status, bodyText } = await runScript(script, [String(nodeId), String(keyId), String(publicKey)], overrides);
  const body = parseBody(bodyText);
  if (status !== 200 || body.ok === false) {
    throw new Error(`DO 注册失败：HTTP ${status} ${body.error || body.raw || bodyText}`.slice(0, 300));
  }
  return body;
}

/**
 * 吊销一把 DO key（重绑失败时回滚刚注册的新 key 用）。
 * 200 → { revoked: true }；404 → { alreadyRevoked: true }（no-op，不算错）；
 * central 401/403 时重试一次 --via setup；其余抛错。
 * 注意：不要拿它吊任何已有论坛行引用的 key——registrar 的 guard 才管那事。
 */
async function revokeWorkspaceKey(nodeId, overrides = {}) {
  if (!nodeId) throw new Error('DO 吊销缺少 nodeId');
  const script = overrides.scriptPath
    || process.env.CHIMERA_REVOKE_KEY_SCRIPT
    || DEFAULT_REVOKE_SCRIPT;
  const attempt = async (viaSetup) => {
    const args = viaSetup ? [String(nodeId), '--via', 'setup'] : [String(nodeId)];
    const { status, bodyText } = await runScript(script, args, overrides);
    return { status, body: parseBody(bodyText) };
  };
  let r = await attempt(false);
  if ((r.status === 401 || r.status === 403) && !overrides.noSetupFallback) {
    r = await attempt(true);
  }
  if (r.status === 200) return { revoked: true, body: r.body };
  if (r.status === 404) return { alreadyRevoked: true, body: r.body };
  throw new Error(`DO 吊销失败：HTTP ${r.status} ${r.body.error || r.body.raw || ''}`.slice(0, 300));
}

module.exports = {
  DEFAULT_REGISTER_SCRIPT,
  DEFAULT_REVOKE_SCRIPT,
  SCRIPT_TIMEOUT_MS,
  registerWorkspaceKey,
  revokeWorkspaceKey,
};
