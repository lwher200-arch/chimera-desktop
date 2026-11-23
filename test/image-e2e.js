'use strict';
/**
 * test/image-e2e.js — 大图发送端到端实测（不在 npm test 里跑，单独跑）。
 *
 * 用法（在 lenghao 上跑，token 从巡查 token 文件读进环境变量，绝不打印）：
 *   WS_TOKEN="$(cat ~/xm001/WSL_System/Eterna_Genesis/muse-forum/.muse-main-token)" \
 *     node test/image-e2e.js
 *
 * 验这些（v0.2.1 性能优化 P0-1 / P0-2 的服务端一侧）：
 *  1. 生成约 1.9MB 的合法 PNG；用 renderer.js 里实际发布的 arrayBufferToBase64
 *     分块转 base64，往返与原字节逐字节一致（旧展开写法在 200KB 即 RangeError，
 *     见 test/smoke.js §5a）。
 *  2. multipart 上传该图（X-AI-Token 鉴权）→ 201、kind=image、size 对得上；
 *     GET /uploads/:id 取回字节与原图一致。
 *  3. 临时私密群发一条带图消息 → GET 群消息里该条在、附件对得上。
 *  4. 增量拉取契约：再发一条，GET ?since=第一条 ts 只回第二条
 *    （渲染进程增量追加依赖这个语义）。
 *
 * 临时群由本脚本建、消息由本脚本删；群本身没有删除接口，脚本末尾打印
 * CLEANUP_GROUP_ID，另在 het 上从 groups.json 里删掉该群并核对计数回落。
 * 全程只写临时群，不发公开消息。失败时仍尽力清理（finally）。
 */
const assert = require('node:assert');
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');

const API_BASE = process.env.CHIMERA_API_BASE || 'https://forum.coldhao.win';
const TOKEN = process.env.WS_TOKEN || '';
if (!TOKEN) {
  console.error('WS_TOKEN 未设：先从巡查 token 文件读进环境变量再跑（见文件头注释）。');
  process.exit(2);
}

let passed = 0;
function ok(name) { passed += 1; console.log(`  ok - ${name}`); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

// CRC32（PNG chunk 校验用，zlib 不导出 crc32）。
const CRC_TABLE = (() => {
  const t = new Int32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    t[n] = c;
  }
  return t;
})();
function crc32(buf) {
  let c = 0xFFFFFFFF;
  for (let i = 0; i < buf.length; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xFF] ^ (c >>> 8);
  return (c ^ 0xFFFFFFFF) >>> 0;
}
function pngChunk(type, data) {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const td = Buffer.concat([Buffer.from(type, 'ascii'), data]);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(td));
  return Buffer.concat([len, td, crc]);
}
// 生成合法 PNG：宽×高 RGB 噪声（几乎压不动，800×800 约 1.9MB）。
function makePng(width, height) {
  const stride = 1 + width * 3;
  const raw = crypto.randomBytes(height * stride);
  for (let y = 0; y < height; y++) raw[y * stride] = 0; // 每行 filter=none
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8; // bit depth
  ihdr[9] = 2; // color type RGB
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A]),
    pngChunk('IHDR', ihdr),
    pngChunk('IDAT', zlib.deflateSync(raw)),
    pngChunk('IEND', Buffer.alloc(0)),
  ]);
}

// REST（X-AI-Token 鉴权；token 只进请求头，绝不打印）。
async function rest(method, p, body) {
  const res = await fetch(API_BASE + p, {
    method,
    headers: { 'Content-Type': 'application/json', 'X-AI-Token': TOKEN },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try { data = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, text, data };
}

async function main() {
  let gid = null;
  const mids = [];
  try {
    console.log('== 1. 生成大图 + 分块 base64 往返 ==');
    const png = makePng(800, 800);
    assert.ok(png.length > 1024 * 1024, `测试图应 >1MB，实际 ${png.length}`);
    assert.strictEqual(png.subarray(1, 4).toString('ascii'), 'PNG', '应为合法 PNG 签名');
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const m = rendererSrc.match(/function arrayBufferToBase64\(buf\) \{[\s\S]*?\n\}/);
    assert.ok(m, 'renderer.js 应包含 arrayBufferToBase64');
    const arrayBufferToBase64 = new Function(`${m[0]}; return arrayBufferToBase64;`)();
    const b64 = arrayBufferToBase64(png.buffer.slice(png.byteOffset, png.byteOffset + png.byteLength));
    assert.ok(Buffer.from(b64, 'base64').equals(png), '分块 base64 往返应与原图逐字节一致');
    ok(`测试图 ${png.length} 字节，分块 base64 往返一致`);

    console.log('== 2. 上传大图（multipart，X-AI-Token） ==');
    const form = new FormData();
    form.append('file', new Blob([png], { type: 'image/png' }), 'perf-test-1mb.png');
    const upRes = await fetch(API_BASE + '/api/upload', {
      method: 'POST',
      headers: { 'X-AI-Token': TOKEN },
      body: form,
    });
    const upText = await upRes.text();
    let up = null;
    try { up = JSON.parse(upText); } catch { /* 非 JSON */ }
    assert.strictEqual(upRes.status, 201, `上传应 201，实际 ${upRes.status}：${upText.slice(0, 200)}`);
    assert.strictEqual(up.upload.kind, 'image', '应识别为 image');
    assert.strictEqual(up.upload.size, png.length, '服务端记的大小应与原图一致');
    const upId = up.upload.id;
    const back = await fetch(API_BASE + up.upload.url, { headers: { 'X-AI-Token': TOKEN } });
    assert.strictEqual(back.status, 200, `取回上传应 200，实际 ${back.status}`);
    assert.ok(Buffer.from(await back.arrayBuffer()).equals(png), '取回字节应与原图一致');
    ok(`上传成功：id=${upId}，${png.length} 字节，取回一致`);

    console.log('== 3. 临时私密群发带图消息 ==');
    const gc = await rest('POST', '/api/groups', { name: 'perf-image-test', kind: 'chat', board: 'general', category: 'chat' });
    assert.strictEqual(gc.status, 201, `建临时群应 201，实际 ${gc.status}：${gc.text.slice(0, 200)}`);
    gid = gc.data.group.id;
    const pv = await rest('PATCH', `/api/groups/${gid}`, { visibility: 'private' });
    assert.strictEqual(pv.status, 200, `临时群转私密应 200，实际 ${pv.status}`);
    const body1 = `perf-image-e2e 大图消息 ${new Date().toISOString()}`;
    const pm1 = await rest('POST', `/api/groups/${gid}/messages`, { body: body1, attachments: [upId] });
    assert.strictEqual(pm1.status, 201, `发带图消息应 201，实际 ${pm1.status}：${pm1.text.slice(0, 200)}`);
    const mid1 = pm1.data.message ? pm1.data.message.id : pm1.data.id;
    mids.push(mid1);
    const gm = await rest('GET', `/api/groups/${gid}/messages`);
    assert.strictEqual(gm.status, 200, `拉群消息应 200，实际 ${gm.status}`);
    const found = (gm.data.messages || []).find((x) => x.id === mid1);
    assert.ok(found, '刚发的带图消息应在群消息里');
    assert.ok(Array.isArray(found.attachments) && found.attachments.length === 1, '消息应带 1 个附件');
    assert.strictEqual(found.attachments[0].id, upId, '附件 id 应与上传一致');
    assert.strictEqual(found.attachments[0].kind, 'image', '附件应为 image');
    ok(`带图消息已发并拉回验证，msg=${mid1}`);

    console.log('== 4. 增量拉取契约（?since= 只回新消息） ==');
    await sleep(20); // 保证两条消息 ts 不撞同一毫秒
    const body2 = `perf-image-e2e 第二条 ${new Date().toISOString()}`;
    const pm2 = await rest('POST', `/api/groups/${gid}/messages`, { body: body2 });
    assert.strictEqual(pm2.status, 201, `发第二条应 201，实际 ${pm2.status}`);
    const mid2 = pm2.data.message ? pm2.data.message.id : pm2.data.id;
    mids.push(mid2);
    const inc = await rest('GET', `/api/groups/${gid}/messages?since=${encodeURIComponent(found.ts)}`);
    assert.strictEqual(inc.status, 200, `增量拉取应 200，实际 ${inc.status}`);
    const items = inc.data.messages || [];
    assert.strictEqual(items.length, 1, `since=第一条 ts 应只回 1 条，实际 ${items.length}`);
    assert.strictEqual(items[0].id, mid2, '增量拉回的应是第二条');
    ok('增量拉取契约成立：since=旧 ts 只回新消息');

    console.log('== 5. 清理测试消息 ==');
    for (const mid of mids) {
      const del = await rest('DELETE', `/api/groups/${gid}/messages/${mid}`, null);
      assert.ok(del.status === 200 || del.status === 204, `删消息应 200/204，实际 ${del.status}`);
    }
    mids.length = 0;
    // 服务端已修（2026-11-23，live f2105cc0）：删上传的 inUse 检查现在过滤
    // 已软删的消息/帖子，删消息后再删上传应回 200/204。严格断言，不再容忍 400。
    const delUp = await fetch(API_BASE + `/api/uploads/${upId}`, { method: 'DELETE', headers: { 'X-AI-Token': TOKEN } });
    assert.ok(delUp.status === 200 || delUp.status === 204, `删上传应 200/204，实际 ${delUp.status}：${(await delUp.text()).slice(0, 200)}`);
    ok('测试消息与上传已删');

    console.log(`\nPASS: ${passed} 项全过`);
  } finally {
    for (const mid of mids) {
      try { await rest('DELETE', `/api/groups/${gid}/messages/${mid}`, null); } catch {}
    }
    if (gid) console.log('CLEANUP_GROUP_ID=' + gid);
  }
}

main().catch((err) => {
  console.error(`\nFAIL: ${err.message}`);
  process.exit(1);
});
