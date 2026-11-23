'use strict';
/**
 * test/window-e2e.js — 消息窗口分页契约端到端实测（不在 npm test 里跑，单独跑）。
 *
 * 用法（在 lenghao 上跑，token 从巡查 token 文件读进环境变量，绝不打印）：
 *   WS_TOKEN="$(cat ~/xm001/WSL_System/Eterna_Genesis/muse-forum/.muse-main-token)" \
 *     node test/window-e2e.js
 *
 * 验这些（v0.2.1+1 消息窗口依赖的服务端分页口径，见 server.js paginate）：
 *  1. 临时私密群发 5 条消息。
 *  2. GET ?limit=2 → 只回最后 2 条，has_more=true。
 *  3. GET ?before_id=<第3条>&limit=2 → 只回前 2 条，has_more=false。
 *  4. GET ?before_id=<第1条> → 0 条。
 *  5. 私聊：取现有 DM 列表里有消息的一组，GET ?limit=1 → 只回 1 条
 *     （只读，不发私聊；没有现成 DM 时跳过并注明）。
 *  6. 清理：删 5 条测试消息；群本身没有删除接口，末尾打印
 *     CLEANUP_GROUP_ID，另在 het 上从 groups.json 里删掉该群并核对计数回落。
 * 全程只写临时群，不发公开消息。失败时仍尽力清理（finally）。
 */
const assert = require('node:assert');

const API_BASE = process.env.CHIMERA_API_BASE || 'https://forum.coldhao.win';
const TOKEN = process.env.WS_TOKEN || '';
if (!TOKEN) {
  console.error('WS_TOKEN 未设：先从巡查 token 文件读进环境变量再跑（见文件头注释）。');
  process.exit(2);
}

let passed = 0;
function ok(name) { passed += 1; console.log(`  ok - ${name}`); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

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
    console.log('== 1. 临时私密群发 5 条消息 ==');
    const gc = await rest('POST', '/api/groups', { name: 'window-page-test', kind: 'chat', board: 'general', category: 'chat' });
    assert.strictEqual(gc.status, 201, `建临时群应 201，实际 ${gc.status}：${gc.text.slice(0, 200)}`);
    gid = gc.data.group.id;
    const pv = await rest('PATCH', `/api/groups/${gid}`, { visibility: 'private' });
    assert.strictEqual(pv.status, 200, `临时群转私密应 200，实际 ${pv.status}`);
    for (let i = 1; i <= 5; i++) {
      const pm = await rest('POST', `/api/groups/${gid}/messages`, { body: `window-e2e 第 ${i} 条 ${new Date().toISOString()}` });
      assert.strictEqual(pm.status, 201, `发第 ${i} 条应 201，实际 ${pm.status}`);
      mids.push(pm.data.message ? pm.data.message.id : pm.data.id);
      await sleep(20); // 保证 ts 不撞同一毫秒
    }
    ok(`临时私密群 ${gid} 已发 5 条`);

    console.log('== 2. ?limit=2 只回最后 2 条，has_more=true ==');
    const l2 = await rest('GET', `/api/groups/${gid}/messages?limit=2`);
    assert.strictEqual(l2.status, 200, `拉消息应 200，实际 ${l2.status}`);
    assert.strictEqual((l2.data.messages || []).length, 2, `limit=2 应回 2 条，实际 ${(l2.data.messages || []).length}`);
    assert.deepStrictEqual((l2.data.messages || []).map((m) => m.id), [mids[3], mids[4]], 'limit=2 应回最后 2 条');
    assert.strictEqual(l2.data.has_more, true, '共 5 条、limit=2 时 has_more 应为 true');
    assert.strictEqual(l2.data.total, 5, 'total 应为 5');
    ok('?limit=2 → 最后 2 条，has_more=true，total=5');

    console.log('== 3. ?before_id=第3条&limit=2 只回前 2 条，has_more=false ==');
    const b2 = await rest('GET', `/api/groups/${gid}/messages?before_id=${mids[2]}&limit=2`);
    assert.strictEqual(b2.status, 200, `拉消息应 200，实际 ${b2.status}`);
    assert.deepStrictEqual((b2.data.messages || []).map((m) => m.id), [mids[0], mids[1]], 'before_id=第3条 应回前 2 条');
    assert.strictEqual(b2.data.has_more, false, '前面只剩 2 条，has_more 应为 false');
    ok('?before_id=第3条&limit=2 → 前 2 条，has_more=false');

    console.log('== 4. ?before_id=第1条 → 0 条 ==');
    const b0 = await rest('GET', `/api/groups/${gid}/messages?before_id=${mids[0]}&limit=2`);
    assert.strictEqual(b0.status, 200, `拉消息应 200，实际 ${b0.status}`);
    assert.strictEqual((b0.data.messages || []).length, 0, 'before_id=第1条 前面应无消息');
    ok('?before_id=第1条 → 0 条');

    console.log('== 5. 私聊 ?limit=1 只回 1 条（只读现有 DM） ==');
    const dl = await rest('GET', '/api/dms');
    assert.strictEqual(dl.status, 200, `拉 DM 列表应 200，实际 ${dl.status}`);
    const dms = dl.data.conversations || dl.data.dms || [];
    let dmChecked = false;
    for (const d of dms) {
      const peer = d.peer || d.name || d.with;
      if (!peer) continue;
      const h = await rest('GET', `/api/dms/${encodeURIComponent(peer)}?limit=1`);
      if (h.status !== 200) continue;
      const items = h.data.messages || [];
      if (items.length === 0) continue;
      assert.strictEqual(items.length, 1, `DM ?limit=1 应回 1 条，实际 ${items.length}`);
      assert.strictEqual(h.data.has_more, (h.data.total || 0) > 1, 'DM has_more 应与 total 对得上');
      ok(`私聊 ${peer}：?limit=1 → 1 条，total=${h.data.total}，has_more=${h.data.has_more}`);
      dmChecked = true;
      break;
    }
    if (!dmChecked) console.log('  注：没有找到带消息的现有 DM，私聊 limit 契约未 live 验（与群聊共用同一 paginate，见 server.js）');

    console.log('== 6. 清理测试消息 ==');
    for (const mid of mids) {
      const del = await rest('DELETE', `/api/groups/${gid}/messages/${mid}`, null);
      assert.ok(del.status === 200 || del.status === 204, `删消息应 200/204，实际 ${del.status}`);
    }
    mids.length = 0;
    ok('5 条测试消息已删');

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
