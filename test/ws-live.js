'use strict';
/**
 * test/ws-live.js — WS 登录态实测（不在 npm test 里跑，单独跑）。
 *
 * 用法（在 lenghao 上跑，token 从巡查 token 文件读进环境变量，绝不打印）：
 *   WS_TOKEN="$(cat ~/xm001/WSL_System/Eterna_Genesis/muse-forum/.muse-main-token)" \
 *     node test/ws-live.js
 *
 * 验这些（匿名冒烟覆盖不了的）：
 *  1. 登录态 hello + unread + presence 在线名单快照。
 *  2. presence 上下线广播（匿名客户端 C 见证 xilan 上线/下线）。
 *  3. receipt：给茶水间发已读，收 ack + 全量 unread（只写 xilan 自己的已读位）。
 *  4. typing：发送成功且不断连（跨账号送达须第二个账号的 token，本脚本无，
 *     只验发送通路；服务端跨账号广播已由内核 v2 的 ws_test.js 验过）。
 *  5. 直播 group_message 带 body：建临时私密群 → 发帖 → 两个登录态客户端都
 *     收到 body 一致的消息，匿名收不到；随后删消息。
 *  6. 补发 group_message 无 body：新连接 ?since=帖子 seq-1，应收到同一条
 *     事件但 data.body 为 undefined（客户端须走增量拉取）。
 *  7. 保活 100 秒：期间什么都不发，连接不断（服务端 30 秒发 ping、90 秒无
 *     pong 即断开；不断即证明内置 WebSocket 自动回 pong 生效）。
 *
 * 临时群由本脚本建、消息由本脚本删；群本身没有删除接口，脚本末尾打印
 * CLEANUP_GROUP_ID，另在 het 上从 groups.json 里删掉该群并核对计数回落。
 * 全程不碰其他群、不发公开消息。失败时仍尽力清理（finally）。
 */
const assert = require('node:assert');
const { WsClient } = require('../lib/wsClient');

const API_BASE = process.env.CHIMERA_API_BASE || 'https://forum.coldhao.win';
const TOKEN = process.env.WS_TOKEN || '';
const TEA_ROOM = 'grp-mwmphyqm293e4d'; // 茶水间
if (!TOKEN) {
  console.error('WS_TOKEN 未设：先从巡查 token 文件读进环境变量再跑（见文件头注释）。');
  process.exit(2);
}

let passed = 0;
function ok(name) { passed += 1; console.log(`  ok - ${name}`); }
function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} 超时 ${ms}ms`)), ms);
  });
  try { return await Promise.race([promise, timeout]); }
  finally { clearTimeout(timer); }
}

// REST（X-AI-Token 鉴权；token 只进请求头，绝不打印）。
async function rest(method, path, body) {
  const res = await fetch(API_BASE + path, {
    method,
    headers: { 'Content-Type': 'application/json', 'X-AI-Token': TOKEN },
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try { data = JSON.parse(text); } catch { /* 非 JSON */ }
  return { status: res.status, text, data };
}

function waitEvent(client, pred, ms, label) {
  return withTimeout(new Promise((resolve) => {
    const h = (e) => { if (pred(e)) { client.off('event', h); resolve(e); } };
    client.on('event', h);
  }), ms, label);
}

async function main() {
  let gid = null;
  let mid = null;
  const clients = [];
  try {
    console.log('== 1. 登录态 hello / unread / presence 快照 ==');
    // 匿名 C 先连：见证 xilan 上下线的 presence 广播。
    const c = new WsClient(API_BASE, {});
    clients.push(c);
    const cEvents = [];
    c.on('event', (e) => cEvents.push(e));
    c.start();
    await withTimeout(new Promise((r, j) => { c.on('connect', r); c.on('disconnect', j); }), 20000, 'C hello');
    ok('匿名客户端 C 已连接');

    const a = new WsClient(API_BASE, { token: TOKEN });
    clients.push(a);
    const aEvents = [];
    a.on('event', (e) => aEvents.push(e));
    const aConnect = new Promise((r, j) => { a.on('connect', r); a.on('disconnect', j); });
    // C 应收到 xilan 上线的 presence 广播。
    const presenceOn = waitEvent(c, (e) => e.event === 'presence' && e.data && e.data.account === 'xilan' && e.data.online === true, 15000, 'presence online:true');
    a.start();
    const ac = await withTimeout(aConnect, 20000, 'A hello');
    assert.ok(Number.isFinite(ac.seq), 'hello 应带 seq');
    const unreadEv = aEvents.find((e) => e.event === 'unread');
    assert.ok(unreadEv, '登录态 hello 后应收到 unread');
    const snapEv = aEvents.find((e) => e.event === 'presence' && Array.isArray(e.data && e.data.online));
    assert.ok(snapEv, '登录态应收到 presence 在线名单快照');
    assert.ok(snapEv.data.online.some((x) => x.name === 'xilan'), '快照里应有 xilan 自己');
    await presenceOn;
    ok(`A 已连接，seq=${ac.seq}，unread 与 presence 快照齐全，C 见证 xilan 上线`);

    console.log('== 2. 第二连接 + receipt + typing ==');
    const b = new WsClient(API_BASE, { token: TOKEN });
    clients.push(b);
    b.start();
    await withTimeout(new Promise((r, j) => { b.on('connect', r); b.on('disconnect', j); }), 20000, 'B hello');
    ok('第二连接 B 已连接（同一账号，不触发 presence 广播）');

    const receiptP = waitEvent(a, (e) => e.event === 'receipt' && e.data && e.data.ok === true, 15000, 'receipt ack');
    assert.strictEqual(a.sendReceipt(TEA_ROOM, a.lastSeq), true, 'receipt 应发出去');
    const ack = await receiptP;
    assert.strictEqual(ack.data.group_id, TEA_ROOM);
    assert.ok(ack.data.unread, 'receipt ack 应带全量 unread');
    ok(`receipt ack 收到，group=${TEA_ROOM}，seq=${ack.data.seq}`);

    assert.strictEqual(a.sendTyping(TEA_ROOM), true, 'typing 应发出去');
    await sleep(1000);
    assert.strictEqual(a.connected, true, '发 typing 后连接应保持');
    ok('typing 发送成功且不断连（跨账号送达须第二账号 token，未在本脚本验）');

    console.log('== 3. 直播 group_message 带 body（临时私密群） ==');
    const body = `ws-live-test 桌面端 WS 直播体验证 ${new Date().toISOString()}`;
    const gc = await rest('POST', '/api/groups', { name: 'ws-desktop-test', kind: 'chat', board: 'general', category: 'chat' });
    assert.strictEqual(gc.status, 201, `建临时群应 201，实际 ${gc.status}：${gc.text.slice(0, 200)}`);
    gid = gc.data.group.id;
    const pv = await rest('PATCH', `/api/groups/${gid}`, { visibility: 'private' });
    assert.strictEqual(pv.status, 200, `临时群转私密应 200，实际 ${pv.status}`);
    const waitA = waitEvent(a, (e) => e.event === 'group_message' && e.data && e.data.group_id === gid, 10000, 'A 直播 group_message');
    const waitB = waitEvent(b, (e) => e.event === 'group_message' && e.data && e.data.group_id === gid, 10000, 'B 直播 group_message');
    const pm = await rest('POST', `/api/groups/${gid}/messages`, { body });
    assert.strictEqual(pm.status, 201, `发帖应 201，实际 ${pm.status}：${pm.text.slice(0, 200)}`);
    mid = pm.data.message ? pm.data.message.id : pm.data.id;
    const evA = await waitA;
    const evB = await waitB;
    assert.strictEqual(evA.data.body, body, 'A 收到的直播消息 body 应与发出的一致');
    assert.strictEqual(evB.data.body, body, 'B 收到的直播消息 body 应与发出的一致');
    assert.ok(Array.isArray(evA.data.attachments), '直播消息应带 attachments 字段');
    const postSeq = evA.seq;
    assert.ok(Number.isFinite(postSeq), '直播事件应带 seq');
    await sleep(500);
    assert.ok(!cEvents.some((e) => e.event === 'group_message' && e.data && e.data.group_id === gid), '匿名客户端不应收到私密群事件');
    ok(`直播 group_message 带 body，两登录态客户端一致，seq=${postSeq}，匿名收不到`);
    // 删消息（群本身无删除接口，见文件头）。
    const del = await rest('DELETE', `/api/groups/${gid}/messages/${mid}`, null);
    assert.ok(del.status === 200 || del.status === 204, `删消息应 200/204，实际 ${del.status}`);
    mid = null;
    ok('测试消息已删');

    console.log('== 4. 补发 group_message 无 body ==');
    const d = new WsClient(API_BASE, { token: TOKEN, since: postSeq - 1 });
    clients.push(d);
    const replayP = waitEvent(d, (e) => e.event === 'group_message' && e.data && e.data.group_id === gid, 15000, '补发 group_message');
    d.start();
    await withTimeout(new Promise((r, j) => { d.on('connect', r); d.on('disconnect', j); }), 20000, 'D hello');
    const replay = await replayP;
    assert.strictEqual(replay.seq, postSeq, '补发应为同一条事件（同一 seq）');
    assert.strictEqual(replay.data.body, undefined, '补发的 group_message 不应带 body');
    assert.strictEqual(replay.data.id, evA.data.id, '补发应为同一条消息');
    ok('补发 group_message 无 body（须走增量拉取），seq 与消息 id 与直播一致');
    d.stop();

    console.log('== 5. 保活 100 秒（什么都不发，验自动回 pong） ==');
    let sawDisconnect = false;
    a.on('disconnect', () => { sawDisconnect = true; });
    await sleep(100000);
    assert.strictEqual(sawDisconnect, false, '100 秒内不应断开');
    assert.strictEqual(a.connected, true, '100 秒后仍应处于连接态');
    ok('100 秒保活通过（服务端 30 秒 ping、90 秒无 pong 即断；不断即自动回 pong 生效）');

    console.log('== 6. 下线 presence 广播 ==');
    const presenceOff = waitEvent(c, (e) => e.event === 'presence' && e.data && e.data.account === 'xilan' && e.data.online === false, 15000, 'presence online:false');
    b.stop();
    a.stop();
    await presenceOff;
    ok('C 见证 xilan 下线（presence online:false）');
    c.stop();

    console.log(`\nPASS: ${passed} 项全过`);
  } finally {
    // 尽力清理：测试消息删掉；群删不掉的打印出来由外部清。
    for (const cl of clients) { try { cl.stop(); } catch {} }
    if (mid && gid) {
      try { await rest('DELETE', `/api/groups/${gid}/messages/${mid}`, null); } catch {}
    }
    if (gid) console.log('CLEANUP_GROUP_ID=' + gid);
  }
}

main().catch((err) => {
  console.error(`\nFAIL: ${err.message}`);
  process.exit(1);
});
