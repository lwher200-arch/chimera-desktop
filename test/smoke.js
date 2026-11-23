'use strict';
/**
 * test/smoke.js — 冒烟测试（npm test）。
 * 1. live API：ForumClient 调 https://forum.coldhao.win/api/health。
 * 2. live SSE：匿名连 /api/stream，等到 hello 事件（含 seq）即断开。
 * 3. 轻节点：对本地 mock 种子起停 NodeManager——开关、端口开合、
 *    哈希链、干净停止、开关持久化（Store round-trip）全验。
 * 不登录、不发消息、不写任何线上状态。
 */
const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const crypto = require('node:crypto');

const { ForumClient } = require('../lib/forumClient');
const { SseClient } = require('../lib/sseClient');
const { WsClient } = require('../lib/wsClient');
const { RealtimeClient } = require('../lib/realtimeClient');
const { NodeManager } = require('../lib/nodeManager');
const { Store } = require('../lib/store');

const API_BASE = process.env.CHIMERA_API_BASE || 'https://forum.coldhao.win';
let passed = 0;

function ok(name) { passed += 1; console.log(`  ok - ${name}`); }

async function withTimeout(promise, ms, label) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} 超时 ${ms}ms`)), ms);
  });
  try { return await Promise.race([promise, timeout]); }
  finally { clearTimeout(timer); }
}

// Mock 种子：只提供 /api/health，供 NodeManager 同步循环用。
function startMockSeed() {
  return new Promise((resolve) => {
    const server = http.createServer((_req, res) => {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ ok: true, posts: 472, accounts: 30, groups: 10, messages: 1676, dms: 7 }));
    });
    server.listen(0, '127.0.0.1', () => resolve(server));
  });
}

async function main() {
  console.log('== 1. live API ==');
  const client = new ForumClient(API_BASE);
  const health = await withTimeout(client.getHealth(), 15000, 'GET /api/health');
  assert.strictEqual(health.ok, true);
  assert.ok(Number.isFinite(health.posts) && health.posts > 0, 'posts 计数应为正数');
  ok(`GET /api/health -> ok, posts=${health.posts}, groups=${health.groups}`);

  console.log('== 2. live SSE ==');
  const hello = await withTimeout(new Promise((resolve, reject) => {
    const sse = new SseClient(API_BASE, {});
    sse.on('event', ({ event, data }) => {
      if (event === 'hello') { sse.stop(); resolve(data); }
    });
    sse.on('disconnect', reject);
    sse.start();
  }), 20000, 'SSE hello');
  assert.ok(Number.isFinite(hello.seq), 'hello 事件应带 seq');
  ok(`SSE hello 收到，seq=${hello.seq}`);

  console.log('== 3. 轻节点（mock 种子） ==');
  const seed = await startMockSeed();
  const seedUrl = `http://127.0.0.1:${seed.address().port}`;
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-test-'));
  try {
    // 开关持久化：Store round-trip
    const storeFile = path.join(tmp, 'state.json');
    const store = new Store(storeFile);
    assert.strictEqual(store.state.node.enabled, false, '节点开关默认关');
    store.update((s) => { s.node.enabled = true; });
    const store2 = new Store(storeFile);
    assert.strictEqual(store2.state.node.enabled, true, '开关开应持久化');
    ok('节点开关默认关，置开后持久化');

    const logs = [];
    const nm = new NodeManager({
      nodeDir: path.join(tmp, 'node'),
      apiBase: seedUrl,
      config: { enabled: false, port: 0, seeds: [seedUrl] }, // port 0 = 随机空闲端口（仅测试）
      onLog: (l) => logs.push(l),
    });
    // port 0 需要解析实际端口：NodeManager 用 config.port 监听，0 会由系统分配；
    // 但 status() 汇报的是 config.port（0），测试里用 server.address() 核对。
    await nm.start();
    assert.strictEqual(nm.state, 'running');
    const actualPort = nm._server.address().port;
    assert.ok(actualPort > 0, '应监听到真实端口');
    const res = await fetch(`http://127.0.0.1:${actualPort}/health`);
    const body = await res.json();
    assert.strictEqual(body.ok, true);
    assert.strictEqual(body.tier, 'read-only');
    ok(`节点启动：/health ok，监听 127.0.0.1:${actualPort}`);

    // 同步一次应写哈希链
    await nm.syncOnce();
    assert.ok(nm.headersCount >= 1, '同步后应有副本头');
    const lines = fs.readFileSync(path.join(tmp, 'node', 'replica', 'headers.jsonl'), 'utf8')
      .trim().split('\n').map((l) => JSON.parse(l));
    assert.ok(lines.length >= 1);
    const last = lines[lines.length - 1];
    const recomputed = crypto.createHash('sha256')
      .update(JSON.stringify(last, Object.keys(last).sort()), 'utf8').digest('hex');
    assert.notStrictEqual(recomputed, last.hash, 'hash 字段本身不参与自哈希（防同义）');
    // 校验链：去掉 hash 后按排序键重算应等于记录的 hash
    const { hash, ...rest } = last;
    const expect = crypto.createHash('sha256')
      .update(JSON.stringify(rest, Object.keys(rest).sort()), 'utf8').digest('hex');
    assert.strictEqual(expect, hash, '副本头哈希链应自洽');
    ok(`哈希链自洽，副本头 ${lines.length} 条`);

    // 干净停止：端口即释放
    await nm.stop();
    assert.strictEqual(nm.state, 'off');
    await assert.rejects(fetch(`http://127.0.0.1:${actualPort}/health`), '停后端口应拒绝连接');
    ok('节点停止：状态 off，端口已释放');

    // setEnabled 开关接口
    await nm.setEnabled(true);
    assert.strictEqual(nm.state, 'running');
    await nm.setEnabled(false);
    assert.strictEqual(nm.state, 'off');
    ok('setEnabled(true/false) 开关接口正常');
  } finally {
    seed.close();
    fs.rmSync(tmp, { recursive: true, force: true });
  }

  console.log('== 4. live WS（内核 v2，匿名，不写线上状态）==');
  // 4a. WS hello：匿名连 wss，应收到带 seq 的 hello。
  {
    const ws = new WsClient(API_BASE, {});
    const c = await withTimeout(new Promise((resolve, reject) => {
      ws.on('connect', resolve);
      ws.on('disconnect', reject);
      ws.start();
    }), 20000, 'WS hello');
    assert.ok(Number.isFinite(ws.lastSeq), 'hello 后 lastSeq 应为有限 seq');
    assert.strictEqual(ws.everConnected, true);
    ws.stop();
    ok(`WS hello 收到，seq=${c.seq}`);
  }
  // 4b. truncated：since=0 远早于缓冲，服务端应先发 truncated。
  {
    const ws = new WsClient(API_BASE, { since: 0 });
    const data = await withTimeout(new Promise((resolve, reject) => {
      ws.on('truncated', resolve);
      ws.on('disconnect', reject);
      ws.start();
    }), 20000, 'WS truncated');
    assert.strictEqual(data.truncated, true, 'truncated 事件应带 truncated:true');
    assert.ok(Number.isFinite(data.seq), 'truncated 应带最新 seq');
    ws.stop();
    ok(`WS truncated 收到，latest seq=${data.seq}`);
  }
  // 4c. 断线续接：主动断开后应按 lastSeq 自动重连，不丢连接。
  {
    const ws = new WsClient(API_BASE, {});
    let connects = 0;
    const second = new Promise((resolve, reject) => {
      ws.on('connect', () => {
        connects += 1;
        if (connects === 2) resolve(ws.lastSeq);
      });
      ws.on('disconnect', () => { /* 重连中，忽略 */ });
      ws.start();
    });
    await withTimeout(new Promise((resolve, reject) => {
      ws.on('connect', () => resolve());
      setTimeout(() => reject(new Error('首次 hello 未到')), 20000);
    }), 20000, 'WS 首次 hello');
    const seq1 = ws.lastSeq;
    assert.ok(Number.isFinite(seq1));
    ws.dropConnection();
    const seq2 = await withTimeout(second, 25000, 'WS 断线重连');
    assert.ok(Number.isFinite(seq2) && seq2 >= seq1, '重连后 seq 不应倒退');
    ws.stop();
    ok(`WS 断线重连成功，seq ${seq1} -> ${seq2}`);
  }
  // 4d. WS 优先：RealtimeClient 正常应走 ws。
  {
    const rt = new RealtimeClient(API_BASE, {});
    const c = await withTimeout(new Promise((resolve, reject) => {
      rt.on('connect', resolve);
      rt.on('disconnect', reject);
      rt.start();
    }), 20000, 'Realtime WS connect');
    assert.strictEqual(c.transport, 'ws', '正常应优先走 WS');
    assert.strictEqual(rt.transport, 'ws');
    rt.stop();
    ok(`RealtimeClient 优先走 WS，seq=${c.seq}`);
  }
  // 4e. WS 挂掉 → SSE 回落：WS 指到不通的本地端口，应切 SSE 并收到 hello。
  {
    const rt = new RealtimeClient(API_BASE, { wsUrl: 'wss://127.0.0.1:9/ws' });
    const fellBack = new Promise((resolve) => rt.on('fallback', resolve));
    const connected = new Promise((resolve, reject) => {
      rt.on('connect', resolve);
      rt.on('disconnect', () => { /* 回落前的 WS 断开，忽略 */ });
    });
    // hello 监听必须在 start() 之前挂上：SSE hello 紧跟 connect 后面到，
    // 晚挂会漏掉（connect 只表示 HTTP 通了，hello 是流里第一条事件）。
    const helloP = new Promise((resolve) => {
      rt.on('event', (e) => { if (e.event === 'hello') resolve(e); });
    });
    rt.start();
    const fb = await withTimeout(fellBack, 20000, 'WS->SSE 回落信号');
    assert.strictEqual(fb.to, 'sse');
    const c = await withTimeout(connected, 20000, '回落 SSE connect');
    assert.strictEqual(c.transport, 'sse', 'WS 挂掉后应走 SSE');
    const helloEv = await withTimeout(helloP, 20000, '回落 SSE hello');
    assert.ok(Number.isFinite(helloEv.seq), '回落 SSE hello 应带 seq');
    rt.stop();
    ok(`WS 挂掉后回落 SSE，hello seq=${helloEv.seq}`);
  }

  console.log('== 5. 性能优化（v0.2.1）：分块 base64 + 副本头轮转 ==');
  // §3 的 mock 种子已关，这里另起一个供 5b/5c 用。
  const seed5 = await startMockSeed();
  const seed5Url = `http://127.0.0.1:${seed5.address().port}`;
  // 5a. 分块 base64：直接测 renderer.js 里实际发布的函数（从源码抽出来跑，
  // 不是复制一份）。5MB 随机 buffer 应与 Buffer.toString('base64') 逐字节一致；
  // 旧的展开写法在 200KB 就应抛 RangeError（回归锚点，证明修的是真 bug）。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const m = rendererSrc.match(/function arrayBufferToBase64\(buf\) \{[\s\S]*?\n\}/);
    assert.ok(m, 'renderer.js 应包含 arrayBufferToBase64');
    const arrayBufferToBase64 = new Function(`${m[0]}; return arrayBufferToBase64;`)();
    assert.strictEqual(arrayBufferToBase64(new ArrayBuffer(0)), '');
    const small = crypto.randomBytes(3 * 1024);
    assert.strictEqual(arrayBufferToBase64(small.buffer), small.toString('base64'), '3KB 应与 Buffer 口径一致');
    const big = crypto.randomBytes(5 * 1024 * 1024);
    const t0 = Date.now();
    const b64 = arrayBufferToBase64(big.buffer);
    const ms = Date.now() - t0;
    assert.strictEqual(b64, big.toString('base64'), '5MB 分块 base64 应与 Buffer 口径逐字节一致');
    assert.ok(Buffer.from(b64, 'base64').equals(big), '5MB base64 往返应无损');
    assert.throws(() => {
      const b = new Uint8Array(200 * 1024);
      btoa(String.fromCharCode(...b));
    }, RangeError, '旧展开写法在 200KB 应抛 RangeError');
    ok(`分块 base64：5MB 往返一致（${ms}ms），旧写法 200KB 确认抛 RangeError`);
  }
  // 5b. 副本头轮转：上限 5 行 + slack 0，写 8 条后文件只剩最新 5 行，链不断。
  {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-rot-'));
    try {
      const nm = new NodeManager({
        nodeDir: path.join(tmp, 'node'),
        apiBase: seed5Url,
        config: { enabled: false, port: 0, seeds: [seed5Url] },
        maxHeadersLines: 5,
        rotateSlackLines: 0,
        onLog: () => {},
      });
      await nm.start();
      for (let i = 0; i < 7; i++) await nm.syncOnce();
      assert.strictEqual(nm.headersCount, 5, '轮转后计数应为上限 5');
      const lines = fs.readFileSync(path.join(tmp, 'node', 'replica', 'headers.jsonl'), 'utf8')
        .split('\n').filter(Boolean).map((l) => JSON.parse(l));
      assert.strictEqual(lines.length, 5, '文件应只剩最新 5 行');
      for (let i = 1; i < lines.length; i++) {
        assert.strictEqual(lines[i].prevHash, lines[i - 1].hash, `第 ${i} 行 prevHash 应接上一行 hash`);
      }
      assert.strictEqual(nm._prevHash, lines[lines.length - 1].hash, '_prevHash 应为尾行 hash');
      ok('副本头轮转：8 条 → 只留最新 5 行，链内 prevHash 逐行衔接');
      await nm.stop();
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  }
  // 5c. _loadHead 有界读：预填 10000 行合法链，构造即读一次、只解析尾行。
  {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-node-head-'));
    try {
      const dir = path.join(tmp, 'node', 'replica');
      fs.mkdirSync(dir, { recursive: true });
      const canonical = (o) => JSON.stringify(o, Object.keys(o).sort());
      let prev = '0'.repeat(64);
      const out = [];
      for (let i = 0; i < 10000; i++) {
        const body = { kind: 'community-head', ts: new Date().toISOString(), posts: i, prevHash: prev };
        const hash = crypto.createHash('sha256').update(canonical(body), 'utf8').digest('hex');
        out.push(JSON.stringify({ ...body, hash }));
        prev = hash;
      }
      fs.writeFileSync(path.join(dir, 'headers.jsonl'), out.join('\n') + '\n');
      const t0 = Date.now();
      const nm = new NodeManager({
        nodeDir: path.join(tmp, 'node'),
        apiBase: seed5Url,
        config: { enabled: false, port: 0, seeds: [seed5Url] },
        onLog: () => {},
      });
      const ms = Date.now() - t0;
      assert.strictEqual(nm.headersCount, 10000, '预填 10000 行应数出 10000');
      assert.strictEqual(nm._prevHash, prev, '_prevHash 应为尾行 hash');
      assert.ok(ms < 1000, `10000 行 _loadHead 应在 1 秒内，实际 ${ms}ms`);
      assert.strictEqual(nm._tailHeaders(3).length, 3, '_tailHeaders(3) 应只回最后 3 行');
      assert.strictEqual(nm._tailHeaders(3)[2].hash, prev, '_tailHeaders 尾行应为最新');
      ok(`_loadHead 有界读：10000 行 ${ms}ms，计数与尾 hash 正确`);
    } finally {
      fs.rmSync(tmp, { recursive: true, force: true });
    }
  }
  seed5.close();

  console.log(`\nPASS: ${passed} 项全过`);
}

main().catch((err) => {
  console.error(`\nFAIL: ${err.message}`);
  process.exit(1);
});
