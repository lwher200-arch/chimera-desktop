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
const { nextBackoffMs, INITIAL_BACKOFF_MS, MAX_BACKOFF_MS } = require('../lib/backoff');
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

// 最小 WS 服务端（仅 §7 测试用）：握手后立刻发一条 hello，之后保持连接。
// 只实现服务端发帧（FIN + opcode 1，小 payload），不解析客户端帧。
function startMockWsServer() {
  return new Promise((resolve) => {
    const sockets = new Set();
    const server = http.createServer((_req, res) => { res.writeHead(404); res.end(); });
    server.on('upgrade', (req, socket) => {
      const key = req.headers['sec-websocket-key'] || '';
      const accept = crypto.createHash('sha1')
        .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11', 'utf8').digest('base64');
      socket.write(
        'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\n' +
        'Connection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`,
      );
      sockets.add(socket);
      socket.on('close', () => sockets.delete(socket));
      socket.on('error', () => sockets.delete(socket));
      const payload = Buffer.from(JSON.stringify({
        type: 'hello',
        data: { seq: 4200, time: new Date().toISOString() },
      }), 'utf8');
      socket.write(Buffer.concat([Buffer.from([0x81, payload.length]), payload]));
    });
    server.listen(0, '127.0.0.1', () => resolve({
      url: `ws://127.0.0.1:${server.address().port}/ws`,
      close() {
        for (const s of sockets) { try { s.destroy(); } catch {} }
        server.close();
      },
    }));
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

  console.log('== 6. 消息窗口（v0.2.1+1）：DOM 上限 + 向前翻页 + 贴底判断 ==');
  // 从 renderer.js 源码里抽出窗口相关函数（与 §5a 同一做法：测实际发布的
  // 代码，不是复制品），跑在一个最小假 DOM 上。假 DOM 只实现渲染进程
  // 用到的接口：innerHTML / insertAdjacentHTML / querySelector(.msg、
  // [data-empty]) / querySelectorAll(.msg) / scrollTop / scrollHeight /
  // clientHeight / remove()。每条消息假高 40px，只验滚动补偿的算术。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const MAX_RENDERED = Number((rendererSrc.match(/const MAX_RENDERED = (\d+);/) || [])[1]);
    const PAGE_SIZE = Number((rendererSrc.match(/const PAGE_SIZE = (\d+);/) || [])[1]);
    const NEAR_BOTTOM_PX = Number((rendererSrc.match(/const NEAR_BOTTOM_PX = (\d+);/) || [])[1]);
    assert.strictEqual(MAX_RENDERED, 300, 'DOM 上限应为 300 条');
    assert.strictEqual(PAGE_SIZE, 100, '每页应为 100 条');
    assert.strictEqual(NEAR_BOTTOM_PX, 120, '贴底判定应为 120px');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const escFn = new Function(`${extractFn('esc')}; return esc;`)();
    const messageHtmlFn = new Function('esc', `${extractFn('messageHtml')}; return messageHtml;`)(escFn);

    function makeEl(cls, id, parent) {
      return {
        cls, dataset: { id: id || '' }, height: 40, _parent: parent,
        remove() { if (this._parent) this._parent._removeChild(this); },
      };
    }
    function parseChildren(html, parent) {
      const out = [];
      const re = /<div class="msg" data-id="([^"]*)">/g;
      let m;
      while ((m = re.exec(html)) !== null) out.push(makeEl('msg', m[1], parent));
      if (/data-empty="1"/.test(html)) {
        const el = makeEl('muted-empty', '', parent);
        el.height = 20;
        out.push(el);
      }
      return out;
    }
    function makeBox() {
      const box = {
        _children: [],
        _scrollTop: 0,
        clientHeight: 400,
        // 真实浏览器里 scrollTop 会被钳制到 [0, scrollHeight - clientHeight]；
        // 假 DOM 也照此钳制，否则“滚到底”的断言会量到未钳制的原值。
        get scrollTop() { return this._scrollTop; },
        set scrollTop(v) {
          const max = Math.max(0, this.scrollHeight - this.clientHeight);
          this._scrollTop = Math.max(0, Math.min(v, max));
        },
        get scrollHeight() { return this._children.reduce((s, c) => s + (c.height || 0), 0); },
        set innerHTML(html) { this._children = parseChildren(html, this); },
        get innerHTML() { return ''; },
        insertAdjacentHTML(pos, html) {
          const kids = parseChildren(html, this);
          if (pos === 'afterbegin') this._children.unshift(...kids);
          else this._children.push(...kids);
        },
        querySelector(sel) {
          if (sel === '.msg') return this._children.find((c) => c.cls === 'msg') || null;
          if (sel === '[data-empty]') return this._children.find((c) => c.cls === 'muted-empty') || null;
          return null;
        },
        querySelectorAll(sel) {
          if (sel === '.msg') return this._children.filter((c) => c.cls === 'msg');
          return [];
        },
        _removeChild(el) { const i = this._children.indexOf(el); if (i >= 0) this._children.splice(i, 1); },
      };
      return box;
    }
    function makeRowBtn() {
      return {
        _hidden: true,
        classList: { toggle(_cls, force) { this._owner._hidden = !!force; } },
        disabled: false,
        textContent: '',
      };
    }
    const box = makeBox();
    const rowEl = makeRowBtn();
    rowEl.classList._owner = rowEl;
    const btnEl = { disabled: false, textContent: '' };
    const $fake = (id) => ({ chatBox: box, chatOlderRow: rowEl, chatOlderBtn: btnEl }[id] || null);
    const mkMsg = (i) => ({
      id: `m${i}`, author: 'a', ts: new Date(Date.UTC(2026, 10, 23, 0, 0, i)).toISOString(), body: `msg ${i}`,
    });
    const freshState = () => ({
      open: { kind: 'group', id: 'g1', name: 't' },
      lastTs: null, renderedIds: new Set(), hasMoreOlder: false, loadingOlder: false,
    });
    let state = freshState();
    const renderMessages = new Function('state', '$', 'messageHtml', 'MAX_RENDERED',
      `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED);
    const appendMessages = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'NEAR_BOTTOM_PX',
      `${extractFn('appendMessages')}; return appendMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, NEAR_BOTTOM_PX);
    const prependMessages = new Function('state', '$', 'messageHtml',
      `${extractFn('prependMessages')}; return prependMessages;`)(state, $fake, messageHtmlFn);
    const syncOlderButton = new Function('state', '$',
      `${extractFn('syncOlderButton')}; return syncOlderButton;`)(state, $fake);

    // 6a. 初拉 120 条：全渲染，水位为最后一条。
    renderMessages(Array.from({ length: 120 }, (_, i) => mkMsg(i + 1)));
    assert.strictEqual(box.querySelectorAll('.msg').length, 120, '初拉 120 条应全渲染');
    assert.strictEqual(state.renderedIds.size, 120, 'renderedIds 应记 120 条');
    assert.strictEqual(state.lastTs, mkMsg(120).ts, '水位应为最后一条 ts');
    ok('初拉 120 条全渲染，水位正确');

    // 6b. 初拉 350 条（防御性上限）：只留最新 300 条，hasMoreOlder 为真。
    state = freshState();
    const render2 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED',
      `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED);
    render2(Array.from({ length: 350 }, (_, i) => mkMsg(i + 1)));
    assert.strictEqual(box.querySelectorAll('.msg').length, 300, '350 条初拉应只渲染 300 条');
    assert.strictEqual(box.querySelector('.msg').dataset.id, 'm51', '应留最新的 300 条（m51 起）');
    assert.strictEqual(state.hasMoreOlder, true, '超上限初拉应标 hasMoreOlder');
    ok('初拉 350 条只留最新 300 条，hasMoreOlder 为真');

    // 6c. 追加超上限：100 条初拉 + 250 条追加 → 留 300 条，最老的 50 条
    // 节点与 id 一起被裁掉，最新一条与水位 intact。
    state = freshState();
    const render3 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED',
      `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED);
    const append3 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED', 'NEAR_BOTTOM_PX',
      `${extractFn('appendMessages')}; return appendMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED, NEAR_BOTTOM_PX);
    render3(Array.from({ length: 100 }, (_, i) => mkMsg(i + 1)));
    box.scrollTop = box.scrollHeight - box.clientHeight; // 贴底
    append3(Array.from({ length: 250 }, (_, i) => mkMsg(101 + i)));
    assert.strictEqual(box.querySelectorAll('.msg').length, 300, '追加后 DOM 应裁到 300 条');
    assert.strictEqual(box.querySelector('.msg').dataset.id, 'm51', '应裁掉最老的 50 条');
    assert.strictEqual(state.renderedIds.size, 300, 'renderedIds 应同步裁到 300');
    assert.ok(!state.renderedIds.has('m1') && state.renderedIds.has('m350'), '被裁 id 应出 Set，最新 id 应在');
    assert.strictEqual(state.lastTs, mkMsg(350).ts, '水位应为最新一条');
    ok('追加超上限：DOM 与 renderedIds 同裁到 300 条，最老 50 条出局');

    // 6d. 贴底判断：滚上去读旧消息时新消息不抢滚动；贴底时才滚到底。
    box.scrollTop = 1000; // 离底 12000-1000-400=10600，远大于 120
    const topBefore = box.scrollTop;
    append3([mkMsg(351)]);
    assert.strictEqual(box.scrollTop, topBefore, '滚上去时追加不应动 scrollTop');
    box.scrollTop = box.scrollHeight - box.clientHeight; // 回到底部
    append3([mkMsg(352)]);
    assert.strictEqual(box.scrollTop, box.scrollHeight - box.clientHeight, '贴底时追加应滚到底');
    ok('贴底判断：上翻时不抢滚动，贴底时滚到底');

    // 6e. 向前追加 2 条：插到顶上，scrollTop 按插入高度补偿，id 补进 Set。
    state = freshState();
    const render5 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED',
      `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED);
    const prepend5 = new Function('state', '$', 'messageHtml',
      `${extractFn('prependMessages')}; return prependMessages;`)(state, $fake, messageHtmlFn);
    render5(Array.from({ length: 100 }, (_, i) => mkMsg(101 + i))); // m101..m200
    box.scrollTop = 500;
    const hBefore = box.scrollHeight;
    prepend5([mkMsg(99), mkMsg(100)]);
    assert.strictEqual(box.querySelector('.msg').dataset.id, 'm99', '前翻的应插到最顶上');
    assert.strictEqual(box.scrollTop, 500 + (box.scrollHeight - hBefore), 'scrollTop 应按插入高度补偿');
    assert.ok(state.renderedIds.has('m99') && state.renderedIds.has('m100'), '前翻的 id 应进 Set');
    assert.strictEqual(state.lastTs, mkMsg(200).ts, '前翻不应动水位');
    ok('向前追加：插顶、scrollTop 补偿、id 进 Set、水位不动');

    // 6f. loadOlderMessages：假服务端 250 条，按服务端 paginate 口径回页。
    // 第一页翻回 100 条、has_more 仍真；第二页翻回剩下 50 条、has_more 假；
    // 再点不发请求。另：已到 300 上限时直接关按钮、不发请求。
    {
      const serverMsgs = Array.from({ length: 250 }, (_, i) => mkMsg(i + 1));
      let calls = 0;
      const fakeApi = {
        groupMessages: async (_id, opts = {}) => {
          calls += 1;
          let list = serverMsgs;
          if (opts.before_id) {
            const idx = list.findIndex((x) => x.id === opts.before_id);
            list = idx >= 0 ? list.slice(0, idx) : [];
          }
          let hasMore = false;
          if (opts.limit) {
            const limit = Math.max(1, Math.min(100, parseInt(opts.limit, 10) || 20));
            if (list.length > limit) { hasMore = true; list = list.slice(list.length - limit); }
          }
          return { messages: list, total: serverMsgs.length, has_more: hasMore };
        },
        dmHistory: async () => { throw new Error('本节只测群聊'); },
      };
      const fakeWindow = { chimera: { api: fakeApi } };
      state = freshState();
      const render6 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED',
        `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED);
      const prepend6 = new Function('state', '$', 'messageHtml',
        `${extractFn('prependMessages')}; return prependMessages;`)(state, $fake, messageHtmlFn);
      const sync6 = new Function('state', '$',
        `${extractFn('syncOlderButton')}; return syncOlderButton;`)(state, $fake);
      const loadOlder = new Function('state', '$', 'window', 'prependMessages', 'syncOlderButton', 'MAX_RENDERED', 'PAGE_SIZE',
        `${extractFn('loadOlderMessages')}; return loadOlderMessages;`)(state, $fake, fakeWindow, prepend6, sync6, MAX_RENDERED, PAGE_SIZE);
      render6(serverMsgs.slice(-100)); // 初拉最新 100 条 m151..m250
      state.hasMoreOlder = true;
      box.scrollTop = 2000;
      const top6 = box.scrollTop;
      await loadOlder();
      assert.strictEqual(calls, 1, '第一页应发 1 次请求');
      assert.strictEqual(box.querySelectorAll('.msg').length, 200, '第一页应翻回 100 条，共 200 条');
      assert.strictEqual(box.querySelector('.msg').dataset.id, 'm51', '第一页最顶上应为 m51');
      assert.strictEqual(state.hasMoreOlder, true, '还剩 50 条，hasMoreOlder 应仍真');
      assert.ok(box.scrollTop > top6, '前翻后 scrollTop 应补偿上移');
      await loadOlder();
      assert.strictEqual(calls, 2, '第二页应发第 2 次请求');
      assert.strictEqual(box.querySelectorAll('.msg').length, 250, '第二页应翻回剩下 50 条，共 250 条');
      assert.strictEqual(state.hasMoreOlder, false, '翻完应无更早，hasMoreOlder 为假');
      await loadOlder();
      assert.strictEqual(calls, 2, '无更早时再点不应发请求');
      ok('向前翻页：两页翻完 250 条，has_more 收敛，不再发请求');

      // 上限守卫：已渲染 300 条时点翻页，直接关按钮、不发请求。
      state = freshState();
      const render7 = new Function('state', '$', 'messageHtml', 'MAX_RENDERED',
        `${extractFn('renderMessages')}; return renderMessages;`)(state, $fake, messageHtmlFn, MAX_RENDERED);
      const prepend7 = new Function('state', '$', 'messageHtml',
        `${extractFn('prependMessages')}; return prependMessages;`)(state, $fake, messageHtmlFn);
      const sync7 = new Function('state', '$',
        `${extractFn('syncOlderButton')}; return syncOlderButton;`)(state, $fake);
      const loadOlder7 = new Function('state', '$', 'window', 'prependMessages', 'syncOlderButton', 'MAX_RENDERED', 'PAGE_SIZE',
        `${extractFn('loadOlderMessages')}; return loadOlderMessages;`)(state, $fake, fakeWindow, prepend7, sync7, MAX_RENDERED, PAGE_SIZE);
      render7(Array.from({ length: 350 }, (_, i) => mkMsg(i + 1))); // 裁到 300 条
      state.hasMoreOlder = true;
      const callsBefore = calls;
      await loadOlder7();
      assert.strictEqual(calls, callsBefore, '已到上限时翻页不应发请求');
      assert.strictEqual(state.hasMoreOlder, false, '已到上限时应关掉翻页');
      assert.strictEqual(rowEl._hidden, true, '已到上限时按钮行应隐藏');
      ok('上限守卫：已渲染 300 条时翻页不发请求、按钮隐藏');
    }

    // 6g. 按钮三态：有更早→显示可点；加载中→禁用“加载中…”；无更早→隐藏。
    state = freshState();
    const syncG = new Function('state', '$',
      `${extractFn('syncOlderButton')}; return syncOlderButton;`)(state, $fake);
    state.hasMoreOlder = true;
    syncG();
    assert.strictEqual(rowEl._hidden, false, '有更早时按钮行应显示');
    assert.strictEqual(btnEl.disabled, false, '有更早时按钮应可点');
    assert.strictEqual(btnEl.textContent, '加载更早的消息', '按钮文案应为加载更早的消息');
    state.loadingOlder = true;
    syncG();
    assert.strictEqual(btnEl.disabled, true, '加载中按钮应禁用');
    assert.strictEqual(btnEl.textContent, '加载中…', '加载中文案应为加载中…');
    state.loadingOlder = false;
    state.hasMoreOlder = false;
    syncG();
    assert.strictEqual(rowEl._hidden, true, '无更早时按钮行应隐藏');
    ok('按钮三态：可点 / 加载中禁用 / 无更早隐藏');
  }

  console.log('== 7. 状态指示器与退避（v0.2.1）：退避表 + 会话失效停重试 ==');
  // 7a. 退避表（纯函数，无 live 依赖）：1s 起步翻倍、30s 封顶。
  {
    assert.strictEqual(INITIAL_BACKOFF_MS, 1000, '起步应为 1s');
    assert.strictEqual(MAX_BACKOFF_MS, 30000, '上限应为 30s');
    assert.strictEqual(nextBackoffMs(undefined), 1000, '未传应回 1000');
    assert.strictEqual(nextBackoffMs(0), 1000, '0 应回 1000');
    assert.strictEqual(nextBackoffMs(-5), 1000, '负数应回 1000');
    assert.strictEqual(nextBackoffMs(NaN), 1000, 'NaN 应回 1000');
    let d = INITIAL_BACKOFF_MS;
    const seq = [d];
    for (let i = 0; i < 6; i++) { d = nextBackoffMs(d); seq.push(d); }
    assert.deepStrictEqual(seq, [1000, 2000, 4000, 8000, 16000, 30000, 30000],
      '退避序列应为 1s 起步翻倍、30s 封顶');
    assert.strictEqual(nextBackoffMs(99999), 30000, '超上限应封顶 30000');
    ok('退避表：1s 起步翻倍、30s 封顶，非法输入回 1000');
  }
  // 7b. 会话失效停重试：WS 在 hello 前就失败 + checkSession 抛 401 →
  // session-expired，不回落 SSE、不再重试。
  {
    const states = [];
    const rt = new RealtimeClient(API_BASE, {
      token: 'dead-token-for-test',
      wsUrl: 'wss://127.0.0.1:9/ws',
      checkSession: async () => {
        const e = new Error('HTTP 401');
        e.status = 401;
        throw e;
      },
    });
    let fellBack = false;
    rt.on('fallback', () => { fellBack = true; });
    rt.on('transport', (p) => states.push(p.state));
    const expired = new Promise((resolve, reject) => {
      rt.on('session-expired', resolve);
      setTimeout(() => reject(new Error('session-expired 未在 10s 内触发')), 10000);
    });
    rt.start();
    const ev = await withTimeout(expired, 12000, 'session-expired');
    assert.match(ev.reason, /会话已失效/, 'session-expired 应带原因');
    assert.strictEqual(rt.sessionExpired, true, 'sessionExpired 标志应为真');
    assert.strictEqual(rt._sse, null, '会话失效后不应起 SSE');
    assert.strictEqual(rt._ws, null, '会话失效后 WS 应停掉');
    assert.strictEqual(fellBack, false, '会话失效后不应回落');
    assert.ok(states.includes('connecting'), '应先报 connecting');
    assert.ok(states.includes('session-expired'), 'transport 应报 session-expired');
    await new Promise((r) => setTimeout(r, 2000));
    assert.strictEqual(rt._sse, null, '2 秒后仍不应起 SSE（不再重试）');
    rt.stop();
    ok('会话失效停重试：hello 前失败 + 401 → session-expired，不回落不重试');
  }
  // 7c. 瞬断仍回落：同一死端口，checkSession 通过 → 回落 SSE，不报会话失效。
  {
    const rt = new RealtimeClient(API_BASE, {
      token: 'live-token-stub',
      wsUrl: 'wss://127.0.0.1:9/ws',
      checkSession: async () => {}, // 会话正常
    });
    let expired = false;
    rt.on('session-expired', () => { expired = true; });
    const fellBack = new Promise((resolve, reject) => {
      rt.on('fallback', resolve);
      setTimeout(() => reject(new Error('fallback 未在 10s 内触发')), 10000);
    });
    rt.start();
    const fb = await withTimeout(fellBack, 12000, '瞬断回落');
    assert.strictEqual(fb.to, 'sse', '瞬断应回落 SSE');
    assert.notStrictEqual(rt._sse, null, '回落后 SSE 客户端应已创建');
    assert.strictEqual(expired, false, '会话正常时不应报 session-expired');
    rt.stop();
    ok('瞬断仍回落：checkSession 通过 → 回落 SSE，不报会话失效');
  }
  // 7d. 网络错不算会话失效：checkSession 抛无 status 的错 → 仍回落。
  {
    const rt = new RealtimeClient(API_BASE, {
      token: 'live-token-stub',
      wsUrl: 'wss://127.0.0.1:9/ws',
      checkSession: async () => { throw new Error('fetch failed'); }, // 网络错，无 status
    });
    let expired = false;
    rt.on('session-expired', () => { expired = true; });
    const fellBack = new Promise((resolve, reject) => {
      rt.on('fallback', resolve);
      setTimeout(() => reject(new Error('fallback 未在 10s 内触发')), 10000);
    });
    rt.start();
    await withTimeout(fellBack, 12000, '网络错回落');
    assert.strictEqual(expired, false, '网络错不应报 session-expired');
    rt.stop();
    ok('网络错不算会话失效：checkSession 抛无 status 的错 → 仍回落 SSE');
  }
  // 7e. hello 后会话失效：mock WS 先连上，随后 session-expired、WS 停掉。
  {
    const mock = await startMockWsServer();
    try {
      const states = [];
      const rt = new RealtimeClient(API_BASE, {
        token: 'dead-token-for-test',
        wsUrl: mock.url,
        checkSession: async () => {
          const e = new Error('HTTP 401');
          e.status = 401;
          throw e;
        },
      });
      rt.on('transport', (p) => states.push(p.state));
      const connected = new Promise((resolve, reject) => {
        rt.on('connect', resolve);
        setTimeout(() => reject(new Error('connect 未在 10s 内触发')), 10000);
      });
      const expired = new Promise((resolve, reject) => {
        rt.on('session-expired', resolve);
        setTimeout(() => reject(new Error('session-expired 未在 10s 内触发')), 10000);
      });
      rt.start();
      const c = await withTimeout(connected, 12000, 'mock WS connect');
      assert.strictEqual(c.transport, 'ws', 'mock WS 应先连上');
      const ev = await withTimeout(expired, 12000, 'hello 后 session-expired');
      assert.match(ev.reason, /会话已失效/);
      assert.strictEqual(rt.sessionExpired, true);
      assert.strictEqual(rt._ws, null, '会话失效后 WS 应停掉');
      assert.ok(states.includes('ws'), 'transport 应先报 ws');
      assert.ok(states.includes('session-expired'), 'transport 应随后报 session-expired');
      rt.stop();
      ok('hello 后会话失效：先连上 WS，随后 session-expired、WS 停掉');
    } finally {
      mock.close();
    }
  }
  // 7f. hello 后会话正常：mock WS 连上后保持 WS，不报 session-expired。
  {
    const mock = await startMockWsServer();
    try {
      let expired = false;
      const rt = new RealtimeClient(API_BASE, {
        token: 'live-token-stub',
        wsUrl: mock.url,
        checkSession: async () => {}, // 会话正常
      });
      rt.on('session-expired', () => { expired = true; });
      const connected = new Promise((resolve, reject) => {
        rt.on('connect', resolve);
        setTimeout(() => reject(new Error('connect 未在 10s 内触发')), 10000);
      });
      rt.start();
      const c = await withTimeout(connected, 12000, 'mock WS connect');
      assert.strictEqual(c.transport, 'ws');
      await new Promise((r) => setTimeout(r, 1500));
      assert.strictEqual(expired, false, '会话正常时不应报 session-expired');
      assert.strictEqual(rt.transport, 'ws', '会话正常时应保持 ws');
      assert.strictEqual(rt.sessionExpired, false);
      rt.stop();
      ok('hello 后会话正常：保持 WS，不报 session-expired');
    } finally {
      mock.close();
    }
  }
  // 7g. live /api/me：假 token 应回 401——这是 main.js 里 checkSession
  // 判定会话失效的依据（只读失败，不写线上状态）。
  {
    await assert.rejects(
      client.getMe('bogus-token-for-test'),
      (e) => e && e.status === 401,
      '假 token 调 /api/me 应回 401',
    );
    ok('live /api/me：假 token 回 401（会话失效检测的依据）');
  }
  // 7h. live transport 事件：正常 WS 应先报 connecting 再报 ws。
  {
    const states = [];
    const rt = new RealtimeClient(API_BASE, {});
    rt.on('transport', (p) => states.push(p.state));
    const c = await withTimeout(new Promise((resolve, reject) => {
      rt.on('connect', resolve);
      rt.on('disconnect', reject);
      rt.start();
    }), 20000, 'live WS transport');
    assert.strictEqual(c.transport, 'ws');
    assert.deepStrictEqual(states, ['connecting', 'ws'], 'transport 应先报 connecting 再报 ws');
    rt.stop();
    ok('live transport 事件：connecting → ws');
  }
  // 7i. live 回落 transport 事件：WS 挂掉后应报 connecting 再报 sse。
  {
    const states = [];
    const rt = new RealtimeClient(API_BASE, { wsUrl: 'wss://127.0.0.1:9/ws' });
    rt.on('transport', (p) => states.push(p.state));
    const helloP = new Promise((resolve) => {
      rt.on('event', (e) => { if (e.event === 'hello') resolve(e); });
    });
    const connected = new Promise((resolve, reject) => {
      rt.on('connect', resolve);
      rt.on('disconnect', () => { /* 回落前的 WS 断开，忽略 */ });
    });
    rt.start();
    const c = await withTimeout(connected, 20000, '回落 SSE connect');
    assert.strictEqual(c.transport, 'sse');
    const helloEv = await withTimeout(helloP, 20000, '回落 SSE hello');
    assert.ok(Number.isFinite(helloEv.seq), '回落 SSE hello 应带 seq');
    assert.ok(states.includes('connecting'), '应报 connecting');
    assert.ok(states.includes('sse'), '回落后应报 sse');
    rt.stop();
    ok(`live 回落 transport 事件：connecting → sse，hello seq=${helloEv.seq}`);
  }

  console.log(`\nPASS: ${passed} 项全过`);
}

main().catch((err) => {
  console.error(`\nFAIL: ${err.message}`);
  process.exit(1);
});
