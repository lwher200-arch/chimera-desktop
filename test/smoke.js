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
const { ServerResolver, PRIMARY_BASE, FALLBACK_BASE } = require('../lib/serverResolver');
const { AppUpdater, compareVersions, sha256Hex } = require('../lib/appUpdater');
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
    // messageHtml 自 v0.3.0 起还用 state.me（判自己的消息）、avatarHtml、fmtTime：
    // 抽出来时一并注入。§6 的假 state 从不设 me（没有一条是自己的消息），
    // 故这里传一个固定的 { me: null }，不随下面 state = freshState() 的重绑而变。
    const avatarClassFn = new Function(`${extractFn('avatarClass')}; return avatarClass;`)();
    const avatarInitialFn = new Function(`${extractFn('avatarInitial')}; return avatarInitial;`)();
    const avatarHtmlFn = new Function('avatarClass', 'avatarInitial', 'esc', `${extractFn('avatarHtml')}; return avatarHtml;`)(avatarClassFn, avatarInitialFn, escFn);
    const fmtTimeFn = new Function(`${extractFn('fmtTime')}; return fmtTime;`)();
    const messageHtmlFn = new Function('esc', 'state', 'avatarHtml', 'fmtTime', `${extractFn('messageHtml')}; return messageHtml;`)(escFn, { me: null }, avatarHtmlFn, fmtTimeFn);

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

  console.log('== 8. 服务端 URL fallback（v0.2.1+1）：主挂切备、恢复切回、不死循环 ==');
  // 可变的 mock health fetch：按基地址前缀决定 /api/health 通不通。
  // state = { primary, fallback }，测试里随时改，模拟主/备恢复与挂掉。
  function mockHealthFetch(state, primary, fallback) {
    return async (url) => {
      const u = String(url);
      if (u.startsWith(primary)) {
        if (state.primary) return { ok: true };
        throw new Error('fetch failed');
      }
      if (u.startsWith(fallback)) {
        if (state.fallback) return { ok: true };
        throw new Error('fetch failed');
      }
      throw new Error(`fetch failed: 未知地址 ${u}`);
    };
  }
  // 既能回 /api/health、又能接 WS（握手后发 hello）的 mock 论坛。
  function startMockForumServer() {
    return new Promise((resolve) => {
      const sockets = new Set();
      const server = http.createServer((req, res) => {
        if (req.url === '/api/health' || req.url.startsWith('/api/health?')) {
          res.setHeader('Content-Type', 'application/json');
          res.end(JSON.stringify({ ok: true, posts: 1, accounts: 1, groups: 1 }));
          return;
        }
        res.writeHead(404); res.end();
      });
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
          data: { seq: 7777, time: new Date().toISOString() },
        }), 'utf8');
        socket.write(Buffer.concat([Buffer.from([0x81, payload.length]), payload]));
      });
      server.listen(0, '127.0.0.1', () => resolve({
        base: `http://127.0.0.1:${server.address().port}`,
        close() {
          for (const s of sockets) { try { s.destroy(); } catch {} }
          server.close();
        },
      }));
    });
  }
  // 8a. probe：通的回 true，挂的回 false；默认主/备地址常量正确。
  {
    assert.strictEqual(PRIMARY_BASE, 'https://forum.coldhao.win', '主地址应为公网域名');
    assert.strictEqual(FALLBACK_BASE, 'http://100.79.244.3:8899', '备地址应为尾网 het');
    const state = { primary: true, fallback: false };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
    });
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:9', '初始应在主地址');
    assert.strictEqual(r.label(), '公网', '主地址标签应为公网');
    assert.strictEqual(await r.probe('http://127.0.0.1:9'), true, '通的基地址 probe 应为真');
    assert.strictEqual(await r.probe('http://127.0.0.1:10'), false, '挂的基地址 probe 应为假');
    r.stop();
    ok('probe：通 true / 挂 false，主备常量正确');
  }
  // 8b. reportFailure：主挂 + 备通 → 切到备地址，发 switch 事件。
  {
    const state = { primary: false, fallback: true };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
    });
    const switches = [];
    r.on('switch', (e) => switches.push(e));
    const to = await r.reportFailure();
    assert.strictEqual(to, 'http://127.0.0.1:10', '应切到备地址');
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:10', '切后 getBase 应为备地址');
    assert.strictEqual(r.isFallback, true, '切后 isFallback 应为真');
    assert.strictEqual(r.label(), '尾网', '备地址标签应为尾网');
    assert.strictEqual(switches.length, 1, '应只发一次 switch');
    assert.strictEqual(switches[0].from, 'http://127.0.0.1:9');
    assert.strictEqual(switches[0].to, 'http://127.0.0.1:10');
    r.stop();
    ok('reportFailure：主挂 + 备通 → 切备地址，发一次 switch');
  }
  // 8c. reportFailure：当前基地址还通 → 不切（只是某条路的问题）。
  {
    const state = { primary: true, fallback: true };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
    });
    let switched = false;
    r.on('switch', () => { switched = true; });
    assert.strictEqual(await r.reportFailure(), null, '当前还通时应返回 null');
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:9', '当前还通时不应切');
    assert.strictEqual(switched, false, '当前还通时不应发 switch');
    r.stop();
    ok('reportFailure：当前还通 → 不切');
  }
  // 8d. reportFailure：两边都挂 → 留在当前基地址，不死循环切来切去。
  {
    const state = { primary: false, fallback: false };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
    });
    let switches = 0;
    r.on('switch', () => { switches += 1; });
    assert.strictEqual(await r.reportFailure(), null, '两边都挂时应返回 null');
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:9', '两边都挂时应留在主地址');
    assert.strictEqual(await r.reportFailure(), null, '再报一次仍应返回 null');
    assert.strictEqual(switches, 0, '两边都挂时不应发 switch');
    r.stop();
    ok('reportFailure：两边都挂 → 留在主地址，不切、不循环');
  }
  // 8e. recheck：切到备地址后主地址恢复 → 60 秒探一次，通了切回。
  {
    const state = { primary: false, fallback: true };
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10'),
      recheckMs: 100,
    });
    const switches = [];
    r.on('switch', (e) => switches.push(e));
    await r.reportFailure();
    assert.strictEqual(r.isFallback, true, '先应切到备地址');
    state.primary = true; // 主地址恢复
    await new Promise((resolve, reject) => {
      const t0 = Date.now();
      const timer = setInterval(() => {
        if (!r.isFallback) { clearInterval(timer); resolve(); }
        else if (Date.now() - t0 > 5000) { clearInterval(timer); reject(new Error('5 秒内未切回主地址')); }
      }, 50);
    });
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:9', '主地址恢复后应切回');
    assert.strictEqual(switches.length, 2, '应共发两次 switch（去备、回主）');
    assert.strictEqual(switches[1].to, 'http://127.0.0.1:9', '第二次 switch 应指回主地址');
    r.stop();
    ok('recheck：备地址上主恢复 → 切回主地址');
  }
  // 8f. RealtimeClient 经备地址连上 WS：主地址是死端口、备地址是 mock
  // 论坛，WS 首次失败应先切基地址再重连 WS（不直接掉到 SSE），
  // 指示器 detail 应带“尾网”。
  {
    const mock = await startMockForumServer();
    try {
      const r = new ServerResolver({ primary: 'http://127.0.0.1:9', fallback: mock.base });
      const states = [];
      const rt = new RealtimeClient('http://127.0.0.1:9', { resolver: r });
      rt.on('transport', (p) => states.push(p));
      const c = await withTimeout(new Promise((resolve, reject) => {
        rt.on('connect', resolve);
        rt.on('disconnect', () => { /* 切地址前的 WS 断开，忽略 */ });
        rt.start();
      }), 15000, 'Realtime 经备地址连 WS');
      assert.strictEqual(c.transport, 'ws', '备地址通时应走 WS（不是 SSE）');
      assert.strictEqual(rt.transport, 'ws');
      assert.strictEqual(r.isFallback, true, '应已切到备地址');
      assert.strictEqual(r.getBase(), mock.base, 'getBase 应为 mock 论坛地址');
      assert.ok(c.seq === 7777, 'hello seq 应为 mock 的 7777');
      const wsState = states.find((p) => p.state === 'ws');
      assert.ok(wsState, 'transport 应报过 ws');
      assert.ok(String(wsState.detail || '').includes('尾网'), `ws 的 detail 应带“尾网”，实际 ${wsState.detail}`);
      rt.stop();
      r.stop();
      ok('RealtimeClient：主挂切备后经 WS 连上，指示器带“尾网”');
    } finally {
      mock.close();
    }
  }
  // 8g. ForumClient 经 resolver：主地址死端口、备地址 mock 论坛，
  // GET /api/health 应自动切备并重试一次成功；HTTP 错误不切地址。
  {
    const mock = await startMockForumServer();
    try {
      const r = new ServerResolver({ primary: 'http://127.0.0.1:9', fallback: mock.base });
      const client2 = new ForumClient('http://127.0.0.1:9', { resolver: r });
      const health = await withTimeout(client2.getHealth(), 15000, 'ForumClient 经备地址 getHealth');
      assert.strictEqual(health.ok, true, '备地址的 /api/health 应回 ok');
      assert.strictEqual(r.isFallback, true, 'GET 失败后应已切到备地址');
      // HTTP 错误不切：备地址回 404 时应抛 ForumApiError，且不切回主地址。
      const before = r.getBase();
      await assert.rejects(
        client2.request('/api/does-not-exist-xyz'),
        (e) => e && e.status === 404,
        '不存在的路径应回 404',
      );
      assert.strictEqual(r.getBase(), before, 'HTTP 404 不应切基地址');
      r.stop();
      ok('ForumClient：GET 失败切备重试成功，HTTP 错误不切地址');
    } finally {
      mock.close();
    }
  }

  // 8h. 并发 reportFailure 只切一次：两路同时报故障，共用同一轮探测，
  // 只发一条 switch（此前实测会切出第二条 from/to 都是备地址的记录）。
  {
    const state = { primary: false, fallback: true };
    let probes = 0;
    const r = new ServerResolver({
      primary: 'http://127.0.0.1:9', fallback: 'http://127.0.0.1:10',
      fetch: async (url) => {
        probes += 1;
        // 探测故意慢一点（20ms），让两路调用撞进同一轮。
        await new Promise((resolve) => setTimeout(resolve, 20));
        return mockHealthFetch(state, 'http://127.0.0.1:9', 'http://127.0.0.1:10')(url);
      },
    });
    const switches = [];
    r.on('switch', (e) => switches.push(e));
    const [a, b] = await Promise.all([r.reportFailure(), r.reportFailure()]);
    assert.strictEqual(a, 'http://127.0.0.1:10', '第一路应切到备地址');
    assert.strictEqual(b, 'http://127.0.0.1:10', '第二路应拿到同一结果');
    assert.strictEqual(r.getBase(), 'http://127.0.0.1:10', '应停在备地址');
    assert.strictEqual(switches.length, 1, '并发两路只应发一条 switch');
    assert.ok(probes <= 2, `并发两路应共用一轮探测，实际探了 ${probes} 次`);
    r.stop();
    ok('并发 reportFailure：两路共用一轮探测，只发一条 switch');
  }

  // 8i. hello 超时直接 reject：TCP 接通但不握手（模拟 SYN 通了、应用层
  // 不回 hello），_connectOnce 应在 ~10 秒 reject，不无限挂住。
  // 这是 lenghao 实测的根因：公网出口抖时 WS 卡在 SYN-SENT，
  // 只靠 close() 等 onclose 会等不到，failover 永远不触发。
  {
    const net = require('node:net');
    const server = net.createServer((socket) => {
      // 接通后什么都不回：不握手、不发 hello、不关。
      socket.on('error', () => {});
    });
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const port = server.address().port;
    try {
      const ws = new WsClient(`http://127.0.0.1:${port}`, {});
      const t0 = Date.now();
      await assert.rejects(
        ws._connectOnce(),
        // 两种都算过：TCP 已通时 close() 会同步触发 onclose（close 1006）；
        // TCP 卡在 SYN-SENT 时 onclose 不触发，靠定时器直接 reject（hello 超时）。
        // 关键是 ~10 秒一定落定，不无限挂住。
        (e) => /hello 超时|WS 连接失败/.test(e && e.message || ''),
        '_connectOnce 挂起时应在 ~10 秒 reject',
      );
      const ms = Date.now() - t0;
      assert.ok(ms >= 9000 && ms < 15000, `hello 超时应在 ~10 秒触发，实际 ${ms}ms`);
      ws.stop();
      ok(`hello 超时 reject：挂起的 _connectOnce 在 ${ms}ms reject，不无限挂住`);
    } finally {
      server.close();
    }
  }
  // 8j. resolver 已预先切到备地址：RealtimeClient 直接走备地址 WS，
  // 不用先失败再切（REST 先报故障的场景）。
  {
    const mock = await startMockForumServer();
    try {
      const r = new ServerResolver({ primary: 'http://127.0.0.1:9', fallback: mock.base });
      await r.reportFailure(); // 先切到备地址
      assert.strictEqual(r.isFallback, true, '应已预先切到备地址');
      const rt = new RealtimeClient('http://127.0.0.1:9', { resolver: r });
      const c = await withTimeout(new Promise((resolve, reject) => {
        rt.on('connect', resolve);
        rt.on('disconnect', reject);
        rt.start();
      }), 15000, '预先切备后 WS 直连');
      assert.strictEqual(c.transport, 'ws', '预先切备后应直接走 WS');
      assert.strictEqual(rt.transport, 'ws');
      rt.stop();
      r.stop();
      ok('预先切备：RealtimeClient 直接走备地址 WS');
    } finally {
      mock.close();
    }
  }

  console.log('== 9. 轻量 asar 自动更新（v0.2.1+1）：版本号比较、sha 校验、失败不换 ==');
  // mock fetch 的最小 Response：{status, ok, json(), arrayBuffer()}。
  function mockResponse({ status = 200, json = null, bytes = null }) {
    return {
      status,
      ok: status >= 200 && status < 300,
      json: async () => {
        if (json === null) throw new Error('no json');
        return json;
      },
      arrayBuffer: async () => {
        const buf = bytes ? Buffer.from(bytes) : Buffer.alloc(0);
        return buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength);
      },
    };
  }
  // 9a. 版本号比较：纯函数，逐段数字比。
  {
    assert.strictEqual(compareVersions('0.2.1', '0.2.0'), 1, '0.2.1 > 0.2.0');
    assert.strictEqual(compareVersions('0.2.0', '0.2.1'), -1, '0.2.0 < 0.2.1');
    assert.strictEqual(compareVersions('0.2.1', '0.2.1'), 0, '相等应为 0');
    assert.strictEqual(compareVersions('1.0.0', '0.9.9'), 1, '1.0.0 > 0.9.9');
    assert.strictEqual(compareVersions('0.10.0', '0.9.0'), 1, '0.10.0 > 0.9.0（数字比，不是字典序）');
    assert.strictEqual(compareVersions('0.2', '0.2.0'), 0, '0.2 与 0.2.0 相等');
    assert.strictEqual(sha256Hex(Buffer.from('abc')), 'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad', 'sha256(abc) 应为已知值');
    ok('版本号比较：逐段数字比，0.10.0 > 0.9.0；sha256(abc) 已知值正确');
  }
  // 9b. checkForUpdates：404（未发布清单）→ 无更新不报错；版本旧 → 有更新；
  // 版本不新 → 已是最新；清单格式错 / 网络错 → check-failed。
  {
    const asarBytes = Buffer.from('fake-asar-bytes-v021');
    const asarSha = sha256Hex(asarBytes);
    const manifest = { version: '0.2.1', asarUrl: 'https://example.invalid/app.asar', sha256: asarSha, notes: 'test' };
    // 404：未发布清单
    {
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: '/tmp/nope',
        fetch: async () => mockResponse({ status: 404 }),
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, false, '404 应无更新');
      assert.strictEqual(info.reason, 'no-manifest', '404 原因应为 no-manifest');
    }
    // 有新版
    {
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: '/tmp/nope',
        fetch: async (url) => {
          assert.ok(String(url).endsWith('/desktop-update.json'), '清单应走服务端基地址');
          return mockResponse({ json: manifest });
        },
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, true, '0.2.1 > 0.2.0 应有更新');
      assert.strictEqual(info.version, '0.2.1');
      assert.strictEqual(info.sha256, asarSha);
    }
    // 已是最新
    {
      const up = new AppUpdater({
        currentVersion: '0.2.1', userDataDir: '/tmp/nope',
        fetch: async () => mockResponse({ json: manifest }),
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, false, '已是 0.2.1 应无更新');
      assert.strictEqual(info.reason, 'up-to-date');
    }
    // 清单格式错
    {
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: '/tmp/nope',
        fetch: async () => mockResponse({ json: { version: '0.2.1' } }),
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, false, '格式错的清单应无更新');
      assert.strictEqual(info.reason, 'check-failed');
    }
    // 网络错
    {
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: '/tmp/nope',
        fetch: async () => { throw new Error('fetch failed'); },
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, false, '网络错应无更新');
      assert.strictEqual(info.reason, 'check-failed');
    }
    ok('checkForUpdates：404 无更新、有新版、已是最新、格式错、网络错五态正确');
  }
  // 9c. downloadAndStage：sha 对上才暂存；sha 不符 / 下载失败不留文件。
  {
    const asarBytes = Buffer.from('fake-asar-bytes-v021');
    const asarSha = sha256Hex(asarBytes);
    const info = { version: '0.2.1', asarUrl: 'https://example.invalid/app.asar', sha256: asarSha };
    // 成功：暂存 asar + pending.json，内容与 sha 一致
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-ok-'));
      try {
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp,
          fetch: async () => mockResponse({ bytes: asarBytes }),
        });
        const r = await up.downloadAndStage(info);
        assert.strictEqual(r.ok, true, 'sha 对上应暂存成功');
        assert.strictEqual(r.bytes, asarBytes.length);
        assert.strictEqual(r.sha256, asarSha);
        assert.ok(fs.existsSync(up.pendingAsar), '暂存 asar 应存在');
        assert.ok(fs.existsSync(up.pendingManifest), 'pending.json 应存在');
        assert.strictEqual(sha256Hex(fs.readFileSync(up.pendingAsar)), asarSha, '暂存文件 sha 应与清单一致');
        const meta = JSON.parse(fs.readFileSync(up.pendingManifest, 'utf8'));
        assert.strictEqual(meta.version, '0.2.1');
        assert.strictEqual(meta.sha256, asarSha);
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    // sha 不符：不暂存
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-badsha-'));
      try {
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp,
          fetch: async () => mockResponse({ bytes: Buffer.from('tampered-bytes') }),
        });
        const r = await up.downloadAndStage(info);
        assert.strictEqual(r.ok, false, 'sha 不符应失败');
        assert.match(r.error, /sha256 不符/, '错误应说明 sha256 不符');
        assert.strictEqual(fs.existsSync(up.pendingAsar), false, 'sha 不符不应留暂存 asar');
        assert.strictEqual(fs.existsSync(up.pendingManifest), false, 'sha 不符不应留 pending.json');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    // 下载 HTTP 失败：不暂存
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-dl-'));
      try {
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp,
          fetch: async () => mockResponse({ status: 500 }),
        });
        const r = await up.downloadAndStage(info);
        assert.strictEqual(r.ok, false, '下载 500 应失败');
        assert.strictEqual(fs.existsSync(up.pendingAsar), false, '下载失败不应留暂存');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    ok('downloadAndStage：sha 对上暂存，sha 不符/下载失败不留文件');
  }
  // 9d. applyPendingUpdate：暂存验过才换，旧 asar 备份为 .bak，暂存清掉；
  // 暂存 sha 坏了不换、旧 asar 不动、坏暂存删掉；开发模式（目录）不换。
  {
    // 换成功
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-apply-'));
      try {
        const appDir = path.join(tmp, 'resources');
        fs.mkdirSync(appDir, { recursive: true });
        const asarPath = path.join(appDir, 'app.asar');
        fs.writeFileSync(asarPath, Buffer.from('old-asar-v020'));
        const newBytes = Buffer.from('new-asar-v021');
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp, appAsarPath: asarPath,
        });
        fs.mkdirSync(up.updatesDir, { recursive: true });
        fs.writeFileSync(up.pendingAsar, newBytes);
        fs.writeFileSync(up.pendingManifest, JSON.stringify({ version: '0.2.1', sha256: sha256Hex(newBytes) }));
        assert.strictEqual(up.applyPendingUpdate(), true, '暂存验过应换上');
        assert.strictEqual(fs.readFileSync(asarPath, 'utf8'), 'new-asar-v021', 'app.asar 应为新内容');
        assert.strictEqual(fs.readFileSync(asarPath + '.bak', 'utf8'), 'old-asar-v020', '旧 asar 应备份为 .bak');
        assert.strictEqual(fs.existsSync(up.pendingAsar), false, '换上后暂存 asar 应清掉');
        assert.strictEqual(fs.existsSync(up.pendingManifest), false, '换上后 pending.json 应清掉');
        assert.strictEqual(up.applyPendingUpdate(), false, '无暂存时再换应为 false');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    // 暂存 sha 坏了：不换，旧 asar 不动，坏暂存删掉
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-applybad-'));
      try {
        const appDir = path.join(tmp, 'resources');
        fs.mkdirSync(appDir, { recursive: true });
        const asarPath = path.join(appDir, 'app.asar');
        fs.writeFileSync(asarPath, Buffer.from('old-asar-v020'));
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp, appAsarPath: asarPath,
        });
        fs.mkdirSync(up.updatesDir, { recursive: true });
        fs.writeFileSync(up.pendingAsar, Buffer.from('tampered'));
        fs.writeFileSync(up.pendingManifest, JSON.stringify({ version: '0.2.1', sha256: sha256Hex(Buffer.from('something-else')) }));
        assert.strictEqual(up.applyPendingUpdate(), false, '暂存 sha 坏了不应换');
        assert.strictEqual(fs.readFileSync(asarPath, 'utf8'), 'old-asar-v020', '旧 asar 应原样不动');
        assert.strictEqual(fs.existsSync(asarPath + '.bak'), false, '没换就不应产生 .bak');
        assert.strictEqual(fs.existsSync(up.pendingAsar), false, '坏暂存应被删掉');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    // 开发模式（appAsarPath 是目录）：不换
    {
      const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-dev-'));
      try {
        const up = new AppUpdater({
          currentVersion: '0.2.0', userDataDir: tmp, appAsarPath: tmp,
        });
        fs.mkdirSync(up.updatesDir, { recursive: true });
        fs.writeFileSync(up.pendingAsar, Buffer.from('x'));
        fs.writeFileSync(up.pendingManifest, JSON.stringify({ version: '0.2.1', sha256: sha256Hex(Buffer.from('x')) }));
        assert.strictEqual(up.applyPendingUpdate(), false, '开发模式（目录）不应换 asar');
      } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    }
    ok('applyPendingUpdate：换成功备 .bak，sha 坏不换、开发模式不换');
  }
  // 9e. 全流程（mock）：查清单 → 有新版 → 下载暂存 → 换上，旧版进 .bak。
  {
    const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-flow-'));
    try {
      const appDir = path.join(tmp, 'resources');
      fs.mkdirSync(appDir, { recursive: true });
      const asarPath = path.join(appDir, 'app.asar');
      fs.writeFileSync(asarPath, Buffer.from('old-asar-v020'));
      const newBytes = Buffer.from('new-asar-v021-full-flow');
      const newSha = sha256Hex(newBytes);
      const manifest = { version: '0.2.1', asarUrl: 'https://example.invalid/app.asar', sha256: newSha };
      const up = new AppUpdater({
        currentVersion: '0.2.0', userDataDir: tmp, appAsarPath: asarPath,
        getBase: () => 'https://forum.coldhao.win',
        fetch: async (url) => {
          if (String(url).endsWith('/desktop-update.json')) return mockResponse({ json: manifest });
          return mockResponse({ bytes: newBytes });
        },
      });
      const info = await up.checkForUpdates();
      assert.strictEqual(info.available, true, '全流程：应发现新版');
      const staged = await up.downloadAndStage(info);
      assert.strictEqual(staged.ok, true, '全流程：应下载暂存成功');
      assert.strictEqual(up.applyPendingUpdate(), true, '全流程：应换上');
      assert.strictEqual(fs.readFileSync(asarPath, 'utf8'), 'new-asar-v021-full-flow', '全流程：app.asar 应为新内容');
      assert.strictEqual(fs.readFileSync(asarPath + '.bak', 'utf8'), 'old-asar-v020', '全流程：旧版应进 .bak');
    } finally { fs.rmSync(tmp, { recursive: true, force: true }); }
    ok('全流程：查清单 → 下载暂存 → 换上，旧版进 .bak');
  }
  // 9f. applyPendingUpdate 走 original-fs 操作 asar（Electron 回归）：
  // lenghao 实测：打补丁的 fs 把 app.asar 当归档，copy/rename 报
  // “ENOENT, not found in .../app.asar”，换 asar 失败。注入一个
  // 计数版 fsOrig，断言碰 asar 文件的操作都走了它。
  {
    const fs2 = require('fs');
    const calls = [];
    const countingFs = {
      existsSync: (p) => { calls.push(['existsSync', p]); return fs2.existsSync(p); },
      copyFileSync: (s, d) => { calls.push(['copyFileSync', s, d]); return fs2.copyFileSync(s, d); },
      renameSync: (s, d) => { calls.push(['renameSync', s, d]); return fs2.renameSync(s, d); },
      unlinkSync: (p) => { calls.push(['unlinkSync', p]); return fs2.unlinkSync(p); },
    };
    const dir = fs2.mkdtempSync(path.join(os.tmpdir(), 'chimera-upd-fsorig-'));
    const appAsar = path.join(dir, 'app.asar');
    fs2.writeFileSync(appAsar, 'old');
    const up = new AppUpdater({
      currentVersion: '0.2.1', userDataDir: dir, appAsarPath: appAsar,
      fsOrig: countingFs,
    });
    fs2.mkdirSync(up.updatesDir, { recursive: true });
    fs2.writeFileSync(up.pendingAsar, 'new');
    fs2.writeFileSync(up.pendingManifest, JSON.stringify({ version: '0.2.2', sha256: sha256Hex(Buffer.from('new')) }));
    const applied = up.applyPendingUpdate();
    assert.strictEqual(applied, true, '应换上');
    // 碰 asar 文件本身的三步（exists、备份 copy、rename）都走了注入的 fsOrig。
    // 任一路径参数以 .asar / .asar.bak 结尾即算碰 asar 文件。
    const asarOps = calls.filter((c) => c.slice(1).some((a) => String(a).endsWith('.asar') || String(a).endsWith('.asar.bak')));
    assert.ok(asarOps.some(([m]) => m === 'existsSync'), 'existsSync(app.asar) 应走 fsOrig');
    assert.ok(asarOps.some(([m, s, d]) => m === 'copyFileSync' && String(d).endsWith('.asar.bak')), '备份 copy 应走 fsOrig');
    assert.ok(asarOps.some(([m, s, d]) => m === 'renameSync' && String(d).endsWith('.asar')), 'rename 到 app.asar 应走 fsOrig');
    assert.strictEqual(fs2.readFileSync(appAsar, 'utf8'), 'new');
    ok('original-fs：换 asar 时碰 asar 文件的操作都走 fsOrig，不走打补丁的 fs');
  }

  console.log('== 10. 自适应翻译 + 未加入标记 + 板块常驻（v0.3.1）==');
  const tr = require('../renderer/translate.js');
  // 10a. 语言检测与散文抽取（纯函数，与网页版同口径）。
  {
    assert.strictEqual(tr.detectLang('你好世界'), 'zh', '纯中文应为 zh');
    assert.strictEqual(tr.detectLang('Hello world, this is English'), 'en', '纯英文应为 en');
    assert.strictEqual(tr.detectLang('こんにちは世界'), 'ja', '含假名应为 ja');
    assert.strictEqual(tr.detectLang('안녕하세요'), 'ko', '含韩文应为 ko');
    assert.strictEqual(tr.detectLang('12345 !!!'), 'unknown', '无字母应为 unknown');
    assert.strictEqual(tr.detectLang(''), 'unknown', '空串应为 unknown');
    assert.strictEqual(tr.proseOf('```js\nconst a = 1;\n```  hello'), 'hello', '代码块应被剥掉只剩散文');
    assert.strictEqual(tr.proseOf('run `npm test` now'), 'run now', '行内 code 应被剥掉');
    assert.strictEqual(tr.proseOf('```\nonly code\n```'), '', '纯代码块散文应为空');
    ok('语言检测 zh/en/ja/ko/unknown 与 proseOf 剥代码块');
  }
  // 10b. 占位生成：同语言/不明/过短不译，异语言才出 trbox。
  {
    tr._resetForTest();
    assert.strictEqual(tr.placeholder('你好世界'), '', '同语言（中文）不应出翻译框');
    assert.strictEqual(tr.placeholder('12345 !!!'), '', '源语言不明不应出翻译框');
    assert.strictEqual(tr.placeholder('a'), '', '散文过短不应出翻译框');
    const html = tr.placeholder('Hello world, this is an English message.');
    assert.ok(html.includes('class="trbox"'), '英文内容应对中文用户出翻译框');
    assert.ok(html.includes('data-trid="at'), '翻译框应带 trid');
    assert.ok(html.includes('翻译中'), '翻译框应带“翻译中”占位文案');
    ok('占位生成：异语言出 trbox，同语言/不明/过短不出');
  }
  // 10c. 队列：批量 10 条、缓存命中不再调接口、节流自动重试、same 删框。
  {
    // 最小假 DOM：只实现队列用到的接口。
    const makeBox = (id) => ({
      _html: '', _removed: false, _attrs: { 'data-trid': id },
      set innerHTML(v) { this._html = v; },
      get innerHTML() { return this._html; },
      getAttribute(k) { return this._attrs[k] || null; },
      removeAttribute(k) { delete this._attrs[k]; },
      remove() { this._removed = true; },
      closest() { return this; },
      querySelector() { return null; },
    });
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    // 12 条英文 → 两批（10+2），译文全部渲染，切换数据齐全。
    tr._resetForTest();
    const calls = [];
    tr.setBatchFn(async (items, target) => {
      calls.push({ n: items.length, target });
      return items.map((it) => ({ translated: `译：${it.text}`, same: false }));
    });
    const boxes = [];
    const root = { querySelectorAll: () => boxes.filter((b) => !b._removed && b.getAttribute('data-trid')) };
    for (let i = 0; i < 12; i++) {
      const html = tr.placeholder(`English message number ${i} for batching test.`);
      assert.ok(html.includes('data-trid'), `第 ${i} 条应出占位`);
      const id = html.match(/data-trid="(at\d+)"/)[1];
      boxes.push(makeBox(id));
    }
    tr.observeTranslations(root);
    await sleep(600); // 250ms flush + 批量往返
    assert.strictEqual(calls.length, 2, `12 条应分 2 批，实际 ${calls.length} 批`);
    assert.deepStrictEqual(calls.map((c) => c.n), [10, 2], '第一批 10 条、第二批 2 条');
    assert.strictEqual(calls[0].target, 'zh', '批量目标应为 zh');
    assert.ok(boxes.every((b) => b._html.includes('译：English message')), '12 条都应渲染出译文');
    assert.ok(boxes.every((b) => b._html.includes('data-tr-orig') && b._html.includes('data-tr-text')), '译文框应带原文/译文切换数据');
    assert.strictEqual(tr._cacheSize(), 12, '缓存应记 12 条');
    ok('队列批量：12 条分 10+2 两批，全部渲染，切换数据齐全');
    // 缓存命中：同一文本再入队，不再调接口。
    const cacheBox = makeBox(tr.placeholder('English message number 0 for batching test.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [cacheBox] });
    await sleep(500);
    assert.strictEqual(calls.length, 2, '缓存命中不应再调接口');
    assert.ok(cacheBox._html.includes('译：English message number 0'), '缓存命中应直接渲染译文');
    ok('缓存命中：同一文本再入队不再调接口，直接渲染');
    // 节流自动重试：第一次 rate_limited，50ms 后自动重试成功。
    tr._resetForTest();
    tr._setRetryDelayMs(50);
    let tries = 0;
    tr.setBatchFn(async (items) => {
      tries += 1;
      if (tries === 1) return items.map(() => ({ error: 'rate_limited' }));
      return items.map((it) => ({ translated: `译：${it.text}`, same: false }));
    });
    const rlBox = makeBox(tr.placeholder('English message for rate limit retry test.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [rlBox] });
    await sleep(900);
    assert.strictEqual(tries, 2, `节流应自动重试一次，实际调了 ${tries} 次`);
    assert.ok(rlBox._html.includes('译：English message for rate limit'), '重试成功后应渲染译文');
    ok('节流自动重试：rate_limited 后自动再试一次并渲染');
    tr._setRetryDelayMs(65000);
    // same：服务端判同语言（回 translated 原文 + same:true），静默删框只留原文。
    tr._resetForTest();
    tr.setBatchFn(async (items) => items.map((it) => ({ translated: it.text, same: true })));
    const sameBox = makeBox(tr.placeholder('English message that server says same.').match(/data-trid="(at\d+)"/)[1]);
    tr.observeTranslations({ querySelectorAll: () => [sameBox] });
    await sleep(500);
    assert.strictEqual(sameBox._removed, true, 'same 时翻译框应被删掉');
    ok('same：服务端判同语言时静默删框');
  }
  // 10d. ForumClient 翻译接线（mock 服务端）：请求体与鉴权头、批量回包解析。
  {
    const mock = await new Promise((resolve) => {
      const seen = [];
      const server = http.createServer((req, res) => {
        let body = '';
        req.on('data', (c) => { body += c; });
        req.on('end', () => {
          seen.push({ method: req.method, url: req.url, auth: req.headers.authorization, body });
          res.setHeader('Content-Type', 'application/json');
          if (req.url === '/api/translate') {
            res.end(JSON.stringify({ ok: true, translated: '译文', cached: false, same: false, src: 'en' }));
          } else if (req.url === '/api/translate/batch') {
            const d = JSON.parse(body);
            res.end(JSON.stringify({ ok: true, results: d.items.map((it) => ({ translated: `译：${it.text}` })) }));
          } else { res.writeHead(404); res.end('{}'); }
        });
      });
      server.listen(0, '127.0.0.1', () => resolve({ server, seen, base: `http://127.0.0.1:${server.address().port}` }));
    });
    try {
      const mc = new ForumClient(mock.base);
      const one = await mc.translate('tok-abc', 'Hello', 'zh');
      assert.strictEqual(one.translated, '译文', '单条翻译应回译文');
      assert.strictEqual(mock.seen[0].auth, 'Bearer tok-abc', '单条翻译应带 Bearer token');
      assert.deepStrictEqual(JSON.parse(mock.seen[0].body), { text: 'Hello', target: 'zh' }, '单条请求体应为 {text, target}');
      const batch = await mc.translateBatch('tok-abc', [{ text: 'a' }, { text: 'b' }], 'zh');
      assert.strictEqual(batch.results.length, 2, '批量应回 2 条结果');
      assert.strictEqual(batch.results[1].translated, '译：b', '批量第二条译文应带原文');
      assert.strictEqual(mock.seen[1].auth, 'Bearer tok-abc', '批量翻译应带 Bearer token');
      ok('ForumClient 翻译接线：单条/批量请求体与鉴权头正确');
    } finally { mock.server.close(); }
  }
  // 10e. 未加入判定 + 板块过滤（抽 renderer.js 实际发布的函数跑）。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const isGroupMember = new Function(`${extractFn('isGroupMember')}; return isGroupMember;`)();
    const g = { id: 'grp-test', members: [{ name: 'coldhao', type: 'human' }, { name: 'xilan', type: 'ai' }] };
    assert.strictEqual(isGroupMember(g, { username: 'coldhao' }), true, '成员应判为已加入');
    assert.strictEqual(isGroupMember(g, { username: 'other' }), false, '非成员应判为未加入');
    assert.strictEqual(isGroupMember(g, null), false, '未登录应判为未加入');
    assert.strictEqual(isGroupMember({ id: 'g2', members: [] }, { username: 'coldhao' }), false, '空成员表应判为未加入');
    assert.strictEqual(isGroupMember({ id: 'g3' }, { username: 'coldhao' }), false, '无 members 字段应判为未加入');
    assert.strictEqual(isGroupMember(g, { username: 'xilan' }), false, '同名 AI 不算人类成员');
    const filterPostsByBoard = new Function(`${extractFn('filterPostsByBoard')}; return filterPostsByBoard;`)();
    const posts = [{ board: 'general' }, { board: 'code' }, { board: 'general' }, {}];
    assert.strictEqual(filterPostsByBoard(posts, null).length, 4, '不过滤应回全部');
    assert.strictEqual(filterPostsByBoard(posts, 'general').length, 2, 'general 应滤出 2 篇');
    assert.strictEqual(filterPostsByBoard(posts, 'code').length, 1, 'code 应滤出 1 篇');
    assert.strictEqual(filterPostsByBoard(posts, 'qa').length, 0, '无帖板块应回空');
    assert.deepStrictEqual(filterPostsByBoard(null, 'general'), [], '帖子为空应回空数组');
    ok('isGroupMember 与 filterPostsByBoard 口径正确');
  }
  // 10f. messageHtml 接翻译：有 ChimeraTranslate 时英文消息带 trbox，没有时只留原文。
  {
    const rendererSrc = fs.readFileSync(path.join(__dirname, '..', 'renderer', 'renderer.js'), 'utf8');
    const extractFn = (name) => {
      const m = rendererSrc.match(new RegExp(`(?:async )?function ${name}\\([\\s\\S]*?\\n\\}`));
      assert.ok(m, `renderer.js 应包含 ${name}`);
      return m[0];
    };
    const escFn = new Function(`${extractFn('esc')}; return esc;`)();
    const avatarClassFn = new Function(`${extractFn('avatarClass')}; return avatarClass;`)();
    const avatarInitialFn = new Function(`${extractFn('avatarInitial')}; return avatarInitial;`)();
    const avatarHtmlFn = new Function('avatarClass', 'avatarInitial', 'esc', `${extractFn('avatarHtml')}; return avatarHtml;`)(avatarClassFn, avatarInitialFn, escFn);
    const fmtTimeFn = new Function(`${extractFn('fmtTime')}; return fmtTime;`)();
    const messageHtmlFn = new Function('esc', 'state', 'avatarHtml', 'fmtTime', `${extractFn('messageHtml')}; return messageHtml;`)(escFn, { me: null }, avatarHtmlFn, fmtTimeFn);
    const msg = { id: 'm1', author: 'alice', ts: new Date().toISOString(), body: 'Hello everyone, this is an English message.' };
    const plain = messageHtmlFn(msg);
    assert.ok(!plain.includes('trbox'), '无翻译模块时消息不应带翻译框');
    assert.ok(plain.includes('Hello everyone'), '无翻译模块时原文应在');
    tr._resetForTest();
    globalThis.window = { ChimeraTranslate: tr };
    try {
      const withTr = messageHtmlFn(msg);
      assert.ok(withTr.includes('class="trbox"'), '有翻译模块时英文消息应带翻译框');
      assert.ok(withTr.includes('Hello everyone'), '原文仍应在气泡里');
      const zhMsg = { id: 'm2', author: 'bob', ts: new Date().toISOString(), body: '大家好，这是一条中文消息。' };
      assert.ok(!messageHtmlFn(zhMsg).includes('trbox'), '中文消息对中文用户不应带翻译框');
    } finally { delete globalThis.window; }
    ok('messageHtml 接翻译：英文带 trbox、中文不带、无模块只留原文');
  }
  // 10g. live 板块 / 社区 / 翻译（只读；上游翻译失败时记注不判败）。
  {
    const boards = await withTimeout(client.getBoards(), 15000, 'GET /api/boards');
    assert.strictEqual(boards.ok, true, 'GET /api/boards 应回 ok');
    assert.ok(Array.isArray(boards.boards) && boards.boards.length > 0, '板块列表应非空');
    const ids = boards.boards.map((b) => b.id);
    assert.ok(ids.includes('general') && ids.includes('code'), '板块应含 general 与 code');
    ok(`live GET /api/boards：${boards.boards.length} 个板块`);
    const comms = await withTimeout(client.getCommunities(), 15000, 'GET /api/communities');
    assert.strictEqual(comms.ok, true, 'GET /api/communities 应回 ok');
    const eterna = (comms.communities || []).find((c) => c.name === 'eternatide');
    assert.ok(eterna, '社区列表应有 eternatide');
    assert.strictEqual(eterna.memberCount, 5, 'eternatide 成员应为 5 人');
    ok(`live GET /api/communities：eternatide 成员 ${eterna.memberCount} 人、下挂 ${eterna.groupCount} 群`);
    // 账本未登录应 401（成员 only 的边界）。
    await assert.rejects(
      client.getCommunityLedger(null, eterna.id),
      (e) => e && e.status === 401,
      '未登录取账本应回 401',
    );
    ok('live GET /api/communities/:id/ledger：未登录回 401（成员 only）');
    // 翻译 live：上游（MyMemory/Google）抖动时 429/502 只记注，不判败。
    try {
      const live = await withTimeout(client.translate(null, 'Hello, this is a test message for translation.', 'zh'), 20000, 'POST /api/translate');
      assert.strictEqual(live.ok, true, 'POST /api/translate 应回 ok');
      assert.ok(typeof live.translated === 'string' && live.translated.length > 0, '应回非空译文');
      ok(`live POST /api/translate：en→zh 译文“${live.translated.slice(0, 20)}…”`);
    } catch (err) {
      if (err && (err.status === 429 || err.status === 502)) {
        console.log('  注：上游翻译暂不可用（' + err.status + '），live 翻译未验');
      } else throw err;
    }
  }

  console.log(`\nPASS: ${passed} 项全过`);
}

main().catch((err) => {
  console.error(`\nFAIL: ${err.message}`);
  process.exit(1);
});
