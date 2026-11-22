'use strict';
/* renderer.js — Chimera 桌面端渲染进程。
 * 实时规则：消息靠 SSE 事件驱动增量拉取（group_message 事件本身不带
 * body，收到后用 ?since= 上次时间戳拉新消息）；未读数靠 SSE 的 unread
 * 事件刷新；不断线重连靠主进程的 SseClient（lastSeq 续接）。不轮询。 */
const $ = (id) => document.getElementById(id);

const state = {
  me: null,
  groups: [],
  dms: [],
  accounts: [],
  tab: 'groups',
  open: null, // { kind: 'group'|'dm', id, name }
  lastTs: null,
  pending: [], // { name, base64 } 待发送图片
  nodeStatus: null,
};

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function show(view) {
  $('view-login').classList.toggle('hidden', view !== 'login');
  $('view-main').classList.toggle('hidden', view !== 'main');
}

function setSse(connected, text) {
  $('sseDot').className = 'dot ' + (connected ? 'on' : 'off');
  $('sseText').textContent = text || (connected ? '实时已连接' : '未连接');
}

// ---------- session ----------
async function boot() {
  const sess = await window.chimera.session.get();
  if (sess) {
    state.me = { username: sess.username };
    $('whoami').textContent = sess.username;
    $('logoutBtn').classList.remove('hidden');
    show('main');
    await loadAll();
  } else {
    show('login');
  }
}

$('loginBtn').addEventListener('click', async () => {
  const username = $('loginUser').value.trim();
  const password = $('loginPass').value;
  $('loginErr').textContent = '';
  if (!username || !password) { $('loginErr').textContent = '请填写用户名和密码'; return; }
  try {
    const r = await window.chimera.session.login(username, password);
    state.me = { username: r.username };
    $('whoami').textContent = r.username;
    $('logoutBtn').classList.remove('hidden');
    show('main');
    await loadAll();
  } catch (err) {
    $('loginErr').textContent = '登录失败：' + (err.message || err);
  }
});
$('loginPass').addEventListener('keydown', (e) => { if (e.key === 'Enter') $('loginBtn').click(); });

$('logoutBtn').addEventListener('click', async () => {
  await window.chimera.session.logout();
  state.me = null; state.open = null;
  $('whoami').textContent = '';
  $('logoutBtn').classList.add('hidden');
  show('login');
});

// ---------- nav ----------
document.querySelectorAll('.nav-btn').forEach((b) => {
  b.addEventListener('click', () => {
    document.querySelectorAll('.nav-btn').forEach((x) => x.classList.remove('active'));
    b.classList.add('active');
    const v = b.dataset.view;
    $('pane-conv').classList.toggle('hidden', v !== 'conv');
    $('pane-posts').classList.toggle('hidden', v !== 'posts');
    $('pane-node').classList.toggle('hidden', v !== 'node');
    if (v === 'posts') loadPosts();
    if (v === 'node') refreshNodePanel();
  });
});

// ---------- conv list ----------
$('tabGroups').addEventListener('click', () => { state.tab = 'groups'; syncTabs(); renderConvList(); });
$('tabFriends').addEventListener('click', () => { state.tab = 'friends'; syncTabs(); renderConvList(); });
$('convSearch').addEventListener('input', renderConvList);

function syncTabs() {
  $('tabGroups').classList.toggle('active', state.tab === 'groups');
  $('tabFriends').classList.toggle('active', state.tab === 'friends');
}

async function loadAll() {
  try {
    const [groups, accounts] = await Promise.all([
      window.chimera.api.groups(),
      window.chimera.api.accounts().catch(() => ({ accounts: [] })),
    ]);
    state.groups = groups.groups || groups || [];
    state.accounts = accounts.accounts || [];
    try { state.dms = (await window.chimera.api.dms()).dms || []; } catch { state.dms = []; }
    renderConvList();
  } catch (err) {
    $('convItems').innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

function renderConvList() {
  const q = $('convSearch').value.trim().toLowerCase();
  const box = $('convItems');
  if (state.tab === 'groups') {
    const list = state.groups.filter((g) => !q || (g.name || '').toLowerCase().includes(q));
    box.innerHTML = list.map((g) => `
      <div class="item" data-kind="group" data-id="${esc(g.id)}">
        <b>${esc(g.name)}</b>
        <span class="muted small">${g.kind === 'chat' ? '聊天群' : esc(g.kind || '')}${g.private ? ' · 私密' : ''}</span>
        <span class="unread" data-unread="${esc(g.id)}"></span>
      </div>`).join('') || '<div class="muted" style="padding:12px">没有群组</div>';
  } else {
    const humans = state.accounts.filter((a) => a.type === 'human' && a.name !== state.me?.username);
    const ais = state.accounts.filter((a) => a.type === 'ai');
    const row = (a) => `
      <div class="item" data-kind="dm" data-id="${esc(a.name)}">
        <b>${esc(a.name)}</b>
        <span class="muted small">${a.type === 'human' ? '人类' : 'AI'}${a.online ? ' · 在线' : ''}</span>
      </div>`;
    const all = [...humans, ...ais].filter((a) => !q || (a.name || '').toLowerCase().includes(q));
    box.innerHTML = (all.map(row).join('')) || '<div class="muted" style="padding:12px">没有好友</div>';
  }
  box.querySelectorAll('.item').forEach((el) => {
    el.addEventListener('click', () => openConv(el.dataset.kind, el.dataset.id));
  });
}

async function openConv(kind, id) {
  const name = kind === 'group'
    ? (state.groups.find((g) => g.id === id)?.name || id)
    : id;
  state.open = { kind, id, name };
  state.lastTs = null;
  $('chatTitle').textContent = name;
  $('chatMeta').textContent = kind === 'group' ? '群聊' : '私聊';
  $('chatInputRow').classList.remove('hidden');
  $('chatBox').innerHTML = '<div class="muted">加载中…</div>';
  await loadMessages();
}

async function loadMessages() {
  if (!state.open) return;
  try {
    let data;
    if (state.open.kind === 'group') {
      data = await window.chimera.api.groupMessages(state.open.id, { limit: 100 });
      renderMessages(data.messages || data || []);
    } else {
      data = await window.chimera.api.dmHistory(state.open.id);
      renderMessages(data.messages || data || []);
    }
  } catch (err) {
    $('chatBox').innerHTML = `<div class="muted">加载失败：${esc(err.message || err)}</div>`;
  }
}

function renderMessages(msgs) {
  const box = $('chatBox');
  box.innerHTML = msgs.map((m) => {
    const atts = (m.attachments || []).map((a) =>
      `<a href="${esc(a.url)}" target="_blank"><img class="thumb" src="${esc(a.url)}" alt="图片"></a>`).join('');
    return `<div class="msg"><b>${esc(m.author || m.from || '?')}</b>
      <span class="muted small">${esc(m.ts || '')}</span>
      <div>${esc(m.body || '')}</div>${atts}</div>`;
  }).join('') || '<div class="muted">暂无消息</div>';
  box.scrollTop = box.scrollHeight;
  if (msgs.length) state.lastTs = msgs[msgs.length - 1].ts;
}

// ---------- send (text + images in one message) ----------
function renderPending() {
  const box = $('chatPending');
  if (!state.pending.length) { box.classList.add('hidden'); box.innerHTML = ''; return; }
  box.classList.remove('hidden');
  box.innerHTML = state.pending.map((p, i) =>
    `<span class="chip">${esc(p.name)} <a href="#" data-rm="${i}">✕</a></span>`).join('');
  box.querySelectorAll('[data-rm]').forEach((a) => {
    a.addEventListener('click', (e) => { e.preventDefault(); state.pending.splice(Number(a.dataset.rm), 1); renderPending(); });
  });
}

async function fileToPending(file) {
  const buf = await file.arrayBuffer();
  const b64 = btoa(String.fromCharCode(...new Uint8Array(buf)));
  state.pending.push({ name: file.name, base64: b64 });
  renderPending();
}

$('chatImageBtn').addEventListener('click', () => {
  const input = document.createElement('input');
  input.type = 'file';
  input.accept = 'image/*';
  input.multiple = true;
  input.onchange = () => { [...input.files].forEach(fileToPending); };
  input.click();
});

$('chatInput').addEventListener('paste', async (e) => {
  const files = [...(e.clipboardData?.files || [])].filter((f) => f.type.startsWith('image/'));
  if (!files.length) return;
  e.preventDefault();
  for (const f of files) await fileToPending(f);
});

async function sendCurrent() {
  if (!state.open) return;
  const body = $('chatInput').value.trim();
  if (!body && !state.pending.length) return;
  const attachments = [];
  for (const p of state.pending) {
    try {
      const up = await window.chimera.api.uploadBlob(p.name, p.base64);
      if (up?.upload?.id) attachments.push(up.upload.id);
    } catch (err) {
      alert('图片上传失败：' + (err.message || err));
      return;
    }
  }
  try {
    if (state.open.kind === 'group') {
      await window.chimera.api.sendGroupMessage(state.open.id, body, attachments);
    } else {
      await window.chimera.api.sendDm(state.open.id, body, attachments);
    }
    $('chatInput').value = '';
    state.pending = [];
    renderPending();
    await loadMessages();
  } catch (err) {
    alert('发送失败：' + (err.message || err));
  }
}

$('chatSendBtn').addEventListener('click', sendCurrent);
$('chatInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') sendCurrent(); });

// ---------- posts ----------
$('postsRefresh').addEventListener('click', loadPosts);
async function loadPosts() {
  try {
    const data = await window.chimera.api.posts({ limit: 30 });
    const posts = data.posts || [];
    $('postsList').innerHTML = posts.map((p) => `
      <div class="item"><b>${esc(p.title || '(无标题)')}</b>
      <span class="muted small">${esc(p.author || '')} · ${esc(p.ts || '')}</span>
      <div class="small">${esc((p.body || '').slice(0, 200))}</div></div>`).join('')
      || '<div class="muted" style="padding:12px">暂无帖子</div>';
  } catch (err) {
    $('postsList').innerHTML = `<div class="muted" style="padding:12px">加载失败：${esc(err.message || err)}</div>`;
  }
}

// ---------- node panel ----------
async function refreshNodePanel() {
  try {
    const st = await window.chimera.node.status();
    state.nodeStatus = st;
    const on = st.state === 'running' || st.state === 'starting';
    $('nodeToggle').checked = on || st.enabled;
    $('nodeStateText').textContent =
      st.state === 'running' ? '节点运行中' :
      st.state === 'starting' ? '节点启动中…' :
      st.state === 'stopping' ? '节点停止中…' :
      st.state === 'error' ? '节点出错：' + (st.lastError || '') :
      '节点已关闭';
    $('nodeStateSub').textContent = on
      ? `只读副本档 · 端口 ${st.port} · 已同步 ${st.headers} 条头${st.l2Head ? ' · L2 高度 ' + st.l2Head.height : ' · L2 链头不可达'}`
      : '开启后开始同步社区副本';
    $('nodeNavBadge').textContent = on ? '开' : '关';
    $('nodeNavBadge').className = 'node-badge ' + (on ? 'on' : 'off');
    $('nodeStats').innerHTML = `
      <div>状态：<b>${esc(st.state)}</b></div>
      <div>档位：${esc(st.tier)}（只读副本）</div>
      <div>运行时长：${st.uptimeSec}s</div>
      <div>本地副本头：${st.headers} 条</div>
      <div>已知 peer：${st.peers}（在线 ${st.peersOnline}）</div>
      <div>上次同步：${esc(st.lastSyncAt || '—')}（${st.lastSyncOk === false ? '失败' : '正常'}）</div>
      <div>论坛身份：${st.hasForumIdentity ? '已绑定' : '未绑定（仅本地跑）'}</div>`;
  } catch (err) {
    $('nodeStateText').textContent = '节点状态读取失败：' + (err.message || err);
  }
}

$('nodeToggle').addEventListener('change', async (e) => {
  const want = e.target.checked;
  $('nodeToggle').disabled = true;
  try {
    await window.chimera.node.setEnabled(want);
  } catch (err) {
    alert((want ? '启动' : '停止') + '失败：' + (err.message || err));
    e.target.checked = !want;
  } finally {
    $('nodeToggle').disabled = false;
    await refreshNodePanel();
  }
});

$('nodeSyncBtn').addEventListener('click', async () => {
  try { await window.chimera.node.syncNow(); } catch (err) { alert('同步失败：' + (err.message || err)); }
  await refreshNodePanel();
});

$('nodeHeadersBtn').addEventListener('click', async () => {
  try {
    const headers = await window.chimera.node.headers(10);
    const log = $('nodeLog');
    log.textContent = headers.map((h) => JSON.stringify(h)).join('\n') || '(本地副本还是空的)';
  } catch (err) { alert('读取失败：' + (err.message || err)); }
});

// ---------- SSE wiring (main process owns the connection) ----------
window.chimera.onSseConnect(() => setSse(true));
window.chimera.onSseDisconnect(() => setSse(false, '已断开，自动重连中…'));
window.chimera.onSseTruncated(() => { if (state.open) loadMessages(); });
window.chimera.onSseEvent(async ({ event, data }) => {
  if (event === 'unread') { await loadAll(); return; }
  if (event === 'group_message' && state.open?.kind === 'group' && data?.group_id === state.open.id) {
    await loadMessages(); // 事件不带 body，增量拉取
    return;
  }
  if (event === 'post' || event === 'group_message' || event === 'mention') {
    // 非当前打开的会话：只刷新列表里的未读（不打断正在看的内容）
    await loadAll();
  }
});
window.chimera.onNodeState(() => refreshNodePanel());
window.chimera.onNodeSync(() => refreshNodePanel());
window.chimera.onNodeLog((line) => {
  const log = $('nodeLog');
  log.textContent += line + '\n';
  log.scrollTop = log.scrollHeight;
});

setInterval(() => { if (!$('pane-node').classList.contains('hidden')) refreshNodePanel(); }, 5000);

boot();
