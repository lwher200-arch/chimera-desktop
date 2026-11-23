'use strict';
/* renderer/translate.js — 自适应翻译（桌面端，v0.3.1）。
 * 与网页版 public/translate-auto.js 同口径：语言检测 zh/ja/ko/en/unknown，
 * 代码块不翻，只译散文；批量每批 10 条、最多 2 批并行；429 自动延迟重试
 * （65 秒，最多 2 次），之后留重试按钮；源语言不明不硬译；同语言不译。
 * 目标语言默认中文，存 localStorage 'chimera_lang'（zh|en）。
 * 本文件是浏览器脚本（index.html 在 renderer.js 之前引入），同时以
 * module.exports 导出纯逻辑供 Node 测试 require（见 test/smoke.js §10）。
 * 批量翻译走注入的 batchFn(items, target) —— 渲染进程里接
 * window.chimera.api.translateBatch（主进程 IPC → /api/translate/batch）。
 */
(function (root) {
  var BATCH_MAX = 10; // 网页版 2026-11-22：20 条一批要等最慢的上游，改小后首批更快出
  var MAX_IN_FLIGHT = 2; // 最多两批并行，首屏多批不再严格串行等
  var FLUSH_MS = 250;
  var QUEUE_TRIGGER = 10;
  var RETRY_DELAY_MS = 65000; // 节流自动重试的延迟：等服务端 60 秒节流窗口过去
  var MAX_AUTO_RETRY = 2; // 节流自动重试最多次数，之后留重试按钮

  var cache = new Map(); // (text + '\n' + target) -> translated
  var texts = {}; // id -> prose（渲染成功或删框后即删；重试失败的保留）
  var retries = {}; // id -> 节流自动重试次数
  var seq = 0;
  var queue = []; // {box, id}
  var queuedIds = {};
  var flushTimer = null;
  var inFlight = 0;
  var batchFn = null; // (items, target) => Promise<results[]>
  var clickBound = false;

  function viewerLang() {
    try {
      var saved = (typeof localStorage !== 'undefined') ? localStorage.getItem('chimera_lang') : null;
      if (saved === 'zh' || saved === 'en') return saved;
    } catch (e) { /* localStorage 不可用时回默认 */ }
    return 'zh';
  }

  function setViewerLang(lang) {
    if (lang !== 'zh' && lang !== 'en') return;
    try { localStorage.setItem('chimera_lang', lang); } catch (e) { /* 存不上就只当次生效 */ }
  }

  var STRINGS = {
    zh: { label: '翻译', loading: '翻译中…', showOriginal: '显示原文', showTranslation: '显示翻译' },
    en: { label: 'Translation', loading: 'Translating…', showOriginal: 'Show original', showTranslation: 'Show translation' },
  };
  function tr(key) {
    var lang = viewerLang();
    return (STRINGS[lang] && STRINGS[lang][key]) || STRINGS.zh[key] || key;
  }

  function escHtml(s) {
    return String(s ?? '').replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  // 语言检测：与服务端 detectTextLang 同口径（zh/ja/ko/en/unknown），保持同步。
  // kana 任一→ja；hangul 任一→ko；CJK 占比≥50%→zh；Latin 占比≥70%→en；其余→unknown。
  function detectLang(text) {
    var s = String(text || '');
    var kana = (s.match(/[\u3040-\u309f\u30a0-\u30ff]/g) || []).length;
    var hangul = (s.match(/[\uac00-\ud7a3]/g) || []).length;
    var cjk = (s.match(/[\u4e00-\u9fa5]/g) || []).length;
    var latin = (s.match(/[A-Za-z]/g) || []).length;
    var total = kana + hangul + cjk + latin;
    if (total === 0) return 'unknown';
    if (kana > 0) return 'ja';
    if (hangul > 0) return 'ko';
    if (cjk * 2 >= total) return 'zh';
    if (latin * 10 >= total * 7) return 'en';
    return 'unknown';
  }

  // 只翻代码块之外的散文，代码/行内 code 不送翻译（与网页版 proseOf 同口径）。
  function proseOf(text) {
    return String(text || '')
      .replace(/```[^\n`]*\n[\s\S]*?(?:```|$)/g, ' ')
      .replace(/`[^`\n]+`/g, ' ')
      .replace(/\s+/g, ' ').trim();
  }

  // 生成翻译占位 HTML。散文过短、与观看者同语言、或源语言无法判定时返回 ''（不译）。
  function placeholder(text) {
    var prose = proseOf(text);
    if (!prose || prose.length < 2) return '';
    var src = detectLang(prose);
    if (src === 'unknown' || src === viewerLang()) return '';
    var id = 'at' + (++seq);
    texts[id] = prose;
    return '<div class="trbox" data-trid="' + id + '"><span class="tr-label">' + escHtml(tr('label')) +
      '</span> <span class="tr-loading muted">' + escHtml(tr('loading')) + '</span></div>';
  }

  function setBatchFn(fn) { batchFn = fn; }

  function renderTranslated(box, text, out) {
    box.innerHTML = '<span class="tr-label">' + escHtml(tr('label')) + '</span> <span class="tr-text">' +
      escHtml(out) + '</span>' +
      ' <button class="tr-toggle" type="button" data-tr-orig="' + escHtml(encodeURIComponent(text)) +
      '" data-tr-text="' + escHtml(encodeURIComponent(out)) + '">' + escHtml(tr('showOriginal')) + '</button>';
  }

  // 429 / 上游失败：保留原文，留一个重试按钮（文本保留在 texts 里供重试用）。
  function renderRetry(box, id) {
    box.innerHTML = '<span class="tr-label">' + escHtml(tr('label')) + '</span> ' +
      '<button class="tr-toggle" type="button" data-tr-retry="' + escHtml(id) + '">' +
      escHtml(tr('showTranslation')) + '</button>';
  }

  // 切换按钮与重试按钮都走这一处委托（渲染进程启动时 init() 挂一次）。
  function handleClick(e) {
    var btn = e.target && e.target.closest ? e.target.closest('.tr-toggle') : null;
    if (!btn) return;
    var retryId = btn.getAttribute('data-tr-retry');
    if (retryId) {
      var box = btn.closest('.trbox');
      if (!box || !texts[retryId]) return;
      btn.disabled = true;
      btn.textContent = tr('loading');
      enqueue(box, retryId, true);
      return;
    }
    var orig = btn.getAttribute('data-tr-orig');
    var text = btn.getAttribute('data-tr-text');
    if (orig === null || text === null) return;
    var boxEl = btn.closest('.trbox');
    if (!boxEl) return;
    var span = boxEl.querySelector('.tr-text');
    if (!span) return;
    var showingOrig = btn.dataset.showing === '1';
    span.textContent = showingOrig ? decodeURIComponent(text) : decodeURIComponent(orig);
    btn.textContent = showingOrig ? tr('showOriginal') : tr('showTranslation');
    btn.dataset.showing = showingOrig ? '' : '1';
  }

  function init() {
    if (clickBound) return;
    if (typeof document !== 'undefined' && document.addEventListener) {
      document.addEventListener('click', handleClick);
      clickBound = true;
    }
  }

  function enqueue(box, id, immediate) {
    if (queuedIds[id] || !texts[id]) return;
    queuedIds[id] = true;
    queue.push({ box: box, id: id });
    if (immediate || queue.length >= QUEUE_TRIGGER) flush();
    else if (!flushTimer) flushTimer = setTimeout(flush, FLUSH_MS);
  }

  function flush() {
    if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
    var batch = queue.splice(0, BATCH_MAX);
    if (!batch.length) return;
    var target = viewerLang();
    var todo = [];
    batch.forEach(function (job) {
      var text = texts[job.id];
      if (!text) { delete queuedIds[job.id]; return; }
      var hit = cache.get(text + '\n' + target);
      if (hit !== undefined) {
        renderTranslated(job.box, text, hit);
        delete queuedIds[job.id];
        delete texts[job.id];
      } else todo.push(job);
    });
    if (!todo.length) { if (queue.length) flush(); return; }
    if (inFlight >= MAX_IN_FLIGHT) {
      // 并行已满：把未译项放回队首，等一批回来再说（缓存命中的上面已渲染）。
      queue.unshift.apply(queue, todo);
      return;
    }
    if (typeof batchFn !== 'function') {
      // 未接批量接口（测试里不设、或主进程未就绪）：留重试按钮，不丢文本。
      todo.forEach(function (job) {
        delete queuedIds[job.id];
        renderRetry(job.box, job.id);
      });
      if (queue.length) flush();
      return;
    }
    inFlight++;
    var promise;
    try {
      promise = batchFn(todo.map(function (job) { return { text: texts[job.id] }; }), target);
    } catch (err) {
      inFlight--;
      todo.forEach(function (job) {
        delete queuedIds[job.id];
        renderRetry(job.box, job.id);
      });
      if (queue.length) flush();
      return;
    }
    Promise.resolve(promise).then(function (res) {
      inFlight--;
      var results = Array.isArray(res) ? res : [];
      todo.forEach(function (job, i) {
        var r = results[i] || { error: 'translation failed' };
        var text = texts[job.id];
        delete queuedIds[job.id];
        if (r.translated) {
          delete retries[job.id];
          if (r.same) {
            // 同语言：不需要翻译框，静默只显示原文
            if (job.box.remove) job.box.remove();
            delete texts[job.id];
          } else if (r.translated === text) {
            // 服务端漏网的回显：不渲染、不缓存，走重试
            renderRetry(job.box, job.id);
          } else {
            cache.set(text + '\n' + target, r.translated);
            renderTranslated(job.box, text, r.translated);
            delete texts[job.id];
          }
        } else if (r.error === 'source language unclear' || r.error === 'text must be 1..5000 chars') {
          delete retries[job.id];
          if (job.box.remove) job.box.remove(); // 无法判定/不合法：静默只显示原文
          delete texts[job.id];
        } else if (r.error === 'rate_limited') {
          // 节流：自动延迟重试，不用用户点按钮；重试最多 2 次，之后留重试按钮。
          var n = (retries[job.id] || 0) + 1;
          retries[job.id] = n;
          if (n <= MAX_AUTO_RETRY) {
            (function (box, id) {
              setTimeout(function () {
                if (!texts[id]) return; // 已渲染/已删框：不用再试
                if (typeof document !== 'undefined' && document.contains && !document.contains(box)) return;
                enqueue(box, id, true);
              }, RETRY_DELAY_MS);
            })(job.box, job.id);
          } else {
            delete retries[job.id];
            renderRetry(job.box, job.id);
          }
        } else {
          renderRetry(job.box, job.id); // 429 / 上游失败：留原文+重试按钮，文本保留
        }
      });
      if (queue.length) flush();
    }).catch(function () {
      inFlight--;
      todo.forEach(function (job) {
        delete queuedIds[job.id];
        renderRetry(job.box, job.id); // 网络失败同样留重试按钮，文本保留
      });
      if (queue.length) flush();
    });
  }

  // 入口：接管 root 下所有未入队的翻译占位；渲染出来就立即入队，不等视口。
  function observeTranslations(rootEl) {
    var scope = rootEl || (typeof document !== 'undefined' ? document : null);
    if (!scope || !scope.querySelectorAll) return;
    var boxes = scope.querySelectorAll('[data-trid]');
    boxes.forEach(function (box) {
      var id = box.getAttribute('data-trid');
      if (!id || !texts[id] || queuedIds[id]) return;
      box.removeAttribute('data-trid');
      enqueue(box, id, false);
    });
  }

  var api = {
    viewerLang: viewerLang,
    setViewerLang: setViewerLang,
    detectLang: detectLang,
    proseOf: proseOf,
    placeholder: placeholder,
    observeTranslations: observeTranslations,
    setBatchFn: setBatchFn,
    init: init,
    _cacheSize: function () { return cache.size; },
    _setRetryDelayMs: function (ms) { if (typeof ms === 'number' && ms > 0) RETRY_DELAY_MS = ms; },
    _resetForTest: function () {
      cache.clear(); texts = {}; retries = {}; queue = []; queuedIds = {};
      seq = 0; inFlight = 0; batchFn = null;
      if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
    },
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.ChimeraTranslate = api;
  if (typeof root !== 'undefined' && root) root.ChimeraTranslate = api;
})(typeof window !== 'undefined' ? window : (typeof globalThis !== 'undefined' ? globalThis : null));
