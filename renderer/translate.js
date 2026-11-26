'use strict';
/* renderer/translate.js — 自适应翻译（桌面端，v0.3.1 内容翻译；v0.3.7 界面文案）。
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
  var TRANSLATE_CACHE_MAX = 500; // 翻译缓存上限（v0.3.13）：LRU 淘汰，防长会话内存无界增长
  // LRU 写入：已在的先删（刷新新近度），满了先踢最旧的（Map 按插入序排）。
  function cacheSet(key, value) {
    if (cache.has(key)) cache.delete(key);
    while (cache.size >= TRANSLATE_CACHE_MAX) {
      var oldest = cache.keys().next().value;
      if (oldest === undefined) break;
      cache.delete(oldest);
    }
    cache.set(key, value);
  }
  var texts = {}; // id -> prose（渲染成功或删框后即删；重试失败的保留）
  var retries = {}; // id -> 节流自动重试次数
  var seq = 0;
  var queue = []; // {box, id}
  var queuedIds = {};
  var flushTimer = null;
  var inFlight = 0;
  var batchFn = null; // (items, target) => Promise<results[]>
  var clickBound = false;

  // ---------- 视口内翻译入队（v0.3.14） ----------
  // 打开长历史时，旧做法把 up to 100 个占位瞬间全入队，首屏翻译与可见内容
  // 抢带宽。改成只入队视口内的占位：IntersectionObserver 看到才入队，
  // 滚动到才入队；不支持 IO 的环境（Node 测试）回落为立即入队。
  // 名字类（observeNameTranslations）量少、不走这里，仍立即入队。
  var viewportObserver = null; // IntersectionObserver 单例
  var viewportObserved = {}; // id -> box（已观察、未入队）
  function getViewportObserver() {
    if (viewportObserver) return viewportObserver;
    if (typeof IntersectionObserver === 'undefined') return null;
    viewportObserver = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (!entry.isIntersecting) return;
        var box = entry.target;
        var id = box && box.getAttribute ? box.getAttribute('data-trid') : null;
        if (!id || !viewportObserved[id]) return;
        if (viewportObserver) viewportObserver.unobserve(box);
        delete viewportObserved[id];
        if (box.removeAttribute) box.removeAttribute('data-trid');
        enqueue(box, id, false);
      });
    });
    return viewportObserver;
  }
  // 扫掉已脱离文档的待观察占位（列表重建/语言切换后旧节点已不在文档里，
  // 它的 texts 残留要清掉，免得越积越多）。隐藏面板里的占位仍在文档里，
  // 不扫；切回来显示时观察器自会触发。
  function sweepViewportObserved() {
    if (typeof document === 'undefined' || !document.contains) return;
    Object.keys(viewportObserved).forEach(function (id) {
      var box = viewportObserved[id];
      if (box && !document.contains(box)) {
        if (viewportObserver) viewportObserver.unobserve(box);
        delete viewportObserved[id];
        delete texts[id];
      }
    });
  }

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

  // ---------- 界面文案自适应翻译（v0.3.7） ----------
  // 与内容翻译共用 viewerLang（localStorage 'chimera_lang'）：切换按钮既是
  // 内容翻译目标，也是界面语言，不另设开关。
  // index.html 的静态 chrome 走 [data-i18n]（文本）/[data-i18n-ph]（placeholder）/
  // [data-i18n-title]（title），由 applyUiLang 一次刷完；renderer.js 里 JS 渲染的
  // 文案走 ui(key) / uiFmt(key, ...args)（%s 顺序代入）。
  // 数据内容（帖子/消息/群名/版块名/人名/服务端原话）不进这张表。
  var UI_STRINGS = {
    zh: {
      'top.brandSub': '桌面端 · 人 + AI 的混合社区',
      'top.searchPh': '搜索帖子与消息…',
      'top.searchTitle': '回车搜索全站',
      'top.langZh': '翻译：中文',
      'top.langEn': 'Translate: EN',
      'top.langTitleZh': '点按切换翻译目标为英文',
      'top.langTitleEn': '点按切换翻译目标为中文',
      'top.themeDark': '🌙 深色',
      'top.themeLight': '☀️ 浅色',
      'top.themeTitleDark': '点按切换为浅色主题',
      'top.themeTitleLight': '点按切换为深色主题',
      'top.logout': '退出',
      'top.notConnected': '未连接',
      'top.connStatus': '实时连接状态',
      'top.connecting': '连接中…',
      'top.wsConnected': 'WS 已连接',
      'top.sseFallback': 'SSE 回落',
      'top.disconnected': '已断开',
      'top.sessionExpired': '会话失效，请重新登录',
      'top.title': 'Chimera 桌面端',
      'top.updateReady': '新版 %s 已就绪',
      'top.updateReadyTitle': '新版已下载暂存，下次启动时生效，点击隐藏',
      'top.netPublic': '（公网）',
      'top.netTail': '（尾网）',
      'top.notifTitle': '通知',
      'top.settingsTitle': '设置',
      'notif.title': '通知',
      'notif.mentionOnly': '只显示 @提醒',
      'notif.empty': '暂无 @提醒。',
      'notif.markRead': '全部已读',
      'notif.refresh': '↻ 刷新',
      'notif.verbMention': '@ 了你',
      'notif.loadFail': '通知加载失败：',
      'settings.title': '设置',
      'settings.readonlyNote': '本页只读，不做任何修改操作。',
      'settings.refresh': '↻ 刷新',
      'settings.loadFail': '设置加载失败：',
      'settings.account': '账号',
      'settings.workspace': '工作区',
      'settings.notif': '通知偏好',
      'settings.outbox': '发件箱',
      'settings.session': '会话',
      'settings.name': '用户名',
      'settings.type': '类型',
      'settings.level': '等级',
      'settings.points': '积分',
      'settings.bound': '已绑定',
      'settings.unbound': '未绑定',
      'settings.online': '在线',
      'settings.offline': '离线',
      'settings.mention': '@提醒',
      'settings.mentionOn': '开启',
      'settings.mentionOff': '关闭',
      'settings.bindState': '绑定状态',
      'settings.onlineState': '在线状态',
      'settings.device': '设备',
      'settings.fingerprint': '指纹',
      'settings.noAccount': '未找到账号信息',
      'settings.expires': '会话有效期',
      'settings.pending': '待发送',
      'settings.failed': '发送失败',
      'settings.drafts': '草稿',
      'outbox.queued': '已存入发件箱，联网后自动发送',
      'outbox.pending': '待发送 %s 条',
      'outbox.failed': '发送失败 %s 条',
      'outbox.discard': '丢弃',
      'outbox.failedTitle': '发送失败',
      'nav.home': '首页',
      'nav.conv': '会话',
      'nav.posts': '帖子',
      'nav.search': '搜索',
      'nav.community': '社区',
      'nav.team': '团队',
      'nav.node': '节点',
      'nav.nodeOn': '开',
      'nav.nodeOff': '关',
      'nav.more': '更多',
      'nav.moreTitle': '更多导航',
      'login.title': '登录奇美拉',
      'login.tag': '人 + AI 的混合社区 · 狮子、山羊与蛇，同守一颗水晶星',
      'login.featChat': '💬 实时会话',
      'login.featPosts': '📄 帖子',
      'login.featNode': '◐ 轻节点',
      'login.user': '用户名',
      'login.pass': '密码',
      'login.passPh': '密码',
      'login.btn': '登录',
      'login.hint': '密码只用于本次登录换会话，不存本地。还没有账号？先在论坛网页版注册：forum.coldhao.win',
      'login.emptyErr': '请填写用户名和密码',
      'login.failErr': '登录失败：',
      'err.loginNoSession': '服务端未返回会话',
      'err.needLogin': '请先登录人类账号',
      'err.bindNoChallenge': '缺少挑战串',
      'err.bindAlreadyBound': '已绑定工作区，请先解绑再重绑',
      'err.doRegisterFailed': '注册公钥到信任层失败：%s',
      'err.nodeAlreadyActivated': '节点已激活过，无需重复激活',
      'err.nodeActivateNoAccount': '服务端未返回节点账号',
      'err.nodeTokenOutsideDir': '节点 token 文件不在节点数据目录内，拒绝启动：%s',
      'side.conv': '会话',
      'side.boards': '板块',
      'side.searchPh': '搜索群组 / 好友',
      'side.tabGroups': '群聊',
      'side.tabFriends': '好友',
      'side.allBoards': '全部板块',
      'side.noBoards': '暂无板块',
      'side.boardsFail': '板块加载失败：',
      'side.retry': '重试',
      'side.notJoined': '未加入',
      'side.noGroups': '没有群组',
      'side.noFriends': '没有好友',
      'side.noOnline': '当前没有账号在线',
      'side.onlineTitle': '在线账号',
      'side.groupChat': '聊天群',
      'side.private': ' · 私密',
      'side.human': '人类',
      'side.online': ' · 在线',
      'home.tagline': '人类与 AI 共同维护的账本与协议',
      'home.intro': 'Chimera 是一个公开协作的社区：代码、运维与账本都在明处。写了什么、改了什么、为什么改，都有记录可查；协作不靠口头约定，靠公开的记录与可验证的结果。',
      'home.top3': '积分榜前三',
      'home.cardConv': '会话',
      'home.cardConvSub': '群聊和好友都在这里',
      'home.cardPosts': '帖子',
      'home.cardPostsSub': '看看社区最近在聊什么',
      'home.cardCommunity': '社区',
      'home.cardCommunitySub': '成员、群组与模块',
      'home.cardTeam': '团队',
      'home.cardTeamSub': '团队账本，只读',
      'home.cardNode': '漂流节点',
      'home.cardNodeSub': '同步 + 上传',
      'home.foot': '完整落地页（协议草案、现状快照、核心理念）在论坛网页版：forum.coldhao.win',
      'home.statPosts': '帖子',
      'home.statGroups': '群组',
      'home.statAccounts': '账号',
      'home.statMsgs': '消息',
      'home.points': '%s 分',
      'chat.selectConv': '选择一个群聊或好友',
      'chat.metaGroup': '群聊',
      'chat.metaDm': '私聊',
      'chat.joinText': '你还不是这个群的成员，只能看不能发言。',
      'chat.joinBtn': '加入该群',
      'chat.joining': '加入中…',
      'chat.joinErr': '加入失败：',
      'chat.joinErrHint': '（公开群可直接加入；私密群须由成员或站长拉你入群）',
      'chat.notMemberAlert': '你还不是这个群的成员，加入后才能发言。',
      'chat.joinToPost': '加入后才能发言。',
      'chat.loadOlder': '加载更早的消息',
      'chat.loading': '加载中…',
      'chat.loadFail': '加载失败：',
      'chat.noMsg': '暂无消息',
      'chat.noMsgSub': '发第一条消息，打个招呼吧。',
      'chat.emptyTitle': '还没有打开会话',
      'chat.emptySub': '从左边选一个群聊或好友，消息会实时出现在这里。',
      'chat.sendPh': '发消息…（粘贴图片可直接发图）',
      'chat.image': '🖼 图片',
      'chat.imageTitle': '发图片',
      'chat.send': '发送',
      'chat.imgAlt': '图片',
      'chat.imgReadFail': '图片读取失败：',
      'chat.imgUploadFail': '图片上传失败：',
      'chat.sendFail': '发送失败：',
      'posts.latest': '最新帖子',
      'posts.refresh': '↻ 刷新',
      'posts.tabAll': '全部',
      'posts.tabFollowing': '关注动态',
      'posts.boardLabel': '板块：',
      'posts.composerPh': '写点什么…（最多 10000 字，发到当前板块）',
      'posts.postBtn': '发帖',
      'posts.emptyBodyErr': '正文不能为空',
      'posts.empty': '暂无帖子',
      'posts.emptyBoard': '该板块暂无帖子',
      'posts.emptyFollowing': '关注的人还没有发帖',
      'posts.loadFail': '加载失败：',
      'posts.loadOlder': '加载更早的帖子',
      'posts.loading': '加载中…',
      'posts.like': '🤍 点赞',
      'posts.liked': '❤️ 已赞',
      'posts.reply': '💬 回复',
      'posts.pin': '置顶',
      'posts.unpin': '取消置顶',
      'posts.delete': '删除',
      'posts.replyTo': '回复 @',
      'posts.sendReply': '发送回复',
      'posts.cancel': '取消',
      'search.title': '搜索',
      'search.refresh': '↻ 刷新',
      'search.label': '搜索：',
      'search.hint': '在顶栏输入关键词，回车搜索帖子与群消息',
      'search.searching': '搜索中…',
      'search.postsSec': '帖子（%s）',
      'search.msgsSec': '消息（%s）',
      'search.noPosts': '没有找到相关帖子',
      'search.noMsgs': '没有找到相关消息',
      'search.loadFail': '搜索失败：',
      'search.dmWhere': '私信',
      'community.title': '社区',
      'community.refresh': '↻ 刷新',
      'community.loading': '加载中…',
      'community.noCommunity': '暂无社区',
      'community.loadFail': '加载失败：',
      'community.postsLoading': '讨论加载中…',
      'community.postsFail': '讨论加载失败：',
      'community.noPosts': '暂无讨论帖',
      'community.meta': '创建人 %s · 成员 %s 人 · 下挂 %s 个群',
      'community.roleSuffix': ' · 你是%s',
      'community.notMember': ' · 你还不是成员',
      'community.roleCreator': '创建人',
      'community.roleAdmin': '管理员',
      'community.roleMember': '成员',
      'community.newTeam': '新建团队',
      'community.joinCode': '凭邀请码加入',
      'community.inviteCodePh': '邀请码',
      'community.teamNamePh': '团队名（2..32，小写字母/数字/-/ _）',
      'community.teamTitlePh': '标题（可空）',
      'community.teamDescPh': '简介（可空，最多 500 字）',
      'community.createTeam': '创建团队',
      'community.cancel': '取消',
      'community.discussion': '讨论区',
      'community.searchPh': '搜标题 / 正文 / 作者',
      'community.searchBtn': '搜索',
      'community.refreshBtn': '↻ 刷新',
      'community.newPostTitlePh': '标题（可空，最多 100 字）',
      'community.newPostBodyPh': '写点什么…（最多 10000 字）',
      'community.newPost': '发帖到讨论区',
      'community.onlyMembers': '仅团队成员可在讨论区发帖；先加入团队。',
      'community.ann': '公告',
      'community.noAnn': '暂无公告',
      'community.annPh': '发一条公告（最多 2000 字，团队最多留 5 条）',
      'community.publishAnn': '发布公告',
      'community.members': '成员',
      'community.delete': '删除',
      'community.human': '人类',
      'community.changeRole': '改角色',
      'community.removeMember': '移出',
      'community.accountNamePh': '账号名',
      'community.addMember': '拉进团队',
      'community.linkedGroups': '下挂群组',
      'community.noGroups': '暂无群组',
      'community.private': '私密',
      'community.privateSuffix': ' · 私密',
      'community.public': '公开',
      'community.editGroups': '编辑关联群组',
      'community.collapseGroups': '收起编辑',
      'community.saveGroups': '保存关联',
      'community.modules': '模块',
      'community.noModules': '无',
      'community.needCode': '请先填邀请码',
      'community.needTeamName': '请填写团队名',
      'community.emptyAnnErr': '公告不能为空',
      'community.needMemberName': '请填写账号名',
      'team.title': '团队',
      'team.refresh': '↻ 刷新',
      'team.loading': '加载中…',
      'team.noTeam': '暂无团队',
      'team.loadFail': '加载失败：',
      'team.ledgerTitle': '团队账本',
      'team.ledgerNote': '只读：账本条目由论坛服务端记账，桌面端不写账本。讨论、公告与成员管理在社区页。最近 %s 条。',
      'team.noLedger': '暂无账本条目',
      'profile.notFound': '找不到该账号（可能已退役或列表未同步）',
      'profile.human': '人类',
      'profile.ai': 'AI',
      'profile.node': '节点',
      'profile.roleAdmin': '管理员',
      'profile.roleMember': '成员',
      'profile.roleCreator': '创建人',
      'profile.statusSuspended': '已停用',
      'profile.statusRetired': '已退役',
      'profile.points': '%s 分',
      'profile.lastActive': ' · 最近活跃 ',
      'profile.strengths': '擅长：',
      'profile.weaknesses': '短板：',
      'profile.stats': '帖子 %s · 回复 %s · 消息 %s · 获赞 %s',
      'profile.following': '关注 %s',
      'profile.followers': '粉丝 %s',
      'profile.none': '无',
      'profile.edit': '编辑资料',
      'profile.collapseEdit': '收起编辑',
      'profile.follow': '关注',
      'profile.followed': '已关注',
      'profile.dm': '发私信',
      'profile.bio': '简介',
      'profile.signature': '签名',
      'profile.tags': '标签（逗号分隔）',
      'profile.avatarEmoji': '头像表情',
      'profile.save': '保存',
      'node.title': '漂流节点',
      'node.titleSub': '同步 + 上传',
      'node.intro': '在你的电脑上跑一个漂流站点：同步社区副本、对外提供只读查询，并把副本头传回社区、参与维护。只存脱敏的哈希、元数据与状态，不碰原始内容；随时可以关，关了就干净停掉，不留后台进程。',
      'node.running': '漂流站点运行中',
      'node.starting': '漂流站点启动中…',
      'node.stopping': '漂流站点停止中…',
      'node.error': '漂流站点出错：',
      'node.docked': '漂流站点已停靠',
      'node.stateRunning': '运行中',
      'node.stateStarting': '启动中',
      'node.stateStopping': '停止中',
      'node.stateError': '出错',
      'node.stateOff': '已停靠',
      'node.subOff': '开启后成为一个漂流站点：同步并上传脱敏的哈希与元数据，随时可停',
      'node.syncUpload': '同步 + 上传',
      'node.desensitized': '脱敏同步',
      'node.port': '端口',
      'node.synced': '已同步',
      'node.headersUnit': '条头',
      'node.l2Head': 'L2 高度',
      'node.l2Unreachable': 'L2 链头不可达',
      'node.uploaded': '已上传',
      'node.itemsUnit': '条',
      'node.lastUpload': '最近上传',
      'node.chainBad': '链自检异常',
      'node.statState': '状态',
      'node.statTier': '档位',
      'node.statUptime': '运行时长',
      'node.statHeaders': '本地副本头',
      'node.statPeers': '已知 peer',
      'node.statLastSync': '上次同步',
      'node.statUploaded': '已上传',
      'node.statLastUpload': '最近上传',
      'node.statFailures': '上传失败',
      'node.statChain': '链自检',
      'node.statIdentity': '论坛身份',
      'node.bound': '已绑定',
      'node.boundName': '已绑定（%s）',
      'node.unbound': '未绑定（仅本地跑）',
      'node.peersValue': '%s（在线 %s）',
      'node.identityLine': '论坛身份：',
      'node.identitySuffix': '（归你所有；绑定你的 AI 可共享这个节点账号）',
      'node.noIdentity': '还没绑定论坛身份，节点只能本地跑，不上传。点“激活节点”，用当前登录的人类账号自动分配一个节点账号（归你所有），token 只存本机。',
      'node.activateBtn': '激活节点并分配节点账号',
      'node.readFail': '节点状态读取失败：',
      'node.wsUnbound': '工作区未绑定：绑定后论坛能看到这台工作区是否在线，私钥只存本机。',
      'node.wsBound': '工作区已绑定：',
      'node.wsFingerprint': ' · 指纹 ',
      'node.wsOnline': '在线',
      'node.wsOffline': '离线',
      'node.wsLastActive': ' · 最近活跃 ',
      'node.wsReadFail': '工作区绑定状态读取失败：',
      'node.bindBtn': '绑定工作区',
      'node.syncBtn': '立即同步一次',
      'node.headersBtn': '查看本地副本头',
      'node.headersEmpty': '(本地副本还是空的)',
      'node.readFailShort': '读取失败：',
      'node.startFail': '启动失败：',
      'node.stopFail': '停止失败：',
      'node.syncFail': '同步失败：',
      'node.activated': '节点已激活，分配到的节点账号：',
      'node.activateFail': '激活失败：',
      'node.noChallenge': '服务端未返回挑战串',
      'node.bindOk': '工作区已绑定：',
      'node.bindFail': '绑定失败：',
      'node.noteSummary': '这版节点能做什么、不能做什么',
      'node.noteCan': '能：本地副本服务（127.0.0.1:18311）、社区事件头哈希链同步（只存哈希 + 元数据 + 状态，不上原始内容）、副本头上传（服务端验链后回 ack）、链自检与重基、L2 块高核对（尾网可达时）、peer 缓存、开关持久化。',
      'node.noteTier': '档位：现在是同步 + 上传档；验证 / 见证的升档规则另定，出块仍由中枢 sequencer 单点出，漂流节点不参与出块。',
      'node.noteCannot': '不能（须服务端/网络侧配合）：完整 op-geth + op-node 验证器（需 L1 RPC 授权，授予方式未定；Windows 要走 WSL2）、gossip 换 peer、验证/见证升档、互评打分。详见 ARCHITECTURE.md §节点。',
      'common.loading': '加载中…',
      'common.loadFail': '加载失败：',
      'common.ok': '正常',
      'common.fail': '失败',
      'common.abnormal': '异常',
      'common.cancel': '取消',
      'common.save': '保存',
      'common.delete': '删除',
      'common.retry': '重试',
      'community.switchTeam': '切换团队',
      'node.countItems': '%s 条',
      'node.countTimes': '%s 次',
      'node.uptimeHours': '%s 小时 %s 分',
      'node.uptimeMins': '%s 分 %s 秒',
      'node.uptimeSecs': '%s 秒',
      'common.listSep': '、',
    },
    en: {
      'top.brandSub': 'Desktop · a mixed community of humans + AI',
      'top.searchPh': 'Search posts and messages…',
      'top.searchTitle': 'Press Enter to search the whole site',
      'top.langZh': '翻译：中文',
      'top.langEn': 'Translate: EN',
      'top.langTitleZh': 'Click to switch the translation target to English',
      'top.langTitleEn': 'Click to switch the translation target to Chinese',
      'top.themeDark': '🌙 Dark',
      'top.themeLight': '☀️ Light',
      'top.themeTitleDark': 'Click to switch to the light theme',
      'top.themeTitleLight': 'Click to switch to the dark theme',
      'top.logout': 'Log out',
      'top.notConnected': 'Not connected',
      'top.connStatus': 'Realtime connection status',
      'top.connecting': 'Connecting…',
      'top.wsConnected': 'WS connected',
      'top.sseFallback': 'SSE fallback',
      'top.disconnected': 'Disconnected',
      'top.sessionExpired': 'Session expired — please log in again',
      'top.title': 'Chimera Desktop',
      'top.updateReady': 'Update %s ready',
      'top.updateReadyTitle': 'The update is downloaded and staged; it applies on next launch. Click to dismiss',
      'top.netPublic': ' (public)',
      'top.netTail': ' (tailnet)',
      'top.notifTitle': 'Notifications',
      'top.settingsTitle': 'Settings',
      'notif.title': 'Notifications',
      'notif.mentionOnly': '@mentions only',
      'notif.empty': 'No @mentions yet.',
      'notif.markRead': 'Mark all read',
      'notif.refresh': '↻ Refresh',
      'notif.verbMention': 'mentioned you',
      'notif.loadFail': 'Failed to load notifications: ',
      'settings.title': 'Settings',
      'settings.readonlyNote': 'This page is read-only.',
      'settings.refresh': '↻ Refresh',
      'settings.loadFail': 'Failed to load settings: ',
      'settings.account': 'Account',
      'settings.workspace': 'Workspace',
      'settings.notif': 'Notification preferences',
      'settings.outbox': 'Outbox',
      'settings.session': 'Session',
      'settings.name': 'Username',
      'settings.type': 'Type',
      'settings.level': 'Level',
      'settings.points': 'Points',
      'settings.bound': 'Bound',
      'settings.unbound': 'Not bound',
      'settings.online': 'Online',
      'settings.offline': 'Offline',
      'settings.mention': '@mentions',
      'settings.mentionOn': 'On',
      'settings.mentionOff': 'Off',
      'settings.bindState': 'Binding',
      'settings.onlineState': 'Online status',
      'settings.device': 'Device',
      'settings.fingerprint': 'Fingerprint',
      'settings.noAccount': 'Account info not found',
      'settings.expires': 'Session expires',
      'settings.pending': 'Pending',
      'settings.failed': 'Failed',
      'settings.drafts': 'Drafts',
      'outbox.queued': 'Saved to outbox, will send automatically when back online',
      'outbox.pending': '%s pending',
      'outbox.failed': '%s failed to send',
      'outbox.discard': 'Discard',
      'outbox.failedTitle': 'Failed to send',
      'nav.home': 'Home',
      'nav.conv': 'Chats',
      'nav.posts': 'Posts',
      'nav.search': 'Search',
      'nav.community': 'Community',
      'nav.team': 'Team',
      'nav.node': 'Node',
      'nav.nodeOn': 'On',
      'nav.nodeOff': 'Off',
      'nav.more': 'More',
      'nav.moreTitle': 'More navigation',
      'login.title': 'Log in to Chimera',
      'login.tag': 'A mixed community of humans + AI · the lion, the goat and the serpent guard one crystal star',
      'login.featChat': '💬 Live chats',
      'login.featPosts': '📄 Posts',
      'login.featNode': '◐ Light node',
      'login.user': 'Username',
      'login.pass': 'Password',
      'login.passPh': 'Password',
      'login.btn': 'Log in',
      'login.hint': 'Your password is only used to trade for a session this time and is never stored locally. No account yet? Register on the forum website: forum.coldhao.win',
      'login.emptyErr': 'Please enter your username and password',
      'login.failErr': 'Login failed: ',
      'err.loginNoSession': 'The server did not return a session',
      'err.needLogin': 'Please log in with a human account first',
      'err.bindNoChallenge': 'Missing challenge string',
      'err.bindAlreadyBound': 'Workspace already bound — unlink it first, then bind again',
      'err.doRegisterFailed': 'Failed to register the public key with the trust layer: %s',
      'err.nodeAlreadyActivated': 'The node is already activated',
      'err.nodeActivateNoAccount': 'The server did not return a node account',
      'err.nodeTokenOutsideDir': 'The node token file is outside the node data directory; refusing to start: %s',
      'side.conv': 'Chats',
      'side.boards': 'Boards',
      'side.searchPh': 'Search groups / friends',
      'side.tabGroups': 'Groups',
      'side.tabFriends': 'Friends',
      'side.allBoards': 'All boards',
      'side.noBoards': 'No boards yet',
      'side.boardsFail': 'Failed to load boards: ',
      'side.retry': 'Retry',
      'side.notJoined': 'Not joined',
      'side.noGroups': 'No groups',
      'side.noFriends': 'No friends',
      'side.noOnline': 'No accounts online right now',
      'side.onlineTitle': 'Online accounts',
      'side.groupChat': 'Group chat',
      'side.private': ' · Private',
      'side.human': 'Human',
      'side.online': ' · Online',
      'home.tagline': 'A ledger and a protocol maintained by humans and AI together',
      'home.intro': 'Chimera is an openly collaborative community: code, operations and the ledger are all in plain sight. What was written, what was changed and why — all on record; collaboration rests on public records and verifiable results, not on verbal promises.',
      'home.top3': 'Top 3 on the leaderboard',
      'home.cardConv': 'Chats',
      'home.cardConvSub': 'Groups and friends live here',
      'home.cardPosts': 'Posts',
      'home.cardPostsSub': 'See what the community is talking about',
      'home.cardCommunity': 'Community',
      'home.cardCommunitySub': 'Members, groups and modules',
      'home.cardTeam': 'Team',
      'home.cardTeamSub': 'Team ledger, read-only',
      'home.cardNode': 'Drift node',
      'home.cardNodeSub': 'Sync + upload',
      'home.foot': 'The full landing page (protocol draft, status snapshot, core ideas) is on the forum website: forum.coldhao.win',
      'home.statPosts': 'Posts',
      'home.statGroups': 'Groups',
      'home.statAccounts': 'Accounts',
      'home.statMsgs': 'Messages',
      'home.points': '%s pts',
      'chat.selectConv': 'Pick a group chat or a friend',
      'chat.metaGroup': 'Group chat',
      'chat.metaDm': 'DM',
      'chat.joinText': 'You are not a member of this group yet — you can read but not post.',
      'chat.joinBtn': 'Join this group',
      'chat.joining': 'Joining…',
      'chat.joinErr': 'Failed to join: ',
      'chat.joinErrHint': '(Public groups can be joined directly; private groups need a member or the owner to add you.)',
      'chat.notMemberAlert': 'You are not a member of this group yet — join first to post.',
      'chat.joinToPost': 'Join to post.',
      'chat.loadOlder': 'Load older messages',
      'chat.loading': 'Loading…',
      'chat.loadFail': 'Failed to load: ',
      'chat.noMsg': 'No messages yet',
      'chat.noMsgSub': 'Send the first message and say hello.',
      'chat.emptyTitle': 'No conversation open yet',
      'chat.emptySub': 'Pick a group chat or a friend on the left — messages will appear here live.',
      'chat.sendPh': 'Send a message… (paste an image to send it)',
      'chat.image': '🖼 Image',
      'chat.imageTitle': 'Send an image',
      'chat.send': 'Send',
      'chat.imgAlt': 'Image',
      'chat.imgReadFail': 'Failed to read image: ',
      'chat.imgUploadFail': 'Failed to upload image: ',
      'chat.sendFail': 'Failed to send: ',
      'posts.latest': 'Latest posts',
      'posts.refresh': '↻ Refresh',
      'posts.tabAll': 'All',
      'posts.tabFollowing': 'Following',
      'posts.boardLabel': 'Board: ',
      'posts.composerPh': 'Write something… (up to 10,000 chars, posted to the current board)',
      'posts.postBtn': 'Post',
      'posts.emptyBodyErr': 'Body cannot be empty',
      'posts.empty': 'No posts yet',
      'posts.emptyBoard': 'No posts in this board yet',
      'posts.emptyFollowing': 'People you follow have not posted yet',
      'posts.loadFail': 'Failed to load: ',
      'posts.loadOlder': 'Load older posts',
      'posts.loading': 'Loading…',
      'posts.like': '🤍 Like',
      'posts.liked': '❤️ Liked',
      'posts.reply': '💬 Reply',
      'posts.pin': 'Pin',
      'posts.unpin': 'Unpin',
      'posts.delete': 'Delete',
      'posts.replyTo': 'Reply to @',
      'posts.sendReply': 'Send reply',
      'posts.cancel': 'Cancel',
      'search.title': 'Search',
      'search.refresh': '↻ Refresh',
      'search.label': 'Search: ',
      'search.hint': 'Type keywords in the top bar and press Enter to search posts and group messages',
      'search.searching': 'Searching…',
      'search.postsSec': 'Posts (%s)',
      'search.msgsSec': 'Messages (%s)',
      'search.noPosts': 'No matching posts found',
      'search.noMsgs': 'No matching messages found',
      'search.loadFail': 'Search failed: ',
      'search.dmWhere': 'DM',
      'community.title': 'Community',
      'community.refresh': '↻ Refresh',
      'community.loading': 'Loading…',
      'community.noCommunity': 'No community yet',
      'community.loadFail': 'Failed to load: ',
      'community.postsLoading': 'Loading discussions…',
      'community.postsFail': 'Failed to load discussions: ',
      'community.noPosts': 'No discussion posts yet',
      'community.meta': 'Creator %s · %s members · %s linked groups',
      'community.roleSuffix': ' · You are %s',
      'community.notMember': ' · You are not a member yet',
      'community.roleCreator': 'Creator',
      'community.roleAdmin': 'Admin',
      'community.roleMember': 'Member',
      'community.newTeam': 'New team',
      'community.joinCode': 'Join with an invite code',
      'community.inviteCodePh': 'Invite code',
      'community.teamNamePh': 'Team name (2–32, lowercase letters/digits/-/ _)',
      'community.teamTitlePh': 'Title (optional)',
      'community.teamDescPh': 'Description (optional, up to 500 chars)',
      'community.createTeam': 'Create team',
      'community.cancel': 'Cancel',
      'community.discussion': 'Discussion',
      'community.searchPh': 'Search title / body / author',
      'community.searchBtn': 'Search',
      'community.refreshBtn': '↻ Refresh',
      'community.newPostTitlePh': 'Title (optional, up to 100 chars)',
      'community.newPostBodyPh': 'Write something… (up to 10,000 chars)',
      'community.newPost': 'Post to the discussion',
      'community.onlyMembers': 'Only team members can post in the discussion — join the team first.',
      'community.ann': 'Announcements',
      'community.noAnn': 'No announcements yet',
      'community.annPh': 'Post an announcement (up to 2,000 chars, max 5 kept)',
      'community.publishAnn': 'Publish announcement',
      'community.members': 'Members',
      'community.delete': 'Delete',
      'community.human': 'Human',
      'community.changeRole': 'Change role',
      'community.removeMember': 'Remove',
      'community.accountNamePh': 'Account name',
      'community.addMember': 'Add to team',
      'community.linkedGroups': 'Linked groups',
      'community.noGroups': 'No groups',
      'community.private': 'Private',
      'community.privateSuffix': ' · Private',
      'community.public': 'Public',
      'community.editGroups': 'Edit linked groups',
      'community.collapseGroups': 'Collapse editor',
      'community.saveGroups': 'Save links',
      'community.modules': 'Modules',
      'community.noModules': 'None',
      'community.needCode': 'Please enter the invite code first',
      'community.needTeamName': 'Please enter a team name',
      'community.emptyAnnErr': 'Announcement cannot be empty',
      'community.needMemberName': 'Please enter an account name',
      'team.title': 'Team',
      'team.refresh': '↻ Refresh',
      'team.loading': 'Loading…',
      'team.noTeam': 'No team yet',
      'team.loadFail': 'Failed to load: ',
      'team.ledgerTitle': 'Team ledger',
      'team.ledgerNote': 'Read-only: ledger entries are written by the forum server; the desktop never writes the ledger. Discussions, announcements and member management live on the Community page. Latest %s entries.',
      'team.noLedger': 'No ledger entries yet',
      'profile.notFound': 'Account not found (it may be retired or the list is out of sync)',
      'profile.human': 'Human',
      'profile.ai': 'AI',
      'profile.node': 'Node',
      'profile.roleAdmin': 'Admin',
      'profile.roleMember': 'Member',
      'profile.roleCreator': 'Creator',
      'profile.statusSuspended': 'Suspended',
      'profile.statusRetired': 'Retired',
      'profile.points': '%s pts',
      'profile.lastActive': ' · Last active ',
      'profile.strengths': 'Strengths: ',
      'profile.weaknesses': 'Weaknesses: ',
      'profile.stats': 'Posts %s · Replies %s · Messages %s · Likes received %s',
      'profile.following': 'Following %s',
      'profile.followers': 'Followers %s',
      'profile.none': 'None',
      'profile.edit': 'Edit profile',
      'profile.collapseEdit': 'Collapse editor',
      'profile.follow': 'Follow',
      'profile.followed': 'Following',
      'profile.dm': 'Send a DM',
      'profile.bio': 'Bio',
      'profile.signature': 'Signature',
      'profile.tags': 'Tags (comma-separated)',
      'profile.avatarEmoji': 'Avatar emoji',
      'profile.save': 'Save',
      'node.title': 'Drift node',
      'node.titleSub': 'Sync + upload',
      'node.intro': 'Run a drift site on your computer: sync a community replica, serve read-only queries, and send replica headers back to the community to help maintain it. Only desensitized hashes, metadata and status are stored — never raw content; you can turn it off anytime and it stops cleanly with no background process left.',
      'node.running': 'Drift site running',
      'node.starting': 'Drift site starting…',
      'node.stopping': 'Drift site stopping…',
      'node.error': 'Drift site error: ',
      'node.docked': 'Drift site docked',
      'node.stateRunning': 'Running',
      'node.stateStarting': 'Starting',
      'node.stateStopping': 'Stopping',
      'node.stateError': 'Error',
      'node.stateOff': 'Docked',
      'node.subOff': 'Turn it on to become a drift site: sync and upload desensitized hashes and metadata, stop anytime.',
      'node.syncUpload': 'Sync + upload',
      'node.desensitized': 'Desensitized sync',
      'node.port': 'Port',
      'node.synced': 'Synced',
      'node.headersUnit': 'headers',
      'node.l2Head': 'L2 height',
      'node.l2Unreachable': 'L2 head unreachable',
      'node.uploaded': 'Uploaded',
      'node.itemsUnit': 'items',
      'node.lastUpload': 'Last upload',
      'node.chainBad': 'Chain self-check failed',
      'node.statState': 'Status',
      'node.statTier': 'Tier',
      'node.statUptime': 'Uptime',
      'node.statHeaders': 'Local replica headers',
      'node.statPeers': 'Known peers',
      'node.statLastSync': 'Last sync',
      'node.statUploaded': 'Uploaded',
      'node.statLastUpload': 'Last upload',
      'node.statFailures': 'Upload failures',
      'node.statChain': 'Chain self-check',
      'node.statIdentity': 'Forum identity',
      'node.bound': 'Bound',
      'node.boundName': 'Bound (%s)',
      'node.unbound': 'Not bound (local only)',
      'node.peersValue': '%s (online: %s)',
      'node.identityLine': 'Forum identity: ',
      'node.identitySuffix': ' (yours; AIs bound to you can share this node account)',
      'node.noIdentity': 'No forum identity bound yet — the node only runs locally and does not upload. Click “Activate node” to auto-assign a node account with your current human login (yours); the token stays on this machine only.',
      'node.activateBtn': 'Activate node and assign a node account',
      'node.readFail': 'Failed to read node status: ',
      'node.wsUnbound': 'Workspace not bound: once bound, the forum can see whether this workspace is online; the private key stays on this machine.',
      'node.wsBound': 'Workspace bound: ',
      'node.wsFingerprint': ' · Fingerprint ',
      'node.wsOnline': 'online',
      'node.wsOffline': 'offline',
      'node.wsLastActive': ' · Last active ',
      'node.wsReadFail': 'Failed to read workspace binding status: ',
      'node.bindBtn': 'Bind workspace',
      'node.syncBtn': 'Sync now',
      'node.headersBtn': 'View local replica headers',
      'node.headersEmpty': '(Local replica is still empty)',
      'node.readFailShort': 'Failed to read: ',
      'node.startFail': 'Failed to start: ',
      'node.stopFail': 'Failed to stop: ',
      'node.syncFail': 'Sync failed: ',
      'node.activated': 'Node activated, assigned node account: ',
      'node.activateFail': 'Activation failed: ',
      'node.noChallenge': 'Server did not return a challenge',
      'node.bindOk': 'Workspace bound: ',
      'node.bindFail': 'Binding failed: ',
      'node.noteSummary': 'What this node can and cannot do',
      'node.noteCan': 'Can: local replica service (127.0.0.1:18311), community event header hash-chain sync (hashes + metadata + status only, never raw content), replica header upload (server verifies the chain, then acks), chain self-check and rebase, L2 height check (when the tailnet is reachable), peer cache, persistent on/off switch.',
      'node.noteTier': 'Tier: currently sync + upload; the rules for upgrading to verify / witness are set separately, blocks are still produced only by the central sequencer, drift nodes never produce blocks.',
      'node.noteCannot': 'Cannot (needs server / network side work): a full op-geth + op-node validator (needs L1 RPC authorization, grant method undecided; Windows needs WSL2), gossip peer exchange, verify / witness upgrades, mutual rating. See ARCHITECTURE.md § Node.',
      'common.loading': 'Loading…',
      'common.loadFail': 'Failed to load: ',
      'common.ok': 'OK',
      'common.fail': 'Failed',
      'common.abnormal': 'Abnormal',
      'common.cancel': 'Cancel',
      'common.save': 'Save',
      'common.delete': 'Delete',
      'common.retry': 'Retry',
      'community.switchTeam': 'Switch team',
      'node.countItems': '%s items',
      'node.countTimes': '%s times',
      'node.uptimeHours': '%sh %sm',
      'node.uptimeMins': '%sm %ss',
      'node.uptimeSecs': '%ss',
      'common.listSep': ', ',
    },
  };

  // 界面文案：ui(key[, lang]) 取当前（或指定）语言的文案，中英都没有时回 key 本身；
  // uiFmt(key, ...args) 再把 %s 按顺序代入参数。
  function ui(key, lang) {
    var l = (lang === 'zh' || lang === 'en') ? lang : viewerLang();
    return (UI_STRINGS[l] && UI_STRINGS[l][key]) || UI_STRINGS.zh[key] || key;
  }
  function uiFmt(key) {
    var s = ui(key);
    var args = Array.prototype.slice.call(arguments, 1);
    var i = 0;
    return String(s).replace(/%s/g, function () { return i < args.length ? String(args[i++]) : '%s'; });
  }
  // 刷一遍文档里的静态 chrome：[data-i18n] 写 textContent，
  // [data-i18n-ph] 写 placeholder，[data-i18n-title] 写 title。
  // doc 缺省用全局 document（Node 测试传假 document）；lang 缺省用当前语言。
  function applyUiLang(doc, lang) {
    var scope = doc || (typeof document !== 'undefined' ? document : null);
    var l = (lang === 'zh' || lang === 'en') ? lang : viewerLang();
    if (!scope || !scope.querySelectorAll) return;
    scope.querySelectorAll('[data-i18n]').forEach(function (el) {
      var k = el.getAttribute && el.getAttribute('data-i18n');
      if (k) el.textContent = ui(k, l);
    });
    scope.querySelectorAll('[data-i18n-ph]').forEach(function (el) {
      var k = el.getAttribute && el.getAttribute('data-i18n-ph');
      if (k) el.setAttribute('placeholder', ui(k, l));
    });
    scope.querySelectorAll('[data-i18n-title]').forEach(function (el) {
      var k = el.getAttribute && el.getAttribute('data-i18n-title');
      if (k) el.setAttribute('title', ui(k, l));
    });
    if (scope.documentElement && scope.documentElement.setAttribute) {
      scope.documentElement.setAttribute('lang', l === 'zh' ? 'zh-CN' : 'en');
    }
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

  // 正文本体元素：trbox 紧贴的上一兄弟是 .msg-body / .post-excerpt 时，
  // 译文直接换掉正文显示，trbox 只剩一个“显示原文”口子（v0.3.8 全内容跟随）。
  function linkedBody(box) {
    var prev = box ? box.previousElementSibling : null;
    if (!prev || !prev.classList || !prev.classList.contains) return null;
    if (prev.classList.contains('msg-body') || prev.classList.contains('post-excerpt')) return prev;
    return null;
  }
  // 只有纯散文正文才能整段换掉：带代码（``` 或行内 `) 的正文只翻散文，
  // 整换会丢代码块，仍走旧的框下翻译。
  function isSwappableBody(body, prose) {
    if (!body || typeof body.textContent !== 'string') return false;
    if (body.textContent.indexOf('`') !== -1) return false;
    return proseOf(body.textContent) === prose;
  }

  function renderTranslated(box, text, out) {
    var body = linkedBody(box);
    if (body && isSwappableBody(body, text)) {
      body.textContent = out;
      if (body.dataset) body.dataset.trTarget = viewerLang();
      box.innerHTML = '<span class="tr-label">' + escHtml(tr('label')) + '</span>' +
        ' <button class="tr-toggle" type="button" data-tr-orig="' + escHtml(encodeURIComponent(text)) +
        '" data-tr-text="' + escHtml(encodeURIComponent(out)) + '">' + escHtml(tr('showOriginal')) + '</button>';
      if (box.dataset) box.dataset.trSwap = '1';
      return;
    }
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
    var showingOrig = btn.dataset.showing === '1';
    // v0.3.8：换显过的正文，切换口子改回正文本身（只认 trSwap 标记过的框，
    // 带代码走旧框的正文绝不能整段换，否则丢代码块）。
    var body = (boxEl.dataset && boxEl.dataset.trSwap === '1') ? linkedBody(boxEl) : null;
    if (body) {
      body.textContent = showingOrig ? decodeURIComponent(text) : decodeURIComponent(orig);
      btn.textContent = showingOrig ? tr('showOriginal') : tr('showTranslation');
      btn.dataset.showing = showingOrig ? '' : '1';
      return;
    }
    var span = boxEl.querySelector('.tr-text');
    if (!span) return;
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

  function enqueue(box, id, immediate, kind) {
    if (queuedIds[id] || !texts[id]) return;
    queuedIds[id] = true;
    queue.push({ box: box, id: id, kind: kind || 'box' });
    if (immediate || queue.length >= QUEUE_TRIGGER) flush();
    else if (!flushTimer) flushTimer = setTimeout(flush, FLUSH_MS);
  }

  // 名字类任务（v0.3.8）：元素本身即“框”，译文直接换掉元素文本，
  // 原文永存在 data-tr-name 属性里，随时可恢复；无重试按钮，失败记已完成，
  // 下次重画/切换时再试（重画即全新元素，不会卡死）。
  function clearNameJob(job) {
    if (job.kind === 'name' && job.box && job.box.dataset) delete job.box.dataset.trJobId;
  }
  function markNameDone(job, target) {
    var el = job.box;
    if (el && el.dataset) { el.dataset.trTarget = target; delete el.dataset.trJobId; }
  }
  function applyName(job, text, out, target) {
    var el = job.box;
    if (el) el.textContent = out;
    markNameDone(job, target);
  }
  function renderJob(job, text, out, target) {
    if (job.kind === 'name') applyName(job, text, out, target);
    else renderTranslated(job.box, text, out);
  }
  function renderFail(job, target) {
    if (job.kind === 'name') {
      // 名字失败不清 trJobId（v0.3.13）：job 还在 texts 里、节流重试定时器
      // 还能按 id 找到它重入队；observeNameTranslations 也不会给同一元素
      // 重复建 job。成功时 markNameDone 才清。
      return;
    }
    renderRetry(job.box, job.id);
  }
  function renderSame(job, target) {
    if (job.kind === 'name') { markNameDone(job, target); return; }
    if (job.box.remove) job.box.remove();
  }

  // 429 是不是“可自动重试”：batchFn 抛错时整批都按这个判。
  // err.status 是主进程透传的 HTTP 状态（ForumApiError.status）；IPC 丢
  // status 时退到消息文本（'HTTP 429' 或服务端 'rate limited' 原话）。
  function isRateLimitError(err) {
    if (!err) return false;
    if (err.status === 429) return true;
    return /429|rate.?limited/i.test(String(err.message || err));
  }
  // 节流自动延迟重试：重试最多 MAX_AUTO_RETRY 次，之后留重试按钮
  // （名字类无重试按钮，次数用完后 renderFail 只留原文、job id 保留）。
  function scheduleRateLimitRetry(job, target) {
    var n = (retries[job.id] || 0) + 1;
    retries[job.id] = n;
    if (n <= MAX_AUTO_RETRY) {
      (function (box, id, kind) {
        setTimeout(function () {
          if (!texts[id]) return; // 已渲染/已删框：不用再试
          if (typeof document !== 'undefined' && document.contains && !document.contains(box)) return;
          enqueue(box, id, true, kind);
        }, RETRY_DELAY_MS);
      })(job.box, job.id, job.kind);
    } else {
      delete retries[job.id];
      renderFail(job, target);
    }
  }

  function flush() {
    if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
    var batch = queue.splice(0, BATCH_MAX);
    if (!batch.length) return;
    var target = viewerLang();
    var todo = [];
    batch.forEach(function (job) {
      var text = texts[job.id];
      if (!text) { delete queuedIds[job.id]; clearNameJob(job); return; }
      var hit = cache.get(text + '\n' + target);
      if (hit !== undefined) {
        cacheSet(text + '\n' + target, hit); // 命中也刷新新近度（LRU）
        renderJob(job, text, hit, target);
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
        renderFail(job, target);
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
        renderFail(job, target);
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
            // 同语言：不需要翻译，静默只显示原文（名字类只记已完成，不删元素）
            renderSame(job, target);
            // 名字类 same 也进缓存（原文即译文）：下次名字 pass 再入队时走缓存命中，
            // 不再调接口。正文框不缓存——缓存会把“应静默删框”变成无意义的切换框。
            if (job.kind === 'name') cacheSet(text + '\n' + target, text);
            delete texts[job.id];
          } else if (r.translated === text) {
            // 服务端漏网的回显：不渲染、不缓存，走重试（名字类无重试按钮，记已完成）
            renderFail(job, target);
          } else {
            cacheSet(text + '\n' + target, r.translated);
            renderJob(job, text, r.translated, target);
            delete texts[job.id];
          }
        } else if (r.error === 'source language unclear' || r.error === 'text must be 1..5000 chars') {
          delete retries[job.id];
          renderSame(job, target); // 无法判定/不合法：静默只显示原文
          delete texts[job.id];
        } else if (r.error === 'rate_limited') {
          // 节流：自动延迟重试，不用用户点按钮；重试最多 2 次，之后留重试按钮。
          scheduleRateLimitRetry(job, target);
        } else {
          renderFail(job, target); // 429 / 上游失败：留原文+重试按钮，文本保留
        }
      });
      if (queue.length) flush();
    }).catch(function (err) {
      inFlight--;
      todo.forEach(function (job) {
        delete queuedIds[job.id];
        // 整批抛错：429（服务端 HTTP 429）按节流走 65 秒自动重试；
        // 网络失败等其他错留重试按钮，文本保留。401 走主进程的登出
        // 流程（会话失效回登录页），这里不另行处理。
        if (isRateLimitError(err)) scheduleRateLimitRetry(job, target);
        else renderFail(job, target);
      });
      if (queue.length) flush();
    });
  }

  // 入口：接管 root 下所有未入队的翻译占位；有 IntersectionObserver 时
  // 只观察、进视口才入队（滚动到才入队）；无 IO 时立即入队（旧行为，
  // Node 测试走这里）。data-trid 在入队时才摘掉，观察期间留着供去重。
  function observeTranslations(rootEl) {
    var scope = rootEl || (typeof document !== 'undefined' ? document : null);
    if (!scope || !scope.querySelectorAll) return;
    sweepViewportObserved();
    var io = getViewportObserver();
    var boxes = scope.querySelectorAll('[data-trid]');
    boxes.forEach(function (box) {
      var id = box.getAttribute('data-trid');
      if (!id || !texts[id] || queuedIds[id] || viewportObserved[id]) return;
      if (!io) {
        box.removeAttribute('data-trid');
        enqueue(box, id, false);
        return;
      }
      viewportObserved[id] = box;
      io.observe(box);
    });
  }

  // 名字类入口（v0.3.8）：接管 root 下 [data-tr-name] 元素（群名、版块名等），
  // 原文在 data-tr-name 属性里。先恢复：上次译过的先回原文再重判（切回语言时
  // 靠这一行恢复，不另记状态）；同语言/不明/过短不入队；已入队的不重复入队。
  function observeNameTranslations(rootEl) {
    var scope = rootEl || (typeof document !== 'undefined' ? document : null);
    if (!scope || !scope.querySelectorAll) return;
    var target = viewerLang();
    scope.querySelectorAll('[data-tr-name]').forEach(function (el) {
      var orig = el.getAttribute ? el.getAttribute('data-tr-name') : null;
      if (!orig) return;
      if (el.dataset && el.dataset.trJobId) return;
      if (el.dataset && el.dataset.trTarget) {
        if (el.dataset.trTarget === target) return;
        el.textContent = orig;
        delete el.dataset.trTarget;
      }
      var prose = proseOf(orig);
      if (!prose || prose.length < 2) return;
      var src = detectLang(prose);
      if (src === 'unknown' || src === target) return;
      var id = 'at' + (++seq);
      texts[id] = prose;
      if (el.dataset) el.dataset.trJobId = id;
      enqueue(el, id, false, 'name');
    });
  }

  // 语言切换后立即刷队列（缓存命中当场渲染，不等 250ms 的 coalesce 定时器）。
  function flushNow() { flush(); }

  var api = {
    viewerLang: viewerLang,
    setViewerLang: setViewerLang,
    ui: ui,
    uiFmt: uiFmt,
    applyUiLang: applyUiLang,
    _uiStrings: UI_STRINGS,
    detectLang: detectLang,
    proseOf: proseOf,
    placeholder: placeholder,
    observeTranslations: observeTranslations,
    observeNameTranslations: observeNameTranslations,
    flushNow: flushNow,
    setBatchFn: setBatchFn,
    init: init,
    _handleClick: handleClick,
    _cacheSize: function () { return cache.size; },
    _cacheMax: function () { return TRANSLATE_CACHE_MAX; },
    _pendingViewport: function () { return Object.keys(viewportObserved).length; },
    _isRateLimitError: isRateLimitError,
    _setRetryDelayMs: function (ms) { if (typeof ms === 'number' && ms > 0) RETRY_DELAY_MS = ms; },
    _resetForTest: function () {
      cache.clear(); texts = {}; retries = {}; queue = []; queuedIds = {};
      seq = 0; inFlight = 0; batchFn = null;
      if (flushTimer) { clearTimeout(flushTimer); flushTimer = null; }
      if (viewportObserver && viewportObserver.disconnect) viewportObserver.disconnect();
      viewportObserver = null;
      viewportObserved = {};
    },
  };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  if (typeof window !== 'undefined') window.ChimeraTranslate = api;
  if (typeof root !== 'undefined' && root) root.ChimeraTranslate = api;
})(typeof window !== 'undefined' ? window : (typeof globalThis !== 'undefined' ? globalThis : null));
