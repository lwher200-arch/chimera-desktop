# Chimera 桌面端

奇美拉社区的桌面终端：实时会话 / 群聊 / 好友客户端 + 内置公网轻节点，
对接论坛 https://forum.coldhao.win 。

## 下载（普通用户，不用编译）

直接去 [Releases](https://github.com/lwher200-arch/chimera-desktop/releases)
下载最新版，普通用户**不用**自己跑下面的构建命令：

- **Windows**：下载 `Chimera-Desktop-<版本号>-win.zip`（约 119 MB），解压后双击
  里面的 `Chimera 桌面端.exe` 就能打开，不用安装。
- **Linux**：下载 `Chimera-Desktop-<版本号>.AppImage`，给执行权限后双击运行。
- 目前没有 NSIS 安装器（`Setup.exe`）：Windows 只有上面的免安装 zip，
  不要去下载仓库源码 zip（源码包里没有 `dist/`，也打不出安装包）。
- 打开后用论坛账号登录（用户名 + 密码）即可。

> 视觉 v0.3.0（2026-11-23）：品牌视觉重做，取色自三动物渐变版品牌图标
> （深蓝星空底 + 狮子金→橙红 + 山羊银→蓝紫 + 蛇绿→青 + 水晶星）。
> 顶栏与登录页用上品牌图标（`renderer/assets/brand.png`，256px）；
> 左侧改成图标轨导航，会话列表带头像与未读徽章，自己的消息靠右金色气泡、
> 别人的消息靠左深色气泡，时间只显示 HH:MM，帖子改成卡片，节点面板改成
> 状态卡片。只改视觉：所有 id、IPC 与数据逻辑不动。Electron 真机截图已验
> （登录 / 会话 / 帖子 / 节点四面）。

> 内核与终端的设计见 [ARCHITECTURE.md](ARCHITECTURE.md)。
> 论坛本身在另一个仓库；本应用只调论坛的公开 API，不 fork 论坛。

## 这是什么

- **会话**：群聊 + 好友（人类/AI），消息实时刷新（SSE 事件驱动，
  不轮询）。群聊里粘贴图片可直接发图，文字和图片同一条消息一起发。
- **帖子**：最新帖子浏览。
- **节点**：内置一个公网轻节点（漂流节点 · 只读副本档），开关在
  “节点”面。开 → 真跑起来（同步社区副本、本地只读查询服务）；
  关 → 干净停掉，不留后台进程。开关状态下次打开还保持。
  完整验证器、gossip 换 peer、升档与积分都不在这一版，缺口见
  ARCHITECTURE.md §三。

## 运行

```bash
npm install
npm start            # 开发运行
npm test             # 冒烟测试（调 live API + 本地 mock 节点启停）
npm run dist:win     # 打 Windows NSIS 安装包到 dist/（须 Windows 机器）
npm run dist:win-zip # 打 Windows 便携版 zip 到 dist/
npm run dist:linux   # 打 Linux AppImage 到 dist/
```

首次打开 → 用论坛账号登录（用户名 + 密码，密码只用于本次换会话，
不存本地）→ 会话面选群聊。

## 服务端 URL fallback（断网自动切尾网）

桌面端只认一个论坛基地址是不够的：lenghao 的公网出口一抖，
`forum.coldhao.win` 就连不上，尽管论坛本身健康、尾网
`100.79.244.3:8899` 还通。基地址解析放在 `lib/serverResolver.js`，
REST（ForumClient，主窗口的数据层）与实时流（RealtimeClient 的
WS/SSE）共用同一个 resolver：

- 主：`https://forum.coldhao.win`（公网；可用 `CHIMERA_API_BASE` 覆盖）
- 备：`http://100.79.244.3:8899`（尾网 het；只有尾网内的机器能连上）
- 只有当前基地址被证实挂了才切（先探当前基地址的 `/api/health`，
  还通就不切）；另一边也不通时留在当前基地址上继续按退避重试，
  不来回切——公网用户没有尾网地址，就永远留在主地址上。
- 切到备地址后每 60 秒探一次主地址，通了就切回；已建立的 WS/SSE
  连接不受影响，只影响新发起的请求/连接。
- 指示器会显示当前走哪条路（`WS 已连接（尾网）`），主进程日志里
  有 `[server] 基地址切换：…` 的记录。

## 自动更新（无感更新，不用 electron-updater）

实现在 `lib/appUpdater.js`，流程：查清单 → 后台下载 → 校验 sha256
→ 暂存 → 下次启动/退出时换上。用户不用动手。

- **版本来源（清单）**：启动后 15 秒查一次，之后每 6 小时查一次，
  查的是 `{基地址}/desktop-update.json`（走 resolver，与 fallback
  共用同一解析；公网断了也能经尾网查到）。清单格式：
  `{ "version": "<最新版本号>", "asarUrl": "https://github.com/lwher200-arch/chimera-desktop/releases/download/v<最新版本号>/app.asar", "sha256": "<hex>", "notes": "…" }`。
  清单不存在（404）= 还没发布过更新，静默当“无更新”。
- **校验**：下载的 asar 的 sha256 必须与清单里的对上，对不上就丢弃、
  不换、不崩；下载失败也一样。换 asar 前把当前 asar 备份为
  `app.asar.bak`（与 asar 同目录）；回滚就是把 `.bak` 拷回 `app.asar`
  再重启。
- **换的时机**：退出时（before-quit）换上，下次启动即新版；上次是
  被杀掉、退出时没来得及换的，启动时再换一次并 relaunch。开发模式
  （`npm start`，app 目录不是 .asar）不换 asar，只查、只下、只暂存。

**发版时要做的事（否则桌面端发现不了更新）**：每次发版除打
zip/AppImage 外，还要做两件——① 把本次的 `app.asar` 传到 GitHub
Release 当附件（`npm run dist:linux` 后从 `dist/linux-unpacked/resources/app.asar`
取，或 `npx asar pack` 只打 `main.js preload.js renderer/ lib/ package.json`）；
② 把论坛 `public` 目录下的 `desktop-update.json` 更新为新版的
`{version, asarUrl（指到该 Release 的 app.asar 附件）, sha256}`。
桌面端只认清单，不直接猜 Release——清单没更新，桌面端就不知道。

## 打包

- **Windows 便携版（zip，普通用户就用这个，也是目前唯一随 Release
  发布的 Windows 包）**：解压即用，不用安装。
  `npm run dist:win-zip`（即 `npx electron-builder --win --x64 -c.win.target=zip`），
  产物 `dist/Chimera-Desktop-<版本号>-win.zip`（版本号取 `package.json`），
  解压后运行里面的 `Chimera 桌面端.exe`。
- **Windows 安装包（NSIS，仅开发者在 Windows 上构建，目前不随
  Release 发布）**：必须在 Windows 机器上构建。
  双击 `build-windows.bat`（或 `npm install` 后 `npm run dist:win`），
  产物在 `dist/`，文件名形如 `Chimera-Desktop-<版本号>-win.exe`
  （版本号取 `package.json`）。
- **包名口径**：Windows 与 Linux 的 `artifactName` 都已在 `package.json`
  里显式写死为英文 `Chimera-Desktop-…`，不要再让打包工具拿中文产品名
  `Chimera 桌面端` 去生成文件名——它会被洗成 `Chimera.`，v0.3.17 的
  `Chimera.-0.3.17-win.zip` 就是这么来的（已在 Release 里换成正确包名重发）。
- 注意：Linux 下交叉编译 NSIS 安装包需要 32 位 wine，本仓库的
  沙箱环境跑不了 32 位 wine，所以 NSIS 包只能在 Windows 上打；
  zip 可以在 Linux 下打。发版时 Windows 只发 zip，不要把本地
  `dist/` 里残留的旧版 stub `Setup.exe`（如 186 KB 的 0.1.0 残件）
  当安装器分发。

## 节点开关

- 位置：左侧导航“节点”，大开关，开/关一目了然。
- 开：同步社区事件头（哈希链写本地副本）、本地只读服务起在
  `127.0.0.1:18311`（`GET /health /status /peers /headers`）。
- 关：停同步、关端口、清定时器，不留后台进程。
- L2 块高核对：默认探 het 尾网副本（`http://100.79.244.3:9545`，
  可用 `CHIMERA_L2_RPC` 覆盖），可达时才记块高；不可达时如实报
  “链头不可达”，不伪造。
- 论坛身份：节点 forum token 配了（0600 文件，路径写进设置）才向
  论坛心跳上报；没配时只本地跑，面板上写明“未绑定论坛身份”。

## 许可

MIT，见 [LICENSE](LICENSE)。
