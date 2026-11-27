'use strict';
/**
 * lib/appMenu.js — macOS 原生应用菜单（v0.3.17 修复轮）。
 *
 * 背景：应用从没自己建过菜单，macOS 上一直显示 Electron 默认英文菜单
 * （File / Edit / View / Window / Help，Minimize / Zoom / Close 全是英文），
 * 而应用内早已是中文——违反“切中文整个软件看到的都是中文”。
 * 修法：按字典 menu.* 键逐项写死 label（不用 role 的默认文案，role 只
 * 管行为），语言切换时重建。
 *
 * 平台口径：只在 darwin 上建菜单。Electron 在 Windows/Linux 上不设菜单
 * 时本就不显示菜单栏（lenghao 走查截图一向没有菜单栏），这里不碰它们，
 * 别让 Linux/Windows 突然冒出一条菜单栏。
 *
 * 测试：buildAppMenuTemplate 是纯函数（语言 + 字典对象 → 模板），
 * smoke.js §24 直接断言中英两套文案；applyAppMenu 在非 darwin 上返回
 * false 且绝不碰 Menu。原生 macOS 菜单栏在 lenghao Linux 走查里看不到，
 * 不进 CDP 断言，Mac 实机效果待发版后在 Mac 上验。
 */
const FORUM_URL = 'https://forum.coldhao.win';

function fmt(text, brand) {
  return String(text == null ? '' : text).replace('%s', brand);
}

// strings: UI_STRINGS[lang]（translate.js 字典对象）；缺键时回退英文原文。
function buildAppMenuTemplate(lang, strings) {
  const S = (strings && typeof strings === 'object') ? strings : {};
  const get = (key, fallback) => (S[key] != null && S[key] !== '') ? S[key] : fallback;
  const brand = get('brand.name', 'Chimera');
  const sep = { type: 'separator' };
  return [
    {
      label: brand,
      submenu: [
        { label: fmt(get('menu.appAbout', 'About %s'), brand), role: 'about' },
        sep,
        { label: fmt(get('menu.appHide', 'Hide %s'), brand), role: 'hide' },
        { label: get('menu.appHideOthers', 'Hide Others'), role: 'hideOthers' },
        { label: get('menu.appShowAll', 'Show All'), role: 'unhide' },
        sep,
        { label: fmt(get('menu.appQuit', 'Quit %s'), brand), role: 'quit' },
      ],
    },
    {
      label: get('menu.file', 'File'),
      submenu: [
        { label: get('menu.fileClose', 'Close Window'), role: 'close' },
      ],
    },
    {
      label: get('menu.edit', 'Edit'),
      submenu: [
        { label: get('menu.editUndo', 'Undo'), role: 'undo' },
        { label: get('menu.editRedo', 'Redo'), role: 'redo' },
        sep,
        { label: get('menu.editCut', 'Cut'), role: 'cut' },
        { label: get('menu.editCopy', 'Copy'), role: 'copy' },
        { label: get('menu.editPaste', 'Paste'), role: 'paste' },
        { label: get('menu.editSelectAll', 'Select All'), role: 'selectAll' },
      ],
    },
    {
      label: get('menu.view', 'View'),
      submenu: [
        { label: get('menu.viewReload', 'Reload'), role: 'reload' },
        { label: get('menu.viewFullscreen', 'Toggle Full Screen'), role: 'togglefullscreen' },
      ],
    },
    {
      label: get('menu.window', 'Window'),
      submenu: [
        { label: get('menu.windowMinimize', 'Minimize'), role: 'minimize' },
        { label: get('menu.windowZoom', 'Zoom'), role: 'zoom' },
        { label: get('menu.windowClose', 'Close'), role: 'close' },
        sep,
        { label: get('menu.windowFront', 'Bring All to Front'), role: 'front' },
      ],
    },
    {
      label: get('menu.help', 'Help'),
      submenu: [
        {
          label: get('menu.helpForum', 'Visit Chimera Forum'),
          click: () => {
            // 只在真应用里点到：electron 在测试进程里不存在，这里才 require。
            require('electron').shell.openExternal(FORUM_URL);
          },
        },
      ],
    },
  ];
}

// 只在 darwin 上建菜单；其他平台返回 false 且什么都不做。
// lang 非 zh/en 时按 zh 处理（与字典 ui() 的回退口径一致）。
function applyAppMenu(lang, strings) {
  if (process.platform !== 'darwin') return false;
  const l = (lang === 'en') ? 'en' : 'zh';
  const { Menu } = require('electron');
  Menu.setApplicationMenu(Menu.buildFromTemplate(buildAppMenuTemplate(l, strings)));
  return true;
}

module.exports = { buildAppMenuTemplate, applyAppMenu, FORUM_URL };
