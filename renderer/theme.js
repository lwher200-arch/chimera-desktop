'use strict';
/* renderer/theme.js — 深浅主题（v0.3.4）。
 * 纯数据 + 纯函数，方便 Node 里直接测：
 *   themeVars(name) 只回变量表；applyTheme(doc, name) 把 data-theme 写到
 *   <html> 并逐个写变量；currentTheme()/saveTheme() 读写 localStorage。
 * 深色是 v0.3.0 的品牌夜空（默认）；浅色是暖纸底 + 同款品牌金/紫。
 * styles.css 里 :root 与 [data-theme="light"] 各有一份同名变量表，
 * 这里的表与之同值，测试断言两边一致（防改一处漏一处）。 */
const THEME_KEY = 'chimera_theme';
const THEMES = {
  dark: {
    '--bg0': '#070d1f',
    '--bg1': '#0b142c',
    '--bg2': '#101c38',
    '--surface': '#131f3d',
    '--surface-2': '#182747',
    '--surface-3': '#1d2f52',
    '--border': 'rgba(148, 163, 255, 0.14)',
    '--border-strong': 'rgba(148, 163, 255, 0.24)',
    '--text': '#eef1fa',
    '--text-2': '#c6cddd',
    '--muted': '#93a0bd',
    '--muted-2': '#6b7896',
    '--gold': '#eab543',
    '--gold-2': '#e2703f',
    '--orange': '#e2703f',
    '--violet': '#7b6cf0',
    '--teal': '#2fbf9b',
    '--cyan': '#6dd6ff',
    '--green': '#3ecf6f',
    '--red': '#f26d6d',
    '--brand-grad': 'linear-gradient(90deg, #f6d47e, #e2703f 55%, #9d8cff)',
    '--glow-violet': 'rgba(123, 108, 240, 0.20)',
    '--glow-teal': 'rgba(47, 191, 155, 0.14)',
    '--glow-gold': 'rgba(234, 181, 67, 0.12)',
    '--active-bg': 'linear-gradient(90deg, rgba(242, 193, 78, 0.16), rgba(123, 108, 240, 0.12))',
    '--nav-active-text': '#ffd98a',
    '--input-bg': 'rgba(7, 13, 31, 0.7)',
    '--panel-bg': 'rgba(11, 20, 44, 0.6)',
    '--topbar-bg': 'rgba(11, 20, 44, 0.88)',
    '--chat-bg': 'rgba(7, 13, 31, 0.35)',
    '--post-card-bg': 'linear-gradient(180deg, var(--surface), rgba(19, 31, 61, 0.75))',
    '--login-card-bg': 'linear-gradient(180deg, rgba(24, 39, 71, 0.92), rgba(16, 28, 56, 0.94))',
    '--log-bg': '#050a18',
    '--log-text': '#a9b6d6',
    '--switch-off': '#5a6478',
    '--scrollbar': '#26355c',
    '--scrollbar-hover': '#33477a',
    '--author': '#ffd98a',
  },
  light: {
    '--bg0': '#f3efe7',
    '--bg1': '#faf8f2',
    '--bg2': '#efe9dc',
    '--surface': '#ffffff',
    '--surface-2': '#f0ebdd',
    '--surface-3': '#e6dfcd',
    '--border': 'rgba(90, 70, 20, 0.16)',
    '--border-strong': 'rgba(90, 70, 20, 0.28)',
    '--text': '#1d2333',
    '--text-2': '#3d465c',
    '--muted': '#6b7280',
    '--muted-2': '#9aa1b0',
    '--gold': '#8a5a00',
    '--gold-2': '#c25e2e',
    '--orange': '#c25e2e',
    '--violet': '#6a5ae0',
    '--teal': '#0f8a6c',
    '--cyan': '#0f7ea8',
    '--green': '#1e8e4a',
    '--red': '#d33',
    '--brand-grad': 'linear-gradient(90deg, #b8860b, #c25e2e 55%, #6a5ae0)',
    '--glow-violet': 'rgba(123, 108, 240, 0.10)',
    '--glow-teal': 'rgba(47, 191, 155, 0.08)',
    '--glow-gold': 'rgba(234, 181, 67, 0.16)',
    '--active-bg': 'linear-gradient(90deg, rgba(234, 181, 67, 0.22), rgba(123, 108, 240, 0.10))',
    '--nav-active-text': '#7a5200',
    '--input-bg': '#ffffff',
    '--panel-bg': 'rgba(255, 255, 255, 0.75)',
    '--topbar-bg': 'rgba(255, 255, 255, 0.88)',
    '--chat-bg': 'rgba(255, 255, 255, 0.5)',
    '--post-card-bg': 'linear-gradient(180deg, #ffffff, #f7f2e6)',
    '--login-card-bg': 'linear-gradient(180deg, #ffffff, #f7f3e8)',
    '--log-bg': '#1a2233',
    '--log-text': '#c6d2ea',
    '--switch-off': '#c9c2ae',
    '--scrollbar': '#d8d0ba',
    '--scrollbar-hover': '#c2b892',
    '--author': '#8a5a00',
  },
};

function themeVars(name) {
  const t = THEMES[name] || THEMES.dark;
  return { ...t };
}

function normalizeTheme(v) {
  return v === 'light' ? 'light' : 'dark';
}

function currentTheme(storage) {
  try {
    const s = storage || (typeof localStorage !== 'undefined' ? localStorage : null);
    return normalizeTheme(s && s.getItem ? s.getItem(THEME_KEY) : null);
  } catch { return 'dark'; }
}

function saveTheme(storage, name) {
  try {
    const s = storage || (typeof localStorage !== 'undefined' ? localStorage : null);
    if (s && s.setItem) s.setItem(THEME_KEY, normalizeTheme(name));
  } catch { /* 隐私模式等写不进时静默保持深色 */ }
}

function applyTheme(doc, name) {
  const n = normalizeTheme(name);
  const el = doc && doc.documentElement;
  if (!el) return n;
  el.setAttribute('data-theme', n);
  const vars = themeVars(n);
  if (el.style && typeof el.style.setProperty === 'function') {
    for (const [k, v] of Object.entries(vars)) el.style.setProperty(k, v);
  }
  return n;
}

if (typeof window !== 'undefined') window.ChimeraTheme = { THEME_KEY, THEMES, themeVars, normalizeTheme, currentTheme, saveTheme, applyTheme };
if (typeof module !== 'undefined' && module.exports) module.exports = { THEME_KEY, THEMES, themeVars, normalizeTheme, currentTheme, saveTheme, applyTheme };
