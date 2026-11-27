@echo off
REM Chimera 桌面端 Windows 安装包一键构建（在 Windows 机器上跑）
REM 要求：已装 Node.js 20+（https://nodejs.org）
REM 产物：dist\Chimera 桌面端 Setup <版本号>.exe（版本号见 package.json）
REM 注意：这是开发者打包脚本，不是普通用户的安装方法；普通用户
REM 直接去 GitHub Releases 下载 Chimera-<版本号>-win.zip 解压即用。
setlocal
cd /d %~dp0
echo [1/2] 安装依赖 ...
call npm install
if errorlevel 1 (
  echo 依赖安装失败，请检查网络 / Node 版本。
  exit /b 1
)
echo [2/2] 构建 Windows 安装包（NSIS）...
call npm run dist:win
if errorlevel 1 (
  echo 构建失败。
  exit /b 1
)
echo.
echo 构建完成，安装包在 dist 目录：
dir /b dist\*.exe
endlocal
