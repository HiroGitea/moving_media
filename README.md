# moving_media

相机储存卡备份工具，使用 Rust + egui 构建。跨平台（macOS / Windows / Linux）。

将 SD 卡上的照片和视频备份到指定目录，通过 SQLite 数据库记录文件信息，并用 **BLAKE3** 散列值校验完整性，防止备份损坏或重复备份。

---

## 功能

- **自动检测** — 插入 SD 卡后自动检测设备（后台轮询），顶部横幅提示可选储存设备；选择设备后，再手动决定是否扫描或备份
- **EXIF 日期提取** — 从照片 EXIF 数据（`DateTimeOriginal`）读取拍摄日期，自动命名目标文件夹
- **多日期处理** — 检测到跨天拍摄时，询问是拆分为多个文件夹还是合并为日期范围文件夹
- **同名文件夹** — 同一天的照片和视频使用相同文件夹名（分别存入 Photos/ 和 Videos/）
- **BLAKE3 校验** — 硬件加速哈希（mmap + rayon 并行），复制完成后立即对比，确保文件无损
- **去重备份** — 同一文件（相同哈希）无论来自哪张 SD 卡都只备份一次
- **丢失文件恢复** — 备份时若数据库有记录但目标文件已丢失，自动删除旧记录并重新备份
- **多卡支持** — 同一次旅行分布在多张 SD 卡的文件自动合并到同一文件夹
- **备份后全量校验** — 备份完成后对所有复制的文件，SD 卡与磁盘各重新哈希一遍，双重确认无损
- **mtime 抽检** — 定期校验时递归检测修改时间有变化的文件夹，对变化的 session 全部文件重新哈希；无变化的跳过
- **数据库版本管理** — `PRAGMA user_version` 记录 schema 版本，启动时自动迁移；版本高于程序时拒绝打开防止损坏
- **数据库镜像** — 主数据库存于外置硬盘，本地同步保留镜像，防止单点故障
- **图形界面** — egui 原生 GUI，支持进度显示、交互输入、文件夹浏览、实时日志面板
- **首次初始化** — 目标目录存在但数据库未创建时，自动引导初始化

---

## 编译

### 前置要求

安装 Rust 工具链（[rustup.rs](https://rustup.rs/)）：

```sh
# macOS / Linux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Windows
# 下载并运行 https://win.rustup.rs/
```

macOS 还需要 Xcode Command Line Tools：

```sh
xcode-select --install
```

### 开发构建

```sh
cargo build
# 产物：target/debug/moving_media
```

### 发布构建（优化，体积更小）

```sh
cargo build --release
# 产物：target/release/moving_media
```

首次构建会自动通过 `curl` 下载 NotoEmoji-Regular.ttf（约 418KB）并编译进二进制，之后不再重复下载。

### 运行

```sh
cargo run --release
# 或直接运行产物
./target/release/moving_media
```

### 运行测试

测试全部在项目目录内进行，不操作生产目录：

```sh
cargo test
```

---

## 使用

### 主界面

启动后显示主界面，包含以下操作：

| 按钮 | 说明 |
|---|---|
| 🔍 扫描（检查已备份） | 扫描 SD 卡，对比数据库，显示已备份 / 未备份数量 |
| 💾 备份 | 扫描并备份未备份的文件；完成后全量校验 SD 卡与磁盘 |
| ✅ 校验已备份文件 | 重新计算已备份文件哈希，检测损坏 |
| 📋 查看备份记录 | 列出数据库中所有备份记录 |
| 🔄 重建索引 | 扫描备份目录的实际文件，将不在数据库中的文件补录；并行哈希、每 64 个文件批量提交，支持断点继续；适用于数据库丢失、文件手动复制、换机器等情况 |

### 备份流程

1. 将 SD 卡插入电脑，等待挂载
2. 在顶部横幅点击「选择此卡」，或点击「浏览…」手动选择源路径
3. 按需要点击「🔍 扫描检测」查看已备份 / 待备份数量，或直接点击「💾 备份」
4. 若照片跨越多天，选择**拆分**或**合并**文件夹
5. 输入文件夹后缀（例如 `新宿`、`东京`、`香港`）
6. 程序自动复制文件（先写临时文件，校验后 rename，防止中断留下残缺文件）
7. 全部复制完成后，对每个文件重新读取 SD 卡与磁盘各一遍，双重确认哈希一致

### 文件夹命名规则

| 情形 | 格式 | 示例 |
|---|---|---|
| 单日 | `YYYYMMDD_后缀` | `20260322_新宿` |
| 多日合并 | `YYYYMMDD-YYYYMMDD_后缀` | `20260320-20260322_东京` |
| 多日拆分 | 每天单独 `YYYYMMDD_后缀` | `20260320_抵达`、`20260321_市区` |

日期从照片 EXIF `DateTimeOriginal` 提取；视频（及 ARRI `.ari`、RED `.r3d`）使用文件修改时间作为 fallback。

### 多张 SD 卡

同一次旅行的照片分布在多张卡上时，依次插入卡，重复执行备份即可。已备份的文件（相同 BLAKE3 哈希）自动跳过，新文件追加到同名文件夹。

### 定期抽检（mtime 驱动）

点击「✅ 校验已备份文件」，或在支持的界面手动触发抽检。程序会：

1. 递归扫描每个 session 文件夹及其子目录的修改时间
2. 将最新 mtime 与数据库中该 session 的最近校验时间（`verified_at`）对比
3. 有变化的 session：对数据库中记录的**所有**文件重新哈希
4. 无变化的 session：跳过，不读文件

> **注意**：文件内容被静默篡改（大小不变）时，父目录 mtime 不变，但文件自身 mtime 会更新，同样会被检测到。

---

## 架构

```
moving_media/
├── Cargo.toml
├── build.rs      # 构建时下载并嵌入 NotoEmoji-Regular.ttf
├── assets/
│   └── NotoEmoji-Regular.ttf
├── README.md
├── CHANGELOG.md
└── src/
    ├── main.rs       # 程序入口，初始化 eframe 窗口，panic 钩子
    ├── app.rs        # GUI 主体，基于状态机的界面逻辑
    ├── config.rs     # 配置加载（路径、上次 SD 卡路径、数据库位置）
    ├── db.rs         # SQLite 数据库封装（CRUD + 版本迁移 + 镜像同步）
    ├── hash.rs       # BLAKE3 哈希（mmap + rayon 并行）
    ├── scanner.rs    # SD 卡扫描、文件分类、EXIF 日期提取
    ├── backup.rs     # 备份核心逻辑（复制、校验、去重、全量验证、mtime 抽检）
    └── watcher.rs    # 储存卡自动检测（后台轮询线程）
```

### 模块职责

#### `main.rs`
初始化 `eframe` 窗口（680×560），注册全局 panic 钩子（写入系统本地数据目录下的 `moving_media/crash.log`），启动 `app::App`。

#### `app.rs`
GUI 状态机，`Screen` 枚举控制当前界面：

```
Home → ScanResult → NamingSingle/NamingMulti → Running → SpotCheckDone
     ↑                DateDecision ──────────────────↗
     └──── VerifyPick → VerifyDone
     └──── ListRecords
     └──── Reindex
```

- 顶部横幅显示已检测到的储存设备，等待手动选择后再扫描
- 底部可调整高度的实时日志面板
- 后台任务通过 `Arc<Mutex<TaskState>>` 报告进度
- 备份线程内直接执行全量校验，完成后跳转 SpotCheckDone

#### `config.rs`
加载媒体根目录路径，优先读取环境变量，否则使用配置文件 (`~/.config/moving_media/config`)，最后回落默认值。同时持久化上次 SD 卡路径（`last_source`）：

```
MOVING_MEDIA_PHOTOS     →  Photos 根目录
MOVING_MEDIA_VIDEOS     →  Videos 根目录
MOVING_MEDIA_DB_BACKUP  →  本地镜像 DB 目录
```

#### `db.rs`
封装 SQLite 操作，关键设计：
- **版本管理**：`PRAGMA user_version` 记录 schema 版本（当前 v1），启动时 `migrate()` 自动迁移；旧库（表已存在但 version=0）自动打标；版本高于程序时拒绝打开
- WAL 模式，提升并发读性能
- `hash` 唯一索引，O(1) 去重查询
- 每次写入后自动 `VACUUM INTO` 同步到镜像
- 重建索引走批量事务，避免逐条提交
- `open_readonly()` 支持主盘离线时读镜像

#### `hash.rs`
使用 BLAKE3 计算哈希：
- 文件 ≥ 128KB：`memmap2` 内存映射 + `blake3::Hasher::update_rayon()` 多核并行
- 文件 < 128KB：BufReader 流式读取
- 重建索引阶段按批次并行读取多个文件，再单线程批量写入 SQLite

#### `scanner.rs`
递归遍历源目录（`walkdir`），按扩展名分类为 Photo / Video，用 `kamadak-exif` 从 RAW/JPEG 文件提取拍摄日期。

#### 重建索引
- 先递归扫描 Photos/ 与 Videos/，按路径稳定排序，保证断点恢复位置可重复
- 每批 64 个文件并行计算哈希，写库时使用 `INSERT OR IGNORE` 去重
- 每批提交成功后写入检查点；程序中断后，下次会从上次完成的最后一个文件后继续
- 全部完成后自动清除检查点

#### `backup.rs`
**备份流程**（`backup_single`）：
1. 计算源文件 BLAKE3 哈希
2. 查询数据库是否已存在该哈希
   - 存在且目标文件在磁盘上 → 跳过
   - 存在但目标文件丢失 → 删除旧记录，继续备份
3. 先写临时文件（`.filename.tmp`），校验哈希通过后 `rename` 到最终路径（原子操作，防中断留下残缺文件）
4. 写入数据库并同步镜像

**备份后全量校验**（`verify_backup`）：
- 对所有成功复制的文件，重新读取 SD 卡（源）与磁盘（目标）各一遍
- 任一侧哈希不符即报告问题

**mtime 抽检**（`spot_check`）：
- 递归扫描 session 文件夹（含子目录）所有文件/目录的 mtime
- 与该 session 最近 `verified_at` 对比
- 有变化 → 校验该 session 全部文件；无变化 → 跳过

#### `watcher.rs`
后台线程每 2 秒轮询系统卷（macOS: `/Volumes/`，Linux: `/media/` 和 `/mnt/`，Windows: 可移除盘符），检测新插入的储存设备，但不自动读取内容；通过 `egui::Context::request_repaint()` 触发界面更新。

---

## 数据库结构

每个媒体根目录（Photos/、Videos/）各有一个 `moving_media.db`（当前 schema 版本：v1）：

```sql
CREATE TABLE files (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    filename     TEXT NOT NULL,
    dest_path    TEXT NOT NULL UNIQUE,  -- 相对于媒体根目录
    hash         TEXT NOT NULL,         -- BLAKE3 哈希（64位十六进制）
    hash_algo    TEXT NOT NULL DEFAULT 'blake3',
    file_size    INTEGER NOT NULL,
    source_path  TEXT,                  -- 来源 SD 卡路径（记录用）
    session_name TEXT NOT NULL,         -- 例如 "20260322_新宿"
    backed_up_at TEXT NOT NULL,         -- ISO8601 时间戳
    verified_at  TEXT                   -- 最近一次校验时间
);

CREATE UNIQUE INDEX idx_hash ON files(hash);
```

数据库镜像路径：

```
主 DB（外置硬盘）                    镜像 DB（本机）
Photos/moving_media.db    →   <系统本地数据目录>/moving_media/photos_moving_media.db
Videos/moving_media.db    →   <系统本地数据目录>/moving_media/videos_moving_media.db
```

外置硬盘未挂载时，扫描功能自动 fallback 到镜像（只读）。

### 升级 schema

在 `db.rs` 中：
1. 将 `CURRENT_VERSION` 加 1
2. 实现 `migration_vN()` 方法（通常是 `ALTER TABLE files ADD COLUMN ...`）
3. 在 `migrate()` 的 `match` 中加一个分支

---

## 支持的文件类型

| 扩展名 | 分类 |
|---|---|
| `.ARW .RAF .CR3 .NEF .DNG .JPG .JPEG .HEIC .ARI .R3D` | 照片 → Photos/ |
| `.MP4 .MOV .MTS .M2TS .AVI .MXF` | 视频 → Videos/ |
| 其他 | 跳过 |

---

## 依赖

| Crate | 版本 | 用途 |
|---|---|---|
| `eframe` | 0.27 | 原生窗口框架 |
| `egui` | 0.27 | 即时模式 GUI |
| `rusqlite` (bundled) | 0.31 | SQLite，静态链接无需系统库 |
| `blake3` (rayon) | 1 | BLAKE3 哈希，多核并行 |
| `memmap2` | 0.9 | 内存映射大文件 |
| `walkdir` | 2 | 递归目录遍历 |
| `kamadak-exif` | 0.5 | 读取 EXIF DateTimeOriginal |
| `chrono` | 0.4 | 日期时间处理 |
| `rfd` | 0.14 | 跨平台原生文件对话框 |
| `dirs` | 5 | 跨平台用户目录路径 |
| `rand` | 0.8 | 抽检随机采样 |
| `anyhow` | 1 | 错误处理 |
| `tempfile` (dev) | 3 | 测试用临时目录 |

---

## 配置

通过环境变量自定义路径（可写入 shell profile 持久化）：

```sh
export MOVING_MEDIA_PHOTOS=/Volumes/My_Files/Backup/Media/Photos
export MOVING_MEDIA_VIDEOS=/Volumes/My_Files/Backup/Media/Videos
export MOVING_MEDIA_DB_BACKUP="$HOME/.local/share/moving_media"
```

Windows 示例：

```cmd
set MOVING_MEDIA_PHOTOS=D:\Backup\Media\Photos
set MOVING_MEDIA_VIDEOS=D:\Backup\Media\Videos
set MOVING_MEDIA_DB_BACKUP=%LOCALAPPDATA%\moving_media
```
