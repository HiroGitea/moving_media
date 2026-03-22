# CHANGELOG

## [Unreleased]

### 新增
- **储存卡自动检测**（`watcher.rs`）：后台线程每 2 秒轮询系统卷，插入储存卡后只提示设备，等待手动选择后再扫描；支持 macOS（`/Volumes/`）、Linux（`/media/` / `/mnt/`）、Windows（可移除盘符）
- **备份后自动抽检**（`backup.rs` `spot_check()`）：备份完成后自动对所有 session 文件夹各随机抽取一个文件重新计算哈希，完整覆盖一轮，结果显示在独立界面（`SpotChecking` / `SpotCheckDone`）；每个文件间隔 5 秒（防止 I/O 集中）
- **数据库未初始化提示**：启动时若目标目录存在但 `moving_media.db` 缺失，自动进入 Setup 界面并提示点击「确认并初始化」
- **实时日志面板**：底部可调整高度的日志窗口，所有操作实时显示，支持清除
- **重建索引断点继续**：重建索引每 64 个文件提交一次，并在每批成功后写入检查点；程序中断或关闭后可从上次完成位置继续
- **CJK 字体自动加载**：macOS 动态搜索 PingFang.ttc（路径含哈希），fallback 至 STHeiti / Hiragino；Windows / Linux 支持 Noto/微软雅黑/黑体
- **Panic 钩子**：未捕获异常写入系统本地数据目录下的 `moving_media/crash.log`
- **配置文件持久化**：路径配置保存到 `~/.config/moving_media/config`，重启后自动恢复

### 变更
- 哈希算法从 **SHA-256** 升级为 **BLAKE3**（硬件加速，mmap + rayon 多核并行，≥128KB 文件并行哈希）
- **重建索引性能优化**：从串行“逐文件查询 + 逐文件写入 + 逐次镜像同步”改为按批并行哈希、`INSERT OR IGNORE` 去重、每 64 个文件批量事务提交
- 数据库文件名从 `backup.db` 改为 `moving_media.db`
- 数据库列名 `sha256` 改为 `hash`，新增 `hash_algo TEXT DEFAULT 'blake3'`
- 唯一索引从 `idx_sha256` 改为 `idx_hash`
- 镜像 DB 路径从 `photos_backup.db` 改为 `photos_moving_media.db`（视频同理）
- 窗口尺寸从 520×400 调整为 680×560
- UI 按钮尺寸统一（`BTN_WIDE=140×32`，`BTN_MED=110×32`，`BTN_BACK=90×28`）
- 移除 `sha2` 依赖，新增 `blake3`（rayon feature）、`memmap2`、`rand`

### 修复
- `eframe::run_native` 回调返回类型：移除多余的 `Ok()` 包装
- `suffix_single.trim()` 借用冲突：改为 `.trim().to_string()` 提前克隆
- `session_name()` 单日格式字符串缺少 `_` 分隔符
