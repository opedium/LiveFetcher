# DouyinFetcher

DouyinFetcher is a Python-based tool for collecting real-time Douyin web live-room data through WebSocket + Protobuf parsing. It is designed for stable, long-running capture sessions and structured local logging.

## Language / 语言

- [English](#english-guide)
- [中文](#中文说明)

## English Guide

### Overall Features

- Real-time data capture from Douyin web live rooms.
- Message parsing for chat, gifts, likes, member entry, room stats, fans club, rank, and control events.
- YAML-driven behavior control (enable/disable handlers without modifying Python source).
- Inactivity watchdog with automatic reconnect to reduce silent disconnections.
- Retry and heartbeat controls for long-running stability.
- Gift combo aggregation with delayed finalization to avoid duplicate gift rows.
- User identity fallback strategy: display ID -> short ID -> long ID.
- Session-based CSV logging for clean data separation and traceability.
- UTF-8-safe logging for better Windows console compatibility.

## 中文说明

### 功能总览

- 支持抖音网页版直播间实时数据抓取。
- 支持解析聊天、礼物、点赞、进场、统计、粉丝团、排行榜、控制消息等。
- 支持通过 YAML 配置开关控制各类消息处理逻辑。
- 内置空闲看门狗，长时间无业务数据可自动重连。
- 支持心跳与重试参数配置，提升长时间运行稳定性。
- 礼物连击聚合统计，减少重复记录并提升价值计算准确度。
- 用户 ID 多级回退（展示 ID -> 短 ID -> 长 ID），增强身份一致性。
- CSV 会话化日志输出，便于数据分析与问题追溯。
- Windows 环境下 UTF-8 友好日志输出，降低编码报错概率。

### 快速开始

1. 在 `main.py` 中设置 `live_id`。
2. （可选）在 `message_handlers.yml` 中调整消息开关与日志配置。
3. 运行 `python main.py` 开始采集。
4. 日志默认输出到 `logs/` 目录。

### Cookie 设置

- 推荐在 `message_handlers.yml` 中填写 `auth_cookie` 以提高稳定性。
- 可用浏览器 DevTools 抓取，或使用 `python get_cookie.py` 自动写入。
- 详细说明见 [get_cookie.md](get_cookie.md)。

## Requirements

- Python 3.9+
- Node.js 18+
- `sign.js` and `a_bogus.js` in project root

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

1. Open `main.py` and set your target `live_id`.
2. (Optional) Edit `message_handlers.yml` to adjust handler switches and runtime settings.
3. Run:

```bash
python main.py
```

## Configuration (message_handlers.yml)

Main options:

- `heartbeat_interval`: heartbeat send interval (seconds)
- `retry_on_failure`: whether to retry on connection failure
- `max_retries`: maximum retries per connection cycle
- `retry_delay_seconds`: delay between retries
- `gift_combo_delay_seconds`: combo aggregation delay
- `inactivity_interval_seconds`: watchdog timeout threshold
- `auth_cookie`: optional full browser cookie for higher reliability

Per-message controls:

- `enabled`: enable/disable specific message handler
- `log_to_csv`: enable/disable CSV logging
- `show_entry`: show/hide member-entry prints
- `log_interval_seconds`: interval for periodic stats logging

## Cookie Setup (Recommended)

`auth_cookie` in `message_handlers.yml` is optional, but providing a valid logged-in browser cookie usually improves connection and message stability.

### How to get cookie

1. Open Douyin live page in your browser and log in.
2. Press `F12` to open Developer Tools.
3. Go to `Network`, refresh the page.
4. Click any request to `live.douyin.com`.
5. Copy the full `cookie` value from Request Headers.
6. Paste it into `auth_cookie` in `message_handlers.yml`.

### Script helper (faster)

You can also use the helper script to update `auth_cookie` automatically:

```bash
python get_cookie.py
```

Then paste your full cookie string when prompted. The script updates `message_handlers.yml` and creates a timestamped backup file.

### Notes

- Keep the cookie in **one single line**.
- Do not share your cookie publicly.
- If capture fails after some time, refresh cookie from browser and update config.
- If `auth_cookie` is empty, the program falls back to `ttwid/msToken` flow (may be less stable in some environments).

## How to Record a Stream (Data Logging)

This project records live-room data events (chat/gift/entry/viewer stats) into CSV files.

1. Set target `live_id` in `main.py`.
2. In `message_handlers.yml`, enable the handlers you want and set `log_to_csv: true`.
3. Ensure `logging.folder` is set (default `logs`).
4. Run:

```bash
python main.py
```

5. Keep the process running during the stream.
6. Stop with `Ctrl + C` when finished.
7. Check generated CSV files in `logs/`.

### Suggested recording config

- `WebcastChatMessage.log_to_csv: true`
- `WebcastGiftMessage.log_to_csv: true`
- `WebcastMemberMessage.log_to_csv: true`
- `WebcastRoomUserSeqMessage.log_to_csv: true`
- `inactivity_interval_seconds`: 30-300 depending on your tolerance for reconnects
- `gift_combo_delay_seconds`: around 3-5 for combo consolidation

## Output

Logs are written to the `logs/` directory in CSV format, typically including:

- `*_chat_log.csv`
- `*_gift_log.csv`
- `*_enter_log.csv`
- `*_viewer_log.csv`

Filenames are prefixed with a stream session ID (`liveId_YYYYMMDD_HHMMSS`) for session isolation.

## Related Guides

- Cookie setup guide: [get_cookie.md](get_cookie.md)
- Cookie helper script: `get_cookie.py`

## Attribution

This project is adapted from:

https://github.com/saermart/DouyinLiveWebFetcher

## Disclaimer

This project is for learning and research purposes only. Do not use it for commercial abuse, privacy infringement, or any illegal activity.
