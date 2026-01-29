# Excel2SQL

把电力/信息披露类 Excel 导入 MySQL，并提供 Web 看板与自动化导入能力。

## 功能概览

- Web 界面上传/导入 Excel
- API 导入与批量导入
- 自动生成日表：`power_data_YYYYMMDD`
- 生成/更新缓存表：`cache_daily_hourly`
- COS 每日自动拉取 + 导入 + 写缓存
- 首页“每日数据监控”banner（读取 COS 运行状态）
- 应用内自动调度（部署到服务器即可按时自动执行）

## 环境要求

- Python 3.7+
- MySQL 5.7+
- pip

## 安装与启动

1. 安装依赖

```bash
pip install -r requirements.txt
```

2. 配置数据库连接  
   编辑 `config.py` 中 `DB_CONFIG`

3. 启动服务

```bash
uvicorn api:app --host 0.0.0.0 --port 8000
```

打开：`http://localhost:8000`

## 核心 API

### 上传与导入

- 上传文件到 `data/`：

```
POST /upload
form-data: file
```

- 按文件名导入（文件需在 `data/` 内）：

```
POST /import
form-data: filename
```

- 导入 `data/` 下全部文件：

```
POST /import-all
```

- 查看 `data/` 文件列表：

```
GET /files
```

### 缓存相关

- 生成电价缓存（按已有日表批量更新 `cache_daily_hourly`）：

```
POST /api/generate-price-cache
```

- 全量天气+缓存更新：

```
POST /api/generate-daily-hourly-cache
```

- 手动更新天气（会同步缓存天气字段）：

```
POST /api/update-weather
```

### COS 运行状态

- 读取每日监控状态（首页 banner 使用）：

```
GET /api/cos_daily/status
```

## 接口清单（详细）

说明：
- `Form` 表示 `multipart/form-data`
- `Query` 表示 URL 查询参数
- `JSON` 表示请求体 JSON

### 页面（HTML）
- `GET /` 首页
- `GET /table_query`（Query: `table_name`）
- `GET /join_query`
- `GET /daily_hourly`
- `GET /similar_day`
- `GET /multi_day_compare`
- `GET /strategy_quote`
- `GET /strategy_review`
- `GET /strategy_review_diag`（Query: `date`, `platform`）

### 系统/状态
- `GET /health`
- `GET /api/cos_daily/status`

### 文件与导入
- `GET /files`
- `POST /upload`（Form: `file`）
- `POST /import`（Form: `filename`）
- `POST /import-all`
- `DELETE /files/{filename}`
- `DELETE /files`
- `GET /download/{filename}`

### 数据表
- `GET /tables`
- `GET /tables/{table_name}`（Query: `limit`）
- `GET /tables/{table_name}/schema`
- `GET /tables/{table_name}/query`（Query: `offset`, `limit`, `conditions` JSON 字符串）
- `GET /tables/{table_name}/export`（Query: `conditions` JSON 字符串）
- `DELETE /tables/{table_name}`
- `DELETE /tables`

### 缓存 / 天气 / 日历
- `POST /api/generate-daily-hourly-cache`
- `POST /api/generate-price-cache`
- `POST /api/update-weather`
- `GET /api/cache-available-dates`（Query: `limit`, `require_load`）

### 日均 / 价差
- `POST /daily-averages`（Form: `dates` JSON, `data_type_keyword`, `station_name?`, `city?`）
- `GET /daily-averages/export`（Query: `dates` JSON, `data_type_keyword`）
- `POST /daily-averages/export-from-result`（Form: `query_result` JSON）
- `POST /price-difference`（Form: `dates` JSON, `region?`, `station_name?`, `city?`）
- `POST /price-difference/export-from-result`（Form: `query_result` JSON, `region?`）

### 分时 / 多日对比 / 缓存导出
- `GET /api/daily-hourly-data`（Query: `date`）
- `GET /api/multi-day-compare-data`（Query: `start`, `end`）
- `POST /api/multi-day-compare-data-by-dates`（JSON: `dates[]`）
- `POST /api/daily-hourly-cache/export`（Form: `dates` JSON）
- `POST /api/daily-averages/export-dr-compare`（Form: `dates` JSON）
- `POST /api/daily-averages/export-new-energy-forecast`（Form: `dates` JSON）

### 价差缓存表
- `POST /api/price-diff-cache`（JSON: `dates[]`, `sort_by?`, `sort_order?`）
- `POST /api/price-diff-cache/export`（JSON: `dates[]`, `sort_by?`, `sort_order?`）

### 策略/复盘
- `POST /api/strategy/import-workbook`（Form: `file`, `platform?`）
- `POST /api/strategy/actual-hourly/upload`（Form: `file`, `sheet_name?`, `target_date?`, `record_date?`, `platform?`）
- `GET /api/strategy/actual-hourly/summary`（Query: `start?`, `end?`, `platform?`）
- `GET /api/strategy/actual-hourly`（Query: `date`, `platform?`, `source?`）
- `POST /api/strategy/actual-hourly`（JSON: `date`, `hourly[24]`, `platform?`, `source?`）
- `POST /api/strategy/actual-hourly/batch`（JSON: 多天列表）
- `POST /api/strategy/day-settings`（JSON: `date`, `strategy_coeff?`, `strategy_coeff_hourly?`, `revenue_transfer?`, `note?`, `platform?`）
- `GET /api/strategy/day-settings`（Query: `date`, `platform?`）
- `GET /api/strategy/quote`（Query: `date`, `platform?`, `strategy_coeff?`）
- `GET /api/strategy/review`（Query: `start?`, `end?`, `platform?`）
- `GET /api/strategy/review/latest-month`（Query: `platform?`）
- `GET /api/strategy/review/diagnose`（Query: `date`, `platform?`）
- `POST /api/strategy/review/refresh`（Form/Query: `month`, `platform?`）

## 支持的 Excel 类型

- 负荷实际信息
- 负荷预测信息
- 信息披露查询实际信息
- 信息披露查询预测信息
- 实时节点电价查询
- 日前节点电价查询

## COS 每日自动拉取 + 导入 + 写缓存

自动从 COS 下载 4 类 Excel（日前/实时节点电价、信息披露预测/实际），导入数据库并更新 `cache_daily_hourly`，状态写入 `state/cos_daily_state.json`。

### 配置文件

`cos_daily_import.config.json`

- `polling.start_hhmm` / `polling.end_hhmm`：时间窗（默认 11:20–12:00）
- `polling.interval_seconds`：每次轮询间隔（默认 60s）
- `targets.*.date_offsets_days_priority`：文件日期偏移策略

### COS 机密信息放在环境变量（推荐）

建议使用 `.env`（项目根目录）：

```
TENCENT_COS_REGION=ap-guangzhou
TENCENT_COS_BUCKET=gaungdong-1327310319
TENCENT_SECRET_ID=...
TENCENT_SECRET_KEY=...
```

读取顺序：

1. `tencent_cos.dotenv_path` 指向的 .env（若存在）
2. 环境变量

可选：

```
TENCENT_COS_DOTENV=/path/to/.env
```

### 手动运行

只命中不下载：

```bash
python3 cos_daily_auto_import.py --once --dry-run
```

立刻跑一次（下载 → 导入 → 更新缓存）：

```bash
python3 cos_daily_auto_import.py --once
```

按时间窗运行（11:20–12:00，每分钟一次，完成即停）：

```bash
python3 cos_daily_auto_import.py
```

## 应用内自动调度（服务器部署推荐）

应用启动后自动运行每日任务（无需系统 cron）：

- 默认启用：`COS_DAILY_SCHEDULER=1`
- 关闭：`COS_DAILY_SCHEDULER=0`
- 每分钟执行一次，全部目标完成后自动停止

多实例部署建议加锁（同机单实例执行）：

```
COS_DAILY_SCHEDULER_LOCK=/tmp/excel2sql_cos_daily.lock
```

## 目录说明

- `data/`：上传的 Excel
- `state/cos_daily_state.json`：COS 每日状态
- `temp_cos_downloads/`：临时下载目录（自动清理）
- `static/`、`templates/`：前端资源

## 备注

- COS 与数据库为核心依赖，建议先确认连通性。
- 首页“每日数据监控”只展示状态，实际导入由自动任务驱动。
