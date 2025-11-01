## 这个项目主要放一些单脚本文件
项目使用uv做依赖管理


### dump-sensor-from-sql.py

该脚本用于通过API执行SQL查询并将结果导出为CSV文件，支持按天分批导出。

#### 主要功能
- 通过API接口执行SQL查询，支持神策等数据平台
- 支持SQL模板，自动替换时间参数
- 支持分天批量导出，避免单次数据量过大
- 支持日志输出和调试模式
- 支持从.env文件读取API密钥

#### 命令行参数
| 参数 | 说明 |
| ---- | ---- |
| --api-key | API密钥（可选，优先级高于环境变量） |
| --project | 项目名称（必填） |
| --sql-file | SQL模板文件路径（必填） |
| --output | 输出CSV文件路径（必填） |
| --start-date | 开始日期 (YYYY-MM-DD)，仅当SQL模板包含{start_time}或{end_time}时必填 |
| --end-date | 结束日期 (YYYY-MM-DD)，仅当SQL模板包含{start_time}或{end_time}时必填 |
| --interval-days | 每次查询的天数间隔，默认1天 |
| --base-url | API基础URL，默认http://bi.stary.ltd/api |
| --env-file | .env文件路径，默认.env |
| --debug | 启用调试模式 |
| --log-file | 日志文件路径 |

#### SQL模板说明
- 支持 {start_time} 和 {end_time} 占位符，脚本会自动替换为对应的日期时间字符串。
- 如果SQL模板不包含时间占位符，则只会执行一次查询，忽略 --start-date、--end-date、--interval-days 参数。

#### 用法示例

1. SQL模板包含时间占位符：
```bash
python dump-sensor-from-sql.py \
	--project myproject \
	--sql-file query.sql \
	--output data.csv \
	--start-date "2025-01-01" \
	--end-date "2025-01-07" \
	--interval-days 1
```

2. SQL模板不包含时间占位符：
```bash
python dump-sensor-from-sql.py \
	--project myproject \
	--sql-file query.sql \
	--output data.csv
```

#### .env文件示例
```
API_KEY_xxx=your_project_api_key
API_KEY=your_default_api_key
```

#### 依赖管理
本项目推荐使用uv或poetry进行依赖管理。
