#!/bin/bash
# 同步 3 月份汇率数据的脚本

# 设置日志文件
LOG_FILE="sync_march_history.log"
echo "开始同步 3 月份汇率数据: $(date)" > $LOG_FILE

# 循环处理 3 月 1 日到 3 月 31 日
for day in {01..17}; do
    date="2023-03-$day"
    echo "正在同步 $date 的汇率数据..." | tee -a $LOG_FILE
    
    # 运行同步命令并记录输出
    poetry run currency_sync -d $date 2>&1 | tee -a $LOG_FILE
    
    # 检查命令执行状态
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo "成功同步 $date 的汇率数据" | tee -a $LOG_FILE
    else
        echo "同步 $date 的汇率数据失败" | tee -a $LOG_FILE
    fi
    
    # 添加间隔，避免频繁请求 API
    echo "等待 3 秒后继续..." | tee -a $LOG_FILE
    sleep 1
done

echo "3 月份汇率数据同步完成: $(date)" | tee -a $LOG_FILE