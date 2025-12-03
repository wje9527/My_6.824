#!/bin/bash

# 使用方法:
#   chmod +x stress_test.sh
#   ./stress_test.sh <测试名正则> <运行次数>
#
# 例子:
#   ./stress_test.sh 2A 30   (跑 Test2A 30次)
#   ./stress_test.sh 2       30   (跑所有名字带 2 的测试，即 2A/2B/2C/2D，30次)

TEST_PATTERN=$1
RUNS=$2

if [ -z "$TEST_PATTERN" ] || [ -z "$RUNS" ]; then
    echo "用法: ./stress_test.sh <测试名(支持正则)> <次数>"
    echo "例如: ./stress_test.sh 2A 30"
    exit 1
fi

# 创建日志目录，别把当前目录搞乱了
LOG_DIR="logs_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$LOG_DIR"

echo "👻 开始测试: 匹配模式 '$TEST_PATTERN'，共运行 $RUNS 次..."
echo "📂 日志将保存在: $LOG_DIR"

FAIL_COUNT=0

for ((i=1; i<=RUNS; i++))
do
    LOG_FILE="$LOG_DIR/run_$i.log"
    
    # 打印进度条效果
    printf "Running iteration $i/$RUNS ... "
    
    # 核心命令！
    # -race: 必须加！这是分布式系统的照妖镜
    # -v: 详细输出，方便看日志
    # -run: 指定跑哪个测试
    # -timeout: 防止死锁卡住，设个上限比如 10分钟
    go test -run "$TEST_PATTERN" -race -v -timeout 10m > "$LOG_FILE" 2>&1
    
    # 检查上一条命令(go test)的退出代码
    if [ $? -eq 0 ]; then
        echo -e "\033[32mPASS\033[0m" # 绿色 PASS
    else
        echo -e "\033[31mFAIL\033[0m" # 红色 FAIL
        echo "   -> 💥 失败日志: $LOG_FILE"
        FAIL_COUNT=$((FAIL_COUNT+1))
        
        # 如果你想一失败就停止，把下面这行注释取消掉
        # break 
    fi
done

echo "------------------------------------------------"
if [ $FAIL_COUNT -eq 0 ]; then
    echo -e "\033[32m🎉 完美！全部 $RUNS 次测试通过！Raft 稳如老狗！\033[0m"
else
    echo -e "\033[31m💀 悲剧！共有 $FAIL_COUNT 次失败。\033[0m"
    echo "请检查 $LOG_DIR 目录下的失败日志。"
fi