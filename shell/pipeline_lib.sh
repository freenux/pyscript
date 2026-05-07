#!/usr/bin/env bash
# ============================================================
# pipeline_lib.sh — Shell Pipeline 公共函数库
# 使用方式:
# > source pipeline_lib.sh
# > pipeline_init "test_pipeline" "/var/log/recall"
# > run_step "step1" echo "hello"
# > run_step "step2" python3 not_exist.py   # 故意失败
# > run_step "step3" echo "world"           # 不会执行到这里
# ============================================================

[ -n "${_PIPELINE_LIB_LOADED:-}" ] && return 0
_PIPELINE_LIB_LOADED=1

# ============================================================
# 一、初始化
# ============================================================

pipeline_init() {
    # 用法: pipeline_init <脚本名> [日志目录]
    local script_name=${1:-$(basename "${BASH_SOURCE[1]}" .sh)}
    local log_dir=${2:-/var/log/pipeline}

    set -Eeuo pipefail

    PIPELINE_NAME="$script_name"
    PIPELINE_LOG_FILE="${log_dir}/${script_name}_$(date +%Y%m%d_%H%M%S).log"
    PIPELINE_START_TIME=$(date +%s)
    PIPELINE_CURRENT_STEP=""

    mkdir -p "$log_dir"
    exec > >(tee -a "$PIPELINE_LOG_FILE") 2>&1

    trap '_on_error "$LINENO" "$?"' ERR
    trap '_on_exit'          EXIT

    log_info "==============================="
    log_info "Pipeline: $PIPELINE_NAME"
    log_info "Started:  $(date)"
    log_info "Log:      $PIPELINE_LOG_FILE"
    log_info "==============================="
}

# ============================================================
# 二、日志
# ============================================================

log_info() {
    echo "[INFO]  $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_warn() {
    echo "[WARN]  $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

log_debug() {
    [ "${PIPELINE_DEBUG:-0}" = "1" ] && \
        echo "[DEBUG] $(date '+%Y-%m-%d %H:%M:%S') $*" || true
}

# ============================================================
# 三、步骤执行
# ============================================================

run_step() {
    # 用法: run_step <步骤名> <命令> [参数...]
    local name=$1; shift

    log_info ">>> START: $name"
    local t_start=$(date +%s)
    PIPELINE_CURRENT_STEP="$name"

    "$@"

    log_info "<<< DONE:  $name (耗时 $(( $(date +%s) - t_start ))s)"
    PIPELINE_CURRENT_STEP=""
}

retry() {
    # 用法: retry <最大次数> <间隔秒> <命令> [参数...]
    local max=$1; shift
    local interval=$1; shift
    local attempt=1

    until "$@"; do
        if [ $attempt -ge $max ]; then
            log_error "重试 $max 次后仍失败: $*"
            return 1
        fi
        log_warn "第 $attempt 次失败，${interval}s 后重试..."
        sleep "$interval"
        (( attempt++ ))
    done
}

# ============================================================
# 四、前置检查
# ============================================================

require_env() {
    # 用法: require_env VAR1 VAR2 ...
    local missing=()
    for var in "$@"; do
        [ -z "${!var:-}" ] && missing+=("$var")
    done
    if [ ${#missing[@]} -gt 0 ]; then
        log_error "缺少必要环境变量: ${missing[*]}"
        return 1
    fi
}

require_cmd() {
    # 用法: require_cmd python3 aws redis-cli
    for cmd in "$@"; do
        if ! command -v "$cmd" &>/dev/null; then
            log_error "缺少依赖命令: $cmd"
            return 1
        fi
    done
}

require_dir() {
    local dir
    for dir in "$@"; do
        if [ ! -d "$dir" ]; then
            log_error "缺少必要目录: $dir"
            return 1
        fi
    done
}

require_file() {
    local file
    for file in "$@"; do
        if [ ! -f "$file" ]; then
            log_error "缺少必要文件: $file"
            return 1
        fi
    done
}

# ============================================================
# 五、内部函数
# ============================================================

_on_error() {
    local line=${1:-unknown}
    local exit_code=${2:-1}
    log_error "==============================="
    if [ -n "${PIPELINE_CURRENT_STEP:-}" ]; then
        log_error "Step '$PIPELINE_CURRENT_STEP' failed with exit code $exit_code"
    fi
    log_error "Pipeline FAILED at line $line"
    log_error "==============================="
}

_on_exit() {
    log_info "Total elapsed: $(( $(date +%s) - ${PIPELINE_START_TIME:-$(date +%s)} ))s"
}
