#!/bin/bash

# ================= 🔧 用户配置区域 =================

# 1. 代理设置 (端口 15236)
PROXY_URL="http://127.0.0.1:15236"

# 2. Claude 可执行文件路径
CLAUDE_BIN="~/.local/bin/claude"

# 3. AWS Bedrock 设置
ENABLE_BEDROCK=true
# 设置临时 Token 的有效期 (秒)，默认 3600 (1小时)，最长 129600 (36小时)
TOKEN_DURATION=3600

# 4. 支持直连的国家代码白名单 (ISO 3166-1 alpha-2)
ALLOWED_COUNTRIES=("US" "GB" "SG" "JP" "CA" "DE" "FR")

# 5. 检测接口 (多个备份以防限流)
CHECK_URLS=(
    "https://ipinfo.io/country"
    "https://ipapi.co/country"
    "https://ifconfig.co/country"
)

# ==============================================

# 获取国家代码 (带重试机制)
get_country_code() {
    for url in "${CHECK_URLS[@]}"; do
        local code
        code=$(curl -s --max-time 2 "$url" | tr -d '[:space:]')
        # 简单校验：应为2位字母代码
        if [[ -n "$code" && ${#code} -eq 2 ]]; then
            echo "$code"
            return 0
        fi
    done
    return 1
}

# 判断国家是否在白名单
is_allowed() {
    local code="$1"
    for allowed in "${ALLOWED_COUNTRIES[@]}"; do
        if [[ "$allowed" == "$code" ]]; then return 0; fi
    done
    return 1
}

# 判断是否由参数或配置触发跳过安全检测
should_skip_checks() {
    # 1. 检查环境变量: 如果定义了自定义 API 地址，通常意味着使用第三方模型（如 Kimi, Minimax）
    if [ -n "$ANTHROPIC_BASE_URL" ] || [ -n "$CLAUDE_BASE_URL" ]; then
        return 0
    fi

    # 2. 检查命令行参数: 针对不调用 AI 模型的命令或帮助信息
    # 包含: 帮助、版本、配置、MCP管理、医生检查、更新、插件管理等
    for arg in "$@"; do
        case "$arg" in
            --help|-h|--version|-v|config|mcp|doctor|update|plugin|install|setup-token|release-notes|status)
                return 0
                ;;
        esac
    done

    # 3. 检查配置文件: 如果 settings.json 中明确配置了非标准的 baseUrl
    local config_files=("$HOME/.claude/settings.json" "./.claude/settings.json" "./.claude/settings.local.json")
    for f in "${config_files[@]}"; do
        if [ -f "$f" ]; then
            if grep -qiE "baseUrl|base_url" "$f"; then
                return 0
            fi
        fi
    done

    return 1
}

# ================= 🚀 主程序开始 =================

# --- 0. 环境预检查 (路径展开与存在性检查) ---
CLAUDE_PATH=$(eval echo "$CLAUDE_BIN")

if [ ! -f "$CLAUDE_PATH" ]; then
    echo "❌ 错误：未找到 Claude 程序: $CLAUDE_PATH"
    echo "请检查配置中的 CLAUDE_BIN 路径是否正确。"
    exit 1
fi

# --- 1. 跳过检测判断 (无感知执行) ---
if should_skip_checks "$@"; then
    exec "$CLAUDE_PATH" "$@"
fi

echo "========================================"
echo "🛡️  Claude 安全启动包装器"
echo "========================================"

# --- 1. 网络环境检测 ---
CURRENT_COUNTRY=$(get_country_code)

if [ -n "$CURRENT_COUNTRY" ] && is_allowed "$CURRENT_COUNTRY"; then
    echo "✅ 直连环境合规 ($CURRENT_COUNTRY)"
    unset HTTP_PROXY HTTPS_PROXY ALL_PROXY
else
    # --- 2. 设置代理 ---
    if [ -z "$CURRENT_COUNTRY" ]; then
        echo "⚠️  直连检测超时，默认启用代理..."
    else
        echo "🌍 当前直连位置: $CURRENT_COUNTRY (不支持)，切换代理..."
    fi

    export HTTP_PROXY="$PROXY_URL"
    export HTTPS_PROXY="$PROXY_URL"
    export ALL_PROXY="$PROXY_URL"

    # --- 3. 代理二次校验 (Fail-Close) ---
    echo "🔄 验证代理连通性..."
    PROXY_COUNTRY=$(get_country_code)

    if [ -z "$PROXY_COUNTRY" ]; then
        echo "❌ 错误：代理无法联网，请检查 Clash/v2ray (端口 15236)。"
        exit 1
    elif ! is_allowed "$PROXY_COUNTRY"; then
        echo "❌ 危险：代理后位置为 $PROXY_COUNTRY (未在支持名单)。"
        echo "🚫 已阻断连接，保护账号安全。"
        exit 1
    else
        echo "✅ 代理验证通过！出口位置: $PROXY_COUNTRY"
    fi
fi

exec "$CLAUDE_PATH" "$@"
