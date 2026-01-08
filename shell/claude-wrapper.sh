#!/bin/bash

# ================= 🔧 用户配置区域 =================

# 1. 代理设置 (端口 15236)
PROXY_URL="http://127.0.0.1:15236"

# 2. Claude 可执行文件路径
CLAUDE_BIN="/usr/local/bin/claude"

# 3. AWS Bedrock 设置
ENABLE_BEDROCK=true
# 设置临时 Token 的有效期 (秒)，默认 3600 (1小时)，最长 129600 (36小时)
TOKEN_DURATION=3600

# 4. 支持直连的国家代码白名单 (ISO 3166-1 alpha-2)
ALLOWED_COUNTRIES=("US" "GB" "SG" "JP" "CA" "DE" "FR")

# 5. 检测接口
CHECK_URL="https://ipinfo.io/country"

# ==============================================

# 获取国家代码
get_country_code() {
    curl -s --max-time 3 "$CHECK_URL" | tr -d '\n'
}

# 判断国家是否在白名单
is_allowed() {
    local code="$1"
    for allowed in "${ALLOWED_COUNTRIES[@]}"; do
        if [[ "$allowed" == "$code" ]]; then return 0; fi
    done
    return 1
}

# ================= 🚀 主程序开始 =================

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

# --- 4. 启动 Claude ---
echo "🚀 启动 Claude Code..."
echo "----------------------------------------"

if [ ! -f "$CLAUDE_BIN" ]; then
    echo "❌ 错误：未找到 Claude 程序: $CLAUDE_BIN"
    exit 1
fi

exec "$CLAUDE_BIN" "$@"
