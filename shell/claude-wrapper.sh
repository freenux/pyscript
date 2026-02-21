#!/bin/bash

# ==================== User Configuration ====================

# 1. Proxy URL (used when direct connection is not in an allowed country)
PROXY_URL="http://127.0.0.1:15236"

# 2. Path to the Claude executable
CLAUDE_BIN="~/.local/bin/claude"

# 3. AWS Bedrock settings
ENABLE_BEDROCK=true
# Duration for temporary tokens in seconds; default 3600 (1 hour), max 129600 (36 hours)
TOKEN_DURATION=3600

# 4. Countries that may connect directly without a proxy (ISO 3166-1 alpha-2)
ALLOWED_COUNTRIES=("US" "GB" "SG" "JP" "CA" "DE" "FR")

# 5. IP geolocation endpoints (multiple fallbacks in case of rate-limiting)
CHECK_URLS=(
    "https://ipinfo.io/country"
    "https://ipapi.co/country"
    "https://ifconfig.co/country"
)

# 6. Set to true to bypass all security checks and launch Claude directly
SKIP_SECURITY_CHECK=false

# ============================================================

# Resolve the current country code, trying Cloudflare Trace first then fallback APIs.
# Prints a 2-letter ISO country code on success; exits with status 1 on failure.
get_country_code() {
    # Cloudflare Trace is fast and rarely rate-limited
    local code
    code=$(curl -s --max-time 2 "https://cloudflare.com/cdn-cgi/trace" | grep "loc=" | cut -d'=' -f2 | tr -d '[:space:]')

    if [[ -n "$code" && ${#code} -eq 2 && "$code" != "XX" ]]; then
        echo "$code"
        return 0
    fi

    # Fallback to public geolocation APIs
    for url in "${CHECK_URLS[@]}"; do
        code=$(curl -s --max-time 2 "$url" | tr -d '[:space:]')
        if [[ -n "$code" && ${#code} -eq 2 ]]; then
            echo "$code"
            return 0
        fi
    done

    return 1
}

# Returns 0 if the given country code is in the allowed list, 1 otherwise.
is_allowed() {
    local code="$1"
    for allowed in "${ALLOWED_COUNTRIES[@]}"; do
        if [[ "$allowed" == "$code" ]]; then
            return 0
        fi
    done
    return 1
}

# Returns 0 if security checks should be skipped for this invocation.
# Checks (in order): --no-check flag, SKIP_SECURITY_CHECK config, third-party API
# base URL env vars, non-AI subcommands, and a non-standard baseUrl in settings files.
should_skip_checks() {
    # Explicit --no-check flag passed on the command line
    if [[ "$IS_SKIP_BY_ARG" == true ]]; then
        return 0
    fi

    # Config-level override
    if [[ "$SKIP_SECURITY_CHECK" == true ]]; then
        return 0
    fi

    # Custom API base URL implies a third-party model provider (e.g. Kimi, Minimax)
    if [[ -n "$ANTHROPIC_BASE_URL" || -n "$CLAUDE_BASE_URL" ]]; then
        return 0
    fi

    # Subcommands that do not invoke an AI model
    for arg in "$@"; do
        case "$arg" in
            --help|-h|--version|-v|config|mcp|doctor|update|plugin|install|setup-token|release-notes|status)
                return 0
                ;;
        esac
    done

    # Non-standard baseUrl in any Claude settings file also implies third-party routing
    local config_files=("$HOME/.claude/settings.json" "./.claude/settings.json" "./.claude/settings.local.json")
    for f in "${config_files[@]}"; do
        if [[ -f "$f" ]] && grep -qiE "baseUrl|base_url" "$f"; then
            return 0
        fi
    done

    return 1
}

# ==================== Main ====================

# Strip the --no-check wrapper flag before passing arguments to Claude
IS_SKIP_BY_ARG=false
FINAL_ARGS=()

for arg in "$@"; do
    if [[ "$arg" == "--no-check" ]]; then
        IS_SKIP_BY_ARG=true
    else
        FINAL_ARGS+=("$arg")
    fi
done

set -- "${FINAL_ARGS[@]}"

# Expand tilde in CLAUDE_BIN and verify the binary exists
CLAUDE_PATH=$(eval echo "$CLAUDE_BIN")

if [[ ! -f "$CLAUDE_PATH" ]]; then
    echo "❌ Error: Claude binary not found at: $CLAUDE_PATH"
    echo "👉 Check the CLAUDE_BIN path in this script's configuration."
    exit 1
fi

# Skip all network checks when not needed
if should_skip_checks "$@"; then
    exec "$CLAUDE_PATH" "$@"
fi

echo "========================================"
echo "  🛡️  Claude Secure Wrapper"
echo "========================================"

# Detect the current outbound country
CURRENT_COUNTRY=$(get_country_code)

if [[ -n "$CURRENT_COUNTRY" ]] && is_allowed "$CURRENT_COUNTRY"; then
    echo "✅ Direct connection allowed ($CURRENT_COUNTRY)"
    unset HTTP_PROXY HTTPS_PROXY ALL_PROXY
else
    if [[ -z "$CURRENT_COUNTRY" ]]; then
        echo "⚠️  Country detection timed out; enabling proxy by default..."
    else
        echo "🌐 Direct connection from $CURRENT_COUNTRY is not allowed; switching to proxy..."
    fi

    export HTTP_PROXY="$PROXY_URL"
    export HTTPS_PROXY="$PROXY_URL"
    export ALL_PROXY="$PROXY_URL"

    # Verify the proxy actually exits from an allowed country (fail-closed)
    echo "🔍 Verifying proxy connectivity..."
    PROXY_COUNTRY=$(get_country_code)

    if [[ -z "$PROXY_COUNTRY" ]]; then
        echo "❌ Error: Proxy cannot reach the internet. Check Clash/v2ray on port 15236."
        exit 1
    elif ! is_allowed "$PROXY_COUNTRY"; then
        echo "❌ Error: Proxy exit country $PROXY_COUNTRY is not in the allowed list."
        echo "🚫 Connection blocked to protect account safety."
        exit 1
    else
        echo "✅ Proxy verified. Exit country: $PROXY_COUNTRY"
    fi
fi

exec "$CLAUDE_PATH" "$@"
