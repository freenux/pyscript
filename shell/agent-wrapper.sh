#!/bin/bash

# ==================== User Configuration ====================

# 1. Proxy URL (used when direct connection is not in an allowed country)
PROXY_URL="http://127.0.0.1:15236"

# 2. Countries that may connect directly without a proxy (ISO 3166-1 alpha-2)
ALLOWED_COUNTRIES=("US" "GB" "SG" "JP" "CA" "DE" "FR")

# 3. IP geolocation endpoints (multiple fallbacks in case of rate-limiting)
CHECK_URLS=(
    "https://ipinfo.io/country"
    "https://ipapi.co/country"
    "https://ifconfig.co/country"
)

# 4. Set to true to bypass all security checks and launch the agent directly
SKIP_SECURITY_CHECK=false

# 5. Per-agent config: binary path and env file to source before exec.
#    env file is optional — missing files are silently skipped.
#    Add more `case` branches below to support new agents.
resolve_agent() {
    case "$1" in
        claude)
            BIN="$HOME/.local/bin/claude"
            ENV_FILE="$HOME/.claude/env"
            ;;
        codex)
            BIN="/opt/homebrew/bin/codex"
            ENV_FILE="$HOME/.codex/env"
            ;;
        gemini)
            BIN="$HOME/.local/bin/gemini"
            ENV_FILE="$HOME/.gemini/env"
            ;;
        *)
            echo "❌ Unknown agent: $1" >&2
            echo "👉 Supported: claude, codex, gemini" >&2
            echo "👉 Add a new case branch in agent-wrapper.sh to extend." >&2
            exit 1
            ;;
    esac
}

# ============================================================

# Resolve the current country code, trying Cloudflare Trace first then fallback APIs.
# Prints a 2-letter ISO country code on success; exits with status 1 on failure.
get_country_code() {
    local code
    code=$(curl -s --max-time 2 "https://cloudflare.com/cdn-cgi/trace" | grep "loc=" | cut -d'=' -f2 | tr -d '[:space:]')

    if [[ -n "$code" && ${#code} -eq 2 && "$code" != "XX" ]]; then
        echo "$code"
        return 0
    fi

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
should_skip_checks() {
    [[ "$IS_SKIP_BY_ARG" == true ]] && return 0
    [[ "$SKIP_SECURITY_CHECK" == true ]] && return 0
    return 1
}

# ==================== Main ====================

if [[ $# -lt 1 ]]; then
    echo "Usage: $(basename "$0") <agent> [--no-check] [agent-args...]" >&2
    echo "Supported agents: claude, codex, gemini" >&2
    exit 1
fi

AGENT="$1"
shift

resolve_agent "$AGENT"

# Strip the --no-check wrapper flag before passing arguments to the agent
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

# Expand tilde in BIN and verify the binary exists
BIN_PATH=$(eval echo "$BIN")

if [[ ! -f "$BIN_PATH" ]]; then
    echo "❌ Error: $AGENT binary not found at: $BIN_PATH"
    echo "👉 Update the resolve_agent() case branch for '$AGENT' in this script."
    exit 1
fi

load_env() {
    if [[ -n "$ENV_FILE" && -f "$ENV_FILE" ]]; then
        # shellcheck disable=SC1090
        source "$ENV_FILE"
    fi
}

# Skip all network checks when not needed
if should_skip_checks; then
    load_env
    exec "$BIN_PATH" "$@"
fi

AGENT_UPPER=$(echo "$AGENT" | tr '[:lower:]' '[:upper:]')
echo "========================================"
echo "  🛡️  $AGENT_UPPER Secure Wrapper"
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
        echo "❌ Error: Proxy cannot reach the internet. Check Clash/v2ray on ${PROXY_URL}."
        exit 1
    elif ! is_allowed "$PROXY_COUNTRY"; then
        echo "❌ Error: Proxy exit country $PROXY_COUNTRY is not in the allowed list."
        echo "🚫 Connection blocked to protect account safety."
        exit 1
    else
        echo "✅ Proxy verified. Exit country: $PROXY_COUNTRY"
    fi
fi

load_env
exec "$BIN_PATH" "$@"
