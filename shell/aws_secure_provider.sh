#!/bin/bash

# ================= 🔧 初始化 =================
TARGET_PROFILE=""
# 默认有效期: 3600秒 (1小时)
DURATION=3600

# 1. 参数解析逻辑
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --profile-name)
            TARGET_PROFILE="$2"
            shift 2 # 移除 flag 和 value
            ;;
        --duration-seconds)
            DURATION="$2"
            shift 2 # 移除 flag 和 value
            ;;
        *)
            # 忽略未知参数
            shift
            ;;
    esac
done

# 2. 参数校验
if [ -z "$TARGET_PROFILE" ]; then
    echo "Error: Missing required argument '--profile-name'." >&2
    echo "Usage: $0 --profile-name <iam-user-profile> [--duration-seconds <seconds>]" >&2
    exit 1
fi

# 3. 核心逻辑：使用 STS 生成带 SessionToken 的新凭证
# 使用传入的 $DURATION 变量
STS_JSON=$(aws sts get-session-token \
    --profile "$TARGET_PROFILE" \
    --duration-seconds $DURATION \
    --output json 2>/dev/null)

# 4. 错误检查
if [ $? -ne 0 ] || [ -z "$STS_JSON" ]; then
    echo "Error: Failed to generate session token for profile '$TARGET_PROFILE'." >&2
    echo "Reason: Check your AccessKey/SecretKey, or if the duration ($DURATION) exceeds the allowed max." >&2
    exit 1
fi

# 5. 格式转换 (STS -> Credential Process 标准格式)
echo "$STS_JSON" | python3 -c "
import sys, json

try:
    data = json.load(sys.stdin)
    creds = data.get('Credentials', {})

    # 构造 AWS Config 要求的标准格式
    output = {
        'Version': 1,
        'AccessKeyId': creds['AccessKeyId'],
        'SecretAccessKey': creds['SecretAccessKey'],
        'SessionToken': creds['SessionToken'],
        'Expiration': creds['Expiration']
    }

    print(json.dumps(output))
except Exception as e:
    sys.exit(1)
"

# 6. 最终检查
if [ $? -ne 0 ]; then
    echo "Error: Failed to process STS JSON." >&2
    exit 1
fi
