#!/usr/bin/env bash
set -euo pipefail

echo "Deploy script started."
set -a
. ./.env
set +a

export AWS_DEFAULT_REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}

if [[ -z "${ACCOUNT_ID:-}" ]]; then
  if ! ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null); then
    echo "ERROR: ACCOUNT_ID not set and could not be derived via AWS STS." >&2
    exit 1
  fi
fi

sed -e "s|<ACCOUNT_ID>|$ACCOUNT_ID|g" \
    -e "s|<REGION>|$AWS_DEFAULT_REGION|g" \
    -e "s|<POLYGON_API_KEY>|$POLYGON_API_KEY|g" \
    -e "s|<REDIS_URL>|$REDIS_URL|g" \
    -e "s|<REDIS_TOKEN>|${REDIS_TOKEN:-}|g" \
    -e "s|<POLYGON_WS_HOST_REALTIME>|$POLYGON_WS_HOST_REALTIME|g" \
    -e "s|<POLYGON_WS_HOST_DELAYED>|$POLYGON_WS_HOST_DELAYED|g" \
    -e "s|<WS_DEBUG>|${WS_DEBUG:-true}|g" \
    -e "s|<WS_HEALTH_INTERVAL_SEC>|${WS_HEALTH_INTERVAL_SEC:-30}|g" \
    -e "s|<AGG1M_TTL_SEC>|${AGG1M_TTL_SEC:-120}|g" \
    -e "s|<FMV_TTL_SEC>|${FMV_TTL_SEC:-60}|g" \
    -e "s|<TRADE_TTL_SEC>|${TRADE_TTL_SEC:-60}|g" \
    -e "s|<QUOTE_TTL_SEC>|${QUOTE_TTL_SEC:-60}|g" \
    -e "s|<AGG5M_FLUSH_INTERVAL_SEC>|${AGG5M_FLUSH_INTERVAL_SEC:-900}|g" \
    -e "s|<AGG5M_TTL_SEC>|${AGG5M_TTL_SEC:-172800}|g" \
    -e "s|<AGG5M_TIMEZONE>|${AGG5M_TIMEZONE:-America/New_York}|g" \
    -e "s|<AGG5M_MAX_BARS>|${AGG5M_MAX_BARS:-120}|g" \
    -e "s|<QUOTE_PL_TIMEZONE>|${QUOTE_PL_TIMEZONE:-America/New_York}|g" \
    -e "s|<QUOTE_PL_MARKET_CLOSE_HOUR>|${QUOTE_PL_MARKET_CLOSE_HOUR:-16}|g" \
    -e "s|<QUOTE_PL_MARKET_CLOSE_MINUTE>|${QUOTE_PL_MARKET_CLOSE_MINUTE:-0}|g" \
    -e "s|<QUOTE_PREV_CLOSE_TTL_SEC>|${QUOTE_PREV_CLOSE_TTL_SEC:-604800}|g" \
    -e "s|<S3_ENABLED>|${S3_ENABLED:-false}|g" \
    -e "s|<S3_BUCKET>|${S3_BUCKET:-}|g" \
    -e "s|<S3_PREFIX>|${S3_PREFIX:-polygon}|g" \
    -e "s|<AWS_REGION>|${AWS_DEFAULT_REGION}|g" \
    -e "s|\"S3_WINDOW_MINUTES\": \"30\"|\"S3_WINDOW_MINUTES\": \"${S3_WINDOW_MINUTES:-30}\"|g" \
    -e "s|\"S3_MAX_OBJECT_BYTES\": \"512000000\"|\"S3_MAX_OBJECT_BYTES\": \"${S3_MAX_OBJECT_BYTES:-512000000}\"|g" \
    -e "s|\"S3_PART_SIZE_BYTES\": \"16777216\"|\"S3_PART_SIZE_BYTES\": \"${S3_PART_SIZE_BYTES:-16777216}\"|g" \
    -e "s|\"S3_USE_MARKER\": \"true\"|\"S3_USE_MARKER\": \"${S3_USE_MARKER:-true}\"|g" \
    ecs/task-def.json > /tmp/ecs-task-def.rendered.json

aws ecs register-task-definition --cli-input-json file:///tmp/ecs-task-def.rendered.json | cat

export CLUSTER=${CLUSTER:-etl-pipeline-cluster}
export SUBNET_ID=${SUBNET_ID:?SUBNET_ID env required}
export SECURITY_GROUP_ID=${SECURITY_GROUP_ID:?SECURITY_GROUP_ID env required}

TASK_ARN=$(aws ecs run-task \
  --cluster "$CLUSTER" \
  --launch-type FARGATE \
  --task-definition "polygon-hot-cold-sink" \
  --platform-version LATEST \
  --network-configuration "awsvpcConfiguration={subnets=[\"$SUBNET_ID\"],securityGroups=[\"$SECURITY_GROUP_ID\"],assignPublicIp=\"ENABLED\"}" \
  --query 'tasks[0].taskArn' --output text)
echo "Started task: $TASK_ARN"
echo "Deploy complete."
