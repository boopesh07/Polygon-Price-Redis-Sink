#!/usr/bin/env bash
set -euo pipefail

echo "Build script started."
set -a
. ./.env
set +a

export AWS_DEFAULT_REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found. Please install/start Docker Desktop." >&2
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "ERROR: aws CLI not found." >&2
  exit 1
fi

if [[ -z "${ACCOUNT_ID:-}" ]]; then
  if ! ACCOUNT_ID=$(aws sts get-caller-identity --query 'Account' --output text 2>/dev/null); then
    echo "ERROR: ACCOUNT_ID not set and could not be derived via AWS STS." >&2
    exit 1
  fi
fi

ECR_REGISTRY="${ACCOUNT_ID}.dkr.ecr.${AWS_DEFAULT_REGION}.amazonaws.com"
ECR_REPO="polygon-hot-cold-sink"
IMAGE_TAG="latest"
IMAGE_URI="$ECR_REGISTRY/$ECR_REPO:$IMAGE_TAG"

echo "Logging into ECR: $ECR_REGISTRY"
aws ecr get-login-password --region "$AWS_DEFAULT_REGION" | docker login --username AWS --password-stdin "$ECR_REGISTRY"

echo "Ensuring ECR repository exists: $ECR_REPO"
aws ecr describe-repositories --repository-names "$ECR_REPO" >/dev/null 2>&1 || aws ecr create-repository --repository-name "$ECR_REPO" >/dev/null

echo "Using docker buildx to build linux/amd64 image"
docker buildx version >/dev/null 2>&1 || { echo "ERROR: docker buildx not available." >&2; exit 1; }

# Prepare a clean, dedicated builder to avoid stale buildkit state
BUILDER="polygon-redis-sink-builder"
if docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
  echo "Using existing builder: $BUILDER"
  docker buildx use "$BUILDER" >/dev/null 2>&1 || true
else
  echo "Creating builder: $BUILDER"
  docker buildx create --name "$BUILDER" --driver docker-container --use >/dev/null 2>&1
fi

# Optional cleanup if CLEAN_BUILDX=true to clear any corrupted cache/state
if [[ "${CLEAN_BUILDX:-}" == "true" ]]; then
  echo "Pruning buildx cache for builder $BUILDER"
  docker buildx prune -af || true
fi

docker buildx build \
  --builder "$BUILDER" \
  --platform linux/amd64 \
  -t "$IMAGE_URI" \
  --push \
  .

echo "Build and push complete: $IMAGE_URI"


