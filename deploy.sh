#!/bin/bash
# Auto-push and deploy to Google Cloud Run
# Usage: ./deploy.sh <GCP_PROJECT_ID> <REGION> <SERVICE_NAME>

set -e

if [ $# -ne 3 ]; then
  echo "Usage: $0 <GCP_PROJECT_ID> <REGION> <SERVICE_NAME>"
  exit 1
fi

PROJECT_ID=$1
REGION=$2
SERVICE_NAME=$3
IMAGE=gcr.io/$PROJECT_ID/$SERVICE_NAME:latest

# Authenticate (assumes gcloud is already authenticated)
echo "Building Docker image..."
docker build -t $IMAGE .

echo "Pushing image to Google Container Registry..."
gcloud auth configure-docker --quiet
docker push $IMAGE

echo "Deploying to Cloud Run..."
gcloud run deploy $SERVICE_NAME \
  --image $IMAGE \
  --region $REGION \
  --platform managed \
  --allow-unauthenticated \
  --project $PROJECT_ID

echo "Deployment complete."
