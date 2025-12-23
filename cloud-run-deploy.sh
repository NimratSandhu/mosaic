#!/bin/bash
# Helper script for deploying to Google Cloud Run
# Usage: ./cloud-run-deploy.sh [project-id] [region] [bucket-name]

set -e

PROJECT_ID=${1:-${GOOGLE_CLOUD_PROJECT}}
REGION=${2:-us-central1}
BUCKET_NAME=${3:-${GCS_BUCKET_NAME}}

if [ -z "$PROJECT_ID" ]; then
    echo "Error: Project ID not specified"
    echo "Usage: $0 [project-id] [region] [bucket-name]"
    echo "Or set GOOGLE_CLOUD_PROJECT environment variable"
    exit 1
fi

if [ -z "$BUCKET_NAME" ]; then
    echo "Error: GCS bucket name not specified"
    echo "Usage: $0 [project-id] [region] [bucket-name]"
    echo "Or set GCS_BUCKET_NAME environment variable"
    exit 1
fi

echo "Deploying to Cloud Run..."
echo "Project: $PROJECT_ID"
echo "Region: $REGION"
echo "Bucket: $BUCKET_NAME"

# Set the project
gcloud config set project "$PROJECT_ID"

# Build and deploy
gcloud builds submit --config=cloudbuild.yaml

# Update environment variables (if not already set in cloudbuild.yaml)
gcloud run services update mosaic-signal-platform \
    --region="$REGION" \
    --update-env-vars="GCS_BUCKET_NAME=$BUCKET_NAME,GCS_ENABLED=true" \
    --quiet

echo "✅ Deployment complete!"
echo ""
echo "Get the service URL:"
gcloud run services describe mosaic-signal-platform \
    --region="$REGION" \
    --format="value(status.url)"

