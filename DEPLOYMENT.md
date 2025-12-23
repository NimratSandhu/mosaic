# Deployment Guide

This guide covers deploying the Unified Signal Monitoring Platform to various hosting providers.

## Memory Requirements

The application is optimized for memory-constrained environments:
- **Minimum**: 256MB RAM
- **Recommended**: 512MB RAM
- **DuckDB Threads**: Limited to 2 for memory efficiency
- **Image Size**: ~500MB (multi-stage build)

## Local Docker Deployment

### Quick Start

```bash
# 1. Build the image
make build

# 2. Deploy (runs dashboard)
make deploy

# 3. Access dashboard at http://localhost:8050
```

### Manual Steps

```bash
# Build
docker build -t unified-signal-platform:latest .

# Run dashboard
docker-compose up -d

# Run ingestion
docker-compose run --rm ingest

# View logs
docker-compose logs -f mosaic

# Stop
docker-compose down
```

## Render.com Deployment (Free Tier)

### Prerequisites

1. Render account (free tier available)
2. GitHub repository (optional, for auto-deploy)

### Steps

1. **Create New Web Service**
   - Connect your GitHub repository OR upload code manually
   - Select "Docker" as the environment
   - Point to `Dockerfile` in root directory

2. **Configure Environment Variables**
   In Render dashboard, add:
   ```
   SEC_EDGAR_USER_AGENT=YourName your.email@example.com
   SEC_EDGAR_USER_EMAIL=your.email@example.com
   SEC_EDGAR_COMPANY_NAME=MosaicSignalPlatform
   PYTHONPATH=/app/src:/app
   DUCKDB_THREADS=2
   ```

3. **Configure Persistent Disk** (for data)
   - Add a disk mount at `/app/data`
   - Size: 1GB (free tier allows up to 1GB)

4. **Deploy**
   - Render will automatically build and deploy
   - Health check: `/_dash-health`
   - Service will be available at `https://your-app.onrender.com`

### Using render.yaml (Optional)

If you have `render.yaml` in your repo:
- Render will auto-detect and use it
- Environment variables still need to be set in dashboard

## Google Cloud Run Deployment

### Architecture

The dashboard runs on Cloud Run (serverless) and reads data from a GCS bucket. The pipeline runs locally on your machine and syncs results to GCS.

```
Local Machine (Pipeline) → GCS Bucket → Cloud Run (Dashboard)
```

### Prerequisites

1. **Google Cloud Account** with billing enabled (free tier available)
2. **gcloud CLI** installed and authenticated:
   ```bash
   gcloud auth login
   gcloud auth application-default login
   ```
3. **Python dependencies** installed locally for pipeline execution

### Step 1: Create GCS Bucket

```bash
# Set your project
export GOOGLE_CLOUD_PROJECT=your-project-id
gcloud config set project $GOOGLE_CLOUD_PROJECT

# Create bucket
gsutil mb gs://mosaic-signal-data

# Optional: Set lifecycle policy to reduce costs
gsutil lifecycle set lifecycle.json gs://mosaic-signal-data
```

Create `lifecycle.json` (optional):
```json
{
  "lifecycle": {
    "rule": [
      {
        "action": {"type": "Delete"},
        "condition": {"age": 365}
      }
    ]
  }
}
```

### Step 2: Set Up Service Account

```bash
# Create service account for Cloud Run
gcloud iam service-accounts create mosaic-dashboard \
    --display-name="Mosaic Signal Platform Dashboard"

# Grant GCS access
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
    --member="serviceAccount:mosaic-dashboard@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"

# For local pipeline, use your user account or create a separate service account
# For local: gcloud auth application-default login
```

### Step 3: Configure Local Environment

Create or update `.env`:
```bash
GCS_BUCKET_NAME=mosaic-signal-data
GCS_ENABLED=true
GCS_MARTS_PREFIX=marts/
SEC_EDGAR_USER_AGENT="YourName your.email@example.com"
SEC_EDGAR_USER_EMAIL=your.email@example.com
```

### Step 4: Run Pipeline and Sync

```bash
# Run the full pipeline locally
make ingest-daily
make curate
make build-features

# Sync results to GCS
export GCS_BUCKET_NAME=mosaic-signal-data
make sync-to-gcs
```

### Step 5: Deploy to Cloud Run

**Option A: Using Makefile**
```bash
make deploy-cloud-run PROJECT_ID=your-project-id BUCKET=mosaic-signal-data
```

**Option B: Using gcloud directly**
```bash
# Build and push image
gcloud builds submit --config=cloudbuild.yaml

# Or deploy manually
gcloud run deploy mosaic-signal-platform \
    --source . \
    --region us-central1 \
    --platform managed \
    --allow-unauthenticated \
    --memory 512Mi \
    --cpu 1 \
    --max-instances 10 \
    --min-instances 0 \
    --timeout 300 \
    --port 8050 \
    --set-env-vars "GCS_BUCKET_NAME=mosaic-signal-data,GCS_ENABLED=true,DUCKDB_THREADS=2,PYTHONPATH=/app/src:/app,PREFECT_HOME=/opt/prefect" \
    --set-env-vars "SEC_EDGAR_USER_AGENT=YourName your.email@example.com" \
    --set-env-vars "SEC_EDGAR_USER_EMAIL=your.email@example.com" \
    --service-account mosaic-dashboard@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com
```

**Option C: Using Cloud Build (automated)**
```bash
gcloud builds submit --config=cloudbuild.yaml
```

### Step 6: Get Service URL

```bash
gcloud run services describe mosaic-signal-platform \
    --region us-central1 \
    --format="value(status.url)"
```

### Updating Data

1. **Run pipeline locally**:
   ```bash
   make ingest-daily
   make curate
   make build-features
   ```

2. **Sync to GCS**:
   ```bash
   make sync-to-gcs
   ```

3. **Trigger Cloud Run sync** (optional, or wait for next request):
   ```bash
   curl -X POST "https://your-service-url/api/sync-data"
   ```

### Cost Estimation (Free Tier)

- **Cloud Run**: 2 million requests/month, 360K GB-seconds free
- **GCS**: 5GB storage, 5K Class A operations, 50K Class B operations free
- **Cloud Build**: 120 build-minutes/day free

For typical usage (<100 requests/day, <1GB data), this should be **completely free**.

### Troubleshooting

**Dashboard shows no data:**
- Check GCS bucket has data: `gsutil ls gs://your-bucket/marts/`
- Check Cloud Run logs: `gcloud run services logs read mosaic-signal-platform --region us-central1`
- Verify environment variables are set correctly

**Sync fails:**
- Verify authentication: `gcloud auth application-default login`
- Check bucket permissions: `gsutil iam get gs://your-bucket`
- Verify bucket name is correct

**Memory issues:**
- Increase Cloud Run memory: `--memory 1Gi`
- Reduce DuckDB threads: `DUCKDB_THREADS=1`

## Fly.io Deployment

### Steps

1. **Install Fly CLI**
   ```bash
   curl -L https://fly.io/install.sh | sh
   ```

2. **Login**
   ```bash
   fly auth login
   ```

3. **Create App**
   ```bash
   fly launch --name mosaic-signal-platform
   ```

4. **Configure fly.toml** (if needed)
   ```toml
   [build]
     dockerfile = "Dockerfile"

   [env]
     PYTHONPATH = "/app/src:/app"
     DUCKDB_THREADS = "2"

   [[services]]
     internal_port = 8050
     protocol = "tcp"
   ```

5. **Deploy**
   ```bash
   fly deploy
   ```

## Railway Deployment

1. **Connect Repository** to Railway
2. **Configure Environment Variables** (same as Render)
3. **Set Build Command**: (auto-detected from Dockerfile)
4. **Set Start Command**: `python -m dash_app.app`
5. **Add Volume**: Mount `/app/data` for persistence

## Memory Optimization Tips

1. **DuckDB Configuration**
   - Threads limited to 2 (`DUCKDB_THREADS=2`)
   - Reduces memory footprint

2. **Prefect Configuration**
   - Request timeout set to 30s
   - Prevents memory leaks from hanging requests

3. **Data Management**
   - Use persistent volumes for data
   - Consider periodic cleanup of old data
   - Monitor disk usage

4. **Dashboard Optimization**
   - Charts use efficient Plotly rendering
   - Ag Grid pagination limits data in memory

## Health Checks

The application includes health checks:
- Endpoint: `/_dash-health`
- Used by Docker and hosting platforms
- Returns 200 OK when dashboard is ready

## Troubleshooting

### Out of Memory Errors

1. Reduce `DUCKDB_THREADS` to 1
2. Limit data ingestion to fewer tickers
3. Increase memory limit in hosting platform

### Dashboard Not Loading

1. Check logs: `docker-compose logs mosaic`
2. Verify port 8050 is exposed
3. Check health endpoint: `curl http://localhost:8050/_dash-health`

### Data Persistence Issues

1. Ensure volumes are properly mounted
2. Check disk space: `df -h`
3. Verify write permissions on `/app/data`

## Production Considerations

1. **Environment Variables**: Never commit `.env` file
2. **Data Backups**: Regularly backup `/app/data` directory
3. **Monitoring**: Set up logging and monitoring
4. **Scaling**: For production, consider separate services for ingestion and dashboard
5. **Security**: Use HTTPS, secure API keys, and proper authentication

