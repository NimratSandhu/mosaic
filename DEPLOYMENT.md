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

