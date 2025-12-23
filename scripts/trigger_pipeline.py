#!/usr/bin/env python3
"""
Simple script to trigger the data pipeline via HTTP endpoint.
Can be run from external cron services or manually.

Usage:
    python scripts/trigger_pipeline.py https://your-app.onrender.com
    python scripts/trigger_pipeline.py https://your-app.onrender.com --token YOUR_TOKEN
    python scripts/trigger_pipeline.py https://your-app.onrender.com --date 2024-12-01
"""

import argparse
import sys
from datetime import date

import requests


def trigger_pipeline(
    base_url: str,
    token: str | None = None,
    run_date: str | None = None,
) -> None:
    """Trigger the pipeline via HTTP endpoint."""
    url = f"{base_url.rstrip('/')}/api/run-pipeline"
    
    params = {}
    if run_date:
        params["date"] = run_date
    else:
        params["date"] = date.today().isoformat()
    
    headers = {}
    if token:
        headers["X-Pipeline-Token"] = token
        params["token"] = token  # Also support query param
    
    print(f"Triggering pipeline for date: {params['date']}")
    print(f"URL: {url}")
    
    try:
        response = requests.post(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ Success: {result.get('message', 'Pipeline started')}")
        print(f"   Note: {result.get('note', '')}")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error: {e}")
        if hasattr(e, "response") and e.response is not None:
            print(f"   Response: {e.response.text}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Trigger data pipeline via HTTP endpoint"
    )
    parser.add_argument(
        "url",
        help="Base URL of your deployed app (e.g., https://your-app.onrender.com)",
    )
    parser.add_argument(
        "--token",
        help="Optional security token (set PIPELINE_TOKEN in Render)",
    )
    parser.add_argument(
        "--date",
        help="Date to run pipeline for (YYYY-MM-DD, defaults to today)",
    )
    
    args = parser.parse_args()
    
    trigger_pipeline(args.url, token=args.token, run_date=args.date)

