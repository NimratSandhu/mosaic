"""Main Dash application."""

from __future__ import annotations

from datetime import date

import dash
from dash import html, dcc
from dash.dependencies import Input, Output
from flask import jsonify, request

from dash_app.pages.market_overview import create_market_overview_layout
from dash_app.pages.single_name import create_single_name_layout

# Initialize Dash app with dark theme
app = dash.Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[
        # Add any external stylesheets if needed
    ],
)

# Add API endpoints for triggering data pipeline
@app.server.route("/api/health")
def health_check():
    """Health check endpoint for Cloud Run and other platforms."""
    return jsonify({"status": "ok"}), 200


@app.server.route("/api/sync-data", methods=["POST"])
def sync_data():
    """
    Manually trigger data sync from GCS.
    
    Useful for refreshing data without restarting the container.
    """
    import os
    import subprocess
    import sys
    from pathlib import Path
    
    try:
        # Run sync script
        script_path = Path(__file__).parent.parent / "scripts" / "sync_from_gcs.py"
        if not script_path.exists():
            return jsonify({"error": "Sync script not found"}), 500
        
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=300,  # 5 minute timeout
            env={**os.environ, "PYTHONPATH": "/app/src:/app"},
        )
        
        if result.returncode == 0:
            return jsonify({
                "status": "success",
                "message": "Data synced from GCS",
                "output": result.stdout,
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Sync failed",
                "error": result.stderr,
            }), 500
            
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Sync timed out"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.server.route("/api/pipeline-status", methods=["GET"])
def pipeline_status():
    """Check if pipeline has been run (check if tables exist)."""
    try:
        from db.duckdb_client import DuckDBClient, get_db_path
        
        db_file = get_db_path()
        with DuckDBClient(db_file) as db:
            # Check if signal_scores table exists and has data
            try:
                result = db.query("SELECT COUNT(*) FROM marts.signal_scores")
                signal_count = result[0][0] if result else 0
                
                result = db.query("SELECT MAX(date) FROM marts.signal_scores")
                latest_date = result[0][0] if result and result[0][0] else None
                
                return jsonify({
                    "status": "ready" if signal_count > 0 else "no_data",
                    "signal_count": signal_count,
                    "latest_date": str(latest_date) if latest_date else None,
                }), 200
            except Exception:
                return jsonify({
                    "status": "no_data",
                    "signal_count": 0,
                    "latest_date": None,
                }), 200
                
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Define app layout with navigation
app.layout = html.Div(
    [
        # Header with modern styling
        html.Div(
            [
                html.H1(
                    "Unified Signal Monitoring Platform",
                    style={
                        "margin": "0",
                        "padding": "24px",
                        "background": "linear-gradient(135deg, #3b82f6 0%, #2563eb 100%)",
                        "color": "white",
                        "fontWeight": "700",
                        "fontSize": "32px",
                        "letterSpacing": "-0.5px",
                        "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.3)",
                    },
                ),
                html.Nav(
                    [
                        dcc.Link(
                            "Market Overview",
                            href="/",
                            className="nav-link",
                            style={
                                "marginRight": "8px",
                                "color": "#cbd5e1",
                                "textDecoration": "none",
                                "fontSize": "16px",
                                "fontWeight": "500",
                                "padding": "8px 16px",
                                "borderRadius": "6px",
                                "transition": "all 0.2s ease",
                                "display": "inline-block",
                            },
                        ),
                        dcc.Link(
                            "Single Name Deep Dive",
                            href="/single-name",
                            className="nav-link",
                            style={
                                "color": "#cbd5e1",
                                "textDecoration": "none",
                                "fontSize": "16px",
                                "fontWeight": "500",
                                "padding": "8px 16px",
                                "borderRadius": "6px",
                                "transition": "all 0.2s ease",
                                "display": "inline-block",
                            },
                        ),
                    ],
                    style={
                        "padding": "12px 24px",
                        "background": "linear-gradient(135deg, #1e293b 0%, #0f172a 100%)",
                        "borderBottom": "1px solid #334155",
                    },
                ),
            ]
        ),
        # Content area with dark background
        dcc.Location(id="url", refresh=False),
        html.Div(
            id="page-content",
            style={
                "minHeight": "600px",
                "backgroundColor": "#0f172a",
                "padding": "0",
            },
        ),
    ]
)


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def display_page(pathname: str) -> html.Div:
    """Route to appropriate page based on URL."""
    if pathname == "/single-name":
        return create_single_name_layout()
    else:
        return create_market_overview_layout()


def main() -> None:
    """Run the Dash server."""
    import os
    import sys
    from pathlib import Path
    
    # Sync data from GCS on startup if enabled
    gcs_enabled = os.getenv("GCS_ENABLED", "false").lower() == "true"
    if gcs_enabled:
        try:
            # Add src to path for imports
            src_path = Path(__file__).parent.parent / "src"
            if str(src_path) not in sys.path:
                sys.path.insert(0, str(src_path))
            
            from utils.storage import sync_marts_from_gcs
            from config.settings import get_settings
            
            settings = get_settings()
            if settings.gcs_bucket_name:
                print("Syncing data from GCS on startup...")
                try:
                    sync_marts_from_gcs(
                        bucket_name=settings.gcs_bucket_name,
                        local_marts_dir=settings.marts_dir,
                        gcs_prefix=settings.gcs_marts_prefix,
                    )
                    print("✅ Data sync complete")
                except Exception as e:
                    print(f"⚠️  GCS sync failed (continuing anyway): {e}")
                    print("Dashboard will start with existing local data or empty state.")
        except Exception as e:
            print(f"⚠️  Could not sync from GCS: {e}")
            print("Dashboard will start with existing local data or empty state.")
    
    # Disable debug in production (set via environment variable)
    debug_mode = os.getenv("DASH_DEBUG", "false").lower() == "true"
    app.run_server(host="0.0.0.0", port=8050, debug=debug_mode)


if __name__ == "__main__":
    main()
