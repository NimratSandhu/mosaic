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
    """Health check endpoint for Render.com."""
    return jsonify({"status": "ok"}), 200


@app.server.route("/api/run-pipeline", methods=["POST"])
def run_pipeline():
    """
    Trigger the data pipeline (ingestion, curation, features).
    
    Requires a secret token in the request header or query param for security.
    """
    # Simple security: check for a token (set via environment variable)
    import os
    expected_token = os.getenv("PIPELINE_TOKEN", "")
    
    # Get token from header or query param
    token = request.headers.get("X-Pipeline-Token") or request.args.get("token")
    
    if expected_token and token != expected_token:
        return jsonify({"error": "Unauthorized"}), 401
    
    try:
        run_date = request.args.get("date") or date.today().isoformat()
        
        # Run pipeline in background (non-blocking)
        # Note: In production, you might want to use a proper task queue
        import threading
        import sys
        from pathlib import Path
        
        def run_pipeline_task():
            # Add src to path
            src_path = Path(__file__).parent.parent / "src"
            if str(src_path) not in sys.path:
                sys.path.insert(0, str(src_path))
            
            try:
                # Import and run flows directly
                from flows.ingest_prices import ingest_prices
                from flows.ingest_fundamentals import ingest_fundamentals
                from flows.curate_data import curate_data
                from flows.build_features import build_features
                
                # Run the pipeline
                ingest_prices(run_date=run_date)
                ingest_fundamentals(run_date=run_date)
                curate_data(run_date=run_date)
                build_features(run_date=run_date)
            except Exception as e:
                # Log error but don't crash the server
                import logging
                logging.error(f"Pipeline error: {e}", exc_info=True)
        
        # Start pipeline in background thread
        thread = threading.Thread(target=run_pipeline_task, daemon=True)
        thread.start()
        
        return jsonify({
            "status": "started",
            "message": f"Pipeline started for date {run_date}",
            "note": "This may take several minutes. Check logs for progress."
        }), 202
        
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
    # Disable debug in production (set via environment variable)
    import os
    debug_mode = os.getenv("DASH_DEBUG", "false").lower() == "true"
    app.run_server(host="0.0.0.0", port=8050, debug=debug_mode)


if __name__ == "__main__":
    main()
