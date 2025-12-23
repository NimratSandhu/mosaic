"""Main Dash application."""

from __future__ import annotations

import dash
from dash import html, dcc
from dash.dependencies import Input, Output

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
    app.run_server(host="0.0.0.0", port=8050, debug=True)


if __name__ == "__main__":
    main()
