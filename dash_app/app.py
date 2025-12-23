"""Main Dash application."""

from __future__ import annotations

import dash
from dash import html, dcc
from dash.dependencies import Input, Output

from dash_app.pages.market_overview import create_market_overview_layout
from dash_app.pages.single_name import create_single_name_layout

# Initialize Dash app
app = dash.Dash(__name__, suppress_callback_exceptions=True)

# Define app layout with navigation
app.layout = html.Div(
    [
        # Header
        html.Div(
            [
                html.H1(
                    "Unified Signal Monitoring Platform",
                    style={
                        "margin": "0",
                        "padding": "20px",
                        "backgroundColor": "#1f77b4",
                        "color": "white",
                    },
                ),
                html.Nav(
                    [
                        dcc.Link(
                            "Market Overview",
                            href="/",
                            style={
                                "marginRight": "20px",
                                "color": "white",
                                "textDecoration": "none",
                                "fontSize": "18px",
                            },
                        ),
                        dcc.Link(
                            "Single Name Deep Dive",
                            href="/single-name",
                            style={
                                "color": "white",
                                "textDecoration": "none",
                                "fontSize": "18px",
                            },
                        ),
                    ],
                    style={
                        "padding": "10px 20px",
                        "backgroundColor": "#2c3e50",
                    },
                ),
            ]
        ),
        # Content area
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content", style={"minHeight": "600px"}),
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
