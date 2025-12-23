"""Single Name Deep Dive page - Page 2."""

from __future__ import annotations

from datetime import date

import dash_ag_grid as dag
import pandas as pd
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

from dashboard.data_access import (
    get_available_dates,
    get_latest_signal_scores,
    get_ticker_features,
    get_ticker_price_history,
)


def create_single_name_layout() -> html.Div:
    """Create the Single Name Deep Dive page layout."""
    available_dates = get_available_dates()
    latest_date = available_dates[0] if available_dates else None
    
    # Get available tickers from latest signal scores
    signals_df = get_latest_signal_scores()
    available_tickers = sorted(signals_df["ticker"].unique().tolist()) if not signals_df.empty else []
    
    return html.Div(
        [
            html.H2("Single Name Deep Dive", className="page-header"),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label("Ticker:", style={"marginRight": "10px"}),
                            dcc.Dropdown(
                                id="ticker-selector",
                                options=[{"label": t, "value": t} for t in available_tickers],
                                value=available_tickers[0] if available_tickers else None,
                                clearable=False,
                                style={"width": "150px"},
                            ),
                        ],
                        style={"display": "inline-block", "marginRight": "30px"},
                    ),
                    html.Div(
                        [
                            html.Label("Date:", style={"marginRight": "10px"}),
                            dcc.Dropdown(
                                id="single-name-date-filter",
                                options=[
                                    {"label": d.strftime("%Y-%m-%d"), "value": d.isoformat()}
                                    for d in available_dates
                                ],
                                value=latest_date.isoformat() if latest_date else None,
                                clearable=False,
                                style={"width": "200px"},
                            ),
                        ],
                        style={"display": "inline-block"},
                    ),
                ],
                style={"marginBottom": "30px"},
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H3("Price Chart with Features", style={"marginBottom": "15px"}),
                            dcc.Graph(id="price-feature-chart"),
                        ],
                        style={"marginBottom": "30px"},
                    ),
                    html.Div(
                        [
                            html.H3("Signal Score Breakdown", style={"marginBottom": "15px"}),
                            html.Div(id="signal-score-display", style={"marginBottom": "15px"}),
                            dag.AgGrid(
                                id="feature-breakdown-table",
                                columnDefs=[
                                    {
                                        "field": "feature_name",
                                        "headerName": "Feature",
                                        "width": 200,
                                    },
                                    {
                                        "field": "value",
                                        "headerName": "Value",
                                        "width": 150,
                                        "type": "numericColumn",
                                        "valueFormatter": {"function": "d3.format('.4f')(params.value)"},
                                    },
                                    {
                                        "field": "description",
                                        "headerName": "Description",
                                        "width": 300,
                                        "flex": 1,
                                    },
                                ],
                                rowData=[],
                                defaultColDef={
                                    "resizable": True,
                                    "sortable": True,
                                },
                                style={"height": "300px", "width": "100%"},
                            ),
                        ],
                    ),
                ],
            ),
        ],
        style={"padding": "20px"},
    )


@callback(
    [
        Output("price-feature-chart", "figure"),
        Output("signal-score-display", "children"),
        Output("feature-breakdown-table", "rowData"),
    ],
    [Input("ticker-selector", "value"), Input("single-name-date-filter", "value")],
)
def update_single_name(ticker: str, selected_date: str) -> tuple:
    """Update single name deep dive based on selected ticker and date."""
    if not ticker:
        empty_fig = go.Figure()
        empty_fig.add_annotation(
            text="Please select a ticker",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )
        return empty_fig, "", []
    
    # Parse date
    as_of_date = date.fromisoformat(selected_date) if selected_date else None
    
    # Get price history
    price_df = get_ticker_price_history(ticker, days=60)
    
    # Get signal score
    signals_df = get_latest_signal_scores(as_of_date=as_of_date)
    ticker_signal = (
        signals_df[signals_df["ticker"] == ticker]["signal_score"].iloc[0]
        if not signals_df.empty and ticker in signals_df["ticker"].values
        else None
    )
    
    # Get features
    features_df = get_ticker_features(ticker, as_of_date=as_of_date)
    
    # Create price chart with features
    fig = go.Figure()
    
    if not price_df.empty:
        # Price line
        fig.add_trace(
            go.Scatter(
                x=price_df["date"],
                y=price_df["close"],
                mode="lines",
                name="Close Price",
                line=dict(color="blue", width=2),
            )
        )
        
        # Add feature indicators if available
        if not features_df.empty:
            # Get the latest feature values
            latest_features = features_df.iloc[-1]
            
            # Add realized volatility indicator (shaded area)
            if "realized_vol_20d" in latest_features and pd.notna(
                latest_features["realized_vol_20d"]
            ):
                vol_value = float(latest_features["realized_vol_20d"])
                # Volatility is typically in decimal form (e.g., 0.02 = 2%)
                # Show as percentage bands around current price
                current_price = price_df["close"].iloc[-1]
                vol_pct = vol_value if vol_value < 1 else vol_value / 100
                fig.add_trace(
                    go.Scatter(
                        x=price_df["date"],
                        y=price_df["close"] * (1 + vol_pct),
                        mode="lines",
                        name=f"Vol Upper (+{vol_pct:.1%})",
                        line=dict(color="gray", width=1, dash="dash"),
                        showlegend=True,
                    )
                )
                fig.add_trace(
                    go.Scatter(
                        x=price_df["date"],
                        y=price_df["close"] * (1 - vol_pct),
                        mode="lines",
                        name=f"Vol Lower (-{vol_pct:.1%})",
                        line=dict(color="gray", width=1, dash="dash"),
                        showlegend=True,
                        fill="tonexty",
                        fillcolor="rgba(128,128,128,0.1)",
                    )
                )
        
        fig.update_layout(
            title=f"{ticker} Price Chart with Feature Indicators",
            xaxis_title="Date",
            yaxis_title="Price ($)",
            hovermode="x unified",
            height=500,
        )
    else:
        fig.add_annotation(
            text=f"No price data available for {ticker}",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )
    
    # Signal score display
    if ticker_signal is not None:
        signal_color = "green" if ticker_signal > 0 else "red"
        signal_display = html.Div(
            [
                html.Span("Signal Score: ", style={"fontSize": "16px"}),
                html.Span(
                    f"{ticker_signal:.2f}",
                    style={
                        "fontSize": "20px",
                        "fontWeight": "bold",
                        "color": signal_color,
                    },
                ),
            ]
        )
    else:
        signal_display = html.Div("Signal score not available", style={"color": "gray"})
    
    # Feature breakdown table
    feature_rows = []
    if not features_df.empty:
        latest_features = features_df.iloc[0]  # First row is the latest
        
        feature_mapping = {
            "realized_vol_20d": {
                "name": "20-Day Realized Volatility",
                "description": "Annualized volatility calculated from 20-day rolling returns",
            },
            "momentum_60d": {
                "name": "60-Day Momentum",
                "description": "Price return over the past 60 trading days",
            },
            "mean_reversion_zscore_5d": {
                "name": "5-Day Mean Reversion Z-Score",
                "description": "Z-score of current price vs 5-day moving average (negative = oversold, positive = overbought)",
            },
        }
        
        for col, info in feature_mapping.items():
            if col in latest_features and pd.notna(latest_features[col]):
                feature_rows.append(
                    {
                        "feature_name": info["name"],
                        "value": latest_features[col],
                        "description": info["description"],
                    }
                )
    
    return fig, signal_display, feature_rows

