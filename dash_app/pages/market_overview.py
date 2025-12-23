"""Market Overview page - Page 1."""

from __future__ import annotations

from datetime import date

import dash_ag_grid as dag
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Input, Output, callback, dcc, html

from dashboard.data_access import (
    get_available_dates,
    get_available_sectors,
    get_latest_positions,
    get_latest_signal_scores,
    get_sector_exposure,
)


def create_market_overview_layout() -> html.Div:
    """Create the Market Overview page layout."""
    available_dates = get_available_dates()
    available_sectors = get_available_sectors()
    latest_date = available_dates[0] if available_dates else None
    
    return html.Div(
        [
            html.H2("Market Overview", className="page-header"),
            html.Div(
                [
                    html.Div(
                        [
                            html.Label(
                                "Date:",
                                style={
                                    "marginRight": "10px",
                                    "color": "#cbd5e1",
                                    "fontWeight": "500",
                                    "fontSize": "14px",
                                },
                            ),
                            dcc.Dropdown(
                                id="date-filter",
                                options=[
                                    {"label": d.strftime("%Y-%m-%d"), "value": d.isoformat()}
                                    for d in available_dates
                                ],
                                value=latest_date.isoformat() if latest_date else None,
                                clearable=False,
                                style={"width": "200px"},
                                className="dark-dropdown",
                            ),
                        ],
                        style={"display": "inline-block", "marginRight": "30px"},
                    ),
                    html.Div(
                        [
                            html.Label(
                                "Sector:",
                                style={
                                    "marginRight": "10px",
                                    "color": "#cbd5e1",
                                    "fontWeight": "500",
                                    "fontSize": "14px",
                                },
                            ),
                            dcc.Dropdown(
                                id="sector-filter",
                                options=[{"label": "All Sectors", "value": "ALL"}]
                                + [{"label": s, "value": s} for s in available_sectors],
                                value="ALL",
                                clearable=False,
                                style={"width": "250px"},
                                className="dark-dropdown",
                            ),
                        ],
                        style={"display": "inline-block"},
                    ),
                ],
                className="filter-container",
                style={
                    "backgroundColor": "#1e293b",
                    "border": "1px solid #334155",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "marginBottom": "24px",
                    "display": "flex",
                    "gap": "24px",
                    "alignItems": "center",
                    "flexWrap": "wrap",
                },
            ),
            html.Div(
                [
                    html.Div(
                        [
                            html.H3(
                                "Top Long Candidates",
                                style={
                                    "marginBottom": "15px",
                                    "color": "#f1f5f9",
                                    "fontWeight": "600",
                                    "fontSize": "20px",
                                },
                            ),
                            dag.AgGrid(
                                id="long-candidates-table",
                                columnDefs=[
                                    {
                                        "field": "rank",
                                        "headerName": "Rank",
                                        "width": 80,
                                        "type": "numericColumn",
                                    },
                                    {
                                        "field": "ticker",
                                        "headerName": "Ticker",
                                        "width": 100,
                                    },
                                    {
                                        "field": "company",
                                        "headerName": "Company",
                                        "width": 200,
                                        "flex": 1,
                                    },
                                    {
                                        "field": "sector",
                                        "headerName": "Sector",
                                        "width": 180,
                                    },
                                    {
                                        "field": "signal_score",
                                        "headerName": "Signal Score",
                                        "width": 130,
                                        "type": "numericColumn",
                                        "valueFormatter": {"function": "d3.format('.2f')(params.value)"},
                                    },
                                ],
                                rowData=[],
                                defaultColDef={
                                    "resizable": True,
                                    "sortable": True,
                                    "filter": True,
                                },
                                dashGridOptions={
                                    "pagination": True,
                                    "paginationPageSize": 10,
                                },
                                style={"height": "400px", "width": "100%"},
                                className="ag-theme-alpine-dark",
                            ),
                        ],
                        style={
                            "width": "48%",
                            "display": "inline-block",
                            "marginRight": "2%",
                            "backgroundColor": "#1e293b",
                            "border": "1px solid #334155",
                            "borderRadius": "12px",
                            "padding": "20px",
                            "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.2)",
                        },
                    ),
                    html.Div(
                        [
                            html.H3(
                                "Top Short Candidates",
                                style={
                                    "marginBottom": "15px",
                                    "color": "#f1f5f9",
                                    "fontWeight": "600",
                                    "fontSize": "20px",
                                },
                            ),
                            dag.AgGrid(
                                id="short-candidates-table",
                                columnDefs=[
                                    {
                                        "field": "rank",
                                        "headerName": "Rank",
                                        "width": 80,
                                        "type": "numericColumn",
                                    },
                                    {
                                        "field": "ticker",
                                        "headerName": "Ticker",
                                        "width": 100,
                                    },
                                    {
                                        "field": "company",
                                        "headerName": "Company",
                                        "width": 200,
                                        "flex": 1,
                                    },
                                    {
                                        "field": "sector",
                                        "headerName": "Sector",
                                        "width": 180,
                                    },
                                    {
                                        "field": "signal_score",
                                        "headerName": "Signal Score",
                                        "width": 130,
                                        "type": "numericColumn",
                                        "valueFormatter": {"function": "d3.format('.2f')(params.value)"},
                                    },
                                ],
                                rowData=[],
                                defaultColDef={
                                    "resizable": True,
                                    "sortable": True,
                                    "filter": True,
                                },
                                dashGridOptions={
                                    "pagination": True,
                                    "paginationPageSize": 10,
                                },
                                style={"height": "400px", "width": "100%"},
                                className="ag-theme-alpine-dark",
                            ),
                        ],
                        style={
                            "width": "48%",
                            "display": "inline-block",
                            "backgroundColor": "#1e293b",
                            "border": "1px solid #334155",
                            "borderRadius": "12px",
                            "padding": "20px",
                            "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.2)",
                        },
                    ),
                ],
                style={"marginBottom": "30px"},
            ),
            html.Div(
                [
                    html.H3(
                        "Sector Exposure",
                        style={
                            "marginBottom": "15px",
                            "color": "#f1f5f9",
                            "fontWeight": "600",
                            "fontSize": "20px",
                        },
                    ),
                    dcc.Graph(id="sector-exposure-chart"),
                ],
                style={
                    "marginBottom": "30px",
                    "backgroundColor": "#1e293b",
                    "border": "1px solid #334155",
                    "borderRadius": "12px",
                    "padding": "20px",
                    "boxShadow": "0 4px 6px -1px rgba(0, 0, 0, 0.2)",
                },
            ),
        ],
        style={
            "padding": "32px",
            "backgroundColor": "#0f172a",
            "minHeight": "100vh",
        },
    )


@callback(
    [
        Output("long-candidates-table", "rowData"),
        Output("short-candidates-table", "rowData"),
        Output("sector-exposure-chart", "figure"),
    ],
    [Input("date-filter", "value"), Input("sector-filter", "value")],
)
def update_market_overview(selected_date: str, selected_sector: str) -> tuple:
    """Update market overview tables and charts based on filters."""
    # Parse date
    as_of_date = date.fromisoformat(selected_date) if selected_date else None
    
    # Get sector filter
    sector_filter = None if selected_sector == "ALL" else selected_sector
    
    # Get top long candidates (from positions or signal scores)
    positions_df = get_latest_positions(as_of_date=as_of_date, position_type="long")
    if positions_df.empty:
        # Fallback to signal scores
        signals_df = get_latest_signal_scores(as_of_date=as_of_date, sector_filter=sector_filter)
        signals_df = signals_df.head(10).copy()
        signals_df["rank"] = range(1, len(signals_df) + 1)
        long_data = signals_df.to_dict("records")
    else:
        if sector_filter:
            positions_df = positions_df[positions_df["sector"] == sector_filter].copy()
        long_data = positions_df.to_dict("records")
    
    # Get top short candidates
    short_positions_df = get_latest_positions(as_of_date=as_of_date, position_type="short")
    if short_positions_df.empty:
        # Fallback to signal scores
        signals_df = get_latest_signal_scores(as_of_date=as_of_date, sector_filter=sector_filter)
        signals_df = signals_df.tail(10).copy()
        signals_df["rank"] = range(1, len(signals_df) + 1)
        short_data = signals_df.to_dict("records")
    else:
        if sector_filter:
            short_positions_df = short_positions_df[
                short_positions_df["sector"] == sector_filter
            ].copy()
        short_data = short_positions_df.to_dict("records")
    
    # Get sector exposure
    exposure_df = get_sector_exposure(as_of_date=as_of_date)
    
    # Create sector exposure chart with dark theme
    if exposure_df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No exposure data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(color="#e2e8f0", size=16),
        )
        fig.update_layout(
            paper_bgcolor="#1e293b",
            plot_bgcolor="#1e293b",
            font=dict(color="#e2e8f0", family="Inter, sans-serif"),
        )
    else:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=exposure_df["sector"],
                y=exposure_df["long_count"],
                name="Longs",
                marker_color="#10b981",
                opacity=0.8,
                marker_line_color="#059669",
                marker_line_width=1,
            )
        )
        fig.add_trace(
            go.Bar(
                x=exposure_df["sector"],
                y=-exposure_df["short_count"],
                name="Shorts",
                marker_color="#ef4444",
                opacity=0.8,
                marker_line_color="#dc2626",
                marker_line_width=1,
            )
        )
        fig.update_layout(
            title=dict(
                text="Sector Exposure (Longs vs Shorts)",
                font=dict(color="#f1f5f9", size=18, family="Inter, sans-serif"),
            ),
            xaxis=dict(
                title="Sector",
                #titlefont=dict(color="#cbd5e1", size=14),
                tickfont=dict(color="#94a3b8", size=12),
                gridcolor="#334155",
                linecolor="#475569",
            ),
            yaxis=dict(
                title="Position Count",
                #titlefont=dict(color="#cbd5e1", size=14),
                tickfont=dict(color="#94a3b8", size=12),
                gridcolor="#334155",
                linecolor="#475569",
            ),
            barmode="overlay",
            hovermode="x unified",
            height=400,
            paper_bgcolor="#1e293b",
            plot_bgcolor="#1e293b",
            font=dict(color="#e2e8f0", family="Inter, sans-serif"),
            legend=dict(
                bgcolor="#0f172a",
                bordercolor="#334155",
                borderwidth=1,
                font=dict(color="#e2e8f0", size=12),
            ),
        )
    
    return long_data, short_data, fig

