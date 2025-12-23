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
                            html.Label("Date:", style={"marginRight": "10px"}),
                            dcc.Dropdown(
                                id="date-filter",
                                options=[
                                    {"label": d.strftime("%Y-%m-%d"), "value": d.isoformat()}
                                    for d in available_dates
                                ],
                                value=latest_date.isoformat() if latest_date else None,
                                clearable=False,
                                style={"width": "200px"},
                            ),
                        ],
                        style={"display": "inline-block", "marginRight": "30px"},
                    ),
                    html.Div(
                        [
                            html.Label("Sector:", style={"marginRight": "10px"}),
                            dcc.Dropdown(
                                id="sector-filter",
                                options=[{"label": "All Sectors", "value": "ALL"}]
                                + [{"label": s, "value": s} for s in available_sectors],
                                value="ALL",
                                clearable=False,
                                style={"width": "250px"},
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
                            html.H3("Top Long Candidates", style={"marginBottom": "15px"}),
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
                            ),
                        ],
                        style={"width": "48%", "display": "inline-block", "marginRight": "2%"},
                    ),
                    html.Div(
                        [
                            html.H3("Top Short Candidates", style={"marginBottom": "15px"}),
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
                            ),
                        ],
                        style={"width": "48%", "display": "inline-block"},
                    ),
                ],
                style={"marginBottom": "30px"},
            ),
            html.Div(
                [
                    html.H3("Sector Exposure", style={"marginBottom": "15px"}),
                    dcc.Graph(id="sector-exposure-chart"),
                ],
                style={"marginBottom": "30px"},
            ),
        ],
        style={"padding": "20px"},
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
    
    # Create sector exposure chart
    if exposure_df.empty:
        fig = go.Figure()
        fig.add_annotation(
            text="No exposure data available",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )
    else:
        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=exposure_df["sector"],
                y=exposure_df["long_count"],
                name="Longs",
                marker_color="green",
                opacity=0.7,
            )
        )
        fig.add_trace(
            go.Bar(
                x=exposure_df["sector"],
                y=-exposure_df["short_count"],
                name="Shorts",
                marker_color="red",
                opacity=0.7,
            )
        )
        fig.update_layout(
            title="Sector Exposure (Longs vs Shorts)",
            xaxis_title="Sector",
            yaxis_title="Position Count",
            barmode="overlay",
            hovermode="x unified",
            height=400,
        )
    
    return long_data, short_data, fig

