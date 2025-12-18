from __future__ import annotations

import dash
from dash import html

app = dash.Dash(__name__)
app.layout = html.Div(
    [
        html.H2("Unified Signal Monitoring Platform"),
        html.P("Dashboard pages will be added in Milestone 4."),
    ]
)


def main() -> None:
    app.run_server(host="0.0.0.0", port=8050, debug=True)


if __name__ == "__main__":
    main()

