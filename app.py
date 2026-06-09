import os
import logging
import dash
from dash import dcc, html, Input, Output, State, callback_context
import plotly.graph_objects as go
import pandas as pd

from config import (
    DASH_HOST,
    DASH_PORT,
    DASH_DEBUG,
    DEFAULT_PROPAGATION_HOURS,
    DEFAULT_TIME_STEP_MINUTES,
    DATA_DIR,
    MAX_VIS_POINTS,
    CA_POC_RED_THRESHOLD,
    CA_POC_YELLOW_THRESHOLD,
)
from dask_scheduler import DaskOrbitScheduler
from tle_generator import generate_sample_tle_dataset
from visualization import (
    build_3d_scene,
    create_debris_snapshot_scatter,
    create_conjunction_warning_lines,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("SpaceDebrisMonitor")

app = dash.Dash(
    __name__,
    external_stylesheets=[],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)
app.title = "Space Debris Monitoring Platform"
app.config.suppress_callback_exceptions = True

scheduler = DaskOrbitScheduler()
_propagation_done = False
_total_objects = 0
_n_time_steps = 0
_category_counts = {"LEO": 0, "MEO": 0, "GEO": 0, "HEO": 0}
_conjunctions = []
_pulse_phase = 0


def _ensure_tle_data():
    tle_path = os.path.join(DATA_DIR, "sample_tle.txt")
    if not os.path.exists(tle_path):
        os.makedirs(DATA_DIR, exist_ok=True)
        generate_sample_tle_dataset(n_leo=2000, n_meo=300, n_geo=200, n_heo=100, output_path=tle_path)
    return tle_path


def _run_initial_propagation():
    global _propagation_done, _total_objects, _n_time_steps, _category_counts, _conjunctions
    if _propagation_done:
        return

    tle_path = _ensure_tle_data()
    logger.info("Starting initial orbital propagation + CA assessment...")

    scheduler.submit_propagation_local(
        tle_path,
        hours=DEFAULT_PROPAGATION_HOURS,
        step_minutes=DEFAULT_TIME_STEP_MINUTES,
    )

    _total_objects = scheduler.total_objects
    _n_time_steps = scheduler.n_time_steps
    _category_counts = scheduler.get_category_counts()
    _conjunctions = scheduler.get_conjunctions()
    _propagation_done = True
    logger.info(f"Pipeline complete: {_total_objects} objects, {_n_time_steps} steps, {len(_conjunctions)} conjunctions")


HEADER_STYLE = {
    "background": "linear-gradient(135deg, #0a0a2e 0%, #1a1a4e 50%, #0d0d35 100%)",
    "borderBottom": "1px solid rgba(100,150,255,0.3)",
    "padding": "12px 24px",
    "display": "flex",
    "alignItems": "center",
    "justifyContent": "space-between",
}

PANEL_STYLE = {
    "background": "rgba(10,10,35,0.95)",
    "border": "1px solid rgba(80,120,200,0.25)",
    "borderRadius": "8px",
    "padding": "16px",
    "margin": "8px",
}

STAT_BOX_STYLE = {
    "background": "rgba(20,30,60,0.8)",
    "border": "1px solid rgba(80,120,200,0.2)",
    "borderRadius": "6px",
    "padding": "10px 14px",
    "textAlign": "center",
    "minWidth": "120px",
}

ALERT_RED_STYLE = {
    "background": "rgba(180,20,20,0.3)",
    "border": "1px solid rgba(255,50,50,0.6)",
    "borderRadius": "6px",
    "padding": "8px 12px",
    "marginBottom": "6px",
    "fontSize": "11px",
    "color": "#ff8888",
}

ALERT_YELLOW_STYLE = {
    "background": "rgba(180,160,20,0.2)",
    "border": "1px solid rgba(255,200,50,0.4)",
    "borderRadius": "6px",
    "padding": "8px 12px",
    "marginBottom": "6px",
    "fontSize": "11px",
    "color": "#ffdd88",
}

ALERT_GREEN_STYLE = {
    "background": "rgba(20,120,20,0.15)",
    "border": "1px solid rgba(80,200,80,0.3)",
    "borderRadius": "6px",
    "padding": "8px 12px",
    "marginBottom": "6px",
    "fontSize": "11px",
    "color": "#88cc88",
}

app.layout = html.Div(
    style={"background": "#050510", "color": "#c0d0e0", "fontFamily": "Consolas, monospace", "minHeight": "100vh"},
    children=[
        html.Div(
            style=HEADER_STYLE,
            children=[
                html.Div(
                    [
                        html.H1(
                            "\u2604 SPACE DEBRIS MONITOR",
                            style={
                                "margin": "0",
                                "fontSize": "22px",
                                "color": "#88bbff",
                                "letterSpacing": "3px",
                                "fontWeight": "300",
                            },
                        ),
                        html.Span(
                            "J2000 \u2022 SGP4 \u2022 Dask/Parquet \u2022 Conjunction Assessment",
                            style={"fontSize": "11px", "color": "#5577aa", "letterSpacing": "1px"},
                        ),
                    ]
                ),
                html.Div(id="system-status", style={"fontSize": "12px", "color": "#66aa66"}),
            ],
        ),
        html.Div(
            style={"display": "flex", "height": "calc(100vh - 70px)"},
            children=[
                html.Div(
                    style={"flex": "1", "position": "relative", "minWidth": "0"},
                    children=[
                        dcc.Loading(
                            id="loading-3d",
                            type="circle",
                            color="#4488ff",
                            children=dcc.Graph(
                                id="debris-3d-plot",
                                config={
                                    "scrollZoom": True,
                                    "displaylogo": False,
                                    "modeBarButtonsToAdd": ["toImage"],
                                },
                                style={"width": "100%", "height": "100%"},
                            ),
                        )
                    ],
                ),
                html.Div(
                    style={
                        "width": "340px",
                        "overflowY": "auto",
                        "borderLeft": "1px solid rgba(80,120,200,0.2)",
                        "padding": "12px",
                    },
                    children=[
                        html.Div(
                            style=PANEL_STYLE,
                            children=[
                                html.H4(
                                    "\u26A0 CONJUNCTION ALERTS",
                                    style={"margin": "0 0 8px 0", "fontSize": "13px", "color": "#ff6644"},
                                ),
                                html.Div(id="ca-summary", style={"marginBottom": "8px", "fontSize": "11px", "color": "#99aabb"}),
                                html.Div(id="ca-alerts-list", style={"maxHeight": "300px", "overflowY": "auto"}),
                            ],
                        ),
                        html.Div(
                            style=PANEL_STYLE,
                            children=[
                                html.H4(
                                    "\U0001F4CA ORBITAL STATISTICS",
                                    style={"margin": "0 0 12px 0", "fontSize": "13px", "color": "#88bbff"},
                                ),
                                html.Div(
                                    id="stats-panel",
                                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "8px"},
                                    children=[
                                        html.Div(style=STAT_BOX_STYLE, children=[
                                            html.Div("OBJECTS", style={"fontSize": "9px", "color": "#5577aa"}),
                                            html.Div(id="stat-total", style={"fontSize": "20px", "color": "#ff6644", "fontWeight": "bold"}),
                                        ]),
                                        html.Div(style=STAT_BOX_STYLE, children=[
                                            html.Div("LEO", style={"fontSize": "9px", "color": "#5577aa"}),
                                            html.Div(id="stat-leo", style={"fontSize": "20px", "color": "#ff4444", "fontWeight": "bold"}),
                                        ]),
                                        html.Div(style=STAT_BOX_STYLE, children=[
                                            html.Div("MEO", style={"fontSize": "9px", "color": "#5577aa"}),
                                            html.Div(id="stat-meo", style={"fontSize": "20px", "color": "#ffaa00", "fontWeight": "bold"}),
                                        ]),
                                        html.Div(style=STAT_BOX_STYLE, children=[
                                            html.Div("GEO", style={"fontSize": "9px", "color": "#5577aa"}),
                                            html.Div(id="stat-geo", style={"fontSize": "20px", "color": "#44aaff", "fontWeight": "bold"}),
                                        ]),
                                        html.Div(style=STAT_BOX_STYLE, children=[
                                            html.Div("HEO", style={"fontSize": "9px", "color": "#5577aa"}),
                                            html.Div(id="stat-heo", style={"fontSize": "20px", "color": "#aa44ff", "fontWeight": "bold"}),
                                        ]),
                                    ],
                                ),
                            ],
                        ),
                        html.Div(
                            style=PANEL_STYLE,
                            children=[
                                html.H4(
                                    "\u23F1 TIME CONTROL",
                                    style={"margin": "0 0 12px 0", "fontSize": "13px", "color": "#88bbff"},
                                ),
                                html.Div(id="time-display", style={"fontSize": "11px", "color": "#88aa88", "marginBottom": "8px"}),
                                dcc.Slider(
                                    id="time-slider",
                                    min=0,
                                    max=432,
                                    step=1,
                                    value=0,
                                    marks={i: f"{i * 10}m" for i in range(0, 434, 72)},
                                    tooltip={"placement": "bottom", "always_visible": False},
                                ),
                                html.Div(
                                    style={"display": "flex", "gap": "8px", "marginTop": "10px"},
                                    children=[
                                        html.Button("\u25B6 Play", id="btn-play", n_clicks=0, style={
                                            "flex": "1", "background": "rgba(50,80,150,0.5)", "color": "#aaccff",
                                            "border": "1px solid rgba(80,120,200,0.3)", "borderRadius": "4px",
                                            "padding": "6px", "cursor": "pointer", "fontSize": "11px",
                                        }),
                                        html.Button("\u23F9 Stop", id="btn-stop", n_clicks=0, style={
                                            "flex": "1", "background": "rgba(150,50,50,0.5)", "color": "#ffaaaa",
                                            "border": "1px solid rgba(200,80,80,0.3)", "borderRadius": "4px",
                                            "padding": "6px", "cursor": "pointer", "fontSize": "11px",
                                        }),
                                    ],
                                ),
                                dcc.Interval(id="play-interval", interval=200, n_intervals=0, disabled=True),
                            ],
                        ),
                        html.Div(
                            style=PANEL_STYLE,
                            children=[
                                html.H4(
                                    "\U0001F30D DISPLAY OPTIONS",
                                    style={"margin": "0 0 12px 0", "fontSize": "13px", "color": "#88bbff"},
                                ),
                                dcc.Checklist(
                                    id="display-options",
                                    options=[
                                        {"label": " Earth Surface", "value": "earth"},
                                        {"label": " Atmosphere Glow", "value": "atmosphere"},
                                        {"label": " Reference Orbits", "value": "ref_orbits"},
                                        {"label": " LEO Debris", "value": "leo"},
                                        {"label": " MEO Debris", "value": "meo"},
                                        {"label": " GEO Debris", "value": "geo"},
                                        {"label": " HEO Debris", "value": "heo"},
                                        {"label": " \u26A0 CA Warning Lines", "value": "ca_warnings"},
                                    ],
                                    value=["earth", "atmosphere", "ref_orbits", "leo", "meo", "geo", "heo", "ca_warnings"],
                                    style={"fontSize": "12px", "color": "#aabbcc"},
                                    inputStyle={"marginRight": "6px"},
                                    labelStyle={"display": "block", "marginBottom": "4px"},
                                ),
                            ],
                        ),
                        html.Div(
                            style=PANEL_STYLE,
                            children=[
                                html.H4(
                                    "\U0001F50D SELECTED OBJECT",
                                    style={"margin": "0 0 12px 0", "fontSize": "13px", "color": "#88bbff"},
                                ),
                                html.Div(id="object-detail", style={"fontSize": "11px", "color": "#99aabb", "lineHeight": "1.6"}),
                            ],
                        ),
                        html.Div(
                            style=PANEL_STYLE,
                            children=[
                                html.H4(
                                    "\u2699 COMPUTATION ENGINE",
                                    style={"margin": "0 0 12px 0", "fontSize": "13px", "color": "#88bbff"},
                                ),
                                html.Div(id="engine-info", style={"fontSize": "11px", "color": "#778899", "lineHeight": "1.6"}),
                            ],
                        ),
                    ],
                ),
            ],
        ),
    ],
)


@app.callback(
    [
        Output("debris-3d-plot", "figure"),
        Output("stat-total", "children"),
        Output("stat-leo", "children"),
        Output("stat-meo", "children"),
        Output("stat-geo", "children"),
        Output("stat-heo", "children"),
        Output("time-display", "children"),
        Output("engine-info", "children"),
        Output("system-status", "children"),
        Output("ca-summary", "children"),
        Output("ca-alerts-list", "children"),
    ],
    [
        Input("time-slider", "value"),
        Input("display-options", "value"),
        Input("play-interval", "n_intervals"),
    ],
)
def update_3d_scene(time_step, display_opts, pulse_n):
    global _pulse_phase
    _pulse_phase = (pulse_n % 10) / 10.0

    if not _propagation_done:
        _run_initial_propagation()

    if not display_opts:
        display_opts = []

    snapshot_df = scheduler.get_snapshot_at_time_sampled(time_index=time_step, max_points=MAX_VIS_POINTS)

    category_map = {"leo": "LEO", "meo": "MEO", "geo": "GEO", "heo": "HEO"}
    visible_categories = []
    for key, cat in category_map.items():
        if key in display_opts:
            visible_categories.append(cat)

    if snapshot_df is not None and not snapshot_df.empty:
        filtered_df = snapshot_df[snapshot_df["category"].isin(visible_categories)]
        debris_traces = create_debris_snapshot_scatter(filtered_df)
    else:
        debris_traces = []

    conjunction_traces = []
    if "ca_warnings" in display_opts and _conjunctions:
        nearby_conjs = scheduler.get_conjunctions_at_time(time_step, tolerance=5)
        if nearby_conjs:
            conjunction_traces = create_conjunction_warning_lines(nearby_conjs, pulse_phase=_pulse_phase)

    fig = build_3d_scene(
        debris_traces,
        conjunction_traces=conjunction_traces if conjunction_traces else None,
        show_earth="earth" in display_opts,
        show_atmosphere="atmosphere" in display_opts,
        show_reference_orbits="ref_orbits" in display_opts,
    )

    fig.update_layout(
        title=dict(
            text=f"Space Debris Field \u2014 T+{time_step * DEFAULT_TIME_STEP_MINUTES}min",
            font=dict(size=14, color="#88bbff"),
            x=0.5,
            y=0.98,
        ),
    )

    stats = dict(_category_counts)
    total = sum(stats.values())
    hours_el = time_step * DEFAULT_TIME_STEP_MINUTES / 60.0
    time_text = f"Elapsed: {hours_el:.1f}h / {DEFAULT_PROPAGATION_HOURS}h  |  Step: {time_step * DEFAULT_TIME_STEP_MINUTES} min"

    n_red = sum(1 for c in _conjunctions if c["alert_level"] == "RED")
    n_yellow = sum(1 for c in _conjunctions if c["alert_level"] == "YELLOW")
    n_green = sum(1 for c in _conjunctions if c["alert_level"] == "GREEN")

    engine_text = (
        f"SGP4/WGS72 + CA Engine\n"
        f"Frame: J2000 ECI\n"
        f"Engine: Dask + Parquet\n"
        f"Partition Size: {scheduler.chunk_size}\n"
        f"Workers: {scheduler.n_workers}\n"
        f"Time Step: {DEFAULT_TIME_STEP_MINUTES} min\n"
        f"PoC Red Line: {CA_POC_RED_THRESHOLD:.0e}\n"
        f"Conjunctions: {len(_conjunctions)}"
    )

    status_text = f"\u2713 {_total_objects} objects | {len(_conjunctions)} CA | PoC>{CA_POC_RED_THRESHOLD:.0e}: {n_red} RED"

    ca_summary = (
        f"Screened: {_total_objects} objects | "
        f"RED: {n_red} | YELLOW: {n_yellow} | GREEN: {n_green} | "
        f"Red threshold: PoC \u2265 {CA_POC_RED_THRESHOLD:.0e}"
    )

    alert_items = []
    sorted_conjs = sorted(_conjunctions, key=lambda c: c["poc"], reverse=True)
    for c in sorted_conjs[:30]:
        if c["alert_level"] == "RED":
            style = ALERT_RED_STYLE
            icon = "\u26A0\ufe0f RED"
        elif c["alert_level"] == "YELLOW":
            style = ALERT_YELLOW_STYLE
            icon = "\u26A1 YEL"
        else:
            style = ALERT_GREEN_STYLE
            icon = "\u2713 GRN"

        alert_items.append(
            html.Div(style=style, children=[
                html.Div(f"{icon}  {c['primary_id'][:14]} \u2194 {c['secondary_id'][:14]}"),
                html.Div(f"Miss: {c['miss_distance_km']:.3f} km  |  Rel.V: {c['relative_velocity_kms']:.2f} km/s"),
                html.Div(f"PoC: {c['poc']:.2e}  |  TCA step: {int(c['tca_index'])}"),
            ])
        )

    if not alert_items:
        alert_items = [html.Div("No conjunctions detected.", style={"fontSize": "11px", "color": "#668866"})]

    return (
        fig,
        str(total),
        str(stats.get("LEO", 0)),
        str(stats.get("MEO", 0)),
        str(stats.get("GEO", 0)),
        str(stats.get("HEO", 0)),
        time_text,
        engine_text,
        status_text,
        ca_summary,
        alert_items,
    )


@app.callback(
    [Output("play-interval", "disabled"), Output("play-interval", "n_intervals")],
    [Input("btn-play", "n_clicks"), Input("btn-stop", "n_clicks")],
    [State("time-slider", "value"), State("play-interval", "disabled")],
)
def control_playback(play_clicks, stop_clicks, current_val, is_disabled):
    ctx = callback_context
    if not ctx.triggered:
        return True, 0

    triggered_id = ctx.triggered[0]["prop_id"].split(".")[0]
    if triggered_id == "btn-play":
        return False, 0
    elif triggered_id == "btn-stop":
        return True, 0
    return True, 0


@app.callback(
    Output("time-slider", "value"),
    [Input("play-interval", "n_intervals")],
    [State("time-slider", "value"), State("play-interval", "disabled")],
)
def animate_time(n_intervals, current_val, is_disabled):
    if is_disabled:
        return current_val
    max_val = 432
    new_val = current_val + 1
    if new_val > max_val:
        new_val = 0
    return new_val


@app.callback(
    Output("object-detail", "children"),
    [Input("debris-3d-plot", "clickData")],
)
def display_object_detail(click_data):
    if not click_data or "points" not in click_data:
        return "Click on a debris object to inspect its orbital parameters."

    point = click_data["points"][0]
    text = point.get("text", "N/A")

    return html.Div([
        html.Div(f"ID: {text}", style={"color": "#88ccff", "fontWeight": "bold"}),
        html.Div(f"X: {point.get('x', 'N/A'):.0f} km"),
        html.Div(f"Y: {point.get('y', 'N/A'):.0f} km"),
        html.Div(f"Z: {point.get('z', 'N/A'):.0f} km"),
    ])


def main():
    global _propagation_done, _total_objects, _n_time_steps, _category_counts, _conjunctions

    logger.info("=" * 60)
    logger.info("  SPACE DEBRIS MONITORING PLATFORM")
    logger.info("  SGP4 + Dask/Parquet + CA Engine + Plotly/Dash")
    logger.info("=" * 60)

    tle_path = _ensure_tle_data()
    logger.info(f"TLE data: {tle_path}")

    _run_initial_propagation()

    logger.info(f"Launching Dash server on {DASH_HOST}:{DASH_PORT}")
    app.run(host=DASH_HOST, port=DASH_PORT, debug=DASH_DEBUG)


if __name__ == "__main__":
    main()
