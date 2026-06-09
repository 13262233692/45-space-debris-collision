import numpy as np
import plotly.graph_objects as go
from config import EARTH_RADIUS_KM, DEBRIS_CATEGORIES


def create_earth_sphere(radius_km=EARTH_RADIUS_KM, resolution=40, opacity=0.85):
    u = np.linspace(0, 2 * np.pi, resolution)
    v = np.linspace(0, np.pi, resolution)
    x = radius_km * np.outer(np.cos(u), np.sin(v))
    y = radius_km * np.outer(np.sin(u), np.sin(v))
    z = radius_km * np.outer(np.ones(np.size(u)), np.cos(v))

    earth_colors = np.empty(x.shape + (3,), dtype=np.uint8)
    for i in range(resolution):
        for j in range(resolution):
            lat = np.arcsin(z[i, j] / radius_km)
            lon = np.arctan2(y[i, j], x[i, j])
            land_noise = np.sin(3 * lon + 1.5) * np.cos(2 * lat + 0.7)
            if land_noise > 0.1:
                earth_colors[i, j] = [34, 85, 51]
            elif land_noise > -0.05:
                earth_colors[i, j] = [50, 110, 60]
            else:
                earth_colors[i, j] = [20, 50, 120]

    surfacecolor = np.zeros(x.shape, dtype=float)
    for i in range(resolution):
        for j in range(resolution):
            surfacecolor[i, j] = earth_colors[i, j, 1] / 255.0

    earth = go.Surface(
        x=x,
        y=y,
        z=z,
        surfacecolor=surfacecolor,
        colorscale=[
            [0.0, "rgb(20,50,120)"],
            [0.3, "rgb(20,60,140)"],
            [0.42, "rgb(50,110,60)"],
            [0.5, "rgb(34,85,51)"],
            [0.6, "rgb(60,130,70)"],
            [0.8, "rgb(40,90,55)"],
            [1.0, "rgb(200,200,210)"],
        ],
        opacity=opacity,
        showscale=False,
        lighting=dict(
            ambient=0.4,
            diffuse=0.8,
            specular=0.2,
            roughness=0.6,
            fresnel=0.3,
        ),
        hoverinfo="skip",
        name="Earth",
    )
    return earth


def create_atmosphere_glow(radius_km=EARTH_RADIUS_KM, thickness=200, resolution=30, opacity=0.08):
    r = radius_km + thickness
    u = np.linspace(0, 2 * np.pi, resolution)
    v = np.linspace(0, np.pi, resolution)
    x = r * np.outer(np.cos(u), np.sin(v))
    y = r * np.outer(np.sin(u), np.sin(v))
    z = r * np.outer(np.ones(np.size(u)), np.cos(v))

    glow = go.Surface(
        x=x,
        y=y,
        z=z,
        colorscale=[[0, "rgba(100,180,255,0.1)"], [1, "rgba(100,180,255,0.02)"]],
        opacity=opacity,
        showscale=False,
        hoverinfo="skip",
        name="Atmosphere",
    )
    return glow


def create_orbital_rings(radius_km, inclination_deg=0, raan_deg=0, opacity=0.15, color="cyan"):
    theta = np.linspace(0, 2 * np.pi, 200)
    x = radius_km * np.cos(theta)
    y = radius_km * np.sin(theta)
    z = np.zeros_like(theta)

    inc = np.radians(inclination_deg)
    raan = np.radians(raan_deg)

    R_raan = np.array(
        [
            [np.cos(raan), -np.sin(raan), 0],
            [np.sin(raan), np.cos(raan), 0],
            [0, 0, 1],
        ]
    )
    R_inc = np.array(
        [
            [1, 0, 0],
            [0, np.cos(inc), -np.sin(inc)],
            [0, np.sin(inc), np.cos(inc)],
        ]
    )
    R = R_raan @ R_inc

    coords = np.vstack([x, y, z])
    rotated = R @ coords

    ring = go.Scatter3d(
        x=rotated[0],
        y=rotated[1],
        z=rotated[2],
        mode="lines",
        line=dict(width=1, color=color),
        opacity=opacity,
        hoverinfo="skip",
        showlegend=False,
    )
    return ring


def create_debris_scatter(data_by_category, time_index=None):
    traces = []

    for category, cat_config in DEBRIS_CATEGORIES.items():
        if category not in data_by_category:
            continue

        d = data_by_category[category]
        x, y, z = d["x"], d["y"], d["z"]

        distances = np.sqrt(x**2 + y**2 + z**2)
        marker_size = np.clip((distances - EARTH_RADIUS_KM) / 500, 0.5, 3.0)

        trace = go.Scatter3d(
            x=x,
            y=y,
            z=z,
            mode="markers",
            marker=dict(
                size=marker_size,
                color=cat_config["color"],
                opacity=cat_config["opacity"],
                line=dict(width=0),
            ),
            hovertemplate="<b>%{text}</b><br>"
            + "X: %{x:.0f} km<br>Y: %{y:.0f} km<br>Z: %{z:.0f} km<extra></extra>",
            text=d.get("ids", [category] * len(x)) if isinstance(d.get("ids"), list) else [category] * len(x),
            name=f"{category} Debris",
            showlegend=True,
        )
        traces.append(trace)

    return traces


def create_debris_snapshot_scatter(snapshot_df):
    if snapshot_df is None or snapshot_df.empty:
        return []

    traces = []
    for category in snapshot_df["category"].unique():
        cat_data = snapshot_df[snapshot_df["category"] == category]
        cat_config = DEBRIS_CATEGORIES.get(category, DEBRIS_CATEGORIES["LEO"])

        trace = go.Scatter3d(
            x=cat_data["x_km"].values,
            y=cat_data["y_km"].values,
            z=cat_data["z_km"].values,
            mode="markers",
            marker=dict(
                size=3,
                color=cat_config["color"],
                opacity=cat_config["opacity"],
                line=dict(width=0),
            ),
            hovertemplate="<b>%{text}</b><br>"
            + "X: %{x:.0f} km<br>Y: %{y:.0f} km<br>Z: %{z:.0f} km<extra></extra>",
            text=cat_data["satellite_id"].values,
            name=f"{category} ({len(cat_data)})",
            showlegend=True,
        )
        traces.append(trace)

    return traces


def create_conjunction_warning_lines(conjunctions, pulse_phase=0):
    traces = []

    if not conjunctions:
        return traces

    for conj in conjunctions:
        p1 = conj["primary_pos_tca_km"]
        p2 = conj["secondary_pos_tca_km"]
        alert = conj["alert_level"]
        poc = conj["poc"]

        if alert == "RED":
            color = "rgb(255,30,30)"
            line_width = 4
            opacity = 0.9
        elif alert == "YELLOW":
            color = "rgb(255,200,30)"
            line_width = 2.5
            opacity = 0.7
        else:
            color = "rgb(100,200,100)"
            line_width = 1.5
            opacity = 0.4

        n_pulse = 20
        t = np.linspace(0, 1, n_pulse)
        x = p1[0] * (1 - t) + p2[0] * t
        y = p1[1] * (1 - t) + p2[1] * t
        z = p1[2] * (1 - t) + p2[2] * t

        pulse_envelope = 0.3 + 0.7 * np.abs(np.sin(2 * np.pi * (t * 3 + pulse_phase)))
        effective_opacity = opacity * pulse_envelope

        line = go.Scatter3d(
            x=x, y=y, z=z,
            mode="lines",
            line=dict(width=line_width, color=color),
            opacity=opacity,
            hovertemplate=(
                f"<b>CONJUNCTION WARNING</b><br>"
                f"Primary: {conj['primary_id']}<br>"
                f"Secondary: {conj['secondary_id']}<br>"
                f"Miss: {conj['miss_distance_km']:.3f} km<br>"
                f"Rel.Vel: {conj['relative_velocity_kms']:.2f} km/s<br>"
                f"PoC: {poc:.2e}<br>"
                f"Alert: {alert}<extra></extra>"
            ),
            name=f"CA {alert}: {conj['primary_id'][:12]}↔{conj['secondary_id'][:12]}",
            showlegend=True,
        )
        traces.append(line)

        marker_size = 10 if alert == "RED" else 7 if alert == "YELLOW" else 4
        marker_color = color

        for pos, label in [(p1, conj["primary_id"]), (p2, conj["secondary_id"])]:
            endpoint = go.Scatter3d(
                x=[pos[0]], y=[pos[1]], z=[pos[2]],
                mode="markers",
                marker=dict(
                    size=marker_size,
                    color=marker_color,
                    opacity=1.0 if alert == "RED" else 0.7,
                    line=dict(width=2 if alert == "RED" else 1, color="white"),
                    symbol="diamond" if alert == "RED" else "circle",
                ),
                hovertemplate=f"<b>{label}</b><br>X: {pos[0]:.0f} km<br>Y: {pos[1]:.0f} km<br>Z: {pos[2]:.0f} km<extra></extra>",
                name=f"{'⚠' if alert == 'RED' else '△'} {label[:16]}",
                showlegend=False,
            )
            traces.append(endpoint)

    return traces


def build_3d_scene(
    debris_traces,
    conjunction_traces=None,
    show_earth=True,
    show_atmosphere=True,
    show_reference_orbits=True,
):
    fig = go.Figure()

    if show_earth:
        fig.add_trace(create_earth_sphere())

    if show_atmosphere:
        fig.add_trace(create_atmosphere_glow())

    if show_reference_orbits:
        fig.add_trace(create_orbital_rings(EARTH_RADIUS_KM + 400, inclination_deg=51.6, raan_deg=0, color="rgba(255,255,100,0.2)"))
        fig.add_trace(create_orbital_rings(EARTH_RADIUS_KM + 780, inclination_deg=86.0, raan_deg=45, color="rgba(100,255,255,0.15)"))
        fig.add_trace(create_orbital_rings(EARTH_RADIUS_KM + 550, inclination_deg=28.5, raan_deg=90, color="rgba(255,150,100,0.15)"))
        fig.add_trace(create_orbital_rings(EARTH_RADIUS_KM + 20200, inclination_deg=55, raan_deg=0, color="rgba(200,150,255,0.1)"))
        fig.add_trace(create_orbital_rings(EARTH_RADIUS_KM + 35786, inclination_deg=0, raan_deg=0, color="rgba(100,200,255,0.15)"))

    for trace in debris_traces:
        fig.add_trace(trace)

    if conjunction_traces:
        for trace in conjunction_traces:
            fig.add_trace(trace)

    axis_range = EARTH_RADIUS_KM * 8
    fig.update_layout(
        scene=dict(
            xaxis=dict(
                range=[-axis_range, axis_range],
                title="X (km, J2000)",
                backgroundcolor="rgb(5,5,15)",
                gridcolor="rgba(50,50,80,0.3)",
                zerolinecolor="rgba(80,80,120,0.3)",
                showbackground=True,
            ),
            yaxis=dict(
                range=[-axis_range, axis_range],
                title="Y (km, J2000)",
                backgroundcolor="rgb(5,5,15)",
                gridcolor="rgba(50,50,80,0.3)",
                zerolinecolor="rgba(80,80,120,0.3)",
                showbackground=True,
            ),
            zaxis=dict(
                range=[-axis_range, axis_range],
                title="Z (km, J2000)",
                backgroundcolor="rgb(5,5,15)",
                gridcolor="rgba(50,50,80,0.3)",
                zerolinecolor="rgba(80,80,120,0.3)",
                showbackground=True,
            ),
            aspectmode="cube",
            camera=dict(
                eye=dict(x=1.5, y=1.5, z=0.8),
                center=dict(x=0, y=0, z=0),
                up=dict(x=0, y=0, z=1),
            ),
            bgcolor="rgb(2,2,8)",
        ),
        paper_bgcolor="rgb(8,8,18)",
        plot_bgcolor="rgb(8,8,18)",
        font=dict(color="rgb(180,200,220)", family="Consolas, monospace"),
        margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(
            bgcolor="rgba(10,10,30,0.8)",
            bordercolor="rgba(50,50,100,0.5)",
            borderwidth=1,
            font=dict(size=10, color="rgb(180,200,220)"),
        ),
    )

    return fig
