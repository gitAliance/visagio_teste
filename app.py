from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Any
import urllib.error
import urllib.request
import json

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

ROOT_DIR = Path(__file__).resolve().parent
EXCEL_PATH = ROOT_DIR / "base.xlsx"
INEP_SLIM_PATH = ROOT_DIR / "MICRODADOS_CADASTRO_CURSOS_2024_SLIM.csv"
INEP_MAIN_PATH = ROOT_DIR / "MICRODADOS_CADASTRO_CURSOS_2024.CSV"
TODOS = "Todos"
MODALIDADES_BASE = ["EAD", "PRESENCIAL", "SEMI"]
CORES_MODALIDADE = {
    "EAD": "#1f77b4",
    "PRESENCIAL": "#2ca02c",
    "SEMI": "#ff7f0e",
}
INEP_MODALIDADE_LABELS = {
    1: "Presencial",
    2: "Curso a distancia",
}
INEP_MODALIDADE_CORES = {
    "Presencial": "#2ca02c",
    "Curso a distancia": "#1f77b4",
    "VEDUCA Presencial": "#ff8c42",
    "VEDUCA EAD": "#ffb26b",
}
INEP_TP_REDE_LABELS = {
    1: "Publica",
    2: "Privada",
}
INEP_CATEGORIA_LABELS = {
    1: "Publica Federal",
    2: "Publica Estadual",
    3: "Publica Municipal",
    4: "Privada com fins lucrativos",
    5: "Privada sem fins lucrativos",
    7: "Especial",
}
INEP_NUMERIC_COLS = [
    "QT_CURSO",
    "QT_VG_TOTAL",
    "QT_VG_TOTAL_EAD",
    "QT_ING",
    "QT_ING_FEM",
    "QT_ING_MASC",
    "QT_MAT",
    "QT_MAT_FEM",
    "QT_MAT_MASC",
    "QT_CONC",
]
INEP_COL_LABELS = {
    "NU_ANO_CENSO": "Ano do censo",
    "NO_REGIAO": "Regiao",
    "SG_UF": "UF",
    "NO_IES": "IES",
    "NO_MANTENEDORA": "Mantenedora",
    "TP_MODALIDADE_ENSINO": "Modalidade",
    "TP_CATEGORIA_ADMINISTRATIVA": "Categoria administrativa",
    "TP_ORGANIZACAO_ACADEMICA": "Organizacao academica",
    "TP_REDE": "Tipo de rede",
    "NO_CINE_AREA_GERAL": "Area CINE",
    "NO_CINE_ROTULO": "Rotulo CINE",
    "QT_CURSO": "Quantidade de cursos",
    "QT_VG_TOTAL": "Vagas totais",
    "QT_VG_TOTAL_EAD": "Vagas EAD",
    "QT_ING": "Ingressantes",
    "QT_ING_FEM": "Ingressantes feminino",
    "QT_ING_MASC": "Ingressantes masculino",
    "QT_MAT": "Matriculados",
    "QT_MAT_FEM": "Matriculados feminino",
    "QT_MAT_MASC": "Matriculados masculino",
    "QT_CONC": "Concluintes",
}

INEP_FILTER_DIMENSIONS = [
    "NU_ANO_CENSO",
    "NO_REGIAO",
    "SG_UF",
    "NO_IES",
    "NO_MANTENEDORA",
    "TP_MODALIDADE_ENSINO",
    "TP_CATEGORIA_ADMINISTRATIVA",
    "TP_ORGANIZACAO_ACADEMICA",
    "TP_REDE",
    "NO_CINE_AREA_GERAL",
    "NO_CINE_ROTULO",
]

UF_CENTROIDS = {
    "AC": (-8.77, -70.55),
    "AL": (-9.71, -35.73),
    "AP": (1.41, -51.77),
    "AM": (-3.47, -65.10),
    "BA": (-12.96, -38.51),
    "CE": (-3.71, -38.54),
    "DF": (-15.78, -47.93),
    "ES": (-19.19, -40.34),
    "GO": (-16.64, -49.31),
    "MA": (-2.53, -44.30),
    "MT": (-15.60, -56.10),
    "MS": (-20.44, -54.64),
    "MG": (-19.92, -43.94),
    "PA": (-1.45, -48.49),
    "PB": (-7.12, -34.86),
    "PR": (-25.42, -49.27),
    "PE": (-8.05, -34.90),
    "PI": (-5.09, -42.80),
    "RJ": (-22.90, -43.17),
    "RN": (-5.79, -35.21),
    "RS": (-30.03, -51.23),
    "RO": (-8.76, -63.90),
    "RR": (2.82, -60.67),
    "SC": (-27.59, -48.55),
    "SP": (-23.55, -46.63),
    "SE": (-10.90, -37.07),
    "TO": (-10.18, -48.33),
}


def extract_main_block(xlsx: Path, sheet: str) -> pd.DataFrame:
    raw = pd.read_excel(xlsx, sheet_name=sheet, header=None)
    non_na_counts = raw.notna().sum(axis=1)
    candidate_rows = non_na_counts[non_na_counts >= 3].index.tolist()
    if not candidate_rows:
        return pd.DataFrame()

    blocks = []
    start = candidate_rows[0]
    prev = candidate_rows[0]
    for row in candidate_rows[1:]:
        if row == prev + 1:
            prev = row
        else:
            blocks.append((start, prev))
            start = row
            prev = row
    blocks.append((start, prev))

    block_start, block_end = max(blocks, key=lambda x: x[1] - x[0])
    block = raw.iloc[block_start : block_end + 1].copy()

    header_candidates = block.head(3)
    best_idx = 0
    best_score = -1.0
    for i in range(len(header_candidates)):
        row = header_candidates.iloc[i].astype(str)
        cleaned = row.replace("nan", "").str.strip()
        non_empty = int((cleaned != "").sum())
        unique = int(cleaned[cleaned != ""].nunique())
        score = non_empty + unique * 0.5
        if score > best_score:
            best_score = score
            best_idx = i

    header = header_candidates.iloc[best_idx].astype(str).replace("nan", "").str.strip()
    renamed_cols = []
    seen = {}
    for j, col in enumerate(header):
        name = col if col else f"col_{j}"
        if name in seen:
            seen[name] += 1
            name = f"{name}_{seen[name]}"
        else:
            seen[name] = 0
        renamed_cols.append(name)

    data = block.iloc[best_idx + 1 :].copy()
    data.columns = renamed_cols
    data = data.dropna(how="all")
    data = data.loc[:, data.notna().any(axis=0)]
    return data


def find_col(cols: pd.Index, token: str) -> str:
    token_up = token.upper()
    for col in cols:
        if token_up in str(col).upper():
            return str(col)
    raise KeyError(f"Coluna contendo token '{token}' nao encontrada.")


def find_col_optional(cols: pd.Index, token: str) -> str | None:
    token_up = token.upper()
    for col in cols:
        if token_up in str(col).upper():
            return str(col)
    return None


@st.cache_data(show_spinner=False)
def load_data() -> tuple[pd.DataFrame, dict]:
    alunos = extract_main_block(EXCEL_PATH, "Alunos V-Educa").copy()

    meta = {
        "ano": find_col(alunos.columns, "ANO"),
        "uf": find_col(alunos.columns, "UF"),
        "area": find_col(alunos.columns, "AREA"),
        "area_inep": find_col_optional(alunos.columns, "NO_CINE_AREA_GERAL"),
        "curso": find_col(alunos.columns, "CURSO"),
        "modalidade": find_col(alunos.columns, "MODAL"),
        "ticket": find_col(alunos.columns, "TICKET"),
        "ingressantes": find_col(alunos.columns, "INGRESS"),
        "matriculados": find_col(alunos.columns, "MATRIC"),
        "no_ies": find_col_optional(alunos.columns, "NO_IES"),
    }

    for c in [meta["ano"], meta["ticket"], meta["ingressantes"], meta["matriculados"]]:
        alunos[c] = pd.to_numeric(alunos[c], errors="coerce")

    alunos["receita_total_estimada"] = alunos[meta["ticket"]] * alunos[meta["matriculados"]]
    alunos[meta["ano"]] = alunos[meta["ano"]].astype("Int64")
    for c in [meta["uf"], meta["area"], meta["curso"], meta["modalidade"]]:
        alunos[c] = alunos[c].astype("string")
    if meta["area_inep"]:
        alunos[meta["area_inep"]] = alunos[meta["area_inep"]].astype("string")
    if meta["no_ies"]:
        alunos[meta["no_ies"]] = alunos[meta["no_ies"]].astype("string")

    return alunos, meta


@st.cache_data(show_spinner=False)
def load_inep_data() -> pd.DataFrame:
    if INEP_SLIM_PATH.exists():
        df = pd.read_csv(
            INEP_SLIM_PATH,
            sep=";",
            encoding="utf-8-sig",
            low_memory=False,
        )
    elif INEP_MAIN_PATH.exists():
        cols = [
            "NU_ANO_CENSO",
            "NO_REGIAO",
            "SG_UF",
            "NO_IES",
            "NO_MANTENEDORA",
            "TP_MODALIDADE_ENSINO",
            "TP_CATEGORIA_ADMINISTRATIVA",
            "TP_REDE",
            "NO_CINE_AREA_GERAL",
            "NO_CINE_ROTULO",
            *INEP_NUMERIC_COLS,
        ]
        header = pd.read_csv(INEP_MAIN_PATH, sep=";", nrows=0, encoding="latin-1")
        usecols = [c for c in cols if c in header.columns]
        df = pd.read_csv(
            INEP_MAIN_PATH,
            sep=";",
            encoding="latin-1",
            usecols=usecols,
            low_memory=False,
        )
    else:
        return pd.DataFrame()

    for col in ["NU_ANO_CENSO", "TP_MODALIDADE_ENSINO", "TP_CATEGORIA_ADMINISTRATIVA", "TP_REDE", *INEP_NUMERIC_COLS]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["NO_REGIAO", "SG_UF", "NO_IES", "NO_MANTENEDORA", "NO_CINE_AREA_GERAL", "NO_CINE_ROTULO"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


def options_for(df: pd.DataFrame, col: str) -> list[str]:
    vals = sorted([str(x) for x in df[col].dropna().unique().tolist()])
    return [TODOS] + vals


def selected_text(values: list[str], limit: int = 3) -> str:
    vals = [str(v) for v in values if v != TODOS]
    if not vals or TODOS in values:
        return TODOS
    if len(vals) > limit:
        return ", ".join(vals[:limit]) + " ..."
    return ", ".join(vals)


def file_token(text: str) -> str:
    token = re.sub(r"[^0-9A-Za-z]+", "_", str(text).strip())
    token = re.sub(r"_+", "_", token).strip("_")
    return token.lower() or "todos"


def normalize_selection(selected: list[str]) -> list[str] | None:
    if not selected or TODOS in selected:
        return None
    return selected


def metric_label(metric_col: str, mat_col: str, ing_col: str) -> str:
    if metric_col == mat_col:
        return "Matriculas"
    if metric_col == ing_col:
        return "Ingressantes"
    return "Receita"


def inep_metric_label(col: str) -> str:
    if col == "__QTD_CURSOS__":
        return "Quantidade de cursos"
    return INEP_COL_LABELS.get(col, col)


def inep_dim_label(col: str) -> str:
    return INEP_COL_LABELS.get(col, col)


def inep_value_label(col: str, value: object) -> str:
    if pd.isna(value):
        return "(vazio)"
    if col == "TP_MODALIDADE_ENSINO":
        key = int(value)
        txt = INEP_MODALIDADE_LABELS.get(key, str(key))
        return f"{txt} ({key})"
    if col == "TP_REDE":
        key = int(value)
        txt = INEP_TP_REDE_LABELS.get(key, str(key))
        return f"{txt} ({key})"
    if col == "TP_CATEGORIA_ADMINISTRATIVA":
        key = int(value)
        txt = INEP_CATEGORIA_LABELS.get(key, str(key))
        return f"{txt} ({key})"
    return str(value)


def inep_filter_option_pairs(df: pd.DataFrame, col: str) -> list[tuple[str, object]]:
    vals = [v for v in df[col].dropna().unique().tolist()]
    vals = sorted(vals, key=lambda x: str(x))
    return [(inep_value_label(col, v), v) for v in vals]


def axis_label(x_col: str, curso_col: str, area_col: str) -> str:
    if x_col == curso_col:
        return "Curso"
    if x_col == area_col:
        return "Area"
    return "Estado (UF)"


def apply_filter(data: pd.DataFrame, col: str, selected: list[str]) -> pd.DataFrame:
    normalized = normalize_selection(selected)
    if normalized is None:
        return data
    return data[data[col].astype(str).isin(normalized)]


def grouped_for_main(
    data_base: pd.DataFrame,
    metrica: str,
    x_col: str,
    modal_col: str,
    topn: int,
    stack_order: list[str] | None = None,
) -> tuple[pd.DataFrame, list[str]]:
    top_values = (
        data_base.groupby(x_col, as_index=False)[metrica]
        .sum()
        .sort_values(metrica, ascending=False)
        .head(topn)[x_col]
        .astype(str)
        .tolist()
    )
    base = data_base[data_base[x_col].astype(str).isin(top_values)].copy()
    if base.empty:
        return pd.DataFrame(), []

    grp = base.groupby([x_col, modal_col], as_index=False)[metrica].sum()
    total = grp.groupby(x_col, as_index=False)[metrica].sum().rename(columns={metrica: "total_grupo"})
    grp = grp.merge(total, on=x_col, how="left", suffixes=("", "_dup"))
    # Remover colunas duplicadas (se houver)
    grp = grp.loc[:, ~grp.columns.duplicated()]
    grp["pct_grupo"] = grp[metrica] / grp["total_grupo"] * 100.0

    order_map = {c: i for i, c in enumerate(top_values)}
    grp["ordem"] = grp[x_col].astype(str).map(order_map)
    categories = stack_order if stack_order else sorted(grp[modal_col].astype(str).unique().tolist())
    grp[modal_col] = pd.Categorical(grp[modal_col], categories=categories, ordered=True)
    grp = grp.sort_values(["ordem", modal_col]).drop(columns=["ordem"])
    return grp, top_values


def render_main_chart(
    data_base: pd.DataFrame,
    metrica: str,
    x_col: str,
    modal_col: str,
    topn: int,
    ano_txt: str,
    uf_txt: str,
    area_txt: str,
    metric_txt: str,
    x_label: str,
    stack_order: list[str] | None = None,
    color_map: dict[str, str] | None = None,
) -> go.Figure | None:
    comp, order = grouped_for_main(data_base, metrica, x_col, modal_col, topn, stack_order=stack_order)
    if comp.empty:
        return None

    order_modal = stack_order if stack_order else sorted(comp[modal_col].astype(str).unique().tolist())
    colors = color_map if color_map else CORES_MODALIDADE

    fig = go.Figure()
    for mod in order_modal:
        part = comp[comp[modal_col].astype(str) == mod]
        if part.empty:
            continue
        fig.add_trace(
            go.Bar(
                x=part[x_col].astype(str),
                y=part[metrica],
                text=part["pct_grupo"].map(lambda v: f"{v:.1f}%"),
                textposition="inside",
                textangle=0,
                insidetextanchor="middle",
                textfont=dict(size=11, color="white"),
                name=mod,
                marker_color=colors.get(mod, "#888888"),
                customdata=part["pct_grupo"],
                hovertemplate=(
                    f"{x_label}: %{{x}}<br>"
                    f"Modalidade: {mod}<br>"
                    "Valor: %{y:,.2f}<br>"
                    "% no grupo: %{customdata:.2f}%<extra></extra>"
                ),
            )
        )

    fig.update_layout(
        barmode="stack",
        title=(
            f"Composicao por {x_label.lower()} - Metrica: {metric_txt}"
            f"<br><sup>Ano: {ano_txt} | UF: {uf_txt} | Area: {area_txt}</sup>"
        ),
        xaxis_title=x_label,
        yaxis_title=metric_txt,
        xaxis=dict(categoryorder="array", categoryarray=order, tickangle=-35),
        height=560,
        margin=dict(l=40, r=20, t=84, b=64),
        uniformtext_minsize=9,
        uniformtext_mode="hide",
        legend=dict(
            x=0.99,
            y=0.99,
            xanchor="right",
            yanchor="top",
            bgcolor="rgba(255,255,255,0.55)",
            bordercolor="rgba(0,0,0,0.15)",
            borderwidth=1,
            orientation="v",
            title="Modalidade",
        ),
    )
    return fig


def render_ratio_chart(
    data_base: pd.DataFrame,
    x_col: str,
    mat_col: str,
    ing_col: str,
    topn: int,
    x_label: str,
) -> go.Figure | None:
    rel = (
        data_base.groupby(x_col, as_index=False)[[mat_col, ing_col]]
        .sum()
        .sort_values(mat_col, ascending=False)
        .head(topn)
    )
    rel = rel[rel[ing_col] > 0].copy()
    if rel.empty:
        return None

    rel["relacao"] = rel[mat_col] / rel[ing_col]
    fig = go.Figure(
        go.Bar(
            x=rel[x_col].astype(str),
            y=rel["relacao"],
            text=rel["relacao"].map(lambda v: f"{v:.2f}x"),
            textposition="outside",
            textangle=0,
            marker_color="#4c78a8",
            hovertemplate=f"{x_label}: %{{x}}<br>Relacao M/I: %{{y:.2f}}x<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Relacao Matriculas / Ingressantes por {x_label.lower()}",
        xaxis_title=x_label,
        yaxis_title="Relacao (x)",
        xaxis_tickangle=-35,
        height=560,
    )
    return fig


def render_pie_chart(
    data_base: pd.DataFrame,
    value_col: str,
    group_col: str,
    title: str,
    color_sequence: list[str] | None = None,
    value_label: str | None = None,
    top_n: int | None = None,
    outros_label: str = "Outros",
) -> go.Figure | None:
    if data_base.empty or group_col not in data_base.columns or value_col not in data_base.columns:
        return None

    pie_df = data_base[[group_col, value_col]].copy()
    pie_df[group_col] = pie_df[group_col].astype("string").fillna("(vazio)").str.strip()
    pie_df[value_col] = pd.to_numeric(pie_df[value_col], errors="coerce").fillna(0)
    pie_df = pie_df[pie_df[value_col] > 0]
    if pie_df.empty:
        return None

    grouped = pie_df.groupby(group_col, as_index=False)[value_col].sum().sort_values(value_col, ascending=False)
    if top_n is not None and top_n > 0 and len(grouped) > top_n:
        top_part = grouped.head(top_n).copy()
        rest_sum = float(grouped.iloc[top_n:][value_col].sum())
        if rest_sum > 0:
            top_part = pd.concat(
                [top_part, pd.DataFrame([{group_col: outros_label, value_col: rest_sum}])],
                ignore_index=True,
            )
        grouped = top_part

    fig = go.Figure(
        go.Pie(
            labels=grouped[group_col],
            values=grouped[value_col],
            hole=0.35,
            sort=False,
            direction="clockwise",
            textinfo="percent+label",
            textposition="inside",
            marker=dict(line=dict(color="rgba(255,255,255,0.15)", width=1)),
            hovertemplate=(
                f"<b>%{{label}}</b><br>"
                + (f"{value_label or value_col}: " if value_label or value_col else "")
                + "%{value:,.0f}<extra></extra>"
            ),
            customdata=grouped[value_col],
            textfont=dict(size=11),
            showlegend=True,
        )
    )
    if color_sequence:
        fig.update_traces(marker=dict(colors=color_sequence))
    fig.update_layout(
        title=title,
        height=420,
        margin=dict(l=10, r=10, t=60, b=10),
        legend=dict(orientation="v", yanchor="top", y=1.0, xanchor="left", x=1.02),
    )
    return fig


def render_rank_chart(
    data_base: pd.DataFrame,
    x_col: str,
    metrica: str,
    topn: int,
    x_label: str,
    metric_txt: str,
) -> go.Figure | None:
    rank = (
        data_base.groupby(x_col, as_index=False)[metrica]
        .sum()
        .sort_values(metrica, ascending=False)
        .head(topn)
    )
    if rank.empty:
        return None

    rank = rank.sort_values(metrica, ascending=True)
    fig = go.Figure(
        go.Bar(
            x=rank[metrica],
            y=rank[x_col].astype(str),
            orientation="h",
            text=rank[metrica],
            textposition="outside",
            marker_color="#2f6f95",
            hovertemplate=f"{x_label}: %{{y}}<br>{metric_txt}: %{{x:,.2f}}<extra></extra>",
        )
    )
    fig.update_layout(
        title=f"Ranking por {x_label.lower()} - {metric_txt} (Top {topn})",
        xaxis_title=metric_txt,
        yaxis_title=x_label,
        height=560,
    )
    return fig


def render_uf_brazil_map(
    uf_values: pd.DataFrame,
    metric_col: str,
    metric_txt: str,
    theme: str = "dark",
    colorscale: list[list[object]] | None = None,
    title: str | None = None,
    show_labels: bool = True,
    height: int = 460,
) -> go.Figure | None:
    if uf_values.empty:
        placeholder = go.Figure()
        placeholder.add_annotation(
            text="Sem dados para esta modalidade",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=16, color="#cfcfcf" if theme.lower() != "light" else "#444444"),
        )
        placeholder.update_layout(
            title=title or f"Mapa do Brasil por UF - {metric_txt}",
            height=height,
            paper_bgcolor="rgba(0,0,0,0)" if theme.lower() != "light" else "white",
            plot_bgcolor="rgba(0,0,0,0)" if theme.lower() != "light" else "white",
            template="plotly_dark" if theme.lower() != "light" else "plotly_white",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
        )
        return placeholder

    map_df = uf_values.copy()
    map_df["SG_UF"] = map_df["SG_UF"].astype("string").str.upper()
    map_df = map_df[map_df["SG_UF"].isin(UF_CENTROIDS.keys())].copy()
    if map_df.empty:
        return None

    map_df["valor_fmt"] = map_df[metric_col].map(lambda v: f"{float(v):,.0f}".replace(",", "."))

    theme_dark = theme.lower() != "light"
    if theme_dark:
        default_palette = [
            [0.0, "#271a0f"],
            [0.2, "#5a3417"],
            [0.4, "#8a4a18"],
            [0.6, "#c5661e"],
            [0.8, "#ea8f35"],
            [1.0, "#ffb25f"],
        ]
        bg_color = "rgba(0,0,0,0)"
        land_color = "#1e1e1e"
        label_color = "#f3f3f3"
        border_color = "#5f5f5f"
        paper_bg = "rgba(0,0,0,0)"
        plot_bg = "rgba(0,0,0,0)"
    else:
        palette = [
            [0.0, "#fbf2ea"],
            [0.2, "#f6dcc6"],
            [0.4, "#f0bf99"],
            [0.6, "#e69959"],
            [0.8, "#d96d1d"],
            [1.0, "#b65200"],
        ]
        bg_color = "rgba(0,0,0,0)"
        land_color = "#f4f0ea"
        label_color = "#2f2f2f"
        border_color = "#9a9a9a"
        paper_bg = "white"
        plot_bg = "white"

    palette = colorscale or default_palette

    # Mapa do Brasil por estado (choropleth).
    try:
        geojson_url = "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
        with urllib.request.urlopen(geojson_url, timeout=12) as response:
            geojson_data = json.loads(response.read().decode("utf-8"))

        choropleth = go.Choropleth(
            geojson=geojson_data,
            featureidkey="properties.sigla",
            locations=map_df["SG_UF"],
            z=map_df[metric_col],
            colorscale=palette,
            marker_line_color=border_color,
            marker_line_width=2.5,
            colorbar=dict(title=metric_txt),
            customdata=map_df[["SG_UF", "valor_fmt"]],
            hovertemplate=(
                "<b>UF: %{customdata[0]}</b><br>"
                + f"{metric_txt}: "
                + "%{customdata[1]}<extra></extra>"
            ),
            showscale=True,
            zmin=float(map_df[metric_col].min()),
            zmax=float(map_df[metric_col].max()),
        )
        fig = go.Figure(choropleth)
        label_df = map_df.copy()
        label_df["lat"] = label_df["SG_UF"].map(lambda uf: UF_CENTROIDS.get(uf, (None, None))[0])
        label_df["lon"] = label_df["SG_UF"].map(lambda uf: UF_CENTROIDS.get(uf, (None, None))[1])
        label_df = label_df.dropna(subset=["lat", "lon"])
        if show_labels and not label_df.empty:
            fig.add_trace(
                go.Scattergeo(
                    lat=label_df["lat"],
                    lon=label_df["lon"],
                    mode="text",
                    text=label_df["SG_UF"],
                    textfont=dict(size=11, color=label_color, family="Arial Black"),
                    hoverinfo="skip",
                    showlegend=False,
                )
            )
        fig.update_geos(
            fitbounds="locations",
            visible=False,
            showcoastlines=False,
            showland=True,
            landcolor=land_color,
            bgcolor=bg_color,
        )
        fig.update_layout(
            title=title or f"Mapa do Brasil por UF - {metric_txt}",
            height=height,
            margin=dict(l=10, r=20, t=70, b=10),
            paper_bgcolor=paper_bg,
            plot_bgcolor=plot_bg,
            template="plotly_dark" if theme_dark else "plotly_white",
            dragmode=False,
            geo=dict(
                showframe=False,
                showcountries=False,
                showlakes=False,
                showrivers=False,
            ),
        )
        return fig
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
        fallback = go.Figure()
        fallback.add_annotation(
            text="Nao foi possivel carregar o mapa do Brasil agora. Verifique a conexao e tente novamente.",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=15, color="#cfcfcf" if theme_dark else "#444444"),
        )
        fallback.update_layout(
            title=title or f"Mapa do Brasil por UF - {metric_txt}",
            height=height,
            paper_bgcolor=paper_bg,
            plot_bgcolor=plot_bg,
            template="plotly_dark" if theme_dark else "plotly_white",
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
        )
        return fallback


def render_top_uf_table(uf_values: pd.DataFrame, metric_col: str, metric_txt: str, limit: int = 10) -> pd.DataFrame:
    if uf_values.empty or metric_col not in uf_values.columns or "SG_UF" not in uf_values.columns:
        return pd.DataFrame(columns=["UF", metric_txt])

    table_df = uf_values[["SG_UF", metric_col]].copy()
    table_df["SG_UF"] = table_df["SG_UF"].astype("string").str.upper()
    table_df[metric_col] = pd.to_numeric(table_df[metric_col], errors="coerce").fillna(0)
    table_df = table_df[table_df["SG_UF"].notna() & (table_df["SG_UF"].str.strip() != "")]
    table_df = table_df[table_df[metric_col] > 0]
    table_df = table_df.sort_values(metric_col, ascending=False).head(limit)
    total_value = float(pd.to_numeric(uf_values[metric_col], errors="coerce").fillna(0).sum())
    total_row = pd.DataFrame([["TOTAL", total_value]], columns=["SG_UF", metric_col])
    table_df = pd.concat([table_df, total_row], ignore_index=True)
    table_df = table_df.rename(columns={"SG_UF": "UF", metric_col: metric_txt})
    return table_df


def aggregate_metric_by_uf(data: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    if data.empty or "SG_UF" not in data.columns:
        return pd.DataFrame(columns=["SG_UF", metric_col])

    base = data.copy()
    base["SG_UF"] = base["SG_UF"].astype("string").str.upper().str.strip()
    # Mantem apenas siglas validas de UF (2 letras).
    base = base[base["SG_UF"].str.match(r"^[A-Z]{2}$", na=False)]

    # Metricas derivadas que nao dependem de coluna pronta na base.
    if metric_col == "__QTD_CURSOS__":
        if "CO_CURSO" in base.columns:
            grouped = base.groupby("SG_UF", as_index=False)["CO_CURSO"].nunique()
            grouped = grouped.rename(columns={"CO_CURSO": metric_col})
        else:
            grouped = base.groupby("SG_UF", as_index=False).size().rename(columns={"size": metric_col})
    else:
        if metric_col not in base.columns:
            return pd.DataFrame(columns=["SG_UF", metric_col])

        # Garante agregacao numerica consistente nas metricas mais usadas.
        for col in ["QT_ING", "QT_MAT", metric_col]:
            if col in base.columns:
                base[col] = pd.to_numeric(base[col], errors="coerce").fillna(0)

        grouped = base.groupby("SG_UF", as_index=False)[metric_col].sum()

    grouped = grouped[grouped[metric_col] > 0]
    return grouped.sort_values(metric_col, ascending=False)


def render_top_uf_inset(uf_values: pd.DataFrame, metric_col: str, metric_txt: str, limit: int = 10) -> go.Figure:
    table_df = render_top_uf_table(uf_values, metric_col, metric_txt, limit=limit)
    fig = go.Figure()
    if table_df.empty:
        fig.add_annotation(
            text="Sem dados",
            x=0.5,
            y=0.5,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=11, color="#cccccc"),
        )
    else:
        row_count = len(table_df)
        fill_colors = [["#111318"] * row_count, ["#111318"] * row_count]
        fill_colors[0][-1] = "#2b2f36"
        fill_colors[1][-1] = "#2b2f36"
        fig.add_trace(
            go.Table(
                header=dict(
                    values=["UF", metric_txt],
                    fill_color="#202124",
                    font=dict(color="white", size=10),
                    align="left",
                    height=20,
                ),
                cells=dict(
                    values=[table_df["UF"], table_df[metric_txt].map(lambda v: f"{float(v):,.0f}".replace(",", "."))],
                    fill_color=fill_colors,
                    font=dict(color="white", size=9),
                    align="left",
                    height=18,
                ),
            )
        )
    fig.update_layout(
        height=300,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return fig


def main() -> None:
    st.set_page_config(page_title="Dashboard V-Educa e INEP", layout="wide")
    st.title("Dashboard Dinamico - V-Educa e INEP")

    if not EXCEL_PATH.exists():
        st.error("Arquivo base.xlsx nao encontrado na raiz do projeto.")
        return

    alunos, meta = load_data()
    df_inep = load_inep_data()

    ano_col = meta["ano"]
    uf_col = meta["uf"]
    area_col = meta["area"]
    area_inep_col = meta["area_inep"]
    curso_col = meta["curso"]
    modal_col = meta["modalidade"]
    veduca_no_ies_col = meta["no_ies"]
    mat_col = meta["matriculados"]
    ing_col = meta["ingressantes"]
    rec_col = "receita_total_estimada"

    with st.sidebar:
        st.header("Filtros")

        st.subheader("V-Educa")
        anos = st.multiselect("Ano", options_for(alunos, ano_col), default=[TODOS], key="vedu_anos")
        ufs = st.multiselect("UF", options_for(alunos, uf_col), default=[TODOS], key="vedu_ufs")
        areas = st.multiselect("Area", options_for(alunos, area_col), default=[TODOS], key="vedu_areas")
        if area_inep_col and area_inep_col in alunos.columns:
            areas_inep = st.multiselect("Area INEP", options_for(alunos, area_inep_col), default=[TODOS], key="vedu_areas_inep")
        else:
            areas_inep = [TODOS]
        cursos = st.multiselect("Curso", options_for(alunos, curso_col), default=[TODOS], key="vedu_cursos")
        modalidades = st.multiselect("Modalidade", options_for(alunos, modal_col), default=[TODOS], key="vedu_modalidades")

        visao = st.selectbox(
            "Eixo X",
            options=[curso_col, area_col, uf_col],
            format_func=lambda c: axis_label(c, curso_col, area_col),
            key="vedu_visao",
        )
        metrica = st.selectbox(
            "Metrica",
            options=[mat_col, ing_col, rec_col],
            format_func=lambda c: metric_label(c, mat_col, ing_col),
            key="vedu_metrica",
        )
        topn = st.slider("Top N", min_value=5, max_value=40, value=15, step=1, key="vedu_topn")

        st.divider()
        st.subheader("INEP")

        destacar_veduca_inep = st.toggle(
            "Destacar V-Educa no grafico INEP",
            value=False,
            key="inep_highlight_veduca",
            help="Quando ativado, separa visualmente VEDUCA EAD e VEDUCA Presencial no grafico de composicao do INEP.",
        )

        available_metrics = [c for c in INEP_NUMERIC_COLS if c in df_inep.columns] if not df_inep.empty else []
        available_dims = [c for c in INEP_FILTER_DIMENSIONS if c in df_inep.columns] if not df_inep.empty else []

        if available_metrics and available_dims:
            inep_metric = st.selectbox(
                "Metrica (INEP)",
                options=available_metrics,
                format_func=inep_metric_label,
                index=available_metrics.index("QT_MAT") if "QT_MAT" in available_metrics else 0,
                key="inep_metric",
            )
            inep_dim = st.selectbox(
                "Dimensao (Eixo X)",
                options=available_dims,
                format_func=inep_dim_label,
                index=available_dims.index("NO_REGIAO") if "NO_REGIAO" in available_dims else 0,
                key="inep_dim",
            )
            inep_topn = st.slider("Top N (INEP)", min_value=5, max_value=40, value=15, step=1, key="inep_topn")

            inep_filterable_dims = [c for c in INEP_FILTER_DIMENSIONS if c in df_inep.columns]
            default_dyn_dims = [
                c
                for c in ["NU_ANO_CENSO", "NO_REGIAO", "SG_UF", "NO_CINE_AREA_GERAL"]
                if c in inep_filterable_dims
            ]

            inep_dyn_dims = st.multiselect(
                "Filtros dinamicos (INEP)",
                options=inep_filterable_dims,
                default=default_dyn_dims,
                format_func=inep_dim_label,
                key="inep_dyn_dims",
                help="Escolha as dimensoes para filtrar dinamicamente. Exemplo: Area CINE.",
            )

            inep_dynamic_filters: dict[str, list[object]] = {}
            for dim_col in inep_dyn_dims:
                option_pairs = inep_filter_option_pairs(df_inep, dim_col)
                option_labels = [label for label, _ in option_pairs]

                selected_labels = st.multiselect(
                    f"{inep_dim_label(dim_col)} (INEP)",
                    options=option_labels,
                    default=[],
                    key=f"inep_dyn_vals_{dim_col}",
                )

                label_to_value = {label: value for label, value in option_pairs}
                selected_values = [label_to_value[label] for label in selected_labels if label in label_to_value]
                if selected_values:
                    inep_dynamic_filters[dim_col] = selected_values
        else:
            st.info("INEP indisponivel: arquivo/campos nao encontrados.")
            inep_metric = "QT_MAT"
            inep_dim = "NO_REGIAO"
            inep_topn = 15
            inep_dynamic_filters = {}

    app_tab1, app_tab2, app_tab3 = st.tabs(["V-Educa", "INEP Cursos", "Análise por Mantenedora"])

    with app_tab1:
        st.subheader("Painel V-Educa")

        f = alunos.copy()
        f = apply_filter(f, ano_col, anos)
        f = apply_filter(f, uf_col, ufs)
        f = apply_filter(f, area_col, areas)
        if area_inep_col and area_inep_col in f.columns:
            f = apply_filter(f, area_inep_col, areas_inep)
        f = apply_filter(f, curso_col, cursos)
        f = apply_filter(f, modal_col, modalidades)

        ano_txt = selected_text(anos)
        uf_txt = selected_text(ufs)
        area_txt = selected_text(areas)
        metric_txt = metric_label(metrica, mat_col, ing_col)
        x_label = axis_label(visao, curso_col, area_col)

        tab1, tab2, tab3 = st.tabs(["Composicao", "Relacao M/I", "Exportacao"])

        with tab1:
            if f.empty:
                fig_main = None
                st.warning("Sem dados para os filtros selecionados.")
            else:
                fig_main = render_main_chart(
                    data_base=f,
                    metrica=metrica,
                    x_col=visao,
                    modal_col=modal_col,
                    topn=topn,
                    ano_txt=ano_txt,
                    uf_txt=uf_txt,
                    area_txt=area_txt,
                    metric_txt=metric_txt,
                    x_label=x_label,
                )
                if fig_main is None:
                    st.info("Sem dados para montar o grafico principal.")
                else:
                    st.plotly_chart(fig_main, use_container_width=True)

                total_mat = float(f[mat_col].sum())
                total_ing = float(f[ing_col].sum())
                total_rec = float(f[rec_col].sum())
                ticket_calc = (total_rec / total_mat) if total_mat > 0 else 0.0

                c1, c2, c3, c4, c5 = st.columns(5)
                c1.metric("Registros", f"{len(f):,}".replace(",", "."))
                c2.metric("Matriculas", f"{total_mat:,.0f}".replace(",", "."))
                c3.metric("Ingressantes", f"{total_ing:,.0f}".replace(",", "."))
                c4.metric("Receita estimada", f"{total_rec:,.2f}".replace(",", "."))
                c5.metric("Ticket medio recalculado", f"{ticket_calc:,.2f}".replace(",", "."))

        with tab2:
            if f.empty:
                st.info("Sem dados para os filtros selecionados.")
            else:
                fig_ratio = render_ratio_chart(
                    data_base=f,
                    x_col=visao,
                    mat_col=mat_col,
                    ing_col=ing_col,
                    topn=topn,
                    x_label=x_label,
                )
                if fig_ratio is None:
                    st.info("Nao ha grupos com ingressantes > 0.")
                else:
                    st.plotly_chart(fig_ratio, use_container_width=True)

        with tab3:
            st.write("Exportacao do grafico principal em PNG")
            if fig_main is None:
                st.info("Nada para exportar com os filtros atuais.")
            else:
                exp1, exp2, exp3 = st.columns(3)
                with exp1:
                    png_w = st.number_input(
                        "Largura (px)",
                        min_value=800,
                        max_value=3200,
                        value=1600,
                        step=100,
                        key="vedu_png_width",
                    )
                with exp2:
                    png_h = st.number_input(
                        "Altura (px)",
                        min_value=500,
                        max_value=2400,
                        value=900,
                        step=100,
                        key="vedu_png_height",
                    )
                with exp3:
                    png_scale = st.slider(
                        "Escala",
                        min_value=1,
                        max_value=4,
                        value=2,
                        step=1,
                        key="vedu_png_scale",
                    )

                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = (
                    f"grafico_principal_ano-{file_token(ano_txt)}_"
                    f"uf-{file_token(uf_txt)}_"
                    f"area-{file_token(area_txt)}_"
                    f"visao-{file_token(x_label)}_"
                    f"metrica-{file_token(metric_txt)}_{int(png_w)}x{int(png_h)}_s{int(png_scale)}_{ts}.png"
                )
                try:
                    png_bytes = fig_main.to_image(
                        format="png",
                        width=int(png_w),
                        height=int(png_h),
                        scale=int(png_scale),
                    )
                    st.download_button(
                        label="Baixar PNG",
                        data=png_bytes,
                        file_name=file_name,
                        mime="image/png",
                        type="primary",
                    )
                except Exception as exc:
                    st.error(f"Falha ao gerar PNG: {exc}")
                    st.info("No Streamlit Cloud, alem do kaleido, o ambiente precisa de Chrome (arquivo packages.txt com chromium).")
                    html_name = file_name.replace(".png", ".html")
                    st.download_button(
                        label="Baixar HTML interativo (fallback)",
                        data=fig_main.to_html(full_html=True, include_plotlyjs="cdn"),
                        file_name=html_name,
                        mime="text/html",
                    )

    with app_tab2:
        st.subheader("Microdados INEP 2024 - Cadastro de Cursos")
        if df_inep.empty:
            st.warning("Arquivos INEP nao encontrados. Adicione MICRODADOS_CADASTRO_CURSOS_2024_SLIM.csv ou MICRODADOS_CADASTRO_CURSOS_2024.CSV na raiz do projeto.")
            return

        f_inep = df_inep.copy()

        for dim_col, selected_values in inep_dynamic_filters.items():
            f_inep = f_inep[f_inep[dim_col].isin(selected_values)]

        if f_inep.empty:
            st.warning("Sem dados para os filtros selecionados no painel INEP.")
            return

        if "TP_MODALIDADE_ENSINO" in f_inep.columns:
            f_inep["__modal_label__"] = f_inep["TP_MODALIDADE_ENSINO"].map(
                lambda v: INEP_MODALIDADE_LABELS.get(int(v), str(v)) if pd.notna(v) else "(vazio)"
            )

            if destacar_veduca_inep:
                in_veduca = pd.Series(False, index=f_inep.index)
                criterio_txt = ""

                # Prioriza identificacao via NO_MANTENEDORA na base SLIM/INEP.
                if "NO_MANTENEDORA" in f_inep.columns:
                    mantenedora_txt = f_inep["NO_MANTENEDORA"].astype("string").fillna("").str.upper()
                    in_veduca = mantenedora_txt.str.contains(r"V-EDUCA|VEDUCA", regex=True, na=False)
                    criterio_txt = "NO_MANTENEDORA"
                elif "NO_IES" in f_inep.columns:
                    no_ies_txt = f_inep["NO_IES"].astype("string").fillna("").str.upper()
                    in_veduca = no_ies_txt.str.contains(r"V-EDUCA|VEDUCA", regex=True, na=False)
                    criterio_txt = "NO_IES"
                else:
                    st.warning("Destaque V-Educa nao aplicado: colunas NO_MANTENEDORA e NO_IES ausentes na base SLIM/INEP.")

                if criterio_txt:
                    qtd_veduca = int(in_veduca.sum())
                    if qtd_veduca == 0:
                        st.warning(f"Destaque V-Educa nao aplicado: nenhum registro identificado por {criterio_txt} na base SLIM/INEP.")
                    else:
                        modalidade_num = pd.to_numeric(f_inep["TP_MODALIDADE_ENSINO"], errors="coerce")
                        f_inep.loc[in_veduca & (modalidade_num == 1), "__modal_label__"] = "VEDUCA Presencial"
                        f_inep.loc[in_veduca & (modalidade_num == 2), "__modal_label__"] = "VEDUCA EAD"
                        st.caption(f"Destaque V-Educa ativo ({qtd_veduca:,} registros identificados por {criterio_txt} na SLIM/INEP).")
        else:
            f_inep["__modal_label__"] = "Sem modalidade"

        if inep_dim in {"TP_MODALIDADE_ENSINO", "TP_REDE", "TP_CATEGORIA_ADMINISTRATIVA"}:
            f_inep["__x_label__"] = f_inep[inep_dim].map(lambda v: inep_value_label(inep_dim, v))
            inep_x_col = "__x_label__"
            inep_x_label = inep_dim_label(inep_dim)
        else:
            inep_x_col = inep_dim
            inep_x_label = inep_dim_label(inep_dim)

        # Exibe de imediato o ranking pela dimensao selecionada (ultimo grafico da analise dinamica).
        fig_inep_rank = render_rank_chart(
            data_base=f_inep,
            x_col=inep_x_col,
            metrica=inep_metric,
            topn=inep_topn,
            x_label=inep_x_label,
            metric_txt=inep_metric_label(inep_metric),
        )
        if fig_inep_rank is None:
            st.info("Sem dados para montar o ranking INEP.")
        else:
            st.plotly_chart(fig_inep_rank, use_container_width=True)

        inep_tabs = st.tabs(["Composicao", "Pizza", "Relacao M/I", "Exportacao"])

        with inep_tabs[0]:
            fig_inep_main = render_main_chart(
                data_base=f_inep,
                metrica=inep_metric,
                x_col=inep_x_col,
                modal_col="__modal_label__",
                topn=inep_topn,
                ano_txt="INEP 2024",
                uf_txt="filtros aplicados",
                area_txt="cursos",
                metric_txt=inep_metric_label(inep_metric),
                x_label=inep_x_label,
                stack_order=["Presencial", "Curso a distancia", "VEDUCA Presencial", "VEDUCA EAD"],
                color_map=INEP_MODALIDADE_CORES,
            )
            if fig_inep_main is None:
                st.info("Sem dados para o grafico principal do INEP.")
            else:
                st.plotly_chart(fig_inep_main, use_container_width=True)

            metric_total = float(pd.to_numeric(f_inep[inep_metric], errors="coerce").fillna(0).sum())
            mat_total = float(pd.to_numeric(f_inep["QT_MAT"], errors="coerce").fillna(0).sum()) if "QT_MAT" in f_inep.columns else 0.0
            ing_total = float(pd.to_numeric(f_inep["QT_ING"], errors="coerce").fillna(0).sum()) if "QT_ING" in f_inep.columns else 0.0
            k1, k2, k3 = st.columns(3)
            k1.metric("Registros", f"{len(f_inep):,}".replace(",", "."))
            k2.metric(inep_metric_label(inep_metric), f"{metric_total:,.0f}".replace(",", "."))
            k3.metric("Matriculados", f"{mat_total:,.0f}".replace(",", "."))

        with inep_tabs[1]:
            st.write("Distribuicao do total filtrado em graficos de pizza dinamicos")

            pie_c1, pie_c2 = st.columns([1.2, 2.8])
            metric_title = inep_metric_label(inep_metric)

            pie_options = {
                "Modalidade": "__modal_label__",
                "Categoria administrativa": "__cat_label__",
                "Regiao": "NO_REGIAO",
                "UF": "SG_UF",
                "Rede": "TP_REDE",
                "Mantenedora": "NO_MANTENEDORA",
                "Area CINE": "NO_CINE_AREA_GERAL",
            }

            with pie_c1:
                pie_dim_nome = st.selectbox(
                    "Dimensao da pizza",
                    options=list(pie_options.keys()),
                    index=0,
                    key="inep_pie_dim",
                )
                pie_top_n = st.number_input(
                    "Top N categorias",
                    min_value=3,
                    max_value=20,
                    value=8,
                    step=1,
                    key="inep_pie_topn",
                )
                pie_outros = st.checkbox(
                    "Agrupar demais em Outros",
                    value=True,
                    key="inep_pie_outros",
                )

            cat_col = "TP_CATEGORIA_ADMINISTRATIVA" if "TP_CATEGORIA_ADMINISTRATIVA" in f_inep.columns else None
            if cat_col is not None:
                f_inep["__cat_label__"] = f_inep[cat_col].map(lambda v: inep_value_label(cat_col, v))

            pie_group_col = pie_options[pie_dim_nome]
            pie_data_table = None

            if pie_group_col == "__cat_label__" and "__cat_label__" not in f_inep.columns:
                fig_pie = None
            else:
                if pie_group_col == "TP_REDE":
                    f_inep["__rede_label__"] = f_inep[pie_group_col].map(lambda v: inep_value_label(pie_group_col, v))
                    pie_group_col_used = "__rede_label__"
                else:
                    pie_group_col_used = pie_group_col

                pie_df = f_inep[[pie_group_col_used, inep_metric]].copy()
                pie_df[pie_group_col_used] = pie_df[pie_group_col_used].astype("string").fillna("(vazio)").str.strip()
                pie_df[inep_metric] = pd.to_numeric(pie_df[inep_metric], errors="coerce").fillna(0)
                pie_df = pie_df[pie_df[inep_metric] > 0]

                if not pie_df.empty:
                    grouped = pie_df.groupby(pie_group_col_used, as_index=False)[inep_metric].sum().sort_values(inep_metric, ascending=False)
                    if int(pie_top_n) > 0 and len(grouped) > int(pie_top_n) and pie_outros:
                        top_part = grouped.head(int(pie_top_n)).copy()
                        rest_sum = float(grouped.iloc[int(pie_top_n):][inep_metric].sum())
                        if rest_sum > 0:
                            top_part = pd.concat(
                                [top_part, pd.DataFrame([{pie_group_col_used: "Outros", inep_metric: rest_sum}])],
                                ignore_index=True,
                            )
                        grouped = top_part

                    total_val = float(grouped[inep_metric].sum())
                    pie_data_table = grouped.copy()
                    pie_data_table["Percentual"] = (pie_data_table[inep_metric] / total_val * 100).round(2)
                    pie_data_table = pie_data_table.rename(columns={
                        pie_group_col_used: "Categoria",
                        inep_metric: metric_title
                    })
                    pie_data_table = pie_data_table[["Categoria", metric_title, "Percentual"]].reset_index(drop=True)

                    if pie_group_col_used == "__modal_label__":
                        fig_pie = render_pie_chart(
                            data_base=f_inep,
                            value_col=inep_metric,
                            group_col=pie_group_col_used,
                            title=f"{metric_title} por {pie_dim_nome.lower()}",
                            color_sequence=["#2ca02c", "#1f77b4", "#ff8c42", "#ffb26b"],
                            value_label=metric_title,
                            top_n=int(pie_top_n) if pie_outros else None,
                            outros_label="Outros",
                        )
                    else:
                        fig_pie = render_pie_chart(
                            data_base=f_inep,
                            value_col=inep_metric,
                            group_col=pie_group_col_used,
                            title=f"{metric_title} por {pie_dim_nome.lower()}",
                            value_label=metric_title,
                            top_n=int(pie_top_n) if pie_outros else None,
                            outros_label="Outros",
                        )
                else:
                    fig_pie = None

            with pie_c2:
                if fig_pie is None:
                    st.info("Sem dados para o grafico de pizza com os filtros atuais.")
                else:
                    st.plotly_chart(fig_pie, use_container_width=True)

            if pie_data_table is not None and not pie_data_table.empty:
                st.markdown("#### Representatividade")
                pie_data_table["Valor Formatado"] = pie_data_table[metric_title].apply(lambda x: f"{x:,.0f}".replace(",", "."))
                display_table = pie_data_table[["Categoria", "Valor Formatado", "Percentual"]].copy()
                display_table["Percentual"] = display_table["Percentual"].apply(lambda x: f"{x:.2f}%")
                st.dataframe(display_table, use_container_width=True, hide_index=True)

        with inep_tabs[2]:
            if "QT_MAT" not in f_inep.columns or "QT_ING" not in f_inep.columns:
                st.info("Relacao M/I indisponivel: colunas QT_MAT ou QT_ING ausentes.")
            else:
                fig_inep_ratio = render_ratio_chart(
                    data_base=f_inep,
                    x_col=inep_x_col,
                    mat_col="QT_MAT",
                    ing_col="QT_ING",
                    topn=inep_topn,
                    x_label=inep_x_label,
                )
                if fig_inep_ratio is None:
                    st.info("Nao ha grupos com ingressantes > 0 no recorte INEP.")
                else:
                    st.plotly_chart(fig_inep_ratio, use_container_width=True)

        with inep_tabs[3]:
            st.write("Exportacao do painel INEP para uso no GitHub/Streamlit")
            expi1, expi2, expi3 = st.columns(3)
            with expi1:
                inep_png_w = st.number_input(
                    "Largura PNG (px)",
                    min_value=800,
                    max_value=3200,
                    value=1600,
                    step=100,
                    key="inep_png_width",
                )
            with expi2:
                inep_png_h = st.number_input(
                    "Altura PNG (px)",
                    min_value=500,
                    max_value=2400,
                    value=900,
                    step=100,
                    key="inep_png_height",
                )
            with expi3:
                inep_png_scale = st.slider(
                    "Escala PNG",
                    min_value=1,
                    max_value=4,
                    value=2,
                    step=1,
                    key="inep_png_scale",
                )

            ts_inep = datetime.now().strftime("%Y%m%d_%H%M%S")

            csv_name = f"inep_filtrado_{file_token(inep_dim)}_{file_token(inep_metric)}_{ts_inep}.csv"
            st.download_button(
                label="Baixar CSV filtrado",
                data=f_inep.to_csv(index=False, encoding="utf-8-sig"),
                file_name=csv_name,
                mime="text/csv",
            )

            if "fig_inep_main" not in locals() or fig_inep_main is None:
                st.info("Nada para exportar em grafico com os filtros atuais.")
            else:
                img_name = (
                    f"grafico_inep_{file_token(inep_dim)}_{file_token(inep_metric)}_"
                    f"{int(inep_png_w)}x{int(inep_png_h)}_s{int(inep_png_scale)}_{ts_inep}.png"
                )
                try:
                    png_bytes_inep = fig_inep_main.to_image(
                        format="png",
                        width=int(inep_png_w),
                        height=int(inep_png_h),
                        scale=int(inep_png_scale),
                    )
                    st.download_button(
                        label="Baixar PNG",
                        data=png_bytes_inep,
                        file_name=img_name,
                        mime="image/png",
                        type="primary",
                    )
                except Exception as exc:
                    st.error(f"Falha ao gerar PNG do INEP: {exc}")
                    html_name = img_name.replace(".png", ".html")
                    st.download_button(
                        label="Baixar HTML interativo (fallback)",
                        data=fig_inep_main.to_html(full_html=True, include_plotlyjs="cdn"),
                        file_name=html_name,
                        mime="text/html",
                    )

    with app_tab3:
        st.subheader("Análise por Mantenedora")
        if df_inep.empty:
            st.warning("Dados INEP não disponíveis para análise por mantenedora.")
            return

        f_mantenedora = df_inep.copy()

        # Filtros desta aba: mantenedora, tipo de curso e metrica.
        col1, col2, col3 = st.columns(3)

        with col1:
            mantenedora_options = options_for(f_mantenedora, "NO_MANTENEDORA") if "NO_MANTENEDORA" in f_mantenedora.columns else [TODOS]
            mantenedora_selecionadas = st.multiselect(
                "Mantenedora",
                options=mantenedora_options,
                default=[TODOS],
                key="mantenedora_nome",
            )

        with col2:
            if "NO_CINE_AREA_GERAL" in f_mantenedora.columns:
                tipo_options = options_for(f_mantenedora, "NO_CINE_AREA_GERAL")
                tipo_curso_selecionado = st.multiselect(
                    "Tipo de curso (Área CINE)",
                    options=tipo_options,
                    default=[TODOS],
                    key="mantenedora_tipo_curso",
                )
            else:
                tipo_curso_selecionado = [TODOS]
                st.info("Coluna NO_CINE_AREA_GERAL não encontrada para o filtro de tipo de curso.")

        with col3:
            metric_options = {
                "Ingressantes": "QT_ING",
                "Matriculados": "QT_MAT",
                "Quantidade de cursos": "__QTD_CURSOS__",
            }
            metrica_nome = st.selectbox(
                "Métrica do mapa",
                options=list(metric_options.keys()),
                index=1,
                key="mantenedora_metrica",
            )
            metrica_col = metric_options[metrica_nome]

        if "NO_MANTENEDORA" not in f_mantenedora.columns:
            st.warning("Coluna NO_MANTENEDORA não encontrada nos dados.")
            return

        if metrica_col != "__QTD_CURSOS__" and metrica_col not in f_mantenedora.columns:
            st.error(f"Métrica {metrica_col} não disponível nos dados.")
            return

        f_mantenedora = apply_filter(f_mantenedora, "NO_MANTENEDORA", mantenedora_selecionadas)
        f_mantenedora = apply_filter(f_mantenedora, "NO_CINE_AREA_GERAL", tipo_curso_selecionado)

        if f_mantenedora.empty:
            st.warning("Sem dados para os filtros selecionados na análise por mantenedora.")
            return

        def subset_for_modalidade(df: pd.DataFrame, modalidade: str) -> pd.DataFrame:
            base = df.copy()
            if "TP_MODALIDADE_ENSINO" in base.columns:
                modalidade_raw = base["TP_MODALIDADE_ENSINO"].astype("string").str.upper().str.strip()
                modalidade_num = pd.to_numeric(base["TP_MODALIDADE_ENSINO"], errors="coerce")
            else:
                modalidade_raw = pd.Series("", index=base.index, dtype="string")
                modalidade_num = pd.Series(pd.NA, index=base.index, dtype="Float64")

            if modalidade == "Todas as modalidades":
                return base
            if modalidade == "EAD":
                if "TP_MODALIDADE_ENSINO" in base.columns:
                    mask = (modalidade_num == 2) | modalidade_raw.isin(["2", "2.0", "EAD", "CURSO A DISTANCIA", "CURSO A DISTÂNCIA"])
                    return base[mask]
                return base.iloc[0:0]
            if modalidade == "Presencial":
                if "TP_MODALIDADE_ENSINO" in base.columns:
                    mask = (modalidade_num == 1) | modalidade_raw.isin(["1", "1.0", "PRESENCIAL"])
                    return base[mask]
                return base.iloc[0:0]
            if modalidade == "Semipresencial":
                if "TP_MODALIDADE_ENSINO" in base.columns:
                    mask = (modalidade_num == 3) | modalidade_raw.isin(["3", "3.0", "SEMIPRESENCIAL", "SEMI-PRESENCIAL"])
                    return base[mask]
                if "NO_CINE_ROTULO" in base.columns:
                    semi_mask = base["NO_CINE_ROTULO"].astype("string").str.contains("SEMI", case=False, na=False)
                    return base[semi_mask]
                return base.iloc[0:0]
            return base.iloc[0:0]

        modality_configs = [
            {
                "label": "Todas as modalidades",
                "subtitle": "Visão geral",
                "palette": [
                    [0.0, "#1a1a1a"],
                    [0.25, "#5c3a1e"],
                    [0.5, "#9b5720"],
                    [0.75, "#d07a28"],
                    [1.0, "#ffb45d"],
                ],
            },
            {
                "label": "EAD",
                "subtitle": "Somente cursos a distância",
                "palette": [
                    [0.0, "#06131f"],
                    [0.25, "#0e3354"],
                    [0.5, "#155f8d"],
                    [0.75, "#2487c7"],
                    [1.0, "#65bfff"],
                ],
            },
            {
                "label": "Presencial",
                "subtitle": "Somente cursos presenciais",
                "palette": [
                    [0.0, "#0d1a10"],
                    [0.25, "#1f4d2a"],
                    [0.5, "#2f7a3d"],
                    [0.75, "#49a65a"],
                    [1.0, "#8be38d"],
                ],
            },
            {
                "label": "Semipresencial",
                "subtitle": "Se houver registros na base",
                "palette": [
                    [0.0, "#1a1026"],
                    [0.25, "#3b2458"],
                    [0.5, "#65429a"],
                    [0.75, "#8e69d4"],
                    [1.0, "#c4a7ff"],
                ],
            },
        ]

        st.markdown("### Mapa do Brasil")
        st.caption("Os quatro painéis usam a mesma métrica e os mesmos filtros de mantenedora e tipo de curso.")

        maintained_maps = []
        rows = [st.columns(2), st.columns(2)]
        for idx, config in enumerate(modality_configs):
            row = rows[idx // 2]
            subset = subset_for_modalidade(f_mantenedora, config["label"])

            if metrica_col != "__QTD_CURSOS__" and metrica_col in subset.columns:
                subset[metrica_col] = pd.to_numeric(subset[metrica_col], errors="coerce").fillna(0)

            uf_map_values = aggregate_metric_by_uf(subset, metrica_col)

            fig_map = render_uf_brazil_map(
                uf_map_values,
                metrica_col,
                inep_metric_label(metrica_col),
                theme="light",
                colorscale=config["palette"],
                title=f"{config['label']} - {inep_metric_label(metrica_col)}",
                show_labels=True,
                height=430,
            )
            top_uf_inset = render_top_uf_inset(uf_map_values, metrica_col, inep_metric_label(metrica_col), limit=10)
            maintained_maps.append({
                "label": config["label"],
                "fig": fig_map,
            })

            with row[idx % 2]:
                st.markdown(f"#### {config['label']}")
                st.caption(config["subtitle"])
                map_col, inset_col = st.columns([3.6, 1.2], gap="small")
                with map_col:
                    st.plotly_chart(
                        fig_map,
                        use_container_width=True,
                        key=f"mantenedora_{idx}",
                        config={
                            "displayModeBar": False,
                            "scrollZoom": False,
                            "doubleClick": False,
                            "showTips": True,
                        },
                    )
                with inset_col:
                    st.plotly_chart(
                        top_uf_inset,
                        use_container_width=True,
                        key=f"mantenedora_{idx}_inset",
                        config={"displayModeBar": False, "staticPlot": True},
                    )

        st.markdown("### Exportacao dos mapas")
        export_c1, export_c2, export_c3 = st.columns(3)
        with export_c1:
            export_w = st.number_input(
                "Largura PNG (px)",
                min_value=800,
                max_value=3200,
                value=1600,
                step=100,
                key="mantenedora_png_width",
            )
        with export_c2:
            export_h = st.number_input(
                "Altura PNG (px)",
                min_value=500,
                max_value=2400,
                value=900,
                step=100,
                key="mantenedora_png_height",
            )
        with export_c3:
            export_scale = st.slider(
                "Escala PNG",
                min_value=1,
                max_value=4,
                value=2,
                step=1,
                key="mantenedora_png_scale",
            )

        export_buttons = st.columns(2)
        for idx, item in enumerate(maintained_maps):
            with export_buttons[idx % 2]:
                if item["fig"] is None:
                    st.info(f"Sem dados para exportar: {item['label']}")
                else:
                    try:
                        png_bytes = item["fig"].to_image(
                            format="png",
                            width=int(export_w),
                            height=int(export_h),
                            scale=int(export_scale),
                        )
                        st.download_button(
                            label=f"Baixar PNG - {item['label']}",
                            data=png_bytes,
                            file_name=f"mantenedora_{file_token(item['label'])}_{int(export_w)}x{int(export_h)}_s{int(export_scale)}.png",
                            mime="image/png",
                            key=f"download_mantenedora_{idx}",
                        )
                    except Exception as exc:
                        st.error(f"Falha ao gerar PNG de {item['label']}: {exc}")


if __name__ == "__main__":
    main()
