from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re
from typing import Any

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


def render_uf_brazil_map(uf_values: pd.DataFrame, metric_col: str, metric_txt: str) -> go.Figure | None:
    if uf_values.empty:
        return None

    map_df = uf_values.copy()
    map_df["SG_UF"] = map_df["SG_UF"].astype("string").str.upper()
    map_df = map_df[map_df["SG_UF"].isin(UF_CENTROIDS.keys())].copy()
    if map_df.empty:
        return None

    map_df["lat"] = map_df["SG_UF"].map(lambda uf: UF_CENTROIDS[uf][0])
    map_df["lon"] = map_df["SG_UF"].map(lambda uf: UF_CENTROIDS[uf][1])
    map_df["valor_fmt"] = map_df[metric_col].map(lambda v: f"{float(v):,.0f}".replace(",", "."))

    fig = go.Figure(
        go.Scattergeo(
            lat=map_df["lat"],
            lon=map_df["lon"],
            mode="markers+text",
            text=map_df["SG_UF"],
            textposition="top center",
            customdata=map_df[["SG_UF", metric_col, "valor_fmt"]],
            marker=dict(
                size=map_df[metric_col].fillna(0).clip(lower=0).pow(0.35) * 4 + 8,
                color=map_df[metric_col],
                colorscale="YlGnBu",
                colorbar=dict(title=metric_txt),
                line=dict(color="white", width=0.7),
                sizemode="diameter",
                opacity=0.88,
            ),
            hovertemplate=(
                "<b>UF: %{customdata[0]}</b><br>"
                + f"{metric_txt}: "
                + "%{customdata[2]}<extra></extra>"
            ),
        )
    )

    fig.update_layout(
        title=f"Mapa do Brasil por UF - {metric_txt}",
        height=560,
        margin=dict(l=10, r=10, t=60, b=10),
        geo=dict(
            scope="south america",
            center=dict(lat=-14.5, lon=-52),
            projection_scale=4.2,
            showland=True,
            landcolor="#f5f5f5",
            showcountries=True,
            countrycolor="#7f7f7f",
            coastlinecolor="#999999",
        ),
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
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = (
                    f"grafico_principal_ano-{file_token(ano_txt)}_"
                    f"uf-{file_token(uf_txt)}_"
                    f"area-{file_token(area_txt)}_"
                    f"visao-{file_token(x_label)}_"
                    f"metrica-{file_token(metric_txt)}_{ts}.png"
                )
                try:
                    png_bytes = fig_main.to_image(format="png", width=1600, height=900, scale=2)
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

        inep_tabs = st.tabs(["Composicao", "Relacao M/I", "Exportacao"])

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
            cursos_total = float(pd.to_numeric(f_inep["QT_CURSO"], errors="coerce").fillna(0).sum()) if "QT_CURSO" in f_inep.columns else 0.0

            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Registros", f"{len(f_inep):,}".replace(",", "."))
            k2.metric(inep_metric_label(inep_metric), f"{metric_total:,.0f}".replace(",", "."))
            k3.metric("Matriculados", f"{mat_total:,.0f}".replace(",", "."))
            k4.metric("Cursos", f"{cursos_total:,.0f}".replace(",", "."))

        with inep_tabs[1]:
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

        with inep_tabs[2]:
            st.write("Exportacao do painel INEP para uso no GitHub/Streamlit")
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
                img_name = f"grafico_inep_{file_token(inep_dim)}_{file_token(inep_metric)}_{ts_inep}.png"
                try:
                    png_bytes_inep = fig_inep_main.to_image(format="png", width=1600, height=900, scale=2)
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

        # Aplicar filtros dinâmicos existentes
        for dim_col, selected_values in inep_dynamic_filters.items():
            f_mantenedora = f_mantenedora[f_mantenedora[dim_col].isin(selected_values)]

        if f_mantenedora.empty:
            st.warning("Sem dados para os filtros selecionados na análise por mantenedora.")
            return

        # Filtros adicionais para essa aba
        col1, col2 = st.columns(2)

        with col1:
            metric_options = {
                "Matriculas": "QT_MAT",
                "Cursos": "QT_CURSO",
                "Ingressantes": "QT_ING",
            }
            metrica_nome = st.selectbox(
                "Selecione a metrica:",
                options=list(metric_options.keys()),
                key="mantenedora_metrica",
            )
            metrica_col = metric_options[metrica_nome]

        with col2:
            top_n_mantenedoras = st.slider(
                "Quantas mantenedoras exibir?",
                min_value=5,
                max_value=50,
                value=20,
                step=5,
                key="mantenedora_topn",
            )

        if "NO_MANTENEDORA" not in f_mantenedora.columns:
            st.warning("Coluna NO_MANTENEDORA não encontrada nos dados.")
            return

        if metrica_col not in f_mantenedora.columns:
            st.error(f"Metrica {metrica_col} nao disponivel nos dados.")
            return

        if "SG_UF" not in f_mantenedora.columns:
            st.warning("Coluna SG_UF nao encontrada. Mapa do Brasil indisponivel para esta base.")
            return

        # Mapa do Brasil por UF: a selecao no mapa vira filtro da analise.
        uf_map_values = (
            f_mantenedora.dropna(subset=["SG_UF"])
            .assign(SG_UF=lambda d: d["SG_UF"].astype("string").str.upper())
            .groupby("SG_UF", as_index=False)[metrica_col]
            .sum()
        )

        fig_map = render_uf_brazil_map(uf_map_values, metrica_col, inep_metric_label(metrica_col))

        map_selected_ufs: list[str] = []
        if fig_map is not None:
            st.markdown("### Mapa do Brasil (filtro por UF)")
            st.caption("Clique em um ou mais pontos do mapa para filtrar a analise por mantenedora.")
            map_event: Any = st.plotly_chart(
                fig_map,
                use_container_width=True,
                key="mantenedora_mapa_brasil",
                on_select="rerun",
            )

            if isinstance(map_event, dict):
                points = map_event.get("selection", {}).get("points", [])
                for point in points:
                    customdata = point.get("customdata") if isinstance(point, dict) else None
                    if isinstance(customdata, (list, tuple)) and customdata:
                        uf = str(customdata[0]).upper()
                        if uf and uf != "NAN":
                            map_selected_ufs.append(uf)
            map_selected_ufs = sorted(set(map_selected_ufs))

        uf_options = sorted(f_mantenedora["SG_UF"].dropna().astype("string").str.upper().unique().tolist())
        default_ufs = map_selected_ufs if map_selected_ufs else uf_options

        ufs_escolhidas = st.multiselect(
            "UFs para analise da mantenedora",
            options=uf_options,
            default=default_ufs,
            key="mantenedora_ufs",
            help="A selecao feita no mapa preenche este filtro automaticamente.",
        )

        if map_selected_ufs:
            st.caption("UFs selecionadas no mapa: " + ", ".join(map_selected_ufs))

        if not ufs_escolhidas:
            st.warning("Selecione ao menos uma UF para exibir o ranking de mantenedoras.")
            return

        f_mantenedora = f_mantenedora[
            f_mantenedora["SG_UF"].astype("string").str.upper().isin(ufs_escolhidas)
        ].copy()

        if f_mantenedora.empty:
            st.warning("Sem dados para as UFs selecionadas no mapa/filtro.")
            return

        # Agregar dados por mantenedora
        dados_mantenedora = f_mantenedora.groupby("NO_MANTENEDORA", as_index=False).agg({
            col: "sum" for col in ["QT_MAT", "QT_ING", "QT_CURSO"] if col in f_mantenedora.columns
        }).dropna(subset=["NO_MANTENEDORA"])

        # Converter para numérico
        for col in ["QT_MAT", "QT_ING", "QT_CURSO"]:
            if col in dados_mantenedora.columns:
                dados_mantenedora[col] = pd.to_numeric(dados_mantenedora[col], errors="coerce").fillna(0)

        # Ordenar e pegar top N
        if metrica_col in dados_mantenedora.columns:
            dados_mantenedora = dados_mantenedora.sort_values(by=metrica_col, ascending=False).head(top_n_mantenedoras)
        else:
            st.error(f"Metrica {metrica_col} nao disponivel nos dados agregados.")
            return

        # Gráfico de ranking
        fig_mantenedora = go.Figure(data=[
            go.Bar(
                x=dados_mantenedora["NO_MANTENEDORA"],
                y=dados_mantenedora[metrica_col],
                marker_color="#2ca02c",
                hovertemplate="<b>%{x}</b><br>" + inep_metric_label(metrica_col) + ": %{y:,.0f}<extra></extra>",
            )
        ])

        fig_mantenedora.update_layout(
            title=f"Top {len(dados_mantenedora)} Mantenedoras - {inep_metric_label(metrica_col)}",
            xaxis_title="Mantenedora",
            yaxis_title=inep_metric_label(metrica_col),
            hovermode="x unified",
            height=500,
            margin=dict(b=100),
        )
        fig_mantenedora.update_xaxes(tickangle=-45)

        st.plotly_chart(fig_mantenedora, use_container_width=True)

        # Tabela com dados
        st.subheader("Dados por Mantenedora")
        
        cols_exibir = ["NO_MANTENEDORA"]
        # Adicionar apenas as colunas que ainda não estão na lista
        for col in ["QT_MAT", "QT_ING", "QT_CURSO"]:
            if col in dados_mantenedora.columns and col not in cols_exibir:
                cols_exibir.append(col)
        
        tabela_exibicao = dados_mantenedora[cols_exibir].copy()
        tabela_nomes = {
            "NO_MANTENEDORA": "Mantenedora",
            "QT_MAT": "Matriculados",
            "QT_ING": "Ingressantes",
            "QT_CURSO": "Cursos",
        }
        tabela_exibicao = tabela_exibicao.rename(columns=tabela_nomes)
        
        st.dataframe(
            tabela_exibicao.style.format(
                {col: "{:,.0f}" for col in tabela_exibicao.columns[1:]}
            ),
            use_container_width=True,
        )

        # Downloads
        st.subheader("Exportação")
        csv_mantenedora = dados_mantenedora.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 Baixar CSV",
            data=csv_mantenedora,
            file_name=f"analise_mantenedora_{inep_metric_label(metrica_col).lower().replace(' ', '_')}_"
                     f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

        try:
            png_bytes = fig_mantenedora.to_image(format="png", width=1600, height=900, scale=2)
            st.download_button(
                label="📥 Baixar Gráfico (PNG)",
                data=png_bytes,
                file_name=f"grafico_mantenedora_{inep_metric_label(metrica_col).lower().replace(' ', '_')}_"
                         f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.png",
                mime="image/png",
            )
        except Exception as exc:
            st.info(f"PNG não disponível neste ambiente: {exc}")
            html_grafico = fig_mantenedora.to_html(full_html=True, include_plotlyjs="cdn")
            st.download_button(
                label="📥 Baixar Gráfico (HTML)",
                data=html_grafico,
                file_name=f"grafico_mantenedora_{inep_metric_label(metrica_col).lower().replace(' ', '_')}_"
                         f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
                mime="text/html",
            )


if __name__ == "__main__":
    main()
