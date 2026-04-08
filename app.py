from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re

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
    "Presencial": "#1f77b4",
    "Curso a distancia": "#2ca02c",
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
    "NO_REGIAO": "Regiao",
    "SG_UF": "UF",
    "NO_IES": "IES",
    "TP_MODALIDADE_ENSINO": "Modalidade",
    "TP_CATEGORIA_ADMINISTRATIVA": "Categoria administrativa",
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


@st.cache_data(show_spinner=False)
def load_data() -> tuple[pd.DataFrame, dict]:
    alunos = extract_main_block(EXCEL_PATH, "Alunos V-Educa").copy()

    meta = {
        "ano": find_col(alunos.columns, "ANO"),
        "uf": find_col(alunos.columns, "UF"),
        "area": find_col(alunos.columns, "AREA"),
        "curso": find_col(alunos.columns, "CURSO"),
        "modalidade": find_col(alunos.columns, "MODAL"),
        "ticket": find_col(alunos.columns, "TICKET"),
        "ingressantes": find_col(alunos.columns, "INGRESS"),
        "matriculados": find_col(alunos.columns, "MATRIC"),
    }

    for c in [meta["ano"], meta["ticket"], meta["ingressantes"], meta["matriculados"]]:
        alunos[c] = pd.to_numeric(alunos[c], errors="coerce")

    alunos["receita_total_estimada"] = alunos[meta["ticket"]] * alunos[meta["matriculados"]]
    alunos[meta["ano"]] = alunos[meta["ano"]].astype("Int64")
    for c in [meta["uf"], meta["area"], meta["curso"], meta["modalidade"]]:
        alunos[c] = alunos[c].astype("string")

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

    for col in ["NO_REGIAO", "SG_UF", "NO_IES", "NO_CINE_AREA_GERAL", "NO_CINE_ROTULO"]:
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
    grp = grp.merge(total, on=x_col, how="left")
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


def main() -> None:
    st.set_page_config(page_title="Dashboard V-Educa e INEP", layout="wide")
    st.title("Dashboard Dinamico - V-Educa e INEP")

    if not EXCEL_PATH.exists():
        st.error("Arquivo base.xlsx nao encontrado na raiz do projeto.")
        return

    alunos, meta = load_data()

    ano_col = meta["ano"]
    uf_col = meta["uf"]
    area_col = meta["area"]
    curso_col = meta["curso"]
    modal_col = meta["modalidade"]
    mat_col = meta["matriculados"]
    ing_col = meta["ingressantes"]
    rec_col = "receita_total_estimada"

    with st.sidebar:
        st.header("Filtros")
        anos = st.multiselect("Ano", options_for(alunos, ano_col), default=[TODOS])
        ufs = st.multiselect("UF", options_for(alunos, uf_col), default=[TODOS])
        areas = st.multiselect("Area", options_for(alunos, area_col), default=[TODOS])
        cursos = st.multiselect("Curso", options_for(alunos, curso_col), default=[TODOS])
        modalidades = st.multiselect("Modalidade", options_for(alunos, modal_col), default=[TODOS])

        visao = st.selectbox(
            "Eixo X",
            options=[curso_col, area_col, uf_col],
            format_func=lambda c: axis_label(c, curso_col, area_col),
        )
        metrica = st.selectbox(
            "Metrica",
            options=[mat_col, ing_col, rec_col],
            format_func=lambda c: metric_label(c, mat_col, ing_col),
        )
        topn = st.slider("Top N", min_value=5, max_value=40, value=15, step=1)

    f = alunos.copy()
    f = apply_filter(f, ano_col, anos)
    f = apply_filter(f, uf_col, ufs)
    f = apply_filter(f, area_col, areas)
    f = apply_filter(f, curso_col, cursos)
    f = apply_filter(f, modal_col, modalidades)

    if f.empty:
        st.warning("Sem dados para os filtros selecionados.")
        return

    ano_txt = selected_text(anos)
    uf_txt = selected_text(ufs)
    area_txt = selected_text(areas)
    metric_txt = metric_label(metrica, mat_col, ing_col)
    x_label = axis_label(visao, curso_col, area_col)

    app_tab1, app_tab2 = st.tabs(["V-Educa", "INEP Cursos"])

    with app_tab1:
        tab1, tab2, tab3 = st.tabs(["Composicao", "Relacao M/I", "Exportacao"])

        with tab1:
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
        df_inep = load_inep_data()
        if df_inep.empty:
            st.warning("Arquivos INEP nao encontrados. Adicione MICRODADOS_CADASTRO_CURSOS_2024_SLIM.csv ou MICRODADOS_CADASTRO_CURSOS_2024.CSV na raiz do projeto.")
            return

        available_metrics = [c for c in INEP_NUMERIC_COLS if c in df_inep.columns]
        available_dims = [
            c
            for c in [
                "NO_REGIAO",
                "SG_UF",
                "NO_IES",
                "TP_MODALIDADE_ENSINO",
                "TP_CATEGORIA_ADMINISTRATIVA",
                "TP_REDE",
                "NO_CINE_AREA_GERAL",
                "NO_CINE_ROTULO",
            ]
            if c in df_inep.columns
        ]
        if not available_metrics or not available_dims:
            st.warning("Nao ha colunas suficientes para montar o painel INEP.")
            return

        c_a, c_b, c_c = st.columns([2, 2, 1])
        with c_a:
            inep_metric = st.selectbox(
                "Metrica (INEP)",
                options=available_metrics,
                format_func=inep_metric_label,
                index=available_metrics.index("QT_MAT") if "QT_MAT" in available_metrics else 0,
            )
        with c_b:
            inep_dim = st.selectbox(
                "Dimensao (Eixo X)",
                options=available_dims,
                format_func=inep_dim_label,
                index=available_dims.index("NO_REGIAO") if "NO_REGIAO" in available_dims else 0,
            )
        with c_c:
            inep_topn = st.slider("Top N", min_value=5, max_value=40, value=15, step=1, key="inep_topn")

        f_inep = df_inep.copy()

        f1, f2, f3, f4 = st.columns(4)
        with f1:
            if "NU_ANO_CENSO" in f_inep.columns:
                anos = sorted([int(v) for v in f_inep["NU_ANO_CENSO"].dropna().unique().tolist()])
                anos_sel = st.multiselect("Ano", anos, default=anos[-1:] if anos else [])
                if anos_sel:
                    f_inep = f_inep[f_inep["NU_ANO_CENSO"].isin(anos_sel)]
        with f2:
            if "NO_REGIAO" in f_inep.columns:
                regs = sorted([str(v) for v in f_inep["NO_REGIAO"].dropna().unique().tolist()])
                regs_sel = st.multiselect("Regiao", regs, default=[])
                if regs_sel:
                    f_inep = f_inep[f_inep["NO_REGIAO"].astype(str).isin(regs_sel)]
        with f3:
            if "SG_UF" in f_inep.columns:
                ufs_inep = sorted([str(v) for v in f_inep["SG_UF"].dropna().unique().tolist()])
                ufs_sel = st.multiselect("UF", ufs_inep, default=[])
                if ufs_sel:
                    f_inep = f_inep[f_inep["SG_UF"].astype(str).isin(ufs_sel)]
        with f4:
            if "TP_MODALIDADE_ENSINO" in f_inep.columns:
                modal_vals = sorted([int(v) for v in f_inep["TP_MODALIDADE_ENSINO"].dropna().unique().tolist()])
                modal_opts = [(inep_value_label("TP_MODALIDADE_ENSINO", v), v) for v in modal_vals]
                modal_labels_default = [label for label, _ in modal_opts]
                modal_sel_labels = st.multiselect("Modalidade", [label for label, _ in modal_opts], default=modal_labels_default)
                map_back = {label: value for label, value in modal_opts}
                modal_sel_values = [map_back[label] for label in modal_sel_labels]
                if modal_sel_values:
                    f_inep = f_inep[f_inep["TP_MODALIDADE_ENSINO"].isin(modal_sel_values)]

        if f_inep.empty:
            st.warning("Sem dados para os filtros selecionados no painel INEP.")
            return

        if "TP_MODALIDADE_ENSINO" in f_inep.columns:
            f_inep["__modal_label__"] = f_inep["TP_MODALIDADE_ENSINO"].map(
                lambda v: INEP_MODALIDADE_LABELS.get(int(v), str(v)) if pd.notna(v) else "(vazio)"
            )
        else:
            f_inep["__modal_label__"] = "Sem modalidade"

        if inep_dim in {"TP_MODALIDADE_ENSINO", "TP_REDE", "TP_CATEGORIA_ADMINISTRATIVA"}:
            f_inep["__x_label__"] = f_inep[inep_dim].map(lambda v: inep_value_label(inep_dim, v))
            inep_x_col = "__x_label__"
            inep_x_label = inep_dim_label(inep_dim)
        else:
            inep_x_col = inep_dim
            inep_x_label = inep_dim_label(inep_dim)

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
                stack_order=["Presencial", "Curso a distancia"],
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


if __name__ == "__main__":
    main()
