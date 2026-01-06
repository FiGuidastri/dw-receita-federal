import streamlit as st
import pandas as pd
import duckdb
import plotly.express as px
from pathlib import Path
import io

# ==================== CONFIGURAÇÕES ====================

st.set_page_config(
    page_title="CNPJ Analytics - Lista Fria de Clientes",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

DATA_DIR = Path("data/cnpj_parquet")
EMPRESAS_PARQUET = DATA_DIR / "empresas.parquet"
ESTAB_PARQUET = DATA_DIR / "estabelecimentos.parquet"
CNAES_PARQUET = DATA_DIR / "cnaes.parquet"
MUNICIPIOS_PARQUET = DATA_DIR / "municipios.parquet"

# CSS customizado
st.markdown(
    """
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .stDownloadButton button {
        background-color: #28a745;
        color: white;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ==================== HELPERS ====================

def _sql_escape(s: str) -> str:
    """Escapa aspas simples para evitar quebra da query."""
    if s is None:
        return ""
    return str(s).replace("'", "''")

@st.cache_resource
def init_duckdb():
    """Inicializa conexão com DuckDB"""
    return duckdb.connect(database=":memory:", read_only=False)

def executar_query(query: str) -> pd.DataFrame:
    """Executa query no DuckDB"""
    try:
        con = init_duckdb()
        return con.execute(query).df()
    except Exception as e:
        st.error(f"Erro ao executar query: {e}")
        return pd.DataFrame()

@st.cache_data
def carregar_cnaes():
    """Carrega lista de CNAEs"""
    try:
        df = executar_query(f"SELECT * FROM '{CNAES_PARQUET.as_posix()}'")
        return df
    except Exception as e:
        st.error(f"Erro ao carregar CNAEs: {e}")
        return pd.DataFrame()

@st.cache_data
def carregar_municipios():
    """Carrega lista de municípios"""
    try:
        df = executar_query(f"SELECT * FROM '{MUNICIPIOS_PARQUET.as_posix()}'")
        return df
    except Exception as e:
        st.error(f"Erro ao carregar municípios: {e}")
        return pd.DataFrame()

def _detectar_cols_municipios(df: pd.DataFrame):
    """
    Tenta detectar automaticamente as colunas de código e nome do município
    no municipios.parquet.
    """
    if df is None or df.empty:
        return None, None, None

    cols = list(df.columns)
    cols_lower = {c.lower(): c for c in cols}

    # candidatos comuns
    cand_cod = ["codigo", "cod_municipio", "codigo_municipio", "id_municipio", "municipio"]
    cand_nome = ["descricao", "nome", "municipio_nome", "nome_municipio", "descricao_municipio"]
    cand_uf = ["uf", "sigla_uf", "estado", "sg_uf"]

    col_cod = next((cols_lower[c] for c in cand_cod if c in cols_lower), None)
    col_nome = next((cols_lower[c] for c in cand_nome if c in cols_lower), None)
    col_uf = next((cols_lower[c] for c in cand_uf if c in cols_lower), None)

    return col_cod, col_nome, col_uf

def obter_estatisticas_gerais():
    """Obtém estatísticas gerais do banco de dados"""
    queries = {
        "total_empresas": f"SELECT COUNT(*) as total FROM '{EMPRESAS_PARQUET.as_posix()}'",
        "total_estabelecimentos": f"SELECT COUNT(*) as total FROM '{ESTAB_PARQUET.as_posix()}'",
        "estabelecimentos_ativos": f"""
            SELECT COUNT(*) as total
            FROM '{ESTAB_PARQUET.as_posix()}'
            WHERE situacao_cadastral = '02'
        """,
        "empresas_por_uf": f"""
            SELECT uf, COUNT(*) as total
            FROM '{ESTAB_PARQUET.as_posix()}'
            WHERE uf IS NOT NULL AND uf != ''
            GROUP BY uf
            ORDER BY total DESC
        """,
    }

    resultados = {}
    for key, q in queries.items():
        resultados[key] = executar_query(q)

    return resultados

def pesquisar_empresas(filtros: dict) -> pd.DataFrame:
    """Pesquisa empresas com filtros"""

    query = f"""
    SELECT
        e.cnpj_basico,
        e.razao_social,
        est.nome_fantasia,
        est.situacao_cadastral,
        est.data_inicio_atividade,
        est.cnae_fiscal_principal,
        est.uf,
        est.municipio,
        est.bairro,
        est.logradouro,
        est.numero,
        est.cep,
        est.ddd_1,
        est.telefone_1,
        est.correio_eletronico,
        e.capital_social,
        e.porte_empresa
    FROM '{EMPRESAS_PARQUET.as_posix()}' e
    JOIN '{ESTAB_PARQUET.as_posix()}' est
        ON e.cnpj_basico = est.cnpj_basico
    WHERE 1=1
    """

    if filtros.get("razao_social"):
        val = _sql_escape(filtros["razao_social"].lower())
        query += f" AND LOWER(e.razao_social) LIKE '%{val}%'"

    if filtros.get("nome_fantasia"):
        val = _sql_escape(filtros["nome_fantasia"].lower())
        query += f" AND LOWER(est.nome_fantasia) LIKE '%{val}%'"

    if filtros.get("cnaes"):
        cnaes = [_sql_escape(x) for x in filtros["cnaes"]]
        cnaes_str = "','".join(cnaes)
        query += f" AND est.cnae_fiscal_principal IN ('{cnaes_str}')"

    if filtros.get("ufs"):
        ufs = [_sql_escape(x) for x in filtros["ufs"]]
        ufs_str = "','".join(ufs)
        query += f" AND est.uf IN ('{ufs_str}')"

    if filtros.get("situacao_cadastral"):
        query += f" AND est.situacao_cadastral = '{_sql_escape(filtros['situacao_cadastral'])}'"

    if filtros.get("porte_empresa"):
        query += f" AND e.porte_empresa = '{_sql_escape(filtros['porte_empresa'])}'"

    if filtros.get("capital_minimo"):
        query += f" AND TRY_CAST(e.capital_social AS DOUBLE) >= {float(filtros['capital_minimo'])}"

    limite = int(filtros.get("limite", 1000))
    query += f" LIMIT {limite}"

    return executar_query(query)

def exportar_para_excel(df: pd.DataFrame) -> bytes:
    """Exporta DataFrame para Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Empresas")
    return output.getvalue()

def exportar_para_csv(df: pd.DataFrame) -> str:
    """Exporta DataFrame para CSV"""
    return df.to_csv(index=False, sep=";", encoding="utf-8-sig")

# ==================== INTERFACE STREAMLIT ====================

def main():
    st.markdown('<h1 class="main-header">🏢 CNPJ Analytics - Lista Fria de Clientes</h1>', unsafe_allow_html=True)

    with st.sidebar:
        st.image(
            "https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/institucional/imagens/logo-receita-federal.png/@@images/image",
            width=200,
        )
        st.title("⚙️ Configurações")

        pagina = st.radio(
            "Navegação",
            ["📊 Dashboard", "🔍 Pesquisa de Empresas", "📋 Lista Fria"],
            label_visibility="collapsed",
        )

    # ==================== PÁGINA: DASHBOARD ====================
    if pagina == "📊 Dashboard":
        st.header("📊 Dashboard Analítico")

        with st.spinner("Carregando estatísticas..."):
            stats = obter_estatisticas_gerais()

        col1, col2, col3 = st.columns(3)

        with col1:
            total_empresas = stats["total_empresas"]["total"].iloc[0] if not stats["total_empresas"].empty else 0
            st.metric("Total de Empresas", f"{total_empresas:,}")

        with col2:
            total_estab = stats["total_estabelecimentos"]["total"].iloc[0] if not stats["total_estabelecimentos"].empty else 0
            st.metric("Total de Estabelecimentos", f"{total_estab:,}")

        with col3:
            ativos = stats["estabelecimentos_ativos"]["total"].iloc[0] if not stats["estabelecimentos_ativos"].empty else 0
            st.metric("Estabelecimentos Ativos", f"{ativos:,}")

        st.divider()

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📍 Empresas por UF")
            if not stats["empresas_por_uf"].empty:
                df_uf = stats["empresas_por_uf"].head(10)
                fig = px.bar(
                    df_uf,
                    x="uf",
                    y="total",
                    title="Top 10 Estados",
                    labels={"uf": "UF", "total": "Quantidade"},
                    color="total",
                    color_continuous_scale="Blues",
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("🏭 CNAEs Mais Comuns")
            query_cnaes = f"""
            SELECT
                c.descricao,
                COUNT(*) as total
            FROM '{ESTAB_PARQUET.as_posix()}' e
            JOIN '{CNAES_PARQUET.as_posix()}' c
                ON e.cnae_fiscal_principal = c.codigo
            GROUP BY c.descricao
            ORDER BY total DESC
            LIMIT 10
            """
            df_cnaes = executar_query(query_cnaes)

            if not df_cnaes.empty:
                fig = px.pie(df_cnaes, values="total", names="descricao", title="Top 10 CNAEs")
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("📅 Empresas Abertas por Ano")
        query_temporal = f"""
            WITH base AS (
                SELECT
                    COALESCE(
                        TRY_CAST(NULLIF(TRIM(CAST(data_inicio_atividade AS VARCHAR)), '') AS DATE),
                        TRY_CAST(
                            TRY_STRPTIME(NULLIF(TRIM(CAST(data_inicio_atividade AS VARCHAR)), ''), '%Y%m%d')
                            AS DATE
                        )
                    ) AS dt_inicio
                FROM '{ESTAB_PARQUET.as_posix()}'
            )
            SELECT
                EXTRACT(YEAR FROM dt_inicio) AS ano,
                COUNT(*) AS total
            FROM base
            WHERE dt_inicio IS NOT NULL
            AND EXTRACT(YEAR FROM dt_inicio) >= 2000
            GROUP BY ano
            ORDER BY ano
        """

        df_temporal = executar_query(query_temporal)

        if not df_temporal.empty:
            fig = px.line(
                df_temporal,
                x="ano",
                y="total",
                title="Evolução de Abertura de Empresas",
                markers=True,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ==================== PÁGINA: PESQUISA ====================
    elif pagina == "🔍 Pesquisa de Empresas":
        st.header("🔍 Pesquisa Avançada de Empresas")

        df_cnaes = carregar_cnaes()

        with st.form("form_pesquisa"):
            col1, col2 = st.columns(2)

            with col1:
                razao_social = st.text_input("Razão Social (contém)")
                nome_fantasia = st.text_input("Nome Fantasia (contém)")

                situacao = st.selectbox(
                    "Situação Cadastral",
                    ["Todas", "02 - Ativa", "01 - Nula", "03 - Suspensa", "04 - Inapta", "08 - Baixada"],
                )

                porte = st.selectbox(
                    "Porte da Empresa",
                    ["Todos", "00 - Não Informado", "01 - Micro Empresa", "03 - Empresa de Pequeno Porte", "05 - Demais"],
                )

            with col2:
                ufs_selecionadas = st.multiselect(
                    "UF",
                    ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
                     "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
                     "RS", "RO", "RR", "SC", "SP", "SE", "TO"],
                )

                if not df_cnaes.empty:
                    cnaes_opcoes = df_cnaes.apply(lambda x: f"{x['codigo']} - {x['descricao']}", axis=1).tolist()
                    cnaes_selecionados = st.multiselect("CNAEs", cnaes_opcoes, help="Selecione um ou mais CNAEs")
                else:
                    cnaes_selecionados = []

                capital_minimo = st.number_input(
                    "Capital Social Mínimo (R$)",
                    min_value=0.0,
                    value=0.0,
                    step=1000.0,
                )

                limite = st.slider("Limite de Resultados", 100, 10000, 1000, 100)

            submitted = st.form_submit_button("🔍 Pesquisar", type="primary")

        if submitted:
            filtros = {"limite": limite}

            if razao_social:
                filtros["razao_social"] = razao_social
            if nome_fantasia:
                filtros["nome_fantasia"] = nome_fantasia
            if situacao != "Todas":
                filtros["situacao_cadastral"] = situacao.split(" - ")[0]
            if porte != "Todos":
                filtros["porte_empresa"] = porte.split(" - ")[0]
            if ufs_selecionadas:
                filtros["ufs"] = ufs_selecionadas
            if cnaes_selecionados:
                filtros["cnaes"] = [cnae.split(" - ")[0] for cnae in cnaes_selecionados]
            if capital_minimo > 0:
                filtros["capital_minimo"] = capital_minimo

            with st.spinner("Pesquisando empresas..."):
                df_resultado = pesquisar_empresas(filtros)

            if not df_resultado.empty:
                st.success(f"✅ Encontradas {len(df_resultado)} empresas")
                st.session_state["resultado_pesquisa"] = df_resultado

                st.dataframe(df_resultado, use_container_width=True, height=400)

                col1, col2, col3 = st.columns([1, 1, 2])

                with col1:
                    csv = exportar_para_csv(df_resultado)
                    st.download_button(
                        label="📥 Baixar CSV",
                        data=csv,
                        file_name="empresas_pesquisa.csv",
                        mime="text/csv",
                        use_container_width=True,
                    )

                with col2:
                    excel = exportar_para_excel(df_resultado)
                    st.download_button(
                        label="📥 Baixar Excel",
                        data=excel,
                        file_name="empresas_pesquisa.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                    )
            else:
                st.warning("⚠️ Nenhuma empresa encontrada com os filtros selecionados")

    # ==================== PÁGINA: LISTA FRIA ====================
    elif pagina == "📋 Lista Fria":
        st.header("📋 Gerador de Lista Fria de Clientes")

        st.info(
            """
            💡 **Dica**: Use esta ferramenta para criar listas segmentadas de potenciais clientes.
            Combine filtros de CNAE, localização e porte para encontrar seu público-alvo ideal.
            """
        )

        df_cnaes = carregar_cnaes()
        df_municipios = carregar_municipios()
        col_cod_mun, col_nome_mun, col_uf_mun = _detectar_cols_municipios(df_municipios)

        with st.form("form_lista_fria"):
            st.subheader("🎯 Defina seu Público-Alvo")

            col1, col2 = st.columns(2)

            with col1:
                st.markdown("**Segmentação por Atividade**")

                if not df_cnaes.empty:
                    cnaes_opcoes = df_cnaes.apply(lambda x: f"{x['codigo']} - {x['descricao']}", axis=1).tolist()
                    cnaes_lista = st.multiselect("CNAEs Alvo", cnaes_opcoes, help="Selecione os CNAEs do seu público-alvo")
                else:
                    cnaes_lista = []

                porte_lista = st.multiselect(
                    "Porte das Empresas",
                    ["01 - Micro Empresa", "03 - Empresa de Pequeno Porte", "05 - Demais"],
                    default=["01 - Micro Empresa", "03 - Empresa de Pequeno Porte"],
                )

            with col2:
                st.markdown("**Segmentação Geográfica**")

                ufs_lista = st.multiselect(
                    "Estados (UF)",
                    ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
                     "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
                     "RS", "RO", "RR", "SC", "SP", "SE", "TO"],
                    default=["SP", "RJ", "MG"],
                )

                # ======= CIDADES VINDO DO MUNICIPIOS.PARQUET (AQUI ESTÁ O QUE VOCÊ QUERIA) =======
                cidade_label_to_codigo = {}
                cidades_opcoes = []

                if df_municipios.empty or not col_cod_mun or not col_nome_mun:
                    st.warning("Não consegui detectar as colunas de código/nome no municipios.parquet. Verifique o arquivo.")
                    cidades_lista = st.multiselect(
                        "Cidades (Municípios)",
                        [],
                        help="Não foi possível carregar cidades do municipios.parquet",
                    )
                else:
                    tmp = df_municipios[[col_cod_mun, col_nome_mun]].copy()

                    # Se existir UF no parquet de municípios, filtra pelas UFs selecionadas (melhora performance)
                    if col_uf_mun and ufs_lista:
                        tmp = tmp[tmp[col_uf_mun].astype(str).isin(ufs_lista)]

                    tmp[col_cod_mun] = tmp[col_cod_mun].astype(str).str.strip()
                    tmp[col_nome_mun] = tmp[col_nome_mun].astype(str).str.strip()

                    tmp["label"] = tmp[col_cod_mun] + " - " + tmp[col_nome_mun]
                    cidades_opcoes = tmp["label"].tolist()
                    cidade_label_to_codigo = dict(zip(tmp["label"], tmp[col_cod_mun]))

                    cidades_lista = st.multiselect(
                        "Cidades (Municípios)",
                        cidades_opcoes,
                        help="Lista carregada do municipios.parquet",
                    )
                # ==============================================================================

                apenas_ativos = st.checkbox("Apenas Empresas Ativas", value=True)
                apenas_com_email = st.checkbox("Apenas com E-mail", value=False)
                apenas_com_telefone = st.checkbox("Apenas com Telefone", value=False)

            st.divider()

            col1, col2 = st.columns(2)
            with col1:
                capital_min = st.number_input("Capital Social Mínimo (R$)", min_value=0.0, value=0.0, step=5000.0)
            with col2:
                limite_lista = st.slider("Quantidade Máxima de Empresas", 100, 50000, 5000, 100)

            gerar_lista = st.form_submit_button("🚀 Gerar Lista Fria", type="primary")

        if gerar_lista:
            if not cnaes_lista:
                st.error("❌ Selecione pelo menos um CNAE")
            else:
                filtros = {
                    "cnaes": [cnae.split(" - ")[0] for cnae in cnaes_lista],
                    "limite": int(limite_lista),
                }

                if ufs_lista:
                    filtros["ufs"] = ufs_lista

                if porte_lista:
                    filtros["portes"] = [p.split(" - ")[0] for p in porte_lista]

                if apenas_ativos:
                    filtros["situacao_cadastral"] = "02"

                if capital_min > 0:
                    filtros["capital_minimo"] = float(capital_min)

                # ======= APLICA O FILTRO DE CIDADES SELECIONADAS =======
                if cidades_lista:
                    filtros["municipios"] = [cidade_label_to_codigo[x] for x in cidades_lista]
                # ======================================================

                query = f"""
                    SELECT
                        -- CNPJ completo (somente dígitos)
                        (LPAD(CAST(e.cnpj_basico AS VARCHAR), 8, '0')
                        || LPAD(CAST(est.cnpj_ordem AS VARCHAR), 4, '0')
                        || LPAD(CAST(est.cnpj_dv    AS VARCHAR), 2, '0')
                        ) AS cnpj_completo,

                        -- CNPJ formatado 00.000.000/0000-00
                        (
                        SUBSTR(
                            LPAD(CAST(e.cnpj_basico AS VARCHAR), 8, '0')
                            || LPAD(CAST(est.cnpj_ordem AS VARCHAR), 4, '0')
                            || LPAD(CAST(est.cnpj_dv AS VARCHAR), 2, '0')
                        ,1,2) || '.' ||
                        SUBSTR(
                            LPAD(CAST(e.cnpj_basico AS VARCHAR), 8, '0')
                            || LPAD(CAST(est.cnpj_ordem AS VARCHAR), 4, '0')
                            || LPAD(CAST(est.cnpj_dv AS VARCHAR), 2, '0')
                        ,3,3) || '.' ||
                        SUBSTR(
                            LPAD(CAST(e.cnpj_basico AS VARCHAR), 8, '0')
                            || LPAD(CAST(est.cnpj_ordem AS VARCHAR), 4, '0')
                            || LPAD(CAST(est.cnpj_dv AS VARCHAR), 2, '0')
                        ,6,3) || '/' ||
                        SUBSTR(
                            LPAD(CAST(e.cnpj_basico AS VARCHAR), 8, '0')
                            || LPAD(CAST(est.cnpj_ordem AS VARCHAR), 4, '0')
                            || LPAD(CAST(est.cnpj_dv AS VARCHAR), 2, '0')
                        ,9,4) || '-' ||
                        SUBSTR(
                            LPAD(CAST(e.cnpj_basico AS VARCHAR), 8, '0')
                            || LPAD(CAST(est.cnpj_ordem AS VARCHAR), 4, '0')
                            || LPAD(CAST(est.cnpj_dv AS VARCHAR), 2, '0')
                        ,13,2)
                        ) AS cnpj,

                        e.razao_social,
                        est.nome_fantasia,
                        est.situacao_cadastral,
                        est.data_inicio_atividade,
                        est.cnae_fiscal_principal,
                        est.uf,

                        -- município (código como está no estabelecimentos.parquet)
                        CAST(est.municipio AS VARCHAR) AS municipio_codigo,

                        est.bairro,
                        est.logradouro,
                        est.numero,
                        est.cep,

                        -- aliases para suas métricas não quebrarem
                        est.correio_eletronico AS email,
                        (CAST(est.ddd_1 AS VARCHAR) || ' ' || CAST(est.telefone_1 AS VARCHAR)) AS telefone,

                        e.capital_social,
                        e.porte_empresa
                    FROM '{EMPRESAS_PARQUET.as_posix()}' e
                    JOIN '{ESTAB_PARQUET.as_posix()}' est
                        ON e.cnpj_basico = est.cnpj_basico
                    WHERE 1=1
                """

                if filtros.get("cnaes"):
                    cnaes = [_sql_escape(x) for x in filtros["cnaes"]]
                    cnaes_str = "','".join(cnaes)
                    query += f" AND est.cnae_fiscal_principal IN ('{cnaes_str}')"

                if filtros.get("ufs"):
                    ufs = [_sql_escape(x) for x in filtros["ufs"]]
                    ufs_str = "','".join(ufs)
                    query += f" AND est.uf IN ('{ufs_str}')"

                if filtros.get("situacao_cadastral"):
                    query += f" AND est.situacao_cadastral = '{_sql_escape(filtros['situacao_cadastral'])}'"

                if filtros.get("portes"):
                    portes = [_sql_escape(x) for x in filtros["portes"]]
                    portes_str = "','".join(portes)
                    query += f" AND e.porte_empresa IN ('{portes_str}')"

                if filtros.get("capital_minimo") is not None and filtros.get("capital_minimo") > 0:
                    query += f" AND TRY_CAST(e.capital_social AS DOUBLE) >= {filtros['capital_minimo']}"

                if apenas_com_email:
                    query += " AND est.correio_eletronico IS NOT NULL AND TRIM(est.correio_eletronico) != ''"

                if apenas_com_telefone:
                    query += " AND est.telefone_1 IS NOT NULL AND TRIM(CAST(est.telefone_1 AS VARCHAR)) != ''"

                # ======= FILTRO POR MUNICÍPIOS SELECIONADOS =======
                if filtros.get("municipios"):
                    cods = [_sql_escape(x) for x in filtros["municipios"]]
                    cods_str = "','".join(cods)
                    query += f" AND CAST(est.municipio AS VARCHAR) IN ('{cods_str}')"
                # ==================================================

                query += f" LIMIT {filtros['limite']}"

                with st.spinner("Gerando lista fria..."):
                    df_lista = executar_query(query)

                # (Opcional) adiciona o nome do município via merge (se o parquet tiver)
                if not df_lista.empty and not df_municipios.empty and col_cod_mun and col_nome_mun:
                    mun_map = df_municipios[[col_cod_mun, col_nome_mun]].copy()
                    mun_map[col_cod_mun] = mun_map[col_cod_mun].astype(str).str.strip()
                    mun_map[col_nome_mun] = mun_map[col_nome_mun].astype(str).str.strip()

                    df_lista = df_lista.merge(
                        mun_map.rename(columns={col_cod_mun: "municipio_codigo", col_nome_mun: "municipio_nome"}),
                        on="municipio_codigo",
                        how="left"
                    )

                if not df_lista.empty:
                    st.success(f"✅ Lista gerada com {len(df_lista)} empresas!")

                    col1, col2, col3, col4 = st.columns(4)

                    with col1:
                        st.metric("Total de Empresas", len(df_lista))

                    with col2:
                        com_email = df_lista["email"].fillna("").astype(str).str.strip().ne("").sum()
                        st.metric("Com E-mail", com_email)

                    with col3:
                        com_telefone = df_lista["telefone"].fillna("").astype(str).str.strip().ne("").sum()
                        st.metric("Com Telefone", com_telefone)

                    with col4:
                        ufs_unicas = df_lista["uf"].nunique()
                        st.metric("Estados", ufs_unicas)

                    st.divider()

                    st.subheader("👀 Preview da Lista")
                    st.dataframe(df_lista.head(50), use_container_width=True)

                    st.subheader("📥 Exportar Lista Fria")

                    col1, col2, col3 = st.columns([1, 1, 2])

                    with col1:
                        csv = exportar_para_csv(df_lista)
                        st.download_button(
                            label="📥 Baixar CSV Completo",
                            data=csv,
                            file_name="lista_fria_clientes.csv",
                            mime="text/csv",
                            use_container_width=True,
                        )

                    with col2:
                        excel = exportar_para_excel(df_lista)
                        st.download_button(
                            label="📥 Baixar Excel Completo",
                            data=excel,
                            file_name="lista_fria_clientes.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )

                    st.divider()
                    st.subheader("📊 Análise da Lista Gerada")

                    col1, col2 = st.columns(2)

                    with col1:
                        df_uf_dist = df_lista["uf"].value_counts().reset_index()
                        df_uf_dist.columns = ["uf", "quantidade"]

                        fig = px.bar(
                            df_uf_dist,
                            x="uf",
                            y="quantidade",
                            title="Distribuição por Estado",
                            color="quantidade",
                            color_continuous_scale="Greens",
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    with col2:
                        df_porte_dist = df_lista["porte_empresa"].value_counts().reset_index()
                        df_porte_dist.columns = ["porte", "quantidade"]

                        fig = px.pie(
                            df_porte_dist,
                            values="quantidade",
                            names="porte",
                            title="Distribuição por Porte",
                        )
                        st.plotly_chart(fig, use_container_width=True)

                else:
                    st.warning("⚠️ Nenhuma empresa encontrada com os critérios selecionados")


if __name__ == "__main__":
    main()
