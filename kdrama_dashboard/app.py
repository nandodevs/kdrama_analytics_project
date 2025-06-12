# app.py
import streamlit as st
import pandas as pd
import pyodbc

# --- Configuração da Página ---
# st.set_page_config define as configurações iniciais da sua página.
# É bom definir um layout 'wide' para dashboards.
st.set_page_config(
    page_title="Dashboard de Kdramas",
    page_icon="📺",
    layout="wide"
)

# --- Conexão com o Banco de Dados (com cache) ---

# A anotação @st.cache_resource diz ao Streamlit para executar esta função apenas uma vez,
# criando um único "recurso" (nossa conexão com o BD) e reutilizando-o.
@st.cache_resource
def init_connection():
    # Usa os segredos definidos em .streamlit/secrets.toml
    connection_string = (
        f"DRIVER={st.secrets.database.driver};"
        f"SERVER={st.secrets.database.server};"
        f"DATABASE={st.secrets.database.database};"
        f"UID={st.secrets.database.username};"
        f"PWD={st.secrets.database.password};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    return pyodbc.connect(connection_string)

# A anotação @st.cache_data diz ao Streamlit para executar esta função apenas se os
# argumentos mudarem. Como não há argumentos, ela rodará uma vez, buscará os dados,
# e guardará o resultado (o DataFrame) em cache para performance.
@st.cache_data
def load_data():
    conn = init_connection()
    query = "SELECT * FROM dbo.KdramaDashboard"
    df = pd.read_sql(query, conn)
    # Converter colunas de data que podem vir como texto
    df['first_air_date'] = pd.to_datetime(df['first_air_date'])
    return df

# --- Início do Layout do Dashboard ---

st.title('📺 Análise de Kdramas Populares (2020-2024)')
st.markdown("Use os filtros na barra lateral para explorar os dados.")

# Carregar os dados
try:
    df = load_data()

    # --- Barra Lateral de Filtros (Sidebar) ---
    st.sidebar.header("Filtros")

    # Filtro por ano de lançamento
    min_year = int(df['release_year'].min())
    max_year = int(df['release_year'].max())
    selected_year_range = st.sidebar.slider(
        "Selecione o Ano de Lançamento:",
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year) # Valor inicial (todos os anos)
    )

    # Filtro por gênero
    # Como 'genres_str' é uma string de gêneros separados por vírgula, precisamos obter os gêneros únicos
    all_genres = set()
    df['genres_str'].str.split(', ').apply(lambda genres: [all_genres.add(g) for g in genres if g])
    sorted_genres = sorted(list(all_genres))
    
    selected_genres = st.sidebar.multiselect(
        "Selecione os Gêneros:",
        options=sorted_genres
    )

    # --- Aplicar Filtros ao DataFrame ---
    
    # Filtrar por ano
    df_filtered = df[
        (df['release_year'] >= selected_year_range[0]) &
        (df['release_year'] <= selected_year_range[1])
    ]

    # Filtrar por gênero (se algum gênero for selecionado)
    if selected_genres:
        # A lógica aqui verifica se QUALQUER um dos gêneros selecionados está na string de gêneros do drama
        df_filtered = df_filtered[
            df_filtered['genres_str'].apply(lambda genres: any(g in genres for g in selected_genres))
        ]

    # --- Exibição dos Dados e Gráficos ---

    # KPIs (Key Performance Indicators)
    total_dramas_filtrados = df_filtered.shape[0]
    nota_media_filtrada = round(df_filtered['vote_average'].mean(), 2) if not df_filtered.empty else 0

    col1, col2 = st.columns(2)
    with col1:
        st.metric(label="Total de Kdramas (Filtro Atual)", value=total_dramas_filtrados)
    with col2:
        st.metric(label="Nota Média (Filtro Atual)", value=nota_media_filtrada)

    st.markdown("---")

    # Gráfico: Top 10 Kdramas por Popularidade
    st.subheader("Top 10 Kdramas Mais Populares (Filtro Atual)")
    df_top_10_pop = df_filtered.nlargest(10, 'popularity').sort_values('popularity', ascending=False)
    st.dataframe(df_top_10_pop[['title_ptbr', 'release_year', 'popularity', 'vote_average']], use_container_width=True)


    # Gráfico: Número de Kdramas por Ano
    st.subheader("Número de Kdramas por Ano de Lançamento")
    dramas_por_ano = df_filtered['release_year'].value_counts().sort_index()
    st.bar_chart(dramas_por_ano)
    

    # Tabela com dados completos (ocultável)
    with st.expander("Ver tabela de dados completa (filtrada)"):
        st.dataframe(df_filtered, use_container_width=True)

except Exception as e:
    st.error(f"Ocorreu um erro ao carregar o dashboard: {e}")
    st.error("Verifique as credenciais no arquivo .streamlit/secrets.toml e a conexão com o banco de dados.")