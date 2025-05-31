import os
import pandas as pd
import logging
import sys

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Configurações do Pipeline Gold ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SILVER_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "silver")
SILVER_INPUT_FILENAME = "kdramas_silver.parquet"
GOLD_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "gold")

# --- Funções Auxiliares ---
def ensure_dir_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Diretório criado: {directory_path}")

def save_df_to_gold(df, filename, base_path=GOLD_DATA_PATH):
    """Salva um DataFrame como Parquet na Camada Gold."""
    if df.empty:
        logging.warning(f"DataFrame para {filename} está vazio. Nenhum arquivo será salvo.")
        return
    
    ensure_dir_exists(base_path)
    file_path = os.path.join(base_path, filename)
    try:
        df.to_parquet(file_path, index=False, engine='pyarrow')
        logging.info(f"Dados da Camada Gold salvos em: {file_path} ({len(df)} linhas)")
    except Exception as e:
        logging.error(f"Erro ao salvar DataFrame da Camada Gold como Parquet ({filename}): {e}")

# --- Lógica Principal do Pipeline Gold ---
def run_gold_pipeline():
    logging.info("Iniciando pipeline da Camada Gold...")
    ensure_dir_exists(GOLD_DATA_PATH)

    # 1. Carregar dados da Camada Silver
    silver_file_path = os.path.join(SILVER_DATA_PATH, SILVER_INPUT_FILENAME)
    if not os.path.exists(silver_file_path):
        logging.error(f"Arquivo da Camada Silver não encontrado: {silver_file_path}. Abortando.")
        return
    
    try:
        df_silver = pd.read_parquet(silver_file_path)
        logging.info(f"Dados da Camada Silver carregados com sucesso ({len(df_silver)} linhas).")
    except Exception as e:
        logging.error(f"Erro ao carregar dados da Camada Silver: {e}. Abortando.")
        return

    if df_silver.empty:
        logging.warning("DataFrame da Camada Silver está vazio. Não há dados para processar para a Camada Gold.")
        return

    # --- Tabela 1: kdramas_finais_para_dashboard.parquet ---
    # Selecionar e talvez renomear colunas para o dashboard
    # Garantir que colunas de lista sejam strings concatenadas ou explodidas dependendo do uso no BI
    df_dashboard = df_silver.copy()
    
    # Para colunas de lista, podemos querer transformá-las em string para exibição fácil em tabelas de BI
    # ou o BI pode lidar com listas diretamente (Power BI lida bem com JSON/listas como colunas)
    # Exemplo de conversão para string:
    list_cols_to_str = ['genres', 'production_companies', 'networks', 'keywords', 
                        'streaming_br', 'cast_top10', 'directors', 'writers']
    for col in list_cols_to_str:
        if col in df_dashboard.columns:
            # Converte a lista para uma string separada por vírgulas, tratando None ou listas vazias
            df_dashboard[f'{col}_str'] = df_dashboard[col].apply(
                lambda x: ', '.join(x) if isinstance(x, list) and x else ''
            )
    
    # Selecionar colunas finais para o dashboard (incluindo as novas _str se desejar)
    # Esta é uma sugestão, ajuste conforme sua necessidade de visualização
    cols_for_dashboard = [
        'id_tmdb', 'title_original', 'title_ptbr', 'overview_ptbr', 'popularity',
        'vote_average_details', 'vote_count_details', 'first_air_date', 'release_year',
        'status', 'number_of_episodes', 'number_of_seasons', 'episode_run_time',
        'genres_str', 'production_companies_str', 'networks_str', 'keywords_str',
        'streaming_br_str', 'cast_top10_str', 'directors_str', 'writers_str',
        'poster_path', 'backdrop_path'
    ]
    # Filtrar para manter apenas colunas que existem no df_dashboard
    final_cols_for_dashboard = [col for col in cols_for_dashboard if col in df_dashboard.columns]
    df_dashboard_final = df_dashboard[final_cols_for_dashboard]
    save_df_to_gold(df_dashboard_final, "kdramas_finais_para_dashboard.parquet")


    # --- Tabela 2: estatisticas_por_genero.parquet ---
    # Requer "explodir" a coluna de gêneros se ela for uma lista
    if 'genres' in df_silver.columns and not df_silver['genres'].empty:
        df_genres_exploded = df_silver.explode('genres').dropna(subset=['genres'])
        if not df_genres_exploded.empty:
            df_estatisticas_genero = df_genres_exploded.groupby('genres').agg(
                total_kdramas=('id_tmdb', 'count'),
                nota_media=('vote_average_details', 'mean'),
                popularidade_media=('popularity', 'mean'),
                total_votos=('vote_count_details', 'sum')
            ).reset_index().sort_values(by='total_kdramas', ascending=False)
            df_estatisticas_genero['nota_media'] = df_estatisticas_genero['nota_media'].round(2)
            df_estatisticas_genero['popularidade_media'] = df_estatisticas_genero['popularidade_media'].round(2)
            save_df_to_gold(df_estatisticas_genero, "estatisticas_por_genero.parquet")
        else:
            logging.warning("Coluna 'genres' vazia após explode ou contém apenas NaNs. Estatísticas por gênero não geradas.")
    else:
        logging.warning("Coluna 'genres' não encontrada ou vazia no DataFrame Silver. Estatísticas por gênero não geradas.")


    # --- Tabela 3: top_kdramas_por_ano.parquet ---
    # Top 5 Kdramas por ano (exemplo, baseado em popularidade)
    # Considerar apenas dramas com um número mínimo de votos para relevância da nota
    VOTE_COUNT_THRESHOLD = 50 # Ajuste conforme necessário
    df_com_votos_suficientes = df_silver[df_silver['vote_count_details'] >= VOTE_COUNT_THRESHOLD].copy()
    
    if not df_com_votos_suficientes.empty and 'release_year' in df_com_votos_suficientes.columns:
        df_com_votos_suficientes['rank_popularidade_ano'] = df_com_votos_suficientes.groupby('release_year')['popularity'].rank(method='first', ascending=False)
        df_top_kdramas_ano = df_com_votos_suficientes[df_com_votos_suficientes['rank_popularidade_ano'] <= 5].sort_values(by=['release_year', 'rank_popularidade_ano'])
        
        # Selecionar colunas relevantes para esta tabela
        cols_top_kdramas = ['release_year', 'rank_popularidade_ano', 'title_ptbr', 'title_original', 'popularity', 'vote_average_details', 'id_tmdb']
        df_top_kdramas_ano = df_top_kdramas_ano[[col for col in cols_top_kdramas if col in df_top_kdramas_ano.columns]]
        save_df_to_gold(df_top_kdramas_ano, "top_kdramas_por_ano.parquet")
    else:
        logging.warning("Não há Kdramas suficientes com contagem de votos acima do threshold ou coluna 'release_year' ausente. Top Kdramas por ano não gerado.")


    # --- Tabela 4: tendencia_anual_kdramas.parquet ---
    if 'release_year' in df_silver.columns and not df_silver['release_year'].dropna().empty:
        df_tendencia_anual = df_silver.groupby('release_year').agg(
            total_kdramas_lancados=('id_tmdb', 'count'),
            nota_media_anual=('vote_average_details', 'mean'),
            popularidade_media_anual=('popularity', 'mean')
        ).reset_index().sort_values(by='release_year')
        df_tendencia_anual['nota_media_anual'] = df_tendencia_anual['nota_media_anual'].round(2)
        df_tendencia_anual['popularidade_media_anual'] = df_tendencia_anual['popularidade_media_anual'].round(2)
        save_df_to_gold(df_tendencia_anual, "tendencia_anual_kdramas.parquet")
    else:
        logging.warning("Coluna 'release_year' não encontrada ou vazia. Tendência anual não gerada.")

    logging.info("Pipeline da Camada Gold finalizado.")

if __name__ == '__main__':
    # Exemplo de execução (da raiz do projeto kdrama_analytics_project/):
    # python src/pipelines/gold.py
    run_gold_pipeline()