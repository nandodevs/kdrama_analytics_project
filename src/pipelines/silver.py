import os
import json
import pandas as pd
import logging
import sys
from datetime import datetime

# Ajustar o path para importações (se necessário, embora não usemos api_client aqui diretamente)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
# sys.path.insert(0, parent_dir) # Descomente se precisar importar de 'common'

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Configurações do Pipeline Silver ---
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, "..", ".."))
BRONZE_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "bronze", "raw_kdramas")
SILVER_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "silver")
SILVER_OUTPUT_FILENAME = "kdramas_silver.parquet"

# --- Funções Auxiliares ---
def ensure_dir_exists(directory_path):
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Diretório criado: {directory_path}")

def load_json_file(file_path):
    """Carrega um único arquivo JSON, retorna None se não existir ou houver erro."""
    if not os.path.exists(file_path):
        logging.warning(f"Arquivo JSON não encontrado: {file_path}")
        return None
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        logging.error(f"Erro ao decodificar JSON de {file_path}: {e}")
        return None
    except Exception as e:
        logging.error(f"Erro inesperado ao carregar {file_path}: {e}")
        return None

def extract_names_from_list_of_dicts(data_list, key_name='name', max_items=None):
    """Extrai uma lista de nomes de uma lista de dicionários."""
    if not isinstance(data_list, list):
        return []
    names = [item.get(key_name) for item in data_list if isinstance(item, dict) and item.get(key_name)]
    return names[:max_items] if max_items else names

# --- Lógica de Transformação para um Kdrama ---
def process_kdrama_data(kdrama_id, base_bronze_path):
    """
    Processa os arquivos JSON de um Kdrama da Camada Bronze e retorna um dicionário com dados limpos.
    """
    logging.debug(f"Processando Kdrama ID: {kdrama_id}")
    
    discover_data = load_json_file(os.path.join(base_bronze_path, f"{kdrama_id}_discover_info.json"))
    details_data = load_json_file(os.path.join(base_bronze_path, f"{kdrama_id}_details.json"))
    credits_data = load_json_file(os.path.join(base_bronze_path, f"{kdrama_id}_credits.json"))

    # Se o discover_info (principal) não existir, não podemos prosseguir para este ID
    if not discover_data:
        logging.warning(f"Dados de 'discover_info' não encontrados para Kdrama ID: {kdrama_id}. Pulando.")
        return None

    # Inicializar o dicionário de saída com dados do discover_info
    processed_data = {
        'id_tmdb': discover_data.get('id'),
        'title_original': discover_data.get('original_name'),
        'title_ptbr': discover_data.get('name'),
        'overview_ptbr': discover_data.get('overview'),
        'popularity': discover_data.get('popularity'),
        'vote_average_discover': discover_data.get('vote_average'),
        'vote_count_discover': discover_data.get('vote_count'),
        'first_air_date_str': discover_data.get('first_air_date'),
        'original_language': discover_data.get('original_language'),
        'poster_path': discover_data.get('poster_path'),
        'backdrop_path': discover_data.get('backdrop_path')
    }

    # Converter 'first_air_date_str' para datetime e extrair ano e data formatada
    if processed_data['first_air_date_str']:
        try:
            dt_object = datetime.strptime(processed_data['first_air_date_str'], '%Y-%m-%d')
            processed_data['first_air_date'] = dt_object # Como objeto datetime
            processed_data['release_year'] = dt_object.year
        except ValueError:
            logging.warning(f"Formato de data inválido para first_air_date: {processed_data['first_air_date_str']} no ID {kdrama_id}")
            processed_data['first_air_date'] = None
            processed_data['release_year'] = None
    else:
        processed_data['first_air_date'] = None
        processed_data['release_year'] = None

    # Campos dos detalhes (details_data)
    if details_data:
        processed_data['status'] = details_data.get('status')
        processed_data['tagline'] = details_data.get('tagline')
        processed_data['number_of_episodes'] = details_data.get('number_of_episodes')
        processed_data['number_of_seasons'] = details_data.get('number_of_seasons')
        processed_data['episode_run_time'] = details_data.get('episode_run_time', []) # Pode ser uma lista
        processed_data['genres'] = extract_names_from_list_of_dicts(details_data.get('genres', []))
        processed_data['production_companies'] = extract_names_from_list_of_dicts(details_data.get('production_companies', []))
        processed_data['networks'] = extract_names_from_list_of_dicts(details_data.get('networks', []))
        processed_data['vote_average_details'] = details_data.get('vote_average') # Pode ser mais atualizado
        processed_data['vote_count_details'] = details_data.get('vote_count')
        # Palavras-chave
        keywords_results = details_data.get('keywords', {}).get('results', []) # TMDB v3 para TV
        if not keywords_results and 'keywords' in details_data: # TMDB v4 pode retornar 'keywords' diretamente
             keywords_results = details_data.get('keywords', [])
        processed_data['keywords'] = extract_names_from_list_of_dicts(keywords_results)

        # Onde assistir (Exemplo simples para 'flatrate' no Brasil)
        watch_providers_br = details_data.get('watch/providers', {}).get('results', {}).get('BR', {})
        if watch_providers_br and 'flatrate' in watch_providers_br:
            processed_data['streaming_br'] = extract_names_from_list_of_dicts(watch_providers_br['flatrate'], 'provider_name')
        else:
            processed_data['streaming_br'] = []
    else: # Preencher com nulos se details_data não existir
        fields_from_details = ['status', 'tagline', 'number_of_episodes', 'number_of_seasons', 
                               'episode_run_time', 'genres', 'production_companies', 'networks',
                               'vote_average_details', 'vote_count_details', 'keywords', 'streaming_br']
        for field in fields_from_details:
            processed_data[field] = [] if field in ['episode_run_time', 'genres', 'production_companies', 'networks', 'keywords', 'streaming_br'] else None


    # Campos dos créditos (credits_data)
    if credits_data:
        processed_data['cast_top10'] = extract_names_from_list_of_dicts(credits_data.get('cast', []), max_items=10)
        
        crew = credits_data.get('crew', [])
        processed_data['directors'] = extract_names_from_list_of_dicts(
            [member for member in crew if member.get('job') == 'Director']
        )
        # Para roteiristas, pode haver diferentes 'jobs' como 'Writer', 'Screenplay', 'Story'
        processed_data['writers'] = extract_names_from_list_of_dicts(
            [member for member in crew if member.get('department') == 'Writing']
        )
    else: # Preencher com nulos se credits_data não existir
        processed_data['cast_top10'] = []
        processed_data['directors'] = []
        processed_data['writers'] = []
        
    return processed_data


# --- Lógica Principal do Pipeline Silver ---
def run_silver_pipeline():
    logging.info("Iniciando pipeline da Camada Silver...")
    ensure_dir_exists(SILVER_DATA_PATH)

    # 1. Listar todos os arquivos de 'discover_info' para obter os IDs
    kdrama_ids = set() # Usar um set para evitar duplicatas de IDs
    if not os.path.exists(BRONZE_DATA_PATH):
        logging.error(f"Caminho da Camada Bronze não encontrado: {BRONZE_DATA_PATH}. Abortando.")
        return

    for filename in os.listdir(BRONZE_DATA_PATH):
        if filename.endswith("_discover_info.json"):
            try:
                # Extrai o ID do nome do arquivo (ex: "12345_discover_info.json" -> "12345")
                kdrama_id = filename.split('_')[0]
                if kdrama_id.isdigit(): # Verifica se é um ID numérico válido
                    kdrama_ids.add(kdrama_id)
            except IndexError:
                logging.warning(f"Não foi possível extrair o ID do arquivo: {filename}")
    
    if not kdrama_ids:
        logging.warning("Nenhum Kdrama ID encontrado na Camada Bronze para processar.")
        return
    
    logging.info(f"Encontrados {len(kdrama_ids)} IDs de Kdramas únicos para processar da Camada Bronze.")

    # 2. Processar cada Kdrama
    all_processed_kdramas = []
    for kdrama_id in kdrama_ids:
        processed_data = process_kdrama_data(kdrama_id, BRONZE_DATA_PATH)
        if processed_data:
            all_processed_kdramas.append(processed_data)
            logging.info(f"Kdrama ID {kdrama_id} processado para a Camada Silver.")
        else:
            logging.warning(f"Falha ao processar dados para o Kdrama ID {kdrama_id}.")

    if not all_processed_kdramas:
        logging.warning("Nenhum Kdrama foi processado com sucesso. Nenhum dado para salvar na Camada Silver.")
        return

    # 3. Converter para DataFrame do Pandas
    df_silver = pd.DataFrame(all_processed_kdramas)
    logging.info(f"DataFrame da Camada Silver criado com {df_silver.shape[0]} linhas e {df_silver.shape[1]} colunas.")
    
    # Algumas limpezas/transformações adicionais no DataFrame completo (opcional)
    # Ex: garantir que colunas de lista sejam de fato listas e não objetos, tratar NaNs específicos
    list_columns = ['genres', 'production_companies', 'networks', 'keywords', 'streaming_br', 'cast_top10', 'directors', 'writers', 'episode_run_time']
    for col in list_columns:
        if col in df_silver.columns:
            df_silver[col] = df_silver[col].apply(lambda x: x if isinstance(x, list) else [])
    
    # Verificar tipos de dados e exibir informações do DataFrame
    logging.info("Informações do DataFrame da Camada Silver (dtypes e amostra):")
    df_silver.info(verbose=True, show_counts=True) # verbose e show_counts para mais detalhes
    logging.info("\nAmostra dos dados da Camada Silver (primeiras 5 linhas):")
    # Para logging, converter para string para evitar problemas com display em alguns terminais
    try:
        logging.info("\n" + df_silver.head().to_string())
    except Exception as e:
        logging.error(f"Erro ao logar head do DataFrame: {e}")


    # 4. Salvar o DataFrame como Parquet na Camada Silver
    silver_file_path = os.path.join(SILVER_DATA_PATH, SILVER_OUTPUT_FILENAME)
    try:
        df_silver.to_parquet(silver_file_path, index=False, engine='pyarrow') # ou 'fastparquet'
        logging.info(f"DataFrame da Camada Silver salvo em: {silver_file_path}")
    except Exception as e:
        logging.error(f"Erro ao salvar DataFrame da Camada Silver como Parquet: {e}")

    logging.info("Pipeline da Camada Silver finalizado.")

if __name__ == '__main__':
    # Exemplo de execução (da raiz do projeto kdrama_analytics_project/):
    # python src/pipelines/silver.py
    run_silver_pipeline()