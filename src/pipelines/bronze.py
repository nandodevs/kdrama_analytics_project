import os
import json
import logging
from datetime import datetime

# Ajustar o path para importar o api_client do diretório common
import sys
# Obtém o diretório do script atual (src/pipelines)
current_dir = os.path.dirname(os.path.abspath(__file__))
# Obtém o diretório pai (src)
parent_dir = os.path.dirname(current_dir)
# Adiciona o diretório src ao sys.path para permitir importações de common
sys.path.insert(0, parent_dir)

from common import api_client # Agora deve importar corretamente

# Configuração básica de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout) # Para ver logs no console
        # Se quiser salvar em arquivo também:
        # logging.FileHandler("bronze_ingestion.log")
    ]
)

# --- Configurações do Pipeline Bronze ---
# Caminho base para salvar os dados brutos.
# Vamos criar uma subpasta com a data da execução para organizar.
PROJECT_ROOT = os.path.abspath(os.path.join(current_dir, "..", "..")) # Raiz do projeto
BRONZE_BASE_PATH = os.path.join(PROJECT_ROOT, "data", "bronze", "raw_kdramas")
# Cria uma subpasta com a data da execução
# CURRENT_EXECUTION_DATE = datetime.now().strftime("%Y-%m-%d")
# BRONZE_SAVE_PATH = os.path.join(BRONZE_BASE_PATH, CURRENT_EXECUTION_DATE)
# Por simplicidade inicial, vamos salvar direto em BRONZE_BASE_PATH
BRONZE_SAVE_PATH = BRONZE_BASE_PATH


# Período de busca para "últimos 5 anos" (ajuste conforme necessário)
# Considerando que estamos no final de 2025, vamos pegar de 2020 a 2024.
START_DATE = "2020-01-01"
END_DATE = "2025-05-30"

KDRAMA_GENRE_NAME_KO = "드라마"
TARGET_ORIGINAL_LANGUAGE = "ko"
DEFAULT_LANGUAGE_PT = "pt-BR"

# Limite de páginas a buscar no endpoint /discover.
# A API do TMDB limita a 500 páginas para /discover.
MAX_PAGES_TO_FETCH_DISCOVER = 5 # Comece com poucas páginas para teste.
                               # Mude para um valor maior (até 500) para uma coleta completa.

# --- Funções Auxiliares ---
def ensure_dir_exists(directory_path):
    """Garante que um diretório exista; se não, cria-o."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logging.info(f"Diretório criado: {directory_path}")

def save_json_to_bronze(data, file_name_prefix, data_type_suffix, base_path=BRONZE_SAVE_PATH):
    """Salva dados (dicionário Python) como um arquivo JSON na camada Bronze."""
    if not data:
        logging.warning(f"Nenhum dado para salvar para {file_name_prefix}_{data_type_suffix}.json")
        return

    ensure_dir_exists(base_path)
    file_path = os.path.join(base_path, f"{file_name_prefix}_{data_type_suffix}.json")
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Dados salvos em: {file_path}")
    except IOError as e:
        logging.error(f"Erro ao salvar JSON em {file_path}: {e}")
    except TypeError as e:
        logging.error(f"Erro de tipo ao serializar JSON para {file_path}: {e}. Dados: {data}")


# --- Lógica Principal do Pipeline Bronze ---
def run_bronze_ingestion():
    """
    Executa o pipeline de ingestão da Camada Bronze.
    Busca Kdramas, seus detalhes e créditos, e salva os JSONs brutos.
    """
    logging.info("Iniciando pipeline de ingestão da Camada Bronze...")
    ensure_dir_exists(BRONZE_SAVE_PATH) # Garante que o diretório base de salvamento exista

    # 1. Obter o ID do gênero "Drama" em Coreano
    logging.info(f"Buscando ID do gênero '{KDRAMA_GENRE_NAME_KO}' em Coreano...")
    genres_data = api_client.get_genres(media_type="tv", language="ko-KR")
    drama_genre_id = None
    if genres_data and 'genres' in genres_data:
        for genre in genres_data['genres']:
            if genre['name'] == KDRAMA_GENRE_NAME_KO:
                drama_genre_id = genre['id']
                logging.info(f"ID do gênero '{KDRAMA_GENRE_NAME_KO}' encontrado: {drama_genre_id}")
                break
    
    if not drama_genre_id:
        logging.error(f"Não foi possível encontrar o ID do gênero '{KDRAMA_GENRE_NAME_KO}'. Abortando.")
        return

    # 2. Descobrir Kdramas (paginado)
    logging.info(f"Descobrindo Kdramas ({TARGET_ORIGINAL_LANGUAGE}, Gênero ID: {drama_genre_id}) entre {START_DATE} e {END_DATE}.")
    
    all_discovered_kdramas_info = []
    
    # Primeira chamada para obter total_pages
    discover_params = {
        'with_original_language': TARGET_ORIGINAL_LANGUAGE,
        'with_genres': drama_genre_id,
        'sort_by': 'popularity.desc',
        'air_date.gte': START_DATE,
        'air_date.lte': END_DATE,
        'page': 1
    }
    initial_discover_response = api_client.discover_media(
        media_type="tv", 
        discover_params=discover_params,
        language=DEFAULT_LANGUAGE_PT
    )

    if not initial_discover_response or 'results' not in initial_discover_response:
        logging.error("Falha ao buscar a primeira página de descobertas. Abortando.")
        return

    total_pages_api = initial_discover_response.get('total_pages', 1)
    total_results_api = initial_discover_response.get('total_results', 0)
    logging.info(f"API reporta {total_results_api} resultados em {total_pages_api} páginas.")

    # Limitar o número de páginas a buscar (considerando o limite da API de 500)
    pages_to_fetch = min(MAX_PAGES_TO_FETCH_DISCOVER, total_pages_api, 500)
    logging.info(f"Serão buscadas até {pages_to_fetch} páginas do /discover.")

    current_kdramas_on_page = initial_discover_response.get('results', [])
    all_discovered_kdramas_info.extend(current_kdramas_on_page)
    logging.info(f"Página 1: {len(current_kdramas_on_page)} Kdramas descobertos.")

    # Loop para as páginas restantes
    for page_num in range(2, pages_to_fetch + 1):
        logging.info(f"Buscando /discover - página {page_num} de {pages_to_fetch}...")
        discover_params['page'] = page_num
        discover_response = api_client.discover_media(
            media_type="tv",
            discover_params=discover_params,
            language=DEFAULT_LANGUAGE_PT
        )
        if discover_response and 'results' in discover_response:
            current_kdramas_on_page = discover_response.get('results', [])
            all_discovered_kdramas_info.extend(current_kdramas_on_page)
            logging.info(f"Página {page_num}: {len(current_kdramas_on_page)} Kdramas descobertos.")
        else:
            logging.warning(f"Falha ao buscar ou nenhum resultado na página {page_num} do /discover.")
            # Pode-se adicionar uma lógica para parar se muitas páginas falharem
    
    logging.info(f"Total de {len(all_discovered_kdramas_info)} informações de Kdramas descobertas (antes de buscar detalhes).")

    # 3. Para cada Kdrama descoberto, buscar detalhes e créditos, e salvar
    kdramas_processed_count = 0
    for discover_info in all_discovered_kdramas_info:
        kdrama_id = discover_info.get('id')
        original_name = discover_info.get('original_name', 'NomeDesconhecido')
        
        if not kdrama_id:
            logging.warning(f"Kdrama descoberto sem ID: {discover_info.get('name')}. Pulando.")
            continue
            
        logging.info(f"Processando Kdrama ID: {kdrama_id} ({original_name})...")
        
        # Salvar a informação do /discover
        save_json_to_bronze(discover_info, str(kdrama_id), "discover_info")
        
        # Buscar e salvar detalhes da série
        details_data = api_client.get_media_details(
            media_type="tv", 
            media_id=kdrama_id, 
            language=DEFAULT_LANGUAGE_PT,
            append_to_response="keywords,watch/providers" # Opcional
        )
        if details_data:
            save_json_to_bronze(details_data, str(kdrama_id), "details")
        else:
            logging.warning(f"Não foram encontrados detalhes para o Kdrama ID: {kdrama_id}")
            
        # Buscar e salvar créditos da série
        credits_data = api_client.get_media_credits(
            media_type="tv", 
            media_id=kdrama_id
            # language=DEFAULT_LANGUAGE_PT # Para nomes de personagens, se aplicável
        )
        if credits_data:
            save_json_to_bronze(credits_data, str(kdrama_id), "credits")
        else:
            logging.warning(f"Não foram encontrados créditos para o Kdrama ID: {kdrama_id}")
        
        kdramas_processed_count += 1
        logging.info(f"Kdrama ID: {kdrama_id} ({original_name}) processado e dados salvos.")

    logging.info(f"Pipeline de ingestão da Camada Bronze finalizado. {kdramas_processed_count} Kdramas processados.")


if __name__ == '__main__':
    # Exemplo de como executar o pipeline
    # Certifique-se que seu .env está na raiz do projeto
    # e que você está executando este script da raiz do projeto ou que o sys.path está correto.
    # Exemplo de execução (da raiz do projeto kdrama_analytics_project/):
    # python src/pipelines/bronze.py
    
    run_bronze_ingestion()