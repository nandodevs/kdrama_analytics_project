import os
import requests
import time
import json
from dotenv import load_dotenv

# Carregar variáveis de ambiente do arquivo .env na raiz do projeto
# Isso assume que o script que usa este cliente está sendo executado da raiz do projeto
# ou que o .env está em um local acessível.
# Para maior robustez, o caminho para .env pode ser especificado.
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(dotenv_path=os.path.join(project_root, '.env'))

TMDB_API_KEY = os.getenv('TMDB_API_KEY')
TMDB_API_BASE_URL = "https://api.themoviedb.org/3"
DEFAULT_LANGUAGE = "pt-BR" # Pode ser configurável

# Verificação inicial da API Key
if not TMDB_API_KEY:
    raise ValueError("API Key do TMDB não encontrada. Verifique seu arquivo .env e a variável TMDB_API_KEY.")

def _make_request(endpoint_path, params, method="GET", retries=3, delay_factor=0.5):
    """
    Função auxiliar para fazer requisições à API com tratamento de erro e retentativas.
    """
    if 'api_key' not in params:
        params['api_key'] = TMDB_API_KEY
    
    url = f"{TMDB_API_BASE_URL}{endpoint_path}"
    
    for attempt in range(retries):
        try:
            response = requests.request(method, url, params=params)
            response.raise_for_status()  # Lança HTTPError para respostas 4xx/5xx
            
            # Respeitar os limites de taxa da API (mesmo em caso de sucesso)
            # O TMDB pode retornar headers de rate limit: X-RateLimit-Limit, X-RateLimit-Remaining, X-RateLimit-Reset
            # Uma abordagem simples é um delay fixo.
            time.sleep(delay_factor) # Pequena pausa após cada requisição bem-sucedida
            return response.json()
        
        except requests.exceptions.HTTPError as http_err:
            # Para erros específicos como 429 (Too Many Requests), esperar mais
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5)) # Espera o tempo indicado ou 5s
                print(f"Rate limit excedido (429). Tentando novamente em {retry_after} segundos...")
                time.sleep(retry_after)
            elif 500 <= response.status_code < 600: # Erros de servidor
                print(f"Erro de servidor ({response.status_code}). Tentando novamente em { (attempt + 1) * 2 } segundos...")
                time.sleep((attempt + 1) * 2) # Backoff exponencial simples
            else: # Outros erros HTTP (401, 404, etc.)
                print(f"Erro HTTP: {http_err} - URL: {response.url}")
                print(f"Response: {response.text}")
                # Para alguns erros como 401 (Unauthorized) ou 404 (Not Found), retentar pode não ajudar
                if response.status_code in [401, 404]:
                    return None # Ou levantar a exceção
                # Se for a última tentativa, levanta o erro
                if attempt == retries - 1:
                    raise
        except requests.exceptions.RequestException as req_err: # Outros erros (conexão, timeout)
            print(f"Erro na requisição: {req_err}")
            if attempt == retries - 1:
                raise
        
        if attempt < retries - 1:
             print(f"Tentativa {attempt + 1} de {retries} falhou. Retentando...")
    return None # Se todas as tentativas falharem


def get_genres(media_type="tv", language=DEFAULT_LANGUAGE):
    """
    Busca a lista de gêneros para um tipo de mídia (tv ou movie).
    """
    endpoint = f"/genre/{media_type}/list"
    params = {'language': language}
    return _make_request(endpoint, params)

def discover_media(media_type="tv", discover_params=None, language=DEFAULT_LANGUAGE):
    """
    Descobre mídias (tv shows ou movies) com base em filtros.
    """
    endpoint = f"/discover/{media_type}"
    default_params = {'language': language}
    
    final_params = default_params.copy()
    if discover_params:
        final_params.update(discover_params)
        
    return _make_request(endpoint, final_params)

def get_media_details(media_type="tv", media_id=None, language=DEFAULT_LANGUAGE, append_to_response=None):
    """
    Busca detalhes de uma mídia específica (tv show ou movie).
    """
    if not media_id:
        raise ValueError("media_id é obrigatório para get_media_details")
        
    endpoint = f"/{media_type}/{media_id}"
    params = {'language': language}
    if append_to_response:
        params['append_to_response'] = append_to_response
        
    return _make_request(endpoint, params)

def get_media_credits(media_type="tv", media_id=None, language=None): # Language pode ser menos relevante aqui
    """
    Busca os créditos (elenco e equipe) de uma mídia específica.
    """
    if not media_id:
        raise ValueError("media_id é obrigatório para get_media_credits")
        
    endpoint = f"/{media_type}/{media_id}/credits"
    params = {}
    if language: # Alguns nomes de personagens podem ser traduzidos
        params['language'] = language
        
    return _make_request(endpoint, params)

if __name__ == '__main__':
    # Pequeno teste para o cliente da API
    print("Testando o TMDB API Client...")
    
    # Teste 1: Buscar gêneros de TV
    print("\n--- Teste: Gêneros de TV ---")
    tv_genres_data = get_genres(media_type="tv", language="ko-KR")
    if tv_genres_data and 'genres' in tv_genres_data:
        drama_genre_info = next((g for g in tv_genres_data['genres'] if g['name'] == '드라마'), None)
        if drama_genre_info:
            print(f"ID do gênero '드라마' (Coreano): {drama_genre_info['id']}")
        else:
            print("Gênero '드라마' não encontrado em coreano.")
    else:
        print("Não foi possível buscar gêneros de TV.")

    # Teste 2: Descobrir alguns Kdramas (apenas 1 página, 3 resultados)
    print("\n--- Teste: Descobrir Kdramas (1ª página) ---")
    if drama_genre_info:
        discover_params_test = {
            'with_original_language': 'ko',
            'with_genres': drama_genre_info['id'],
            'sort_by': 'popularity.desc',
            'air_date.gte': '2024-01-01', # Exemplo: dramas de 2024
            'air_date.lte': '2024-12-31',
            'page': 1
        }
        kdramas_discovered = discover_media(media_type="tv", discover_params=discover_params_test, language="pt-BR")
        if kdramas_discovered and 'results' in kdramas_discovered:
            print(f"Encontrados {len(kdramas_discovered['results'])} Kdramas na primeira página (total: {kdramas_discovered.get('total_results')}).")
            for kdrama in kdramas_discovered['results'][:3]: # Mostrar os 3 primeiros
                print(f"  ID: {kdrama.get('id')}, Título: {kdrama.get('name')}, Original: {kdrama.get('original_name')}")
                
                # Teste 3: Detalhes e Créditos de um Kdrama específico
                if kdrama.get('id'):
                    print(f"\n--- Teste: Detalhes do Kdrama ID: {kdrama.get('id')} ---")
                    details = get_media_details(media_type="tv", media_id=kdrama.get('id'), language="pt-BR")
                    if details:
                        print(f"  Nº de Temporadas: {details.get('number_of_seasons')}, Status: {details.get('status')}")
                    
                    print(f"\n--- Teste: Créditos do Kdrama ID: {kdrama.get('id')} ---")
                    credits = get_media_credits(media_type="tv", media_id=kdrama.get('id'))
                    if credits and 'cast' in credits:
                        print(f"  Primeiro ator do elenco: {credits['cast'][0]['name'] if credits['cast'] else 'N/A'}")
                    break # Testar detalhes e créditos apenas para o primeiro descoberto
        else:
            print("Não foi possível descobrir Kdramas.")