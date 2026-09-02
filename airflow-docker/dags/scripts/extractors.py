import logging
import requests
logger = logging.getLogger(__name__)

def get_twitch_streams(ingestion_time: str, twitch_id: str, twitch_oauth: str, twitch_game_list: list):
    """Busca as streams ao vivo para uma lista de IDs de jogos."""

    url = "https://api.twitch.tv/helix/streams"
    headers = {
        'Client-ID': twitch_id, 
        'Authorization': f'Bearer {twitch_oauth}'
    }

    # Armazena as streams e o ponteiro da paginação
    all_streams = []
    cursor = None

    # Loop que extrai os dados juntamente com paginação
    logger.info(f"Iniciando extração para {len(twitch_game_list)} jogos.")
    
    while True:
        params = {
            'game_id': twitch_game_list,
            'first': 100,
            'after': cursor
        }
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data_json = response.json()
        
        # Verifica a existência de dados dentro do json / interrompe o loop e retorna erro
        if 'data' not in data_json:
            error_msg = f"API da Twitch retornou arquivo sem dados:{data_json}"
            logging.error(error_msg)
            raise ValueError(error_msg)
            
        all_streams.extend(data_json.get('data', []))
        cursor = data_json.get('pagination', {}).get('cursor')
        if not cursor:
            break
    
    dados_twitch = {
        "ingestion_timestamp_utc": ingestion_time, 
        "twitch_data": all_streams
    }

    return dados_twitch

def get_steam_metrics(ingestion_time: str, secret_steam_auth: str, secret_steam_game_list: dict):
    """
    Consome a API da Steam para uma lista de jogos e retorna um dicionário formatado.
    """
    url = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"
    dados_acumulados = []

    logger.info(f"Iniciando extração Steam para {len(secret_steam_game_list)} jogos.")

    for app_id, nome_jogo in secret_steam_game_list.items():
        params = {
            'key': secret_steam_auth,
            'appid': app_id,
            'format': 'json'
        }
        
        timestamp_iso = ingestion_time
        player_count = None

        try:
            # Adicionado timeout de 10s para não travar o worker do Airflow
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            # Verificação segura do retorno da Steam
            if data.get('response', {}).get('result') == 1:
                player_count = data['response'].get('player_count')
            else:
                logger.warning(f"Jogo {nome_jogo} (ID: {app_id}) não retornou resultado válido.")

        except Exception as e:
            logger.error(f"Falha na extração do jogo {nome_jogo}: {str(e)}")
            # O player_count continua None, mas o pipeline não para

        dados_acumulados.append({
            "ingestion_timestamp_utc": timestamp_iso, 
            "id": app_id,
            "jogo": nome_jogo,
            "qtd_jogadores": player_count
        })

    return dados_acumulados