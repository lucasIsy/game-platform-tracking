# =============================================================================
# --- IMPORTS AIRFLOW ---
# =============================================================================
from airflow.sdk import dag, task, Variable
from datetime import datetime, timedelta
import json

# =============================================================================
# --- MODULOS da pasta src ---
# =============================================================================
from scripts.auth import get_twitch_auth_token
from scripts.extractors import get_twitch_streams
from scripts.extractors import get_steam_metrics
from scripts.transforms import steam_data_to_stream_format
from scripts.transforms import twitch_data_to_stream_format
from scripts.loaders import upload_to_databricks

# =============================================================================
# --- ARGUMENTOS PADRÃO DO DAG ---
# =============================================================================

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# =============================================================================
# --- DEFINIÇÃO DO DAG ---
# =============================================================================

@dag(
    dag_id='Pipeline_twitch_steam',
    default_args=default_args,
    description='Extrai (Twitch/Steam) e Carrega (Databricks)',
    schedule='*/5 * * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['databricks', 'twitch', 'steam', 'elt_project'],
)
def twitch_steam_pipeline():
    """
    Este DAG define o pipeline completo:
    1. Ingestão dos dados da Twitch e Steam via API.
    2. Converte os dados para formato stream e faz o carregamento para o Databricks.
    """
    # --- Task 1: Extrai os dados da twitch e faz o upload para o databricks ---
    @task
    def get_twitch_data(ingestion_time:str, ts_nodash):
        "Extrai os dados da API da Twitch e faz o upload para o databricks"
        
        # Leitura do arquivo JSON mapeado no container
        with open('/opt/airflow/jogos-procurados/jogos.json', 'r', encoding='utf-8') as f:
            dados_config = json.load(f)
        secret_twitch_game_list = dados_config.get("twitch", [])
        
        # Autenticação para a dados_twitch
        secret_twitch_id = Variable.get("TWITCH_CLIENT_ID")
        secret_twitch_client= Variable.get("TWITCH_CLIENT_SECRET")
        secret_twitch_token = get_twitch_auth_token(secret_twitch_id,secret_twitch_client)

        # Extrai e armazena os resultados da extração
        dados_twitch = twitch_data_to_stream_format(
            get_twitch_streams(
                ingestion_time,
                secret_twitch_id,
                secret_twitch_token,
                secret_twitch_game_list
            )
        )
        
        # Coloca o horario da extração no nome dos arquivos e define o local de salavamento no databricks
        path_data_twitch = "/Volumes/workspace/default/my_volume/raw/twitch"
        upload_to_databricks(
            dados_twitch, 
            path_data_twitch, 
            ts_nodash, 
            "twitch"
        )

    # --- Task 2: Extrai os dados da steam e faz o upload para o databricks ---  
    @task
    def get_steam_data(ingestion_time:str, ts_nodash):
        
        # Leitura do arquivo JSON mapeado no container
        with open('/opt/airflow/jogos-procurados/jogos.json', 'r', encoding='utf-8') as f:
            dados_config = json.load(f)
        secret_steam_game_list = dados_config.get("steam", {})

        # Autenticação para metricas_steam
        secret_steam_auth = Variable.get("STEAM_API_KEY")
        
        # Extrai e armazena os resultados da extração 
        metricas_steam = steam_data_to_stream_format(
            get_steam_metrics(
                ingestion_time,
                secret_steam_auth,
                secret_steam_game_list
            ) 
        )

        # Coloca o horario da extração no nome dos arquivos e define o local de salavamento no databricks
        path_data_steam = "/Volumes/workspace/default/my_volume/raw/steam"
        upload_to_databricks(
            metricas_steam,
            path_data_steam,
            ts_nodash,
            "steam"
        )

    # Chama as tasks
    # Garantir que os resultados da steam/twitch tenham o mesmo tempo de ingestão para futuros joins
    get_twitch_data(
        ingestion_time="{{ logical_date.int_timestamp }}",
        ts_nodash="{{ ts_nodash }}"
    )
    get_steam_data(
        ingestion_time="{{ logical_date.int_timestamp }}",
        ts_nodash="{{ ts_nodash }}"
    )

# --- CHAMADA DO DAG ---
twitch_steam_pipeline()