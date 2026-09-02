# Twitch Auth Token
def get_twitch_auth_token(client_id:str, client_secret:str)-> str: 
    """Obtém um token de autenticação do Twitch"""
    import requests # <--- Lazy Import
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
    }
    response = requests.post(url, params=params)
    response.raise_for_status()
    return response.json()['access_token']

# Databricks Auth
def auth_databricks():
    """Autentica no Databricks"""
    from databricks.sdk import WorkspaceClient
    from airflow.sdk import Variable
    return WorkspaceClient(
        host=Variable.get("DATABRICKS_HOST"),
        token=Variable.get("DATABRICKS_TOKEN")
        )

