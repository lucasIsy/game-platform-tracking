import os
import json
import requests

def send_discord_alert(context):
    """
    Função chamada pelo callback on_failure_callback do Airflow.
    O Airflow injeta automaticamente o dicionário 'context'.
    """
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("DISCORD_WEBHOOK_URL não configurada. Alerta ignorado.")
        return

    # Extração dos metadados da falha a partir do context
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    exception = context.get('exception')

    payload = {
        "embeds": [{
            "title": f"🚨 Falha no Pipeline: {dag_id}",
            "color": 15158332,  # Vermelho
            "fields": [
                {"name": "Task", "value": task_id, "inline": True},
                {"name": "Execução", "value": str(execution_date), "inline": True},
                {"name": "Erro", "value": f"```{str(exception)[:300]}```", "inline": False},
                {"name": "Logs", "value": f"[Acessar Logs]({log_url})", "inline": False}
            ]
        }]
    }

    try:
        response = requests.post(
            webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10
        )
        response.raise_for_status()
    except Exception as e:
        print(f"Erro ao enviar webhook: {e}")