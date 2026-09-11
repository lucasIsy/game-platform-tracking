import os
import time
import requests
import json

def check_databricks_token_expiration(days_threshold=7):
    """
    Consulta a API do Databricks e dispara alerta caso o token expire em breve.
    """
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    if not host or not token:
        print("Credenciais do Databricks não encontradas.")
        return

    # Endpoint oficial para listar tokens do usuário atual
    url = f"{host.rstrip('/')}/api/2.0/token/list"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        # Se retornar 401 ou 403, o token já está inválido ou expirado!
        if response.status_code in [401, 403]:
            _send_discord_warning(webhook_url, "⛔ O token do Databricks JÁ EXPIROU ou é inválido!")
            raise ValueError("Token do Databricks expirado ou inválido.")

        response.raise_for_status()
        tokens_info = response.json().get("token_infos", [])

        now_ms = int(time.time() * 1000)
        warning_ms = days_threshold * 24 * 60 * 60 * 1000

        for t in tokens_info:
            expiry_time = t.get("expiry_time", -1)
            token_comment = t.get("comment", "Sem descrição")

            # expiry_time == -1 significa token sem data de expiração
            if expiry_time != -1:
                ms_remaining = expiry_time - now_ms
                days_remaining = int(ms_remaining / (1000 * 60 * 60 * 24))

                if ms_remaining < warning_ms:
                    mensagem = (
                        f"⚠️ **Atenção: Token do Databricks prestes a expirar!**\n"
                        f"• **Descrição:** {token_comment}\n"
                        f"• **Dias restantes:** {days_remaining} dia(s)\n"
                        f"• Ação necessária: Renove o Personal Access Token no Databricks."
                    )
                    _send_discord_warning(webhook_url, mensagem)
                    print(f"Alerta emitido: restam {days_remaining} dias.")
                    return

        print("Token do Databricks válido e dentro do prazo seguro.")

    except Exception as e:
        print(f"Erro ao verificar token do Databricks: {e}")
        raise e

def _send_discord_warning(webhook_url, message):
    if not webhook_url:
        return
    payload = {
        "embeds": [{
            "title": "🔐 Manutenção de Credenciais (Databricks)",
            "description": message,
            "color": 16753920 # Amarelo/Laranja de aviso
        }]
    }
    requests.post(webhook_url, data=json.dumps(payload), headers={"Content-Type": "application/json"}, timeout=10)