from scripts.auth import auth_databricks

def upload_to_databricks(data: dict, path: str, ts_nodash, prefix):
    """Realiza o upload de um stream de dados em memória para um Volume do Databricks."""
    w = auth_databricks()
    
    # Define o nome e local de salvamento do arquivo no databricks
    file_name = f"{prefix}_{ts_nodash}.parquet"
    full_path = f"{path.rstrip('/')}/{file_name}"

    # Faz o upload
    try:
        print(f"Iniciando upload de stream de dados para '{full_path}'...")
        w.files.upload(
            file_path=full_path,
            contents=data,
            overwrite=True 
        )
        print("Upload de stream concluído com sucesso!")
        
    except Exception as e:
        print(f"Ocorreu um erro durante o upload do stream: {e}")
        raise