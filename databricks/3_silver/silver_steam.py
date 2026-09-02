import dlt as dp

dp.create_streaming_table(
    name="silver_steam",
    comment="Tabela deduplicada da Steam",
    table_properties={"quality": "silver"}
)

dp.apply_changes(
    target="silver_steam",
    source="stg_steam",
    keys=["ingested_at_utc", "steam_game_id"],  
    sequence_by="ingested_at_utc",              
    stored_as_scd_type="1"                      
)