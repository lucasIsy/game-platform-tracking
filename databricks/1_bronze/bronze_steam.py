import dlt as dp
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, LongType

schema_steam = StructType([
    StructField("ingestion_timestamp_utc", TimestampType(), True),
    StructField("id", StringType(), True),
    StructField("jogo", StringType(), True),
    StructField("qtd_jogadores", LongType(), True)
])

@dp.table(
    name="bronze_steam",
    comment="Ingestão bruta incremental dos dados da Steam via Auto Loader",
    table_properties={"quality": "bronze"}
)
def bronze_steam():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.maxFilesPerTrigger", 1000) 
        .schema(schema_steam)
        .load("/Volumes/workspace/default/my_volume/testes/parquet/steam")
    )