from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, sum as _sum, count, when, last, round as spark_round,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
)
import json, requests

# CONFIGURACIÓN
KAFKA_SERVERS = "kafka-broker-1:9092"
KAFKA_TOPIC = "football_events"

ES_HOST = "http://elasticsearch:9200" # dentro de Docker
ES_INDEX = "football_metrics"

HDFS_OUTPUT = "hdfs://namenode:9000/data/processed/streaming_metrics"
CHECKPOINT_DIR = "hdfs://namenode:9000/checkpoints/streaming_etl"


# ESQUEMA JSON CON LOS EVENTOS DE FÚTBOL
SCHEMA = StructType([
    StructField("match_id", StringType(), True), # ID único para cada partido que permite filtrar por el partido actual
    StructField("timestamp", DoubleType(), True), # Esto mide el tiempo del evento en segundos desde el inicio del partido
    StructField("tipo", StringType(), True), # "posicional" o "evento"
    StructField("player_id", StringType(), True),
    StructField("player_nombre", StringType(), True),
    StructField("player_equipo", StringType(), True), # Nombre del club
    StructField("player_posicion", StringType(), True), # Posición del jugador en el campo (ST, CB, GK, …)
    StructField("x", DoubleType(), True), # Posición X (0-100)
    StructField("y", DoubleType(), True), # Posición Y (0-100)
    StructField("speed_jugador", DoubleType(), True), # Velocidad instantánea del jugador en m/s (calculada por el generador a partir de la posición y el tiempo)
    StructField("estamina", DoubleType(), True), # Estamina del jugador (0-100), que se va reduciendo con el tiempo y la actividad física
    StructField("ball_x", DoubleType(), True), # Posición X de la pelota (0-100)
    StructField("ball_y", DoubleType(), True), # Posición Y de la pelota (0-100)
    StructField("ball_speed", DoubleType(), True), # Velocidad de la pelota en m/s
    StructField("action", StringType(), True), # Tipo de acción (pase, tiro, gol, parada, pérdida, recuperación, etc.)
    StructField("is_successful", BooleanType(), True), # Si la acción fue exitosa (pase acertado, gol marcado, etc.)
])


# Escritura en Elasticsearch usando Bulk API enviando una lista de dicts (cada dict es un documento) para cada batch. Esto es más eficiente que enviar un documento por petición.
def send_to_elasticsearch(records):
    if not records: # Si no hay registros, no hacemos nada (evita llamadas innecesarias a ES)
        return
    bulk_body = "" # Esto es un string con formato NDJSON: cada línea es un JSON.
    for rec in records: # Para cada registro, creamos una línea de acción y una línea de datos
        doc_id = f"{rec.get('match_id','X')}_{rec.get('player_id','0')}" # ID único por jugador y partido
        meta = json.dumps({"index": {"_index": ES_INDEX, "_id": doc_id}}) # Línea de acción para Bulk API
        body = json.dumps(rec, ensure_ascii=False, default=str) # Línea de datos con el registro convertido a JSON
        bulk_body += meta + "\n" + body + "\n" # Concatenamos ambas líneas al cuerpo del bulk request
    try: # Enviamos el bulk request a Elasticsearch
        r = requests.post(
            f"{ES_HOST}/_bulk",
            data=bulk_body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=10,
        )
        if r.status_code not in (200, 201): # Si la respuesta no es exitosa, imprimimos una advertencia con el status y parte del mensaje de error
            print(f"[WARN] Elasticsearch bulk status {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"[ERROR] Enviando a Elasticsearch: {e}")


# Función encargada de escribir cada batch tanto en Elasticsearch como en HDFS. Recibe un DataFrame con las métricas agregadas por jugador y partido, y el ID del batch (epoch_id) que corresponde al número del batch procesado.
def write_batch(df, epoch_id):
    if df.rdd.isEmpty(): # Si el batch está vacío, no hacemos nada (evita llamadas innecesarias a ES y HDFS)
        print(f"[WARN] Batch {epoch_id} vacío.")
        return

    # Esto sirve para depurar el contenido del batch
    try:
        rows = df.collect() # Recogemos las filas del DataFrame como una lista de Row objects
        records = [r.asDict(recursive=True) for r in rows] # Convertimos cada fila a un diccionario (esto es necesario para enviarlo a Elasticsearch)
        send_to_elasticsearch(records) # Enviamos la lista de registros a Elasticsearch usando la función definida anteriormente
        print(f"[OK] Batch {epoch_id} → Elasticsearch ({len(records)} docs)") 
    except Exception as e:
        print(f"[ERROR] Elasticsearch batch {epoch_id}: {e}")

    # HDFS Parquet para almacenar un histórico de métricas por partido (cada batch corresponde a un partido diferente gracias al match_id único)
    try:
        df.write.mode("append").parquet(f"{HDFS_OUTPUT}/batch_{epoch_id}")
        print(f"[OK] Batch {epoch_id} → HDFS Parquet")
    except Exception as e:
        print(f"[ERROR] HDFS batch {epoch_id}: {e}")


# MAIN
def main():
    spark = SparkSession.builder \
        .appName("Football_Streaming_ETL") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Leer stream de Kafka
    print(f"[INFO] Leyendo de Kafka: {KAFKA_SERVERS}  topic: {KAFKA_TOPIC}")
    raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parsear el JSON del mensaje Kafka con el esquema y extraer sus campos.
    parsed = raw.selectExpr("CAST(value AS STRING) as js") \
        .select(from_json(col("js"), SCHEMA).alias("d")).select("d.*")

    # Zona del campo (tercios del eje X)
    with_zone = parsed.withColumn(
        "zona",
        when(col("x") <= 35, "defensa")
        .when(col("x") <= 70, "medio")
        .otherwise("ataque")
    )

    # Agregación: una fila por jugador POR partido
    metrics = with_zone.groupBy(
        "match_id", "player_id", "player_nombre", "player_equipo", "player_posicion"
    ).agg(
        spark_round(_sum("speed_jugador") * 2, 1).alias("distancia_m"), # La velocidad se da en m/s y el timestamp en segundos, así que multiplicamos por 2 para estimar la distancia recorrida entre eventos (esto es una simplificación)
        spark_round(last("estamina", ignorenulls=True), 1).alias("estamina_actual"), # Tomamos la última estamina del jugador (ignorar nulos para no perder el valor si hay eventos posicionales sin acción)
        spark_round(last("x", ignorenulls=True), 2).alias("ultima_x"), # Tomamos la última posición X e Y del jugador (ignorar nulos para no perder el valor si hay eventos posicionales sin acción)
        spark_round(last("y", ignorenulls=True), 2).alias("ultima_y"), # Esto nos da una idea de dónde estaba el jugador en el último evento registrado, aunque no es perfecto porque puede haber eventos posicionales sin acción.

        count(when(col("action") == "pase", True)).alias("pases_intentados"), # Contamos los pases intentados (cualquier evento con acción "pase")
        # Contamos los pases acertados (acción "pase" y is_successful = True)
        count(when((col("action") == "pase") &
                   (col("is_successful") == True), True)).alias("pases_acertados"),

        count(when(col("action") == "tiro", True)).alias("tiros"), # Contamos los tiros intentados (cualquier evento con acción "tiro")
        count(when(col("action") == "gol", True)).alias("goles"), # Contamos los goles marcados (cualquier evento con acción "gol")
        count(when(col("action") == "parada", True)).alias("paradas"), # Contamos las paradas realizadas (cualquier evento con acción "parada")
        count(when(col("action") == "perdida", True)).alias("perdidas"), # Contamos las pérdidas de balón (cualquier evento con acción "perdida")
        count(when(col("action") == "recuperacion", True)).alias("recuperaciones"), # Contamos las recuperaciones de balón (cualquier evento con acción "recuperacion")

        # Contamos los eventos que ocurrieron en cada zona del campo (defensa, medio, ataque) para tener una idea de dónde estuvo más activo el jugador. Esto se hace con la columna "zona" que creamos anteriormente a partir de la posición X.
        count(when(col("zona") == "defensa", True)).alias("eventos_zona_defensa"), # Contamos los eventos que ocurrieron en cada zona de defensa
        count(when(col("zona") == "medio", True)).alias("eventos_zona_medio"), # Contamos los eventos que ocurrieron en cada zona de medio
        count(when(col("zona") == "ataque",  True)).alias("eventos_zona_ataque"), # Contamos los eventos que ocurrieron en cada zona de ataque
    )

    # Calculamos la eficiencia de pase y tiro como porcentaje. Usamos when para evitar divisiones por cero y spark_round para redondear a 1 decimal.
    final = metrics \
        .withColumn("eficiencia_pase",
            spark_round(
                when(col("pases_intentados") > 0,
                     col("pases_acertados") / col("pases_intentados") * 100
                ).otherwise(0.0), 1)) \
        .withColumn("eficiencia_tiro",
            spark_round(
                when(col("tiros") > 0,
                     col("goles") / col("tiros") * 100
                ).otherwise(0.0), 1))

    # Lanzar stream
    print("[INFO] Stream activo. Escribiendo en Elasticsearch + HDFS cada 10 s...")
    query = final.writeStream \
        .outputMode("complete") \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()