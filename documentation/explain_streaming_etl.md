# Documentación: Procesamiento en Streaming (`streaming_etl.py`)

Este script es el núcleo del procesamiento en tiempo real. Está corriendo continuamente durante todo el partido: cada 10 segundos coge todos los eventos que han llegado de Kafka hasta ese momento, calcula las métricas acumuladas de cada jugador y manda los resultados a dos sitios: Elasticsearch (para los dashboards en vivo) y HDFS (para guardar el histórico). Utiliza **PySpark Structured Streaming**, que es la forma que tiene Spark de procesar datos que llegan en tiempo real.

---

## Configuración inicial

```python
KAFKA_SERVERS = "kafka-broker-1:9092"
KAFKA_TOPIC = "football_events"

ES_HOST = "http://elasticsearch:9200"
ES_INDEX = "football_metrics"

HDFS_OUTPUT = "hdfs://namenode:9000/data/processed/streaming_metrics"
CHECKPOINT_DIR = "hdfs://namenode:9000/checkpoints/streaming_etl"
```

- **`KAFKA_SERVERS` y `KAFKA_TOPIC`**: dirección del broker de Kafka y nombre del canal del que leer. Tienen que coincidir exactamente con lo que usa el generador para publicar los eventos.
- **`ES_HOST` y `ES_INDEX`**: dirección de Elasticsearch dentro de Docker y nombre del índice donde se van a guardar las métricas de los jugadores.
- **`HDFS_OUTPUT`**: carpeta en HDFS donde se guardarán los resultados en formato Parquet como histórico permanente.
- **`CHECKPOINT_DIR`**: carpeta especial que Spark usa internamente para saber hasta qué punto ha procesado los mensajes de Kafka. Si el proceso se cae y se reinicia, Spark mira aquí para continuar desde donde lo dejó sin perder ni repetir datos.

---

## El esquema (`SCHEMA`)

```python
SCHEMA = StructType([
    StructField("match_id", StringType(),  True),
    StructField("timestamp", DoubleType(),  True),
    StructField("tipo", StringType(),  True),
    StructField("player_id", StringType(),  True),
    StructField("player_nombre", StringType(),  True),
    StructField("player_equipo", StringType(),  True),
    StructField("player_posicion", StringType(),  True),
    StructField("x", DoubleType(),  True),
    StructField("y", DoubleType(),  True),
    StructField("speed_jugador", DoubleType(),  True),
    StructField("estamina", DoubleType(),  True),
    StructField("ball_x", DoubleType(),  True),
    StructField("ball_y", DoubleType(),  True),
    StructField("ball_speed", DoubleType(),  True),
    StructField("action", StringType(),  True),
    StructField("is_successful", BooleanType(), True),
])
```

Los mensajes que llegan de Kafka son texto plano en formato JSON. Spark no sabe qué contiene ese texto ni qué tipo de dato es cada campo hasta que se lo decimos explícitamente con este esquema. Cada `StructField` define el nombre del campo, su tipo de dato y si puede ser nulo. Sin este esquema, Spark no puede convertir el JSON en una tabla con columnas sobre las que hacer cálculos.

---

## `send_to_elasticsearch(records)`

Esta función recibe una lista de diccionarios (uno por jugador) y los manda todos a Elasticsearch de golpe usando la **Bulk API**.

La Bulk API de Elasticsearch permite enviar muchos documentos en una sola petición HTTP en lugar de hacer una petición por cada jugador. Con 22 jugadores por partido eso ya sería 22 peticiones cada 10 segundos, lo que generaría una carga innecesaria en la red y en Elasticsearch.

El formato que exige la Bulk API se llama **NDJSON** (JSON separado por saltos de línea): cada documento requiere dos líneas consecutivas, una de metadatos y una de datos:

```python
doc_id = f"{rec.get('match_id','X')}_{rec.get('player_id','0')}"
meta = json.dumps({"index": {"_index": ES_INDEX, "_id": doc_id}})
body = json.dumps(rec, ensure_ascii=False, default=str)
bulk_body += meta + "\n" + body + "\n"
```

- La línea de metadatos (`meta`) le dice a Elasticsearch qué operación hacer (`index` significa "guarda o actualiza este documento"), en qué índice guardarlo y con qué ID.
- El `doc_id` se construye combinando el `match_id` y el `player_id`. Esto es importante, ya que al usar siempre el mismo ID para el mismo jugador dentro del mismo partido, cada escritura **sobreescribe** el documento anterior en lugar de crear uno nuevo. El resultado es que en Elasticsearch siempre hay exactamente un documento por jugador con sus métricas acumuladas al momento, que es exactamente lo que necesitan los dashboards para mostrar el estado actual.
- `default=str` en el `json.dumps` es una red de seguridad: si algún campo tiene un tipo de dato que JSON no sabe serializar (como un `datetime`), lo convierte a texto en lugar de dar error.

La petición se envía con un `timeout=10` segundos. Si Elasticsearch tarda más de eso en responder, se considera un error y se imprime un aviso en lugar de dejar el proceso bloqueado indefinidamente esperando respuesta.

---

## `write_batch(df, epoch_id)`

Esta función es la que Spark llama automáticamente cada 10 segundos con los datos procesados. Recibe dos elementos, el DataFrame con las métricas de todos los jugadores calculadas hasta ese momento (`df`) y un número de identificación del batch (`epoch_id`) que empieza en 0 y va subiendo en cada llamada.

Lo primero que hace es comprobar si el DataFrame está vacío:

```python
if df.rdd.isEmpty():
    print(f"[WARN] Batch {epoch_id} vacío.")
    return
```

Si en los últimos 10 segundos no ha llegado ningún evento de Kafka (porque el generador está parado o hay un problema de red), no tiene sentido intentar escribir nada ni llamar a Elasticsearch o HDFS con una petición vacía.

Si hay datos, hace dos cosas en orden:

**Envío a Elasticsearch:**

```python
rows = df.collect()
records = [r.asDict(recursive=True) for r in rows]
ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
for rec in records:
    rec["timestamp"] = ts
send_to_elasticsearch(records)
```

`df.collect()` trae todas las filas del DataFrame a la memoria del driver de Spark como una lista de objetos `Row`. Esto es necesario porque `send_to_elasticsearch` es código Python normal que no entiende de DataFrames de Spark. Luego `asDict(recursive=True)` convierte cada `Row` en un diccionario de Python normal que sí se puede serializar a JSON.

Antes de mandar los datos se añade un `timestamp` con la hora actual real (no el tiempo simulado del partido) en formato (`2024-01-15T10:30:00Z`). Grafana y Kibana necesitan un timestamp en este formato para poder ordenar los documentos en el tiempo y construir series temporales en los gráficos. Sin él, los dashboards no sabrían en qué momento se registró cada estado del jugador.

**Escritura en HDFS:**

```python
df.write.mode("append").parquet(f"{HDFS_OUTPUT}/batch_{epoch_id}")
```

Cada batch se guarda como una carpeta separada dentro de `streaming_metrics` con el nombre `batch_0`, `batch_1`, `batch_2`... El modo `append` significa que nunca borra lo anterior, solo añade. Esto construye un histórico completo de cómo han ido evolucionando las métricas a lo largo del partido, útil para análisis posteriores fuera del tiempo real.

Tanto el bloque de Elasticsearch como el de HDFS tienen su propio `try/except` independiente. Esto es importante ya que si Elasticsearch falla, el script sigue intentando guardar en HDFS y viceversa. Si ambos bloques compartieran un solo `try`, un fallo en Elasticsearch impediría también escribir en HDFS.

---

## `main()`

La función principal define el flujo completo del streaming en cuatro pasos.

**Paso 1 - Leer de Kafka**

```python
raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()
```

`readStream` en lugar del `read` normal le dice a Spark que esto es un stream: los datos no tienen fin, van llegando continuamente. La opción `startingOffsets: latest` hace que Spark ignore todos los mensajes que ya había en Kafka antes de arrancar este script y empiece a leer solo los nuevos que lleguen a partir de ahora. Esto evita que al arrancar el ETL se procesen de golpe todos los eventos de un partido que ya lleva tiempo corriendo.

**Paso 2 - Parsear el JSON**

```python
parsed = raw.selectExpr("CAST(value AS STRING) as js") \
    .select(from_json(col("js"), SCHEMA).alias("d")).select("d.*")
```

Los mensajes de Kafka llegan como bytes crudos. La primera parte (`CAST(value AS STRING)`) los convierte a texto. La segunda (`from_json`) usa el `SCHEMA` definido antes para convertir ese texto JSON en columnas reales de un DataFrame. A partir de aquí, ya se puede trabajar con los datos como si fueran una tabla normal con columnas `player_id`, `x`, `estamina`, etc.

**Paso 3 - Transformar y agregar**

Primero se clasifica a cada jugador en una zona del campo:

```python
with_zone = parsed.withColumn(
    "zona",
    when(col("x") <= 35, "defensa")
    .when(col("x") <= 70, "medio")
    .otherwise("ataque")
)
```

El campo mide 105 metros de largo. Se divide en tres tercios: los primeros 35 metros son zona de defensa, del 35 al 70 zona de medio, y del 70 en adelante zona de ataque. Esto añade una columna `zona` a cada evento que luego se usa para contar en qué parte del campo ha estado más activo cada jugador.

Luego se agrupan todos los eventos por jugador y partido y se calculan las métricas acumuladas:

```python
metrics = with_zone.groupBy(
    "match_id", "player_id", "player_nombre", "player_equipo", "player_posicion"
).agg(
    spark_round(_sum("speed_jugador") * 2, 1).alias("distancia_m"),
    spark_round(last("estamina", ignorenulls=True), 1).alias("estamina_actual"),
    spark_round(last("x", ignorenulls=True), 2).alias("ultima_x"),
    spark_round(last("y", ignorenulls=True), 2).alias("ultima_y"),
    count(when(col("action") == "pase", True)).alias("pases_intentados"),
    count(when((col("action") == "pase") & (col("is_successful") == True), True)).alias("pases_acertados"),
    count(when(col("action") == "tiro", True)).alias("tiros"),
    count(when(col("action") == "gol", True)).alias("goles"),
    count(when(col("action") == "parada", True)).alias("paradas"),
    count(when(col("action") == "perdida", True)).alias("perdidas"),
    count(when(col("action") == "recuperacion", True)).alias("recuperaciones"),
    count(when(col("zona") == "defensa", True)).alias("eventos_zona_defensa"),
    count(when(col("zona") == "medio", True)).alias("eventos_zona_medio"),
    count(when(col("zona") == "ataque", True)).alias("eventos_zona_ataque"),
)
```

Vale la pena detenerse en las métricas menos obvias:

- **`distancia_m`**: suma todas las velocidades del jugador a lo largo del partido y multiplica por 2 (el `INTERVALO`). Como la velocidad está en m/s y cada evento representa 2 segundos, `velocidad × 2 = metros recorridos en ese ciclo`. Sumar todos los ciclos da la distancia total aproximada. Es una estimación, no una medición exacta, pero es suficiente para comparar jugadores entre sí.

- **`last("estamina", ignorenulls=True)`**: en lugar de sumar o promediar la estamina, interesa saber cuál es el valor más reciente, porque ese es el estado actual del jugador. El `ignorenulls=True` hace que si el último evento de un jugador tiene la estamina a nulo (lo que puede pasar en ciertos eventos de acción), Spark retroceda hasta el último valor no nulo en lugar de devolver nulo directamente.

- **`count(when(...))`**: la combinación de `count` y `when` es la forma que tiene Spark de hacer un conteo condicional. `when(col("action") == "pase", True)` devuelve `True` cuando la condición se cumple y `null` cuando no. `count` ignora los nulos, así que en la práctica solo cuenta las filas donde la condición era verdadera.

Por último se calculan las eficiencias como porcentaje:

```python
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
```

El `when(...).otherwise(0.0)` es la protección contra la división por cero, si un jugador no ha intentado ningún pase todavía, `pases_intentados` vale 0 y dividir entre 0 daría error. En ese caso se devuelve directamente 0.0 en lugar de intentar el cálculo.

**Paso 4 - Lanzar el stream**

```python
query = final.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
```

- **`outputMode("complete")`**: en cada ciclo de 10 segundos, Spark emite el resultado completo de todas las agregaciones desde el inicio del partido hasta ahora, no solo lo que ha llegado en los últimos 10 segundos. Esto es necesario porque las métricas son acumulativas (el total de pases de un jugador incluye todos los pases desde que arrancó el partido, no solo los del último batch).
- **`foreachBatch(write_batch)`**: le dice a Spark que en lugar de usar un sink estándar (como escribir directamente en un fichero), llame a la función `write_batch` con cada batch procesado. Es la forma de usar lógica personalizada de escritura en Spark Streaming.
- **`trigger(processingTime="10 seconds")`**: controla cada cuánto se procesa un batch. Sin esto, Spark procesaría en continuo tan rápido como pudiera, lo que saturaria Elasticsearch con demasiadas actualizaciones. Con 10 segundos se consigue un equilibrio.
- **`awaitTermination()`**: bloquea el script en este punto para que no termine solo. El stream necesita seguir corriendo indefinidamente, y esta línea es la que lo mantiene vivo hasta que alguien lo para manualmente.

Volver atrás: [Introducción](/Proyecto.md)
