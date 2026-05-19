# Guía de Ejecución: Análisis de Rendimiento en Fútbol en Tiempo Real

Esta guía explica paso a paso cómo arrancar toda la infraestructura, cargar los datos históricos en HDFS, lanzar la simulación del partido en streaming y visualizar los resultados en Kibana y Grafana.

> **Prerequisito**: Docker Desktop instalado y corriendo.  
> Todos los comandos se ejecutan desde la **carpeta raíz del proyecto** (donde está el `docker-compose.yaml`).

---

## PASO 1 - Levantar la infraestructura completa

El `docker-compose.yaml` incluye  **Elasticsearch y Kibana** como tecnologías de investigación propia, además de todo el stack principal.

```bash
docker compose up -d
```

Esperamos unos 90 segundos (Elasticsearch tarda algo más en arrancar) y verificamos el estado:

```bash
docker compose ps
```

Todos los contenedores deben aparecer en estado `running` (es posible que el kafka exporter haya que levantarlo a mano).  
Comprueba que Elasticsearch responde:

```bash
curl http://localhost:9200
```

Debes ver un JSON con `"cluster_name"` y `"status": "green"` o `"yellow"`.

---

## PASO 2 - Preparar el índice de Elasticsearch para el partido

Antes de cada partido nuevo, **borra el índice anterior** para que solo se muestren datos del partido actual:

```bash
curl -X DELETE http://localhost:9200/football_metrics
```

O también (Es el mismo comando):

```bash
Invoke-RestMethod -Uri "http://localhost:9200/football_metrics" -Method DELETE
```

> Si es la primera vez, el índice no existe todavía y devolverá un error 404 inofensivo. El ETL lo crea automáticamente.

---

## PASO 3 - Cargar los datos históricos en HDFS (ETL Batch)

### 3.1 - Copiar los CSV al NameNode

```bash
docker cp ./data/Men_Players.csv namenode:/tmp/Men_Players.csv
docker cp ./data/Women_Players.csv namenode:/tmp/Women_Players.csv
```

### 3.2 - Subir al sistema de ficheros HDFS

```bash
docker exec namenode hdfs dfs -mkdir -p /data
docker exec namenode hdfs dfs -put -f /tmp/Men_Players.csv /data/
docker exec namenode hdfs dfs -put -f /tmp/Women_Players.csv /data/
```

Verifica que los ficheros están en HDFS:

```bash
docker exec namenode hdfs dfs -ls /data
```

##### Resultado:
![Ejemplo 1](/img/img1.png "Salida de Compass: R1")

### 3.3 - Copiar los scripts ETL al contenedor Spark

```bash
docker cp ./ETL spark-master:/opt/spark/ETL
```

### 3.4 - Ejecutar el ETL Batch

Lee los dos CSV de HDFS, limpia columnas innecesarias (URLs, playstyles…), une masculino y femenino, añade `estamina=100` y guarda en formato **Parquet** optimizado.

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/ETL/batch_etl.py
```

Al terminar verás `[OK] Batch ETL completado con éxito.`  
Verifica el resultado:

```bash
docker exec namenode hdfs dfs -ls /data/processed/players_parquet
```

##### Resultado:
![Ejemplo 2](/img/img2.png "Salida de Compass: R1")

---

## PASO 4 - Crear el topic de Kafka

```bash
docker exec kafka-client /opt/kafka/bin/kafka-topics.sh --create --topic football_events --bootstrap-server kafka-broker-1:9092 --partitions 3 --replication-factor 3
```

Verifica el topic:

```bash
docker exec kafka-client /opt/kafka/bin/kafka-topics.sh --describe --topic football_events --bootstrap-server kafka-broker-1:9092
```

##### Resultado:
![Ejemplo 3](/img/img3.png "Salida de Compass: R1")

---

## PASO 5 - Lanzar el simulador de partido (Streaming Generator)

El generador simula un partido completo en tiempo real:
- Selecciona jugadores del CSV y forma **2 equipos con formación 4-4-2** (1 GK + 4 DEF + 4 MID + 2 DEL por equipo).
- Cada ejecución genera un **`match_id` único** (UUID): los datos del partido anterior nunca se mezclan con los del nuevo.
- Simula posiciones de jugadores y del balón, y genera eventos: `pase`, `perdida`, `recuperacion`, `tiro`, `gol`, `parada`.
- Envía los eventos en JSON a Kafka cada 2 segundos.

### 5.1 - Instalar dependencias Python (solo la primera vez)

```bash
pip install kafka-python
```

### 5.2 - Iniciar el simulador

**Abre una terminal nueva** y déjala corriendo durante toda la demo:

```bash
python ETL/streaming_generator.py
```

Verás mensajes como:
```
[OK] Conectado a Kafka en localhost:9094  |  match_id=3f2a1c...
[INFO] Plantillas  A: 11 jugadores (1 GK)  |  B: 11 jugadores (1 GK)
[INFO] Partido iniciado. Enviando a Kafka topic "football_events" cada 2s
  [t= 2s] balón=(63.4,28.1) posesión=A
  [t= 4s] balón=(71.2,35.6) posesión=A
```

Para **iniciar un nuevo partido**: para el generador con `Ctrl+C` y vuélvelo a lanzar. El nuevo `match_id` aislará automáticamente los datos nuevos.

---

## PASO 6 - Lanzar el ETL Streaming (PySpark Structured Streaming)

**Abre otra terminal nueva.** Este job:
- Lee el stream de Kafka en tiempo real.
- Agrupa métricas por jugador y partido: distancia, estamina, pases, tiros, goles, paradas, pérdidas, recuperaciones, zonas del campo, eficiencias.
- Escribe en **Elasticsearch** cada 10 segundos (para Kibana/Grafana).
- Escribe en **HDFS Parquet** cada batch (histórico).

```bash
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 /opt/spark/ETL/streaming_etl.py
```

En caso de dar error por no existir "requests", usar este comando y reejecutar el anterior.
(Hacemos uso de requests para mandar a Elasticsearch los datos procesados por medio del método request.post()).

```bash
docker exec -u 0 spark-master pip install requests
```


> La primera ejecución descarga las dependencias de Maven (~1-2 min). Es normal.

Verás mensajes cada 10 segundos:
```
[OK] Batch 0 → Elasticsearch (22 docs)
[OK] Batch 0 → HDFS Parquet
[OK] Batch 1 → Elasticsearch (22 docs)
```

Verifica que los datos llegan a Elasticsearch:

```bash
curl "http://localhost:9200/football_metrics/_count"
```

Ver los datos de los jugadores con más goles:

```bash
curl -s "http://localhost:9200/football_metrics/_search?pretty" -H "Content-Type: application/json" -d '{
  "size": 5,
  "sort": [{"goles": {"order": "desc"}}],
  "_source": ["player_nombre", "player_equipo", "goles", "tiros", "eficiencia_tiro", "estamina_actual"]
}'
```

---

## PASO 7 - Visualizar en Kibana (Investigación Propia)

Kibana es la herramienta de visualización nativa de Elasticsearch y forma parte de la investigación propia del proyecto.

### 7.1 - Acceder a Kibana

Abre: **http://localhost:5601**

### 7.2 - Crear el Data View

1. En el menú lateral ve a **Management -> Stack Management -> Data Views**
2. Haz clic en **Create data view**
3. Rellena:
   - **Name:** `football_metrics`
   - **Index pattern:** `football_metrics`
4. Pulsa **Save data view to Kibana**

### 7.3 - Explorar los datos

- Ve a **Analytics -> Discover**: verás los documentos en tiempo real.
- Ve a **Analytics -> Dashboard -> Create dashboard** para crear visualizaciones.

Ejemplos de visualizaciones recomendadas:
- **Bar chart**: `goles` por `player_nombre` (goleadores del partido)
- **Bar chart**: `eficiencia_pase` por `player_nombre`
- **Data table**: todos los jugadores con sus métricas completas
- **Pie chart**: distribución de `eventos_zona_defensa/medio/ataque` por equipo
- **Metric**: jugador con menor `estamina_actual` (candidato a sustitución)

##### Resultado:
![Ejemplo 4](/img/img4.png "Salida de Compass: R1")

---

## PASO 8 - Visualizar en Grafana

### 8.1 - Acceder a Grafana

Abre: **http://localhost:3000**  
Usuario: `admin` / Contraseña: `admin`

### 8.2 - Añadir Elasticsearch como fuente de datos

1. Ve a **Connections -> Data Sources -> Add data source**
2. Busca y selecciona **Elasticsearch**
3. Rellena:
   - **URL:** `http://elasticsearch:9200`
   - **Index name:** `football_metrics`
   - **Time field:** `timestamp`
4. Posteriormente despliga **HTTP headers** y añade:
   - **Header**: `X-Elastic-Product`
   - **Value**: `Elasticsearch`

   ![Ejemplo 7](/img/img7.png "Salida de Compass: R1")
5. Pulsa **Save & Test** -> debe aparecer `Index OK`

### 8.3 - Queries de ejemplo para dashboards

**Jugadores con más fatiga (candidatos a sustitución):**  
Ordena por `estamina_actual` ascendente.

**Tabla de goleadores:**  
Filtra `goles > 0`, ordena por `goles` descendente.

**Eficiencia de pases:**  
Muestra `eficiencia_pase` con un gauge o bar chart por jugador.

**Heatmap de zonas:**  
Usa `eventos_zona_defensa`, `eventos_zona_medio`, `eventos_zona_ataque`.

> Activa **Auto-refresh cada 10s** (esquina superior derecha del dashboard) para ver los datos actualizarse en tiempo real.

##### Resultado:
![Ejemplo 6](/img/img6.png "Salida de Compass: R1")

---

## PASO 9 - Accesos a todas las UIs

| Servicio              | URL directa                    | Descripción                          |
|-----------------------|--------------------------------|--------------------------------------|
| **Kibana**            | http://localhost:5601          | Exploración de datos en Elasticsearch|
| **Grafana**           | http://localhost:3000          | Dashboards de métricas del partido   |
| **Elasticsearch**     | http://localhost:9200          | API REST de Elasticsearch            |
| HDFS NameNode         | http://localhost:9870          | Estado del sistema de ficheros HDFS  |
| YARN ResourceManager  | http://localhost:8088          | Jobs de Spark en YARN                |
| Spark Master UI       | http://localhost:8080          | Estado del clúster Spark             |
| Kafka UI (Kafbat)     | http://kafka.localhost         | Topics, mensajes y conectores Kafka  |
| Prometheus            | http://localhost:9090          | Métricas de infraestructura          |
| Portainer             | http://localhost:9010          | Gestión visual de contenedores       |
| Traefik Dashboard     | http://localhost:8089          | Estado del proxy inverso             |

---

## PASO 10 - Iniciar un nuevo partido (reset)

Para limpiar los datos del partido anterior y empezar uno nuevo:

### 1. Para el generador y el streaming ETL con Ctrl+C en sus terminales
Si el ETL está en otra terminal, deténlo con Ctrl+C.
Si dejó un job en espera, mata la app vieja desde Spark Master:
Abre http://localhost:8080 y usa el enlace (kill) junto a la app antigua.

![Ejemplo 5](/img/img5.png "Salida de Compass: R1")

```bash
# 2. Borra el índice de Elasticsearch (datos del partido anterior)
curl -X DELETE http://localhost:9200/football_metrics

# 3. Borra los checkpoints de Spark para empezar limpio
#    Usa bash -lc para evitar errores de ruta desde Docker en Windows.
docker exec namenode bash -lc 'hdfs dfs -rm -r /checkpoints/streaming_etl'

# 4. Copia los scripts ETL al contenedor Spark si no lo has hecho aún
docker cp ./ETL spark-master:/opt/spark/ETL

# 5. Vuelve a lanzar el ETL Streaming
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 /opt/spark/ETL/streaming_etl.py

# 6. Vuelve a lanzar el generador (el nuevo match_id se genera automáticamente)
python ETL/streaming_generator.py
```

---

## PASO 11 - Detener el sistema

Para el generador: `Ctrl+C` en su terminal.  
Para el streaming ETL: `Ctrl+C` en su terminal.  
Para parar toda la infraestructura:

```bash
docker compose down
```

Para eliminar también todos los volúmenes (datos de HDFS, Kafka, Elasticsearch, etc.):

```bash
docker compose down -v
```

Volver atrás: [Introducción](/Readme.md)
