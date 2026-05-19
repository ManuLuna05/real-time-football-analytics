# Proyecto Big Data: Análisis de Rendimiento en Fútbol en Tiempo Real

## Descripción del Proyecto

Este proyecto monta un sistema completo de Big Data para analizar en tiempo real cómo rinden los jugadores de fútbol durante un partido simulado.

La idea es replicar el entorno de un equipo deportivo que necesita tomar decisiones rápidas durante un partido: a quién cambiar porque está agotado, qué jugador está fallando demasiados pases, o qué zona del campo está siendo un desastre. Para ello procesamos dos capas de datos a la vez: los datos históricos de los jugadores (sus estadísticas reales de FIFA) y los datos que se van generando segundo a segundo durante la simulación del partido.

Todo el sistema corre sobre contenedores Docker con las siguientes herramientas:

| Herramienta | Rol en el sistema |
|---|---|
| **HDFS** (NameNode + DataNode) | Almacenamiento distribuido de los CSV originales y de los resultados procesados |
| **Apache Kafka** | Canal de mensajes por donde viajan los eventos del partido en tiempo real |
| **Apache Spark** (Batch + Structured Streaming) | Motor de procesamiento y transformación de datos |
| **Elasticsearch** | Base de datos de búsqueda rápida que alimenta los dashboards en vivo |
| **Kibana** | Panel de visualización conectado a Elasticsearch |
| **Grafana** | Panel de visualización conectado a HDFS |
| **Prometheus** | Monitorización del estado interno del sistema |
| **Portainer** | Gestión visual de los contenedores Docker |
| **Traefik** | Proxy inverso que enruta el tráfico entre servicios |
| **Kafbat** | Interfaz web para inspeccionar topics y mensajes de Kafka |

---

## Procedencia de los datos

El sistema trabaja con dos tipos de datos que tienen orígenes muy distintos.

### 1. Datos Reales - CSV de jugadores de FIFA

Usamos dos archivos CSV (`Men_Players.csv` y `Women_Players.csv`) con las estadísticas reales de jugadores profesionales. De todas las columnas que traen estos archivos, el sistema solo utiliza las que tienen un efecto directo en la simulación:

- **Físico (`PHY`):** controla la velocidad a la que cae la energía de cada jugador mientras corre.
- **Ritmo (`PAC`):** determina la velocidad máxima a la que puede moverse un jugador en el campo.
- **Pase (`PAS`):** fija la probabilidad de que un pase salga bien.
- **Tiro (`SHO`):** influye en la probabilidad de marcar gol.
- **Defensa (`DEF`):** determina la probabilidad de que el portero pare un tiro a puerta.

Antes de que arranque la simulación, estos CSV pasan por un proceso de limpieza y preparación. Puedes ver en detalle qué transformaciones se aplican y por qué en la [documentación del ETL Batch](documentation/explain_batch_etl.md).

### 2. Datos Sintéticos - Simulación del partido en tiempo real

El script `streaming_generator.py` es el encargado de simular el partido. Elige jugadores al azar de los CSV, forma dos equipos con alineación 4-4-2 y arranca un bucle que cada 2 segundos genera lo que ha pasado en el campo: hacia dónde se ha movido cada jugador, si alguien ha dado un pase, si ha habido un tiro, si ha entrado un gol... Todo eso se publica como mensajes en Kafka para que el resto del sistema lo procese.

Si quieres entender cómo funciona la simulación por dentro, cómo se calculan los movimientos, el desgaste físico o las probabilidades de cada acción, está todo explicado en la [documentación del generador de streaming](documentation/explain_streaming_generator.md).

### Datos manejados

Entre los datos de entrada de los CSV, los eventos que genera la simulación y las métricas que calcula el ETL, el sistema maneja en total más de 30 campos distintos a lo largo de todo el flujo. Si quieres ver de forma detallada qué contiene cada uno, de dónde viene y qué se hace con él, está todo recogido en la [documentación de datos del proyecto](documentation/explain_data.md).

---

## Cómo se procesan los datos (ETL)

Una vez que los datos existen (bien en CSV, bien llegando por Kafka), Apache Spark se encarga de limpiarlos, transformarlos y calcular las métricas que nos interesan. Este procesamiento tiene dos fases que funcionan de forma completamente independiente.

### 1. ETL Batch - Preparación inicial de los jugadores

Se ejecuta una sola vez antes de arrancar la simulación. Su trabajo es leer los dos CSV desde HDFS, quedarse solo con las columnas útiles, unir a hombres y mujeres en una sola tabla, inicializar la estamina de todos los jugadores a 100 y guardar el resultado en formato Parquet, que es mucho más eficiente que un CSV para trabajar con Spark.

Todo el detalle de este proceso, incluyendo por qué se toman ciertas decisiones técnicas, está en la [documentación del ETL Batch](documentation/explain_batch_etl.md).

### 2. ETL Streaming - Procesamiento del partido en vivo

Este proceso corre continuamente mientras dura el partido. Consume los mensajes de Kafka, calcula métricas acumuladas por jugador (distancia recorrida, estamina restante, pases acertados, goles, recuperaciones...) y cada 10 segundos manda los resultados a dos destinos:

- **Elasticsearch:** para que los dashboards muestren el estado actual de cada jugador casi en tiempo real.
- **HDFS:** para conservar un histórico permanente de cómo han ido evolucionando las métricas a lo largo del partido.

Si quieres saber cómo se calculan exactamente estas métricas o cómo funciona la escritura en Elasticsearch, está explicado en la [documentación del ETL Streaming](documentation/explain_streaming_etl.md).

> **Nota:** La razón de incluir Elasticsearch en el proyecto es puramente técnica: Kibana necesita Elasticsearch como base de datos para funcionar, no puede conectarse directamente a HDFS. Esto nos permite además comparar la experiencia de visualización entre las dos herramientas, que era uno de los objetivos de investigación del proyecto.

---

## Qué se puede ver en los dashboards

Con el sistema en marcha, tanto Kibana como Grafana muestran los datos del partido actualizados cada 10 segundos. Cada herramienta ofrece una perspectiva diferente sobre los mismos datos.

Puedes ver qué muestra cada panel, cómo está configurado y cómo interpretarlo en la [documentación de los dashboards](documentation/explain_dashboards.md).

### Business Intelligence

El entrenador (o nosotros) puede abrir los paneles (Dashboards) en Kibana y Grafana y ver datos muy interesantes como:
- **Cansancio en directo:** Quién se está quedando sin estamina y necesita un cambio ya.
- **Eficacia:** Quién está acertando más pases o tirando mejor a puerta.
- **Resumen rápido:** Los goles totales, robos de balón clave, o pases que ha metido cada jugador...

---

## Toma de Decisiones

Viendo estos gráficos, se pueden tomar decisiones casi como en la vida real:
1. **Hacer cambios con cabeza:** En lugar de cambiar por pura intuición, ves el ránking de estamina en el panel y cambias al que de verdad esté agotado físicamente.
2. **Cambiar la táctica:** Si notas que estás perdiendo muchos balones en el medio del campo, el entrenador puede cambiar la formación o forzar ataques por las bandas.
3. **Análisis de jugadores:** Si ves que un centrocampista falla demasiados pases, puedes indicarle que juegue más seguro o con pases cortos.
4. **Entrenamientos para la semana:** Si un jugador corre mucho pero se cansa demasiado rápido respecto a sus compañeros, los preparadores sabrán que tiene que entrenar más físico para tener mejor `PHY`.

---

## Cómo probar el proyecto

Si quieres levantar el sistema y ver todo esto en funcionamiento, la [Guía de Ejecución](documentation/Ejecucion.md) contiene todas las instrucciones paso a paso: cómo arrancar los contenedores Docker, cómo cargar los datos en HDFS, cómo lanzar cada script y cómo acceder a los paneles.
