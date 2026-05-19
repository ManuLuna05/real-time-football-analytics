# Documentación: Datos del Proyecto

En este documento se explica de dónde vienen todos los datos que usa el proyecto, qué se hace con ellos y qué información nueva se genera a partir de ellos. La idea es tener en un solo sitio una visión clara del ciclo completo de los datos: desde que están en un CSV hasta que aparecen en un dashboard.

---

## 1. De dónde vienen los datos originales

La base de todo el proyecto son dos archivos CSV con estadísticas reales de jugadores de fútbol extraídas de FIFA:

- `Men_Players.csv` - jugadores masculinos
- `Women_Players.csv` - jugadores femeninas

Estos archivos tienen decenas de columnas, pero el proyecto solo utiliza las que tienen un efecto directo en la simulación. El resto se descartan durante el proceso de limpieza.

### Columnas que se usan de los CSV

| Campo original (CSV) | Nombre interno | Tipo | Descripción |
|---|---|---|---|
| `ID` | `id` | Texto | Identificador único del jugador. Se usa como clave principal para no confundir jugadores con el mismo nombre. |
| `Name` | `nombre` | Texto | Nombre del jugador. Aparece en los dashboards y en los eventos de Kafka. |
| `GENDER` | `genero` | Texto | Indica si el jugador viene del CSV masculino o femenino. |
| `OVR` | `media` | Número | Valoración global del jugador (0–99). Se usa como filtro: los jugadores con OVR menor de 65 se descartan porque se consideran de muy bajo nivel para la simulación. |
| `Position` | `posicion` | Texto | Posición táctica del jugador: GK, CB, ST, CM... Se usa para colocarlo en su línea dentro de la formación 4-4-2. |
| `Team` | `equipo` | Texto | Club al que pertenece el jugador en FIFA. Se guarda como dato informativo. |
| `PAC` | `velocidad` | Número (0–99) | Ritmo del jugador. Controla la distancia máxima que puede recorrer en cada ciclo de 2 segundos. |
| `PAS` | `pase` | Número (0–99) | Habilidad de pase. Determina la probabilidad de que un pase salga bien: si PAS=80, hay un 80% de probabilidad de éxito. |
| `SHO` | `tiro` | Número (0–99) | Habilidad de tiro. Influye en la probabilidad de marcar cuando el tiro va entre los palos. |
| `PHY` | `resistencia` | Número (0–99) | Físico del jugador. Controla la velocidad a la que cae su estamina mientras corre. |
| `DEF` | `defensa` | Número (0–99) | Capacidad defensiva. Se usa exclusivamente para el portero: determina la probabilidad de que pare un tiro a puerta. |

### Columna añadida en el proceso de limpieza

Además de las columnas del CSV, el ETL Batch añade una columna nueva que no existía en el fichero original:

| Campo | Tipo | Valor inicial | Descripción |
|---|---|---|---|
| `estamina` | Decimal | `100.0` | Energía del jugador al empezar el partido. Arranca siempre a 100 para todos y va bajando durante la simulación según cuánto corra y lo resistente que sea. |

### Jugadores que se descartan

No todos los jugadores del CSV llegan a la simulación. Se eliminan los que:
- Tienen la valoración global (`OVR`) por debajo de 65.
- Tienen alguna estadística clave vacía o nula (`id`, `nombre`, `velocidad`, `pase`, `tiro`, `resistencia` o `defensa`).

---

## 2. Datos que genera la simulación en tiempo real

El script [streaming_generator.py](documentation/explain_streaming_generator.md). lee los jugadores ya limpios, forma dos equipos al azar y arranca un bucle que cada 2 segundos genera una tanda de mensajes JSON. Estos mensajes se publican en Kafka y representan lo que ha pasado en el campo durante ese intervalo.

Hay dos tipos de mensaje: uno que se envía siempre (posición de cada jugador) y otro que solo aparece cuando ocurre una acción con el balón.

---

### Tipo `posicion` - se genera para los 22 jugadores en cada ciclo

Cada 2 segundos, independientemente de si hay pases o tiros, el sistema emite un evento de posición para cada uno de los 22 jugadores con su ubicación actual y su estado físico.

| Campo | Tipo | Descripción |
|---|---|---|
| `match_id` | Texto | Identificador único del partido. Se genera al arrancar el script y no cambia durante toda la simulación. Sirve para no mezclar datos de partidos distintos. |
| `timestamp` | Decimal | Tiempo simulado en segundos desde el inicio del partido. Sube de 2 en 2 con cada ciclo. |
| `tipo` | Texto | Siempre `"posicion"` en este tipo de evento. |
| `player_id` | Texto | ID del jugador, sacado directamente del CSV original. |
| `player_nombre` | Texto | Nombre del jugador. |
| `player_equipo` | Texto | `"A"` o `"B"`, según el equipo al que fue asignado al formar los equipos. |
| `player_posicion` | Texto | Posición táctica del jugador (GK, CB, ST...). |
| `x` | Decimal | Posición horizontal del jugador en el campo, entre 0.5 y 104.5 metros. |
| `y` | Decimal | Posición vertical del jugador en el campo, entre 0.5 y 67.5 metros. |
| `speed_jugador` | Decimal | Velocidad a la que se ha movido el jugador en este ciclo, en metros por segundo. Se calcula a partir de su `PAC` y su estamina actual. |
| `estamina` | Decimal | Energía restante del jugador en este momento (0–100). Baja en cada ciclo según cuánto corra y su `PHY`. |
| `ball_x` | Decimal | Posición horizontal del balón en este momento. |
| `ball_y` | Decimal | Posición vertical del balón en este momento. |

#### Cómo se calcula la velocidad y la estamina

Todos los cálculos se encuentran detallados y explicados en la documentación destinada a explicar el funcionamiento del script [streaming_generator.py](documentation/explain_streaming_generator.md).

---

### Tipo `accion` - solo cuando ocurre algo con el balón

Además de los eventos de posición, cada ciclo puede generar entre 0 y 3 eventos adicionales si hay una acción con el balón. Comparten los mismos campos de cabecera que el evento de posición, más tres campos exclusivos:

| Campo | Tipo | Descripción |
|---|---|---|
| `ball_speed` | Decimal | Velocidad del balón después de la acción, en m/s. Varía según el tipo: pase (10–25 m/s), tiro (20–35 m/s), recuperación (5–15 m/s). |
| `action` | Texto | Tipo de acción ocurrida. Ver tabla de acciones posibles abajo. |
| `is_successful` | Booleano | `true` si la acción tuvo éxito, `false` si no. |

> En los eventos de acción, `speed_jugador` siempre vale `0.0` porque se asume que el jugador está parado en el momento de ejecutar la acción.

---

## 3. Datos que produce el ETL Streaming

El script [streaming_etl.py](documentation/explain_streaming_etl.md). consume todos esos eventos de Kafka y los agrega por jugador. En lugar de guardar cada evento individual, calcula los totales acumulados de cada jugador desde que empezó el partido y los actualiza cada 10 segundos.

El resultado es una tabla con una fila por jugador que contiene todo su rendimiento hasta ese momento:

### Métricas por jugador

| Campo | Tipo | Cómo se calcula |
|---|---|---|
| `match_id` | Texto | Identificador del partido al que pertenecen los datos. |
| `player_id` | Texto | ID del jugador. |
| `player_nombre` | Texto | Nombre del jugador. |
| `player_equipo` | Texto | Equipo A o B. |
| `player_posicion` | Texto | Posición táctica. |
| `distancia_m` | Decimal | Suma de todas las velocidades del jugador * 2 (segundos por ciclo). Aproxima los metros totales recorridos. |
| `estamina_actual` | Decimal | Último valor de estamina registrado para el jugador. Representa su estado físico en este momento. |
| `ultima_x` | Decimal | Última posición X conocida del jugador en el campo. |
| `ultima_y` | Decimal | Última posición Y conocida del jugador en el campo. |
| `pases_intentados` | Entero | Número total de eventos `action = "pase"` del jugador. |
| `pases_acertados` | Entero | Número de eventos `action = "pase"` con `is_successful = true`. |
| `tiros` | Entero | Número total de eventos `action = "tiro"`. |
| `goles` | Entero | Número de eventos `action = "gol"`. |
| `paradas` | Entero | Número de eventos `action = "parada"` (exclusivo de porteros). |
| `perdidas` | Entero | Número de eventos `action = "perdida"`. |
| `recuperaciones` | Entero | Número de eventos `action = "recuperacion"`. |
| `eventos_zona_defensa` | Entero | Veces que el jugador estaba en la zona `x <= 35` cuando se registró un evento. |
| `eventos_zona_medio` | Entero | Veces que el jugador estaba en la zona `35 < x <= 70`. |
| `eventos_zona_ataque` | Entero | Veces que el jugador estaba en la zona `x > 70`. |
| `eficiencia_pase` | Decimal (%) | `pases_acertados / pases_intentados * 100`. Si no ha intentado ningún pase, vale `0`. |
| `eficiencia_tiro` | Decimal (%) | `goles / tiros * 100`. Si no ha tirado ninguna vez, vale `0`. |

### Dónde se guardan estos datos

Cada 10 segundos, el ETL Streaming escribe estos datos en dos sitios:

- **Elasticsearch**: guarda o actualiza un documento por jugador usando como ID la combinación `match_id + player_id`. Cada escritura sobreescribe el documento anterior, así que en Elasticsearch siempre hay exactamente el estado más reciente de cada jugador. De aquí los lee Kibana para los dashboards.

- **HDFS (Parquet)**: guarda una copia de cada batch en modo `append`, acumulando el historial completo de cómo han ido evolucionando las métricas durante el partido. De aquí los lee Grafana.

Volver atrás: [Introducción](/Proyecto.md)
