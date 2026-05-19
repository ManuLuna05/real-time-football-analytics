# Documentación: Dashboards de Visualización

Una vez que el sistema está en marcha, los datos del partido se pueden consultar en tiempo real a través de dos herramientas de visualización: **Grafana** y **Kibana**. Aunque las dos muestran datos del mismo partido, cada una tiene un propósito diferente: Grafana da una visión general de cómo está yendo el partido globalmente, y Kibana permite profundizar en los datos de jugadores concretos para análisis más detallados.

Ambos paneles se actualizan automáticamente cada 10 segundos, que es el intervalo con el que el ETL Streaming manda los datos procesados a Elasticsearch y a HDFS.

## Vídeo demostrativo

En el siguiente vídeo se puede ver el sistema funcionando en tiempo real, con los datos del partido actualizándose en ambos dashboards según van llegando los eventos de Kafka:

<video src="https://github.com/iesgrancapitan-CEIABD-BDA/proyecto-final-bda-2025-26-mlunala-grancapitan/blob/main/Video_dashboards.mp4" controls="controls" style="max-width: 100%;">
  Tu navegador no soporta el tag de video.
</video>

El enlace del video es (en caso de que no se pueda ver desde aquí): https://github.com/iesgrancapitan-CEIABD-BDA/proyecto-final-bda-2025-26-mlunala-grancapitan/blob/main/Video_dashboards.mp4

---

---

## Grafana - Resumen General del Partido

![Ejemplo 6](/img/img6.png "Salida de Compass: R1")

Grafana lee los datos directamente desde HDFS y muestra una foto global del partido en ese momento. La idea es que de un vistazo, sin tener que buscar nada, se pueda saber cómo está el partido: cuántos jugadores hay en campo, cuántos tiros se han dado, cuántos goles han entrado y qué tal está aguantando físicamente el equipo en general.

El dashboard está dividido en dos bloques.

### Bloque 1 - Resumen General del Partido

Este bloque muestra seis métricas globales, cada una en su propio panel con un color diferente para facilitar la lectura rápida:

- **Jugadores Activos (22):** el número total de jugadores en campo en ese momento, sumando ambos equipos. En una situación normal siempre serán 22 (11 por equipo). Si el número bajara indicaría que hay algún problema en la recepción de datos.

- **Total Tiros (5):** todos los tiros que se han producido en el partido desde el inicio, independientemente de si han ido a puerta o no y de qué equipo los ha hecho. Un número de tiros bajo con muchos minutos de partido puede indicar que ningún equipo está siendo capaz de llegar al área rival.

- **Pases Acertados (16):** el total de pases que han llegado a su destino entre los dos equipos. Junto con el total de pérdidas de balón, da una idea del ritmo y la calidad del juego: una proporción alta de pases acertados frente a pérdidas sugiere que el partido está siendo fluido y controlado.

- **Pérdidas de Balón (14):** el número de veces que un equipo ha perdido la posesión por un pase fallido. Si este número es alto respecto a los pases acertados, el partido está siendo muy disputado o los jugadores están bajo mucha presión.

- **Total Goles (3):** los goles marcados en total por ambos equipos desde que arrancó el partido.

- **Recuperaciones (14):** el número de veces que un jugador ha robado el balón al rival tras una pérdida. Un número alto de recuperaciones indica que hay mucha disputa en el medio del campo.

### Bloque 2 - Estamina y Físico: Candidatos a Sustitución

Este bloque muestra una única métrica pero es probablemente la más importante para la toma de decisiones en caliente:

- **Estamina Media de los Jugadores (78.7%):** la media de energía restante de todos los jugadores activos en ese momento. El indicador usa una barra de color degradado que va del rojo (agotamiento total) al verde (energía plena), de forma que no hace falta ni leer el número para saber si el equipo está fresco o empezando a acusar el cansancio.

  En el ejemplo de la captura, el 78.7% indica que el partido lleva un tiempo razonable en juego y los jugadores están empezando a notar el desgaste, pero todavía hay margen. Cuando este valor empiece a bajar del 60%, es señal de que la fatiga va a empezar a afectar al rendimiento general del equipo y habría que plantearse hacer cambios.

### Filtros disponibles

En la parte superior del dashboard hay cuatro filtros que permiten acotar los datos mostrados: **Equipo**, **Posición**, **Jugador** y un cuarto filtro genérico. Esto permite, por ejemplo, ver solo las estadísticas del Equipo A, o filtrar por porteros para ver cuántos tiros han parado. En la captura todos están en "All", mostrando los datos globales de ambos equipos.

---

## Kibana - Análisis Detallado por Jugador

![Ejemplo 8](/img/img8.png "Salida de Compass: R1")

Kibana lee los datos desde Elasticsearch y permite profundizar en el rendimiento individual de los jugadores. Mientras Grafana responde a "¿cómo está yendo el partido?", Kibana responde a "¿quién está rindiendo bien y quién no?". Es la herramienta que usaría el cuerpo técnico para tomar decisiones concretas sobre jugadores específicos.

El dashboard está dividido en tres bloques, cada uno con su propia visualización.

### Bloque 1 - Estamina de los Jugadores

Un gráfico de barras horizontales que muestra la estamina actual de los cinco jugadores con menos energía restante, más un valor agregado "Other" que representa la media del resto. Las barras más largas indican jugadores más frescos. Las más cortas, jugadores que están acusando el cansancio y que podrían ser candidatos a sustitución.

Visualmente, la diferencia entre las barras es pequeña, lo que indica que el cansancio está repartido de forma bastante homogénea entre los jugadores mostrados.

**Decisión que facilita:** si el cuerpo técnico necesita hacer un cambio y hay dos candidatos tácticos equivalentes, este gráfico le dice cuál de ellos está más agotado y necesita salir antes.

### Bloque 2 - Distancia Media Recorrida

Un gráfico de área que muestra la distancia media recorrida por los cinco jugadores que más metros han acumulado durante el partido. La línea desciende de izquierda a derecha porque los jugadores están ordenados de mayor a menor distancia.

La forma descendente y suave de la curva indica que no hay una diferencia brutalmente grande entre los más activos y los menos, aunque sí se aprecia que hay jugadores que han trabajado bastante más que otros.

**Decisión que facilita:** Esto nos permitirá también ver la relación que hay entre el cansancio de los jugadores y la distancia que han recorrido, pudiendo así poder saber que jugadores tienen mejor físico, ya que si han recorrido mucha distancia pero están menos cansados que otros jugadores que han recorrido menos distancia, podremos deducir que esos jugadores tienen un físico superior. Esto servirá al equipo técnico a decidir qué jugadores deben entrenar más la parte física, pudiendo así crear unos entrenamientos específicos.

### Bloque 3 - Goles

Un gráfico de barras horizontales que muestra los goles marcados por cada jugador durante el partido. En la captura, Alisson y Femi Azeez son los únicos que han marcado, con una barra claramente destacada frente al resto de jugadores que aparecen en el ranking pero con valor cero.

Vale la pena fijarse en que Alisson aparece como goleador: en un partido simulado donde los jugadores se eligen al azar de los CSV, puede ocurrir que un portero (que es lo que es Alisson en la realidad) aparezca en el ranking de goles porque la simulación no impide que cualquier jugador pueda intentar un tiro. No es un error del sistema, es una consecuencia de que la simulación es sintética y probabilística.

**Decisión que facilita:** identificar qué jugadores están siendo efectivos de cara a gol y, combinado con el dato de tiros totales de Grafana, ver si el equipo está generando ocasiones pero no las está convirtiendo, o si directamente no está llegando al área.

---

## Grafana vs Kibana: cuándo usar cada una

Aunque los dos dashboards muestran datos del mismo partido, tienen usos complementarios:

| | Grafana | Kibana |
|---|---|---|
| **Nivel de detalle** | Global (ambos equipos) | Individual (jugador a jugador) |
| **Para qué sirve** | Saber cómo está el partido en general | Saber quién está rindiendo bien o mal |
| **Pregunta que responde** | ¿Está siendo un partido disputado? ¿Hay muchos goles? ¿El equipo está cansado? | ¿Quién necesita un cambio? ¿Quién está corriendo más? ¿Quién está marcando? |
| **Fuente de datos** | HDFS | Elasticsearch |
| **Cuándo mirarlo** | Continuamente, para tener el contexto del partido | Cuando hay que tomar una decisión concreta sobre un jugador |

La idea es usar Grafana como pantalla de fondo siempre visible, y acudir a Kibana cuando algo en Grafana llame la atención: por ejemplo, si la estamina media baja mucho, se abre Kibana para ver exactamente quiénes son los jugadores más agotados y decidir el cambio con datos en la mano.

Volver atrás: [Introducción](/Proyecto.md)
