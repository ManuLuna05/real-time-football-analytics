# Documentación: Generador de Datos en Streaming (`streaming_generator.py`)

Este script es el encargado de simular un partido de fútbol completo. Funciona en un bucle infinito: cada 2 segundos decide qué ha pasado en el campo (si alguien ha pasado, tirado, marcado...), genera esa información en forma de mensajes y los manda a Kafka para que el resto del sistema los procese. Utiliza las estadísticas reales de los jugadores sacadas de los CSV de FIFA para que las probabilidades de que algo salga bien o mal no sean completamente inventadas, sino que dependan de lo bueno que sea el jugador.

---

## Configuración inicial

Antes de que empiece cualquier simulación, el script define una serie de valores fijos que controlan cómo se va a comportar todo:

```python
KAFKA_BROKER = 'localhost:9094'
TOPIC = 'football_events'
INTERVALO = 2

GOAL_Y_MIN = 22.32
GOAL_Y_MAX = 45.68
GOAL_X_A = 105.0
GOAL_X_B = 0.0

MATCH_ID = str(uuid.uuid4())
```

- **`KAFKA_BROKER`**: la dirección donde está escuchando Kafka. Indicamos donde hay que mandar los mensajes.
- **`TOPIC`**: el nombre del canal dentro de Kafka donde se van a publicar todos los eventos del partido.
- **`INTERVALO = 2`**: yiempo en el que se genera nueva información del partido. Cada 2 segundos se mueven todos los jugadores y puede ocurrir una acción.
- **`GOAL_Y_MIN` y `GOAL_Y_MAX`**: las coordenadas verticales que marcan dónde están los postes de la portería. El campo mide 68 metros de ancho, y la portería ocupa la franja entre el metro 22.32 y el 45.68. Si un tiro cae fuera de ese rango, va fuera.
- **`GOAL_X_A` y `GOAL_X_B`**: la posición horizontal de cada portería. La portería que defiende el equipo A está en el extremo derecho del campo (`x = 105`), y la que defiende el equipo B está en el extremo izquierdo (`x = 0`).
- **`MATCH_ID`**: un identificador único que se genera automáticamente al arrancar el script. Sirve para que todos los eventos de este partido lleven la misma "etiqueta" y no se mezclen nunca con los datos de otro partido anterior o futuro.

---

## Grupos de posiciones y formación

```python
GK_POS = {'GK'}
DEF_POS = {'CB', 'RB', 'LB', 'RWB', 'LWB'}
MID_POS = {'CDM', 'CM', 'CAM', 'RM', 'LM'}
FWD_POS = {'ST', 'CF', 'LW', 'RW', 'SS'}

FORMATION = [
    ('GK', GK_POS, 1),
    ('DEF', DEF_POS, 4),
    ('MID', MID_POS, 4),
    ('FWD', FWD_POS, 2),
]
```

Aquí se definen los grupos de posiciones reconocidas por el script y cuántos jugadores de cada grupo necesita cada equipo. La formación es una 4-4-2: 1 portero, 4 defensas, 4 centrocampistas y 2 delanteros. Esto es simplemente una lista que el script consulta para saber cuántos jugadores de cada tipo tiene que buscar en el CSV cuando forma los equipos.

---

## Funciones del código

### `create_producer()`

```python
def create_producer():
    p = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
        retries=5,
    )
```

Esta función arranca la conexión con Kafka. Solo se llama una vez al inicio del script.

Lo más importante que hace es configurar el **serializador**: Kafka no entiende diccionarios de Python, solo entiende texto puro. Por eso `value_serializer` convierte automáticamente cada evento (que es un diccionario con campos como `player_id`, `x`, `y`...) a formato JSON en texto antes de mandarlo. El `retries=5` significa que si el envío falla por un problema de red, lo intentará hasta 5 veces antes de rendirse.

---

### `safe_int(v, default=50)`

```python
def safe_int(v, default=50):
    try: return int(v)
    except: return default
```

 Los datos del CSV de FIFA a veces tienen celdas vacías o con caracteres raros. Si el script intenta convertir una celda vacía a número entero da error. Esta función lo intenta de forma segura: si falla, devuelve 50 (un valor medio neutro) en lugar de romper todo el programa. Se usa cada vez que se lee una estadística del CSV como `PAC`, `PAS`, `PHY`, etc.

---

### `load_players(filepath)`

Esta función es la que lee el CSV de jugadores y monta los dos equipos. Hace tres cosas en orden:

**1. Lee el CSV y clasifica a los jugadores por posición:**

```python
for row in csv.DictReader(f):
    pos = row.get('Position', '').strip()
    ovr = safe_int(row.get('OVR', 0))
    if ovr < 65:
        continue
    for grp, pos_set, _ in FORMATION:
        if pos in pos_set:
            buckets[grp].append(row)
```

Recorre el CSV línea por línea. Si un jugador tiene una media global (`OVR`) menor de 65, directamente lo descarta: no queremos jugadores de muy bajo nivel en la simulación. Si pasa ese filtro, lo mete en su zona correspondiente (porteros, defensas, medios o delanteros) según su posición.

**2. Elige los jugadores de cada equipo al azar:**

```python
chosen = random.sample(pool, needed)
```

Para cada equipo (A y B) y para cada línea de la formación, elige aleatoriamente los jugadores necesarios del cubo correspondiente. Esto hace que cada partido tenga jugadores diferentes.

**3. Crea el diccionario de cada jugador con todos sus datos:**

```python
players.append({
    'id': row['ID'],
    'nombre': row['Name'],
    'equipo': team, # 'A' o 'B'
    'posicion': pos, # GK, CB, ST...
    'is_gk': is_gk, # True si es portero
    'velocidad': safe_int(row.get('PAC', 50)),
    'pase': safe_int(row.get('PAS', 50)),
    'tiro': safe_int(row.get('SHO', 50)),
    'resistencia': safe_int(row.get('PHY', 50)),
    'defensa': safe_int(row.get('DEF', 50)),
    'x': x_ini,
    'y': random.uniform(5, 63),
    'estamina': 100.0,
})
```

Para cada jugador se guarda toda la información que la simulación va a necesitar: sus estadísticas de FIFA (velocidad, pase, tiro, resistencia, defensa), su posición inicial en el campo y su estamina, que siempre arranca en 100.

La posición inicial en `x` se asigna así:

```python
x_ini = random.uniform(5, 50) # Equipo A: mitad izquierda
x_ini = random.uniform(55, 100) # Equipo B: mitad derecha

# Los porteros van pegados a su portería:
x_ini = 5.0 # Portero del equipo A
x_ini = 100.0 # Portero del equipo B
```

El equipo A ocupa la mitad izquierda del campo y el B la derecha. Los porteros se colocan directamente en su línea de portería.

---

### `nearest(players, bx, by, team, exclude_id=None)`

```python
return min(cands, key=lambda p: (p['x']-bx)**2 + (p['y']-by)**2)
```

Busca qué jugador de un equipo concreto está más cerca de una posición dada (normalmente la del balón). La fórmula `(p['x']-bx)**2 + (p['y']-by)**2` es la distancia entre dos puntos (el teorema de Pitágoras): cuanto menor sea ese resultado, más cerca está el jugador. No se saca la raíz cuadrada porque no hace falta el valor exacto en metros, solo comparar quién está más cerca.

Se usa, por ejemplo, para saber quién del equipo en posesión va a realizar el pase, o quién del equipo rival puede interceptarlo.

---

### `get_gk(players, team)`

```python
gks = [p for p in players if p['equipo'] == team and p['is_gk']]
return gks[0] if gks else None
```

Devuelve el portero del equipo que se le indique. Se usa en la simulación de tiros para saber quién tiene que intentar parar el balón y con qué estadística de defensa.

---

### `base_event(match_time, player, ball)`

```python
return {
    'match_id': MATCH_ID,
    'timestamp': match_time,
    'tipo': 'accion',
    'player_id': player['id'],
    'player_nombre': player['nombre'],
    'player_equipo': player['equipo'],
    'player_posicion': player['posicion'],
    'x': round(player['x'], 2),
    'y': round(player['y'], 2),
    'speed_jugador': 0.0,
    'estamina': round(player['estamina'], 2),
    'ball_x': round(ball['x'], 2),
    'ball_y': round(ball['y'], 2),
    'ball_speed': round(ball['speed'], 2),
    'action': None,
    'is_successful': None,
}
```

Cuando ocurre una acción (pase, tiro, gol...), hay un montón de campos que siempre son iguales en todos los eventos: el ID del partido, el tiempo, los datos del jugador, la posición del balón... Esta función genera ese esqueleto común para no repetir el mismo código en cada tipo de acción. Después, cada acción solo tiene que rellenar los campos `action` e `is_successful` con sus valores concretos.

Es importante resaltar que `speed_jugador` siempre es `0.0` en los eventos de acción ya que se asume que en el momento de ejecutar una acción (pasar, tirar...) el jugador está parado.

---

### `move_player(player, bx, by)`

Esta función mueve a un jugador y le descuenta estamina. Es la que se llama para cada uno de los 22 jugadores en cada ciclo.

#### Movimiento

```python
max_speed = player['velocidad'] / 10.0
fatigue = max(0.4, player['estamina'] / 100.0)
speed = random.uniform(0, max_speed * fatigue)
```

- **`max_speed`**: la estadística `PAC` del jugador (0–99) se divide entre 10 para convertirla a metros por segundo. Un jugador con PAC 90 puede correr hasta 9 m/s, que es una velocidad realista para un sprint en fútbol.
- **`fatigue`**: representa el porcentaje de capacidad física que le queda al jugador. Si tiene 80 de estamina, `fatigue = 0.80` (va al 80% de su máximo). Hay un mínimo de `0.4`: incluso un jugador completamente agotado (`estamina = 0`) sigue moviéndose al 40% de su capacidad, porque en un partido real los jugadores nunca se quedan literalmente parados por cansancio.
- **`speed`**: la velocidad real en este ciclo es un número aleatorio entre 0 y el máximo que le permite su fatiga. El aleatorio simula que no siempre corre a tope: a veces camina, a veces trota, a veces sprinta.

Los porteros no se mueven libremente. Tienen un comportamiento especial para que no abandonen su portería:

```python
# El portero del equipo A se ancla al punto x=5, el de B al x=100
tx = (GOAL_X_B + 5) if player['equipo'] == 'A' else (GOAL_X_A - 5)
player['x'] = player['x'] + (tx - player['x']) * 0.3
player['y'] = player['y'] + (34.0 - player['y']) * 0.1
```

El `* 0.3` y el `* 0.1` hacen que el movimiento sea gradual y suave, como si el portero se fuera "deslizando" hacia su posición ideal en cada ciclo en lugar de teletransportarse. El `34.0` es el centro del campo en el eje Y (la mitad de 68 metros), que es donde idealmente se centra un portero.

Los jugadores de campo simplemente se desplazan una cantidad aleatoria en X e Y dentro del rango que permite su velocidad:

```python
player['x'] = max(0.5, min(104.5, player['x'] + random.uniform(-speed, speed)))
player['y'] = max(0.5, min(67.5, player['y'] + random.uniform(-speed, speed)))
```

El `max(0.5, min(104.5, ...))` es simplemente para que nadie se salga del campo.

#### Desgaste de estamina

```python
desgaste = (speed / max(max_speed, 0.01)) * (1.0 - player['resistencia'] / 200.0) * INTERVALO * 0.5
player['estamina'] = max(0.0, player['estamina'] - desgaste)
```

La fórmula tiene cuatro partes que se multiplican entre sí:

**`speed / max(max_speed, 0.01)` — ¿Cuánto esfuerzo estás haciendo?**
Divide la velocidad real del ciclo entre la velocidad máxima posible. Da un resultado entre 0 y 1. Si el jugador va al sprint máximo, el resultado es 1 (esfuerzo total). Si va al trote, puede ser 0.3. El `max(..., 0.01)` evita dividir entre cero si por algún motivo `max_speed` fuera 0.

**`(1.0 - player['resistencia'] / 200.0)` — ¿Cuánto te cansas?**
Usa la estadística `PHY` del jugador para controlar cómo de resistente es. Dividir entre 200 en lugar de 100 es una decisión de diseño: hace que el factor nunca baje de 0.5, lo que significa que incluso el jugador más resistente (PHY=99) tiene un factor de desgaste de `1 - 99/200 = 0.505`. Si dividiéramos entre 100, un jugador con PHY=99 casi no se cansaría nunca (`0.01`), lo que haría la simulación aburrida. Así todos los jugadores notan el cansancio de forma apreciable durante el partido.

| PHY del jugador | Factor de desgaste |
|---|---|
| 99 (el más resistente) | 0.505 |
| 80 (bueno) | 0.600 |
| 65 (el mínimo aceptado) | 0.675 |

**`* INTERVALO` — Escala por el tiempo**
`INTERVALO = 2` segundos. Sin esto, el desgaste sería el mismo independientemente de cada cuánto se llama la función. Al multiplicar por el intervalo, el desgaste es proporcional al tiempo real que ha pasado.

**`* 0.5` — Ajuste fino de velocidad de agotamiento**
Reduce el desgaste a la mitad para que la estamina no caiga demasiado rápido. Es un parámetro de calibración: sin él, los jugadores se agotarían el doble de rápido. Con él, un jugador medio corriendo a tope aguanta aproximadamente entre 5 y 7 minutos simulados antes de llegar a estamina baja, lo que da tiempo suficiente para que los dashboards muestren diferencias entre jugadores.

El `max(0.0, ...)` al final garantiza que la estamina nunca sea negativa.

---

### `simulate_cycle(players, ball, match_time)`

Esta es la función principal que se ejecuta cada 2 segundos. Coordina todo lo que pasa en un ciclo del partido y devuelve la lista de todos los eventos generados.

**Paso 1 — Mover a todos los jugadores y emitir sus posiciones**

```python
for p in players:
    sp = move_player(p, ball['x'], ball['y'])
    events.append({
        'tipo': 'posicion',
        'speed_jugador': sp,
        ...
    })
```

Llama a `move_player` para cada uno de los 22 jugadores y genera un evento de tipo `posicion` para cada uno. Estos eventos siempre se generan, haya o no acción con el balón.

**Paso 2 — Decidir qué pasa con el balón**

```python
r = random.random()

if r < 0.45:
    # PASE (45% de probabilidad)
elif r < 0.55:
    # TIRO (10% de probabilidad)
# Si r >= 0.55 no pasa nada con el balón (45% de probabilidad)
```

Se genera un número aleatorio entre 0 y 1. Según dónde caiga ese número se decide la acción. Las probabilidades están elegidas para que la simulación se parezca a un partido real: los pases son lo más frecuente, los tiros ocurren mucho menos, y en muchos ciclos simplemente los jugadores se mueven sin que nadie haga nada especial con el balón.

---

## Acciones posibles y cómo se calculan

### Pase

```python
receiver = random.choice(mates)
ok = random.randint(1, 100) <= passer['pase']
```

Se elige un compañero al azar para recibir el pase (nunca el portero, nunca el propio pasador). El éxito del pase se decide comparando un número aleatorio del 1 al 100 con la estadística `PAS` del jugador. Si `PAS = 80`, hay un 80% de probabilidad de que el pase salga bien. Si `PAS = 50`, solo un 50%.

- **Si el pase sale bien** -> el balón se mueve a la posición del receptor (con un pequeño margen aleatorio de unos 3 metros para simular que el balón no llega exactamente al pie) y la posesión se mantiene en el mismo equipo. Se genera un evento `action: "pase"`, `is_successful: true`.

- **Si el pase falla** -> se genera un evento `action: "perdida"`, `is_successful: false`. El balón queda suelto cerca del rival más cercano, que lo recupera automáticamente. Se genera también un evento `action: "recuperacion"`, `is_successful: true` para ese jugador rival.

La velocidad del balón en un pase se fija en `random.uniform(10, 25)` m/s, que es un rango realista para un pase en fútbol.

---

### Tiro

```python
shot_y = random.uniform(GOAL_Y_MIN - 3, GOAL_Y_MAX + 3)
on_target = (GOAL_Y_MIN <= shot_y <= GOAL_Y_MAX)
```

Se elige un jugador de campo del equipo atacante al azar para realizar el tiro. La dirección vertical del tiro (`shot_y`) es aleatoria dentro de un rango que incluye la portería y un pequeño margen exterior, simulando que algunos tiros van ligeramente desviados.

- **Tiro fuera** (`on_target = False`): el balón no va entre los palos. Se genera `action: "tiro"`, `is_successful: false`. El balón pasa al equipo rival.

- **Tiro a puerta** (`on_target = True`): el balón va entre los palos y el portero tiene que intentar pararlo:

```python
gk_ok = rival_gk and (random.randint(1, 100) <= rival_gk['defensa'])
```

La probabilidad de que el portero pare el tiro depende de su estadística `DEF`. Si `DEF = 70`, para el 70% de los tiros a puerta.

  - **Si el portero para**: se genera `action: "tiro"`, `is_successful: false` para el tirador, y `action: "parada"`, `is_successful: true` para el portero. El balón queda en poder del equipo del portero.

  - **Si es gol**: se genera `action: "tiro"`, `is_successful: true` para el tirador, y `action: "gol"`, `is_successful: true`. El balón se reinicia en el centro del campo (`x=52.5, y=34.0`) y la posesión pasa al equipo que ha encajado el gol (para el saque de centro).

La velocidad del balón en un tiro se fija en `random.uniform(20, 35)` m/s, será mayor que en un pase.

---

## Bucle principal y envío a Kafka

```python
while True:
    match_time += INTERVALO
    for ev in simulate_cycle(players, ball, match_time):
        producer.send(TOPIC, value=ev)
    producer.flush()
    time.sleep(INTERVALO)
```

El bucle principal es lo que mantiene la simulación corriendo indefinidamente. En cada iteración:

1. Suma 2 segundos al reloj del partido.
2. Llama a `simulate_cycle`, que devuelve todos los eventos del ciclo (entre 22 y 25 eventos dependiendo de si hubo acción).
3. Manda cada evento a Kafka con `producer.send`. El serializador definido en `create_producer` lo convierte automáticamente a JSON.
4. Llama a `producer.flush()` para asegurarse de que todos los mensajes se han enviado realmente antes de esperar. Sin esto, podrían quedarse en un buffer interno y no llegar a Kafka a tiempo.
5. Espera 2 segundos reales (`time.sleep`) antes de simular el siguiente ciclo.

Si se pulsa `Ctrl+C` para parar el script, el bloque `finally` se encarga de cerrar la conexión con Kafka de forma ordenada, liberando recursos.

Volver atrás: [Introducción](/Readme.md)
