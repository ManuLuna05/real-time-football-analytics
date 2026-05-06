import time, json, random, csv, os, uuid, sys
from kafka import KafkaProducer


# CONFIGURACIÓN
KAFKA_BROKER = 'localhost:9094'
TOPIC = 'football_events'
INTERVALO = 2 # Segundos entre ciclos

GOAL_Y_MIN = 22.32
GOAL_Y_MAX = 45.68
GOAL_X_A = 105.0 # Portería que defiende el equipo A (ataca B)
GOAL_X_B = 0.0 # Portería que defiende el equipo B (ataca A)

# Identificador único de este partido (se renueva en cada ejecución)
MATCH_ID = str(uuid.uuid4())

# GRUPOS DE POSICIONES
GK_POS  = {'GK'}
DEF_POS = {'CB', 'RB', 'LB', 'RWB', 'LWB'}
MID_POS = {'CDM', 'CM', 'CAM', 'RM', 'LM'}
FWD_POS = {'ST', 'CF', 'LW', 'RW', 'SS'}

# Formación 4-4-2: cuántos de cada línea por equipo
FORMATION = [
    ('GK',  GK_POS,  1),
    ('DEF', DEF_POS, 4),
    ('MID', MID_POS, 4),
    ('FWD', FWD_POS, 2),
]

# Crear el productor Kafka con bootstrap, serializador JSON y reintentos; devuelve None si falla.
def create_producer():
    try:
        # El serializador convierte el diccionario del evento a un string JSON codificado en UTF-8, y retries=5 permite reintentar enviar el mensaje hasta 5 veces
        p = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8'),
            retries=5,
        )
        print(f"[OK] Conectado a Kafka en {KAFKA_BROKER}  |  match_id={MATCH_ID}")
        return p
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a Kafka: {e}")
        return None


# Función auxiliar que intenta convertir a entero y usa el valor por defecto si falla.
def safe_int(v, default=50):
    try: return int(v)
    except: return default

# Cargar el CSV de jugadores, clasificarlos por posición y formar dos equipos con la formación 4-4-2. Devuelve una lista de diccionarios con los datos de cada jugador.
def load_players(filepath):
    buckets = {grp: [] for grp, _, _ in FORMATION} # Diccionario para clasificar jugadores por grupo de posición (GK, DEF, MID, FWD)

    # Lectura del CSV y clasificación de los jugadores por su posición
    with open(filepath, encoding='utf-8') as f:
        for row in csv.DictReader(f):
            pos = row.get('Position', '').strip()
            ovr = safe_int(row.get('OVR', 0))
            if ovr < 65: # Descartamos jugadores de bajo nivel
                continue
            for grp, pos_set, _ in FORMATION:
                if pos in pos_set:
                    buckets[grp].append(row)
                    break

    players = [] # Lista final de jugadores con su información y atributos necesarios para la simulación
    for team in ('A', 'B'): # Clasificamos entre dos equipos (A y B) asignando jugadores de cada grupo según la formación 4-4-2
        squad = []
        for grp, _, needed in FORMATION: # Colocamos a los jugadores en su posción correspondiente
            pool = buckets[grp] # Tomamos el grupo de jugadores que corresponden a esa posición (por ejemplo, DEF_POS para los defensas)
            if len(pool) < needed:
                print(f"[WARN] No hay suficientes jugadores en {grp} (tenemos {len(pool)}, necesitamos {needed})")
                chosen = pool[:needed]
            else:
                chosen = random.sample(pool, needed) # Elegimos aleatoriamente los jugadores necesarios para esa posición (por ejemplo, 4 defensas)
            squad.extend(chosen)

        # Para cada jugador, creamos un diccionario con su información. La posición inicial se asigna aleatoriamente dentro de la mitad del campo que le corresponde a su equipo, y los porteros se colocan cerca de su portería.
        for i, row in enumerate(squad): 
            pos = row.get('Position', 'CM').strip()
            is_gk = (pos == 'GK')

            # Posición inicial: A en mitad izquierda, B en mitad derecha
            x_ini = random.uniform(5, 50) if team == 'A' else random.uniform(55, 100)

            # Porteros cerca de su portería
            if is_gk:
                x_ini = 5.0 if team == 'A' else 100.0

            players.append({
                'id': row['ID'],
                'nombre': row['Name'],
                'equipo': team,
                'posicion': pos,
                'is_gk': is_gk,
                'velocidad': safe_int(row.get('PAC', 50)),
                'pase': safe_int(row.get('PAS', 50)),
                'tiro': safe_int(row.get('SHO', 50)),
                'resistencia': safe_int(row.get('PHY', 50)),
                'defensa': safe_int(row.get('DEF', 50)),
                'x': x_ini,
                'y': random.uniform(5, 63),
                'estamina': 100.0,
            })

    gk_a = sum(1 for p in players if p['equipo'] == 'A' and p['is_gk']) # Contamos cuántos porteros hay en el equipo A (debería ser 1)
    gk_b = sum(1 for p in players if p['equipo'] == 'B' and p['is_gk']) # Contamos cuántos porteros hay en el equipo B (debería ser 1)
    print(f"[INFO] Plantillas  A: {sum(1 for p in players if p['equipo']=='A')} jugadores ({gk_a} GK)"
          f"  |  B: {sum(1 for p in players if p['equipo']=='B')} jugadores ({gk_b} GK)")
    return players


# Funciones auxiliares
def nearest(players, bx, by, team, exclude_id=None): # Esta función se utiliza para determinar el jugador más cercano al balón
    cands = [p for p in players if p['equipo'] == team and p['id'] != exclude_id]
    return min(cands, key=lambda p: (p['x']-bx)**2 + (p['y']-by)**2) if cands else None

def get_gk(players, team): # Esta función se utiliza para obtener el portero de un equipo determinado, lo cual es importante para simular las acciones de tiro y parada.
    gks = [p for p in players if p['equipo'] == team and p['is_gk']]
    return gks[0] if gks else None

def base_event(match_time, player, ball): # Esta función se utiliza para crear un evento base con la información común de cada acción.
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

# Función encargada del movimiento de los jugadores
def move_player(player, bx, by):
    max_speed = player['velocidad'] / 10.0 # Convertimos la velocidad del jugador a una escala más manejable (0-10 m/s)
    fatigue = max(0.4, player['estamina'] / 100.0) # El desgaste afecta a la velocidad máxima, pero no la reduce a cero para evitar que los jugadores se queden completamente inmóviles (esto es una simplificación)
    speed = random.uniform(0, max_speed * fatigue) # La velocidad real en este ciclo es un valor aleatorio entre 0 y la velocidad máxima ajustada por el desgaste, lo que introduce variabilidad en el movimiento de los jugadores.

    if player['is_gk']: # Los porteros tienden a moverse hacia el balón pero con cierta aleatoriedad para simular su comportamiento de posicionamiento.
        tx = (GOAL_X_B + 5) if player['equipo'] == 'A' else (GOAL_X_A - 5) # El portero del equipo A se posiciona cerca de su portería, y el del equipo B también
        player['x'] = max(0.5, min(104.5, player['x'] + (tx - player['x']) * 0.3))
        player['y'] = max(0.5, min(67.5,  player['y'] + (34.0 - player['y']) * 0.1)) 
    else: # Los jugadores de campo se mueven hacia el balón con cierta aleatoriedad para simular su comportamiento en el campo.
        player['x'] = max(0.5, min(104.5, player['x'] + random.uniform(-speed, speed)))
        player['y'] = max(0.5, min(67.5,  player['y'] + random.uniform(-speed, speed)))

    # Cálculo del desgaste de la estamina
    desgaste = (speed / max(max_speed, 0.01)) * (1.0 - player['resistencia'] / 200.0) * INTERVALO * 0.5 
    player['estamina'] = max(0.0, player['estamina'] - desgaste)
    return round(speed, 2)


# Función principal que configura el productor Kafka, carga los jugadores, simula el partido y envía los eventos a Kafka.
def simulate_cycle(players, ball, match_time):
    events = [] # Lista de eventos generados en este ciclo, que se enviarán a Kafka.
    rival = 'B' if ball['possession'] == 'A' else 'A' # Identificamos a los dos equipos determinando rivales
    poss = ball['possession'] # El equipo que tiene la posesión del balón en este momento (A o B)

    # Mover todos los jugadores y emitir eventos de posición
    for p in players:
        sp = move_player(p, ball['x'], ball['y'])
        events.append({
            'match_id': MATCH_ID,
            'timestamp': match_time,
            'tipo': 'posicion',
            'player_id': p['id'],
            'player_nombre': p['nombre'],
            'player_equipo': p['equipo'],
            'player_posicion': p['posicion'],
            'x': round(p['x'], 2),
            'y': round(p['y'], 2),
            'speed_jugador': sp,
            'estamina': round(p['estamina'], 2),
            'ball_x': round(ball['x'], 2),
            'ball_y': round(ball['y'], 2),
            'ball_speed': round(ball['speed'], 2),
            'action': None,
            'is_successful': None,
        })

    # Acción con el balón
    passer = nearest(players, ball['x'], ball['y'], poss)
    if passer is None:
        return events

    r = random.random()

    if r < 0.45: # Si el número aleatorio es menor que 0.45, el jugador con el balón intentará un pase. Esto ocurre aproximadamente en el 45% de las acciones con balón, lo que refleja la frecuencia típica de los pases en un partido de fútbol.
        # PASE
        mates = [p for p in players if p['equipo'] == poss and p['id'] != passer['id'] and not p['is_gk']] # Hacemos que el pase se puede hacer a cualquier compañero (evitando a si mismo y al portero)
        if not mates: # Si no hay compañeros disponibles para pasar (lo cual es raro pero posible si el equipo tiene pocos jugadores o están muy dispersos), simplemente no hacemos nada con el balón en este ciclo.
            return events
        receiver = random.choice(mates) # Elegimos aleatoriamente un compañero para recibir el pase entre los disponibles (esto introduce variabilidad en el juego y evita que siempre se pase al mismo jugador)
        ok = random.randint(1, 100) <= passer['pase'] # La probabilidad de que el pase sea exitoso depende de la habilidad de pase del jugador que lo realiza

        ev = base_event(match_time, passer, ball) # Creamos un evento base para el pase, que luego actualizaremos con la acción específica y si fue exitosa o no.
        if ok: # Si el pase es exitoso, actualizamos el evento para reflejar que fue un pase exitoso, y movemos el balón hacia el receptor con cierta aleatoriedad para simular la trayectoria del pase.
            ev.update({'action': 'pase', 'is_successful': True}) # Actualizamos el evento para indicar que se trata de un pase exitoso.
            # El balón se mueve hacia el receptor, pero con cierta aleatoriedad para simular la trayectoria del pase. La velocidad del balón también se ajusta aleatoriamente dentro de un rango típico para un pase.
            ball.update({'x': receiver['x'] + random.uniform(-3,3),
                         'y': receiver['y'] + random.uniform(-3,3),
                         'speed': random.uniform(10,25),
                         'possession': poss})
        else:
            ev.update({'action': 'perdida', 'is_successful': False}) # Actualizamos el evento para indicar que se trata de una pérdida de balón.
            rv = nearest(players, ball['x'], ball['y'], rival) # Si el pase falla, el balón queda suelto y el jugador rival más cercano puede intentar recuperarlo.
            if rv: # Si hay un jugador rival cercano, actualizamos el balón para reflejar que ahora está en posesión del equipo rival, y creamos un evento de recuperación exitoso para ese jugador.
                ball.update({'x': rv['x']+random.uniform(-2,2),
                             'y': rv['y']+random.uniform(-2,2),
                             'speed': random.uniform(5,15),
                             'possession': rival})
                rec = base_event(match_time, rv, ball) # Creamos un evento base para la recuperación, que luego actualizaremos con la acción específica y que fue exitosa.
                rec.update({'action': 'recuperacion', 'is_successful': True}) # Actualizamos el evento para indicar que se trata de una recuperación exitosa.
                events.append(rec)
        events.append(ev)

    elif r < 0.55: # Si el número aleatorio está entre 0.45 y 0.55, el jugador con el balón intentará un tiro a puerta.
        # TIRO
        attackers = [p for p in players if p['equipo'] == poss and not p['is_gk']] # Solo los jugadores de campo pueden intentar un tiro, por lo que filtramos para excluir a los porteros.
        if not attackers: # Si no hay jugadores de campo disponibles para intentar un tiro (lo cual es raro pero posible si el equipo tiene pocos jugadores o están muy dispersos), no se hace nada con el balón.
            return events
        shooter = random.choice(attackers) # Elegimos un jugador de campo para intentar el tiro entre los disponibles (esto  evita que siempre sea el mismo jugador quien intente los tiros).
        goal_x = GOAL_X_A if poss == 'A' else GOAL_X_B # Determinamos a qué portería se dirige el tiro según el equipo que tiene la posesión del balón.
        rival_gk = get_gk(players, rival) # Obtenemos el portero del equipo rival, lo cual es importante para simular las acciones de tiro y parada.

        shot_y = random.uniform(GOAL_Y_MIN - 3, GOAL_Y_MAX + 3) # El tiro se dirige a una posición aleatoria dentro de un rango que incluye el área de la portería y un poco más allá para simular tiros desviados.
        on_target = (GOAL_Y_MIN <= shot_y <= GOAL_Y_MAX) # Determinamos si el tiro va entre los tres palos de la portería, lo cual es necesario para que pueda ser un gol o una parada.

        ev = base_event(match_time, shooter, ball) # Creamos un evento base para el tiro, que luego actualizaremos con la acción específica y si fue exitoso o no.
        ev['action'] = 'tiro' # Actualizamos el evento para indicar que se trata de un tiro.

        if not on_target: # Si el tiro no va entre los palos, es un tiro fallido que no requiere intervención del portero, y el balón queda suelto para que el equipo rival intente recuperarlo.
            ev['is_successful'] = False # Actualizamos el evento para indicar que el tiro no fue exitoso (no fue a puerta).
            # El balón se mueve hacia la posición del tiro, pero con cierta aleatoriedad para simular la trayectoria del tiro. La velocidad del balón también se ajusta aleatoriamente dentro de un rango típico para un tiro.
            ball.update({'x': goal_x + random.uniform(-10,10), 'y': shot_y, 
                         'speed': random.uniform(20,35), 'possession': rival})
        else: # Si el tiro va entre los palos, el portero tiene la oportunidad de intentar una parada. La probabilidad de que el portero realice una parada exitosa depende de su habilidad de defensa.
            gk_ok = rival_gk and (random.randint(1,100) <= rival_gk['defensa'])
            if gk_ok: # Si el portero realiza una parada exitosa, actualizamos el evento para reflejar que el tiro fue detenido por el portero, y creamos un evento de parada para el portero.
                ev['is_successful'] = False # Actualizamos el evento del tiro para indicar que no fue exitoso (fue detenido por el portero).
                gkev = base_event(match_time, rival_gk, ball) # Creamos un evento base para la parada, que luego actualizaremos con la acción específica y que fue exitosa.
                gkev.update({'action': 'parada', 'is_successful': True}) # Actualizamos el evento para indicar que se trata de una parada exitosa.
                events.append(gkev)
                ball.update({'x': rival_gk['x']+random.uniform(-5,5),
                             'y': rival_gk['y']+random.uniform(-3,3),
                             'speed': random.uniform(5,15), 'possession': rival})
            else: # Si el portero no logra detener el tiro, es un gol para el equipo atacante. Actualizamos el evento para reflejar que el tiro fue exitoso y se convirtió en gol.
                ev['is_successful'] = True
                gev = base_event(match_time, shooter, ball)
                gev.update({'action': 'gol', 'is_successful': True})
                events.append(gev)
                ball.update({'x': 52.5, 'y': 34.0, 'speed': 0.0, 'possession': rival}) # Después de un gol, el balón se reinicia en el centro del campo y la posesión pasa al equipo rival para el saque inicial.

        events.append(ev)

    return events


# PUNTO DE ENTRADA
if __name__ == '__main__':
    candidates = ['../data/Men_Players.csv', 'data/Men_Players.csv'] # Rutas donde se puede encontrar el archivo CSV de jugadores. Esto permite flexibilidad en la ubicación del archivo.
    csv_path = next((p for p in candidates if os.path.exists(p)), None) # Buscamos la primera ruta que exista y la usamos como csv_path. Si no se encuentra ningún archivo, csv_path será None.
    if not csv_path: # Si no se encontró el archivo CSV en ninguna de las rutas candidatas, mostramos un mensaje de error y salimos del programa.
        print('[ERROR] No se encontró Men_Players.csv')
        sys.exit(1)

    producer = create_producer() # Intentamos crear el productor Kafka para enviar los eventos. Si la conexión falla, create_producer devuelve None.
    if not producer:
        print('Instala: pip install kafka-python')
        sys.exit(1)

    print(f'[INFO] Cargando jugadores desde {csv_path}...')
    players = load_players(csv_path) # Cargamos los jugadores desde el archivo CSV, clasificándolos por posición y formando dos equipos según la formación 4-4-2.

    ball = {'x': 52.5, 'y': 34.0, 'speed': 0.0, 'possession': 'A'} # El balón comienza en el centro del campo con el equipo A en posesión.
    match_time = 0 # El tiempo del partido comienza en 0 segundos.

    print(f'[INFO] Partido iniciado. Enviando a Kafka topic "{TOPIC}" cada {INTERVALO}s  (Ctrl+C para parar)')
    try:
        while True: # En cada ciclo del partido, incrementamos el tiempo, simulamos las acciones de los jugadores y el balón, generamos eventos y los enviamos a Kafka.
            match_time += INTERVALO # Incrementamos el tiempo del partido en el intervalo definido.
            for ev in simulate_cycle(players, ball, match_time): # Simulamos un ciclo del partido, lo que genera una lista de eventos que ocurrieron en ese ciclo (movimientos de jugadores, acciones con el balón, etc.).
                producer.send(TOPIC, value=ev) # Enviamos cada evento generado en este ciclo al topic de Kafka definido. El productor se encarga de serializar el evento a JSON y enviarlo al broker.
            producer.flush() # Aseguramos que todos los eventos generados en este ciclo se envíen a Kafka antes de continuar con el siguiente ciclo.
            print(f'  [t={match_time:>5}s] balón=({ball["x"]:.1f},{ball["y"]:.1f}) posesión={ball["possession"]}')
            time.sleep(INTERVALO) # Esperamos el intervalo definido antes de simular el siguiente ciclo, lo que controla la velocidad a la que se generan los eventos y se simula el partido.
    except KeyboardInterrupt:
        print('\n[INFO] Simulación detenida.')
    finally:
        producer.close() # Cerramos el productor Kafka para liberar recursos y cerrar la conexión con el broker de manera ordenada.