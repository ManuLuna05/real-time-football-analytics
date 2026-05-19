# Documentación: Procesamiento Batch (`batch_etl.py`)

Este script se encarga de la preparación y limpieza inicial de los datos históricos de los jugadores. Se ejecuta **una única vez** antes de arrancar la simulación en streaming, y su trabajo es dejar lista una tabla limpia y optimizada con todos los jugadores que podrían participar en un partido. Utiliza **PySpark** para hacer ese trabajo de forma distribuida sobre los ficheros que están almacenados en HDFS.

El proceso sigue la estructura clásica de cualquier ETL, primero extrae los datos de su origen, luego los transforma y limpia, y finalmente los guarda en un formato mejor para el sistema.

---

## Configuración inicial: rutas de ficheros

```python
men_data_path = "hdfs://namenode:9000/data/Men_Players.csv"
women_data_path = "hdfs://namenode:9000/data/Women_Players.csv"
output_path = "hdfs://namenode:9000/data/processed/players_parquet"
```

Antes de hacer nada, se definen las rutas de entrada y salida. Las tres rutas apuntan a HDFS. `namenode:9000` es el nombre del contenedor Docker que actúa como puerta de entrada a HDFS.

---

## `main()`

Es la única función del script y contiene todo el proceso de principio a fin. Se divide en tres bloques bien diferenciados.

---

### Bloque 1 - Extracción: leer los CSV desde HDFS

```python
spark = SparkSession.builder \
    .appName("FIFA_Players_Batch_ETL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
```

Lo primero es arrancar Spark. La `SparkSession` es el punto de entrada a todo lo que hace PySpark: sin ella no se puede leer, transformar ni escribir nada. El `appName` es simplemente el nombre con el que este trabajo aparecerá en los logs y en la interfaz web de Spark. El `setLogLevel("WARN")` le dice a Spark que solo muestre mensajes de aviso o error, y que se calle todo el ruido informativo que genera por defecto, que es bastante.

```python
try:
    df_men = spark.read.csv(men_data_path, header=True, inferSchema=True)
    df_women = spark.read.csv(women_data_path, header=True, inferSchema=True)
except Exception as e:
    print(f"[ERROR] Leyendo CSVs desde HDFS: {e}")
    spark.stop()
    return
```

Aquí se leen los dos CSV. Los parámetros más importantes son:
- `header=True` indica a Spark que la primera fila del CSV contiene los nombres de las columnas, no datos reales.
- `inferSchema=True` hace que Spark analice el contenido de cada columna y decida automáticamente si es un número, un texto, etc. Sin esto, leería todo como texto plano.

El bloque `try/except` protege la lectura: si HDFS no está disponible o los ficheros no existen, el script imprime el error, cierra Spark correctamente y termina en lugar de dar un error incontrolado.

---

### Bloque 2 - Transformación: limpiar y estructurar los datos

**Unir los dos CSV en uno solo**

```python
df_all = df_men.unionByName(df_women, allowMissingColumns=True)
```

`unionByName` apila las filas de ambos DataFrames en uno solo, emparejando las columnas por nombre en lugar de por posición. Esto es importante porque los CSV de hombres y mujeres pueden no tener exactamente las mismas columnas ni en el mismo orden. El parámetro `allowMissingColumns=True` hace que si una columna existe en uno pero no en el otro, en lugar de dar error la rellene con nulos. Sin este parámetro, si un CSV tuviera aunque fuera una columna de diferencia, el script fallaría.

**Seleccionar y renombrar solo las columnas que interesan**

```python
columns_to_keep = {
    "ID": "id",
    "Name": "nombre",
    "GENDER": "genero",
    "PAC": "velocidad",
    "PAS": "pase",
    "SHO": "tiro",
    "PHY": "resistencia",
    "DEF": "defensa",
    "Position": "posicion",
    "OVR": "media",
    "Team": "equipo",
}

df_cleaned = df_all.select(*[col(c).alias(alias) for c, alias in columns_to_keep.items()])
```

Los CSV de FIFA tienen decenas de columnas (URLs de fotos, valoraciones de habilidades específicas, datos de contrato...) que no se usan en ningún momento en este proyecto. Esta línea se queda solo con las 11 columnas útiles y, de paso, les da nombres en español más legibles para el resto del sistema. La expresión `col(c).alias(alias)` coge cada columna por su nombre original (`c`) y la renombra al alias definido en el diccionario.

**Añadir la columna de estamina**

```python
df_final = df_cleaned.withColumn("estamina", lit(100.0))
```

Todos los jugadores arrancan con la energía al máximo. `withColumn` añade una columna nueva al DataFrame, y `lit(100.0)` es la forma de decirle a Spark que esa columna tiene un valor fijo para todas las filas, no que lo calcule a partir de otra columna. Si se pusiera `100.0` directamente sin `lit()`, Spark no sabría interpretarlo.

**Eliminar filas con datos incompletos**

```python
df_final = df_final.na.drop(
    subset=["id", "nombre", "velocidad", "pase", "tiro", "resistencia", "defensa"]
)
```

Si un jugador tiene alguna de estas estadísticas vacía, no sirve para la simulación porque el generador necesita todos esos valores para calcular movimientos, pases y tiros. `na.drop` elimina del DataFrame cualquier fila que tenga un nulo en al menos una de las columnas del `subset`. No se incluyen columnas como `equipo` o `posicion` porque un nulo ahí no impide la simulación, solo es un dato que falta.

---

### Bloque 3 - Carga: guardar el resultado en HDFS

```python
total = df_final.count()
print(f"[INFO] Escribiendo {total} jugadores en {output_path} (formato Parquet)...")
df_final.write.mode("overwrite").parquet(output_path)
```

Primero se cuenta cuántos jugadores han sobrevivido a la limpieza, simplemente para tener ese dato visible en los logs y saber que el proceso ha ido bien. Luego se escribe el resultado.

El formato **Parquet** es columnar: en lugar de guardar los datos fila a fila como hace un CSV, los guarda columna a columna. Esto tiene dos ventajas grandes para este proyecto: ocupa mucho menos espacio en disco gracias a la compresión, y cuando Spark tiene que leer solo algunas columnas (por ejemplo, solo `velocidad` y `resistencia`), no necesita leer el fichero entero, solo las columnas que le interesan.

El `mode("overwrite")` significa que si ya existe un resultado de una ejecución anterior en esa ruta, lo borra y escribe el nuevo. Esto es lo correcto aquí porque el batch ETL siempre produce la misma tabla base.

```python
spark.stop()
```

Al final se cierra la sesión de Spark. Sin esto, el proceso Spark seguiría consumiendo recursos del sistema aunque el script haya terminado de hacer su trabajo.

Volver atrás: [Introducción](/Proyecto.md)
