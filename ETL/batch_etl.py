from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def main():
    # Inicializar SparkSession
    spark = SparkSession.builder \
        .appName("FIFA_Players_Batch_ETL") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Rutas de entrada y salida (HDFS)
    men_data_path = "hdfs://namenode:9000/data/Men_Players.csv"
    women_data_path = "hdfs://namenode:9000/data/Women_Players.csv"
    output_path = "hdfs://namenode:9000/data/processed/players_parquet"

    # Extracción de los datos desde los CSV en HDFS.
    try:
        df_men = spark.read.csv(men_data_path, header=True, inferSchema=True)
        df_women = spark.read.csv(women_data_path, header=True, inferSchema=True)
    except Exception as e: # Controlamos el error en caso de que no se puedan leer los archivos CSV desde HDFS.
        print(f"[ERROR] Leyendo CSVs desde HDFS: {e}")
        spark.stop()
        return

    # TRANSFORMACIÓN
    # Unimos ambos DataFrames de hombres y mujeres en uno solo. Esto nos permite procesar ambos géneros juntos y aplicar las mismas transformaciones a todos los jugadores.
    df_all = df_men.unionByName(df_women, allowMissingColumns=True)

    # Seleccionamos y renombramos las columnas relevantes:
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

    df_cleaned = df_all.select(*[col(c).alias(alias) for c, alias in columns_to_keep.items()]) # Seleccionamos solo las columnas que nos interesan y les damos nombres más amigables para nuestro análisis.

    # Añadir columna estamina inicializada a 100 para todos los jugadores
    df_final = df_cleaned.withColumn("estamina", lit(100.0))

    # Eliminar registros con valores nulos en estadísticas clave
    df_final = df_final.na.drop(
        subset=["id", "nombre", "velocidad", "pase", "tiro", "resistencia", "defensa"]
    )

    # CARGA DE LOS DATOS PROCESADOS
    total = df_final.count() # Contamos el total de jugadores procesados para mostrarlo en el mensaje de información antes de escribir los datos en HDFS.
    print(f"[INFO] Escribiendo {total} jugadores en {output_path} (formato Parquet)...")
    df_final.write.mode("overwrite").parquet(output_path) # Escribimos el DataFrame final en formato Parquet en HDFS, sobrescribiendo cualquier dato existente en esa ruta.
    print("[OK] Batch ETL completado con éxito.")

    spark.stop() # Detenemos la sesión de Spark para liberar recursos.

if __name__ == "__main__":
    main()