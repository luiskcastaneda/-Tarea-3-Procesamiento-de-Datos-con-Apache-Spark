# Importamos librerías necesarias
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DateType

# Iniciamos la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

# Definimos la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'

# Leemos el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# Imprimimos el esquema del DataFrame
df.printSchema()

# Mostramos las primeras filas del DataFrame
df.show()

# Realizamos Estadísticas básicas
df.summary().show()

# -------------------------------------------------
# Limpieza de Datos

# 1. Eliminamos columnas que no son necesarias
# Ejemplo: Si hay alguna columna irrelevante, puedes eliminarla.
# df = df.drop("columna_irrelevante")

# 2. Manejo de valores nulos
# Eliminamos filas con valores nulos en las columnas críticas, por ejemplo, 'Cantidad' y 'Laboratorio_Vacuna'
df = df.dropna(subset=["Cantidad", "Laboratorio_Vacuna"])

# 3. Convertir tipos de datos si es necesario
# Aseguramos que la columna 'Cantidad' sea del tipo Integer y 'fecha_corte' sea de tipo Date
df = df.withColumn("Cantidad", df["Cantidad"].cast(IntegerType()))
df = df.withColumn("fecha_corte", df["fecha_corte"].cast(DateType()))

# 4. Eliminamos duplicados
df = df.dropDuplicates()

# 5. Verificamos si existen valores inconsistentes o negativos en la columna 'Cantidad'
# Por ejemplo, valores negativos en 'Cantidad' no deberían existir.
df = df.filter(F.col('Cantidad') >= 0)

# 6. Transformaciones adicionales si es necesario (por ejemplo, convertir a mayúsculas, limpieza de cadenas)
df = df.withColumn("Laboratorio_Vacuna", F.upper(F.col("Laboratorio_Vacuna")))

# -------------------------------------------------
# Realizamos la consulta y transformación de los datos

# Consulta: Filtrar por valor y seleccionar columnas
print("Vacunas aplicadas con cantidad mayor a 100\n")
dias = df.filter(F.col('Cantidad') > 100).select('Cantidad', 'Laboratorio_Vacuna', 'fecha_corte')
dias.show()

# Ordenar filas por los valores en la columna "Cantidad" en orden descendente
print("Valores ordenados de mayor a menor\n")
sorted_df = df.sort(F.col("Cantidad").desc())
sorted_df.show()

# -------------------------------------------------
# Si deseas realizar más transformaciones como agregados, agrupar datos, etc.

# Ejemplo: Contar el total de vacunas aplicadas por laboratorio
print("Total de vacunas aplicadas por laboratorio")
vacunas_por_laboratorio = df.groupBy('Laboratorio_Vacuna').agg(F.sum('Cantidad').alias('Total_Vacunas'))
vacunas_por_laboratorio.show()

# Si quieres guardar el resultado transformado en HDFS
# sorted_df.write.format('csv').option('header', 'true').save('hdfs://localhost:9000/Tarea3/vacunas_transformadas.csv')
