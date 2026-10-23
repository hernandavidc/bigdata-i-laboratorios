from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("laboratorio-lotes").getOrCreate()

# Paso 2. Leer con un esquema explícito y explorar
esquema = "id_estacion STRING, fecha_hora TIMESTAMP, pm25 DOUBLE"
crudo = spark.read.option("header", True).schema(esquema).csv("datos/crudo/calidad_aire/")
crudo.printSchema()
print("Filas crudas:", crudo.count())

# Paso 3. Transformaciones estrechas: cada partición se procesa por separado
validas = (crudo
           .filter(F.col("pm25").between(0, 1000))
           .withColumn("fecha", F.to_date("fecha_hora")))
print("Filas con valores válidos:", validas.count())

# Paso 4. Transformaciones amplias: requieren redistribuir datos (shuffle)
limpias = validas.dropDuplicates(["id_estacion", "fecha_hora"])
print("Filas sin duplicados:", limpias.count())
resumen = (limpias.groupBy("fecha", "id_estacion")
           .agg(F.round(F.avg("pm25"), 1).alias("pm25_promedio"),
                F.max("pm25").alias("pm25_maximo"),
                F.count("*").alias("lecturas"))
           .orderBy("fecha", "id_estacion"))
resumen.explain()          # busca las líneas "Exchange": son los shuffles

# Paso 5. Acción: escribir particionado por fecha y comprobar
resumen.write.mode("overwrite").partitionBy("fecha").parquet("datos/consumo/calidad_aire_diaria")
spark.read.parquet("datos/consumo/calidad_aire_diaria").orderBy("fecha", "id_estacion").show()
