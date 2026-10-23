"""Paso por completar: promedios en ventanas deslizantes de 10 minutos que avanzan cada 5 minutos.
Usa la misma fuente simulada de la sección 3.2 (ejecuta antes: python3 generar_eventos.py)."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("flujo-deslizante").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

esquema = "id_evento STRING, id_estacion STRING, hora_evento TIMESTAMP, pm25 DOUBLE"
lecturas = (spark.readStream.schema(esquema)
            .option("maxFilesPerTrigger", 1)
            .json("eventos/"))

# COMPLETA: reemplaza None por una ventana de 10 minutos que avance cada 5 minutos sobre "hora_evento".
# Busca en la guía de Structured Streaming cómo se declara una ventana deslizante.
ventana = None

promedios = (lecturas
             .withWatermark("hora_evento", "10 minutes")
             .dropDuplicates(["id_evento", "hora_evento"])
             .groupBy(ventana, "id_estacion")
             .agg(F.avg("pm25").alias("pm25_promedio"), F.count("*").alias("lecturas")))

consulta = (promedios.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", "datos/consumo/aire_deslizante")
            .option("checkpointLocation", "checkpoints/aire_deslizante")
            .start())
consulta.processAllAvailable()
consulta.stop()

(spark.read.parquet("datos/consumo/aire_deslizante")
      .select(F.date_format("window.start", "HH:mm").alias("inicio"),
              F.date_format("window.end", "HH:mm").alias("fin"),
              "id_estacion", F.round("pm25_promedio", 1).alias("pm25_promedio"), "lecturas")
      .orderBy("inicio", "id_estacion")
      .show(50))
