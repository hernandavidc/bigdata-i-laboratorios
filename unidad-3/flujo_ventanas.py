from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("flujo-ventanas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# Para repetir desde cero, borra las carpetas checkpoints/ y datos/consumo/aire_5min/

esquema = "id_evento STRING, id_estacion STRING, hora_evento TIMESTAMP, pm25 DOUBLE"

# Fuente simulada: cada archivo de la carpeta "eventos" se lee como un micro-lote, en orden
lecturas = (spark.readStream.schema(esquema)
            .option("maxFilesPerTrigger", 1)
            .json("eventos/"))

promedios = (lecturas
             .withWatermark("hora_evento", "10 minutes")
             .dropDuplicates(["id_evento", "hora_evento"])
             .groupBy(F.window("hora_evento", "5 minutes"), "id_estacion")
             .agg(F.avg("pm25").alias("pm25_promedio"), F.count("*").alias("lecturas")))

consulta = (promedios.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", "datos/consumo/aire_5min")
            .option("checkpointLocation", "checkpoints/aire_5min")   # posiciones leídas y estado
            .start())
consulta.processAllAvailable()                      # procesa todos los lotes disponibles

for p in consulta.recentProgress:                   # evidencia: qué pasó en cada micro-lote
    descartadas = sum(op.get("numRowsDroppedByWatermark", 0) for op in p["stateOperators"])
    print(f"micro-lote {p['batchId']}: filas leídas={p['numInputRows']}, "
          f"marca de agua={p['eventTime'].get('watermark', '-')[11:16]}, "
          f"descartadas por tardías={descartadas}")

(spark.read.parquet("datos/consumo/aire_5min")
      .select(F.date_format("window.start", "HH:mm").alias("inicio"),
              F.date_format("window.end", "HH:mm").alias("fin"),
              "id_estacion", "pm25_promedio", "lecturas")
      .orderBy("inicio", "id_estacion")
      .show())
consulta.stop()
