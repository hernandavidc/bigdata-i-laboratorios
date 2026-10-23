"""Versión operable del laboratorio: registros, cuarentena de filas corruptas y validación de volumen."""
import logging
import sys

from pyspark.sql import SparkSession, functions as F

from transformaciones import limpiar, resumir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("calidad_aire")
MINIMO_ESPERADO = 1000          # regla de volumen acordada con el negocio

spark = SparkSession.builder.appName("calidad-aire-diaria").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

esquema = "id_estacion STRING, fecha_hora TIMESTAMP, pm25 DOUBLE, _registro_corrupto STRING"
crudo = (spark.read.option("header", True).schema(esquema)
         .option("mode", "PERMISSIVE")                       # no detiene la carga por una fila mala
         .option("columnNameOfCorruptRecord", "_registro_corrupto")
         .csv("datos/crudo/calidad_aire/")
         .cache())

corruptas = crudo.filter(F.col("_registro_corrupto").isNotNull())
n_corruptas = corruptas.count()
if n_corruptas:
    corruptas.write.mode("overwrite").json("datos/cuarentena/calidad_aire")
    log.warning("filas corruptas enviadas a cuarentena: %d", n_corruptas)

limpias = limpiar(crudo.filter(F.col("_registro_corrupto").isNull()).drop("_registro_corrupto"))
n_crudas, n_limpias = crudo.count(), limpias.count()
log.info("filas crudas=%d limpias=%d", n_crudas, n_limpias)

if n_limpias < MINIMO_ESPERADO:                            # mejor no publicar que publicar mal
    log.error("volumen insuficiente: %d < %d; no se publica", n_limpias, MINIMO_ESPERADO)
    sys.exit(1)

resumir(limpias).write.mode("overwrite").partitionBy("fecha").parquet("datos/consumo/calidad_aire_diaria")
log.info("publicado en datos/consumo/calidad_aire_diaria")
