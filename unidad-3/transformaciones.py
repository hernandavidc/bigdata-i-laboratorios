from pyspark.sql import DataFrame, functions as F


def limpiar(df: DataFrame) -> DataFrame:
    """Descarta valores imposibles y lecturas repetidas; agrega la fecha."""
    return (df.filter(F.col("pm25").between(0, 1000))
              .dropDuplicates(["id_estacion", "fecha_hora"])
              .withColumn("fecha", F.to_date("fecha_hora")))


def resumir(df: DataFrame) -> DataFrame:
    """Promedio, máximo y número de lecturas por estación y día."""
    return (df.groupBy("fecha", "id_estacion")
              .agg(F.round(F.avg("pm25"), 1).alias("pm25_promedio"),
                   F.max("pm25").alias("pm25_maximo"),
                   F.count("*").alias("lecturas")))
