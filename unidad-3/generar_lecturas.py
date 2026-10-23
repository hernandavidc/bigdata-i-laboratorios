"""Genera un conjunto de lecturas de calidad del aire reproducible (siempre el mismo)."""
import csv
import os
import random
from datetime import datetime, timedelta

random.seed(2026)                      # misma semilla = mismos datos en todos los computadores
ESTACIONES = ["EST-01", "EST-02", "EST-03", "EST-04", "EST-05"]
INICIO = datetime(2026, 10, 13, 0, 0)

filas = []
for est in ESTACIONES:
    for i in range(2 * 24 * 6):        # dos días, una lectura cada 10 minutos
        hora = INICIO + timedelta(minutes=10 * i)
        pm25 = round(random.gauss(22, 8), 1)
        filas.append([est, hora.strftime("%Y-%m-%d %H:%M:%S"), max(pm25, 0.5)])

filas += random.sample(filas, 20)       # 20 lecturas duplicadas (reenvíos del sensor)
for f in random.sample(filas, 10):      # 10 lecturas con valores imposibles
    filas.append([f[0], f[1], random.choice([-1, 9999])])

os.makedirs("datos/crudo/calidad_aire", exist_ok=True)
with open("datos/crudo/calidad_aire/lecturas.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["id_estacion", "fecha_hora", "pm25"])
    w.writerows(filas)
print(f"Archivo generado con {len(filas)} filas")
