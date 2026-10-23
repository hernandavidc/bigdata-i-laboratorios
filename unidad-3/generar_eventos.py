"""Simula la llegada de eventos en cinco lotes (un archivo JSON por lote), sin servicios externos.
Incluye a propósito un duplicado, un dato tardío aceptado y un dato tardío descartado."""
import json
import os
import shutil
import time

LOTES = {
    "lote_01": [("E1", "EST-A", "12:01", 20), ("E2", "EST-A", "12:03", 30), ("E3", "EST-B", "12:02", 10)],
    "lote_02": [("E4", "EST-A", "12:06", 40), ("E5", "EST-B", "12:08", 20),
                ("E2", "EST-A", "12:03", 30),     # duplicado: el sensor reenvió E2
                ("E11", "EST-B", "12:04", 40)],   # tardío, pero dentro de la marca de agua
    "lote_03": [("E6", "EST-A", "12:14", 50), ("E7", "EST-B", "12:16", 30)],
    "lote_04": [("E9", "EST-A", "12:27", 10)],
    "lote_05": [("E8", "EST-A", "12:04", 90),     # tardío: su ventana ya se publicó
                ("E10", "EST-B", "12:41", 15)],
}

shutil.rmtree("eventos", ignore_errors=True)
os.makedirs("eventos")
# Spark lee los archivos en orden de fecha de modificación: se separan un segundo para fijar ese orden
inicio = time.time() - 60
for i, (nombre, eventos) in enumerate(LOTES.items()):
    ruta = f"eventos/{nombre}.json"
    with open(ruta, "w") as f:
        for id_evento, estacion, hora, pm25 in eventos:
            f.write(json.dumps({"id_evento": id_evento, "id_estacion": estacion,
                                "hora_evento": f"2026-10-14 {hora}:00", "pm25": pm25}) + "\n")
    os.utime(ruta, (inicio + i, inicio + i))
print("Lotes generados:", ", ".join(LOTES))
