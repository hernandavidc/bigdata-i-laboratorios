import json

# Simula que, después de una interrupción, llega un lote nuevo
with open("eventos/lote_06.json", "w") as f:
    f.write(json.dumps({"id_evento": "E12", "id_estacion": "EST-A",
                        "hora_evento": "2026-10-14 12:58:00", "pm25": 35}) + "\n")
