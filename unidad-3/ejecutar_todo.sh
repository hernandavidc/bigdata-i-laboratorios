#!/bin/bash
# Ejecuta todos los laboratorios de la Unidad 3 desde cero y guarda las salidas en una carpeta.
# Uso (dentro del contenedor de Spark):  bash ejecutar_todo.sh [carpeta_de_salidas]
set -u
cd "$(dirname "$0")"
SALIDAS="${1:-salidas}"
rm -rf datos eventos checkpoints "$SALIDAS" && mkdir -p "$SALIDAS"
SUBMIT="/opt/spark/bin/spark-submit --conf spark.ui.showConsoleProgress=false"
export PYTHONPATH=/opt/spark/python:$(ls /opt/spark/python/lib/py4j-*.zip)
python3 -m pip install --quiet pytest >/dev/null 2>&1

# Laboratorio por lotes (sección 2.2)
{ python3 generar_lecturas.py; $SUBMIT laboratorio_lotes.py 2>/dev/null; } > "$SALIDAS/lotes_completo.txt"
grep -E "Exchange" "$SALIDAS/lotes_completo.txt" | sed -E 's/#[0-9]+//g; s/\[plan_id=[0-9]+\]//' > "$SALIDAS/lotes_exchange.txt"
ls datos/consumo/calidad_aire_diaria > "$SALIDAS/lotes_carpetas.txt"

# Pruebas automáticas (sección 4.3)
python3 -m pytest -v -p no:cacheprovider test_transformaciones.py 2>&1 | grep -E "PASSED|FAILED|passed|failed" > "$SALIDAS/pruebas.txt"

# Trabajo operable: ejecución normal con una fila corrupta y ejecución con datos incompletos (sección 4.3)
echo "EST-03,no-es-una-fecha,abc" >> datos/crudo/calidad_aire/lecturas.csv
$SUBMIT trabajo_diario.py 2>&1 | grep "calidad_aire:" > "$SALIDAS/trabajo_normal.txt"
echo "código de salida: ${PIPESTATUS[0]}" >> "$SALIDAS/trabajo_normal.txt"
cat datos/cuarentena/calidad_aire/*.json > "$SALIDAS/cuarentena.txt"
head -500 datos/crudo/calidad_aire/lecturas.csv > /tmp/parcial.csv && cp /tmp/parcial.csv datos/crudo/calidad_aire/lecturas.csv
$SUBMIT trabajo_diario.py 2>&1 | grep "calidad_aire:" > "$SALIDAS/trabajo_incompleto.txt"
echo "código de salida: ${PIPESTATUS[0]}" >> "$SALIDAS/trabajo_incompleto.txt"

# Flujo con fuente simulada (sección 3.2) y reanudación desde el checkpoint (sección 4.3)
python3 generar_eventos.py > "$SALIDAS/flujo.txt"
$SUBMIT flujo_ventanas.py 2>/dev/null >> "$SALIDAS/flujo.txt"
python3 agregar_lote_06.py
$SUBMIT flujo_ventanas.py 2>/dev/null > "$SALIDAS/reanudacion.txt"
echo "Salidas guardadas en $SALIDAS/"
