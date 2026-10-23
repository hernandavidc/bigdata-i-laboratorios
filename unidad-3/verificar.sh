#!/bin/bash
# Ejecuta los laboratorios y compara el resultado con las salidas de referencia de la guía.
# Uso (dentro del contenedor de Spark):  bash verificar.sh
set -u
cd "$(dirname "$0")"
bash ejecutar_todo.sh salidas_verificacion
fallos=0

# Salidas que deben ser idénticas a las de la guía
for f in lotes_carpetas.txt lotes_exchange.txt cuarentena.txt flujo.txt reanudacion.txt; do
    if diff -q "salidas_de_referencia/$f" "salidas_verificacion/$f" >/dev/null; then
        echo "OK      $f"
    else
        echo "DIFIERE $f"; diff "salidas_de_referencia/$f" "salidas_verificacion/$f"; fallos=1
    fi
done

# Salidas con fecha u hora: se comprueba solo lo que no cambia entre ejecuciones
grep -q "2 passed" salidas_verificacion/pruebas.txt && echo "OK      pruebas: 2 passed" || { echo "FALLA   pruebas"; fallos=1; }
grep -q "código de salida: 0" salidas_verificacion/trabajo_normal.txt && echo "OK      trabajo normal publica" || { echo "FALLA   trabajo normal"; fallos=1; }
grep -q "código de salida: 1" salidas_verificacion/trabajo_incompleto.txt && echo "OK      trabajo incompleto no publica" || { echo "FALLA   trabajo incompleto"; fallos=1; }

exit $fallos
