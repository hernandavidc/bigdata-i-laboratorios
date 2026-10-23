# Laboratorios de Big Data I

Código de los laboratorios del espacio académico **Big Data I** (programa de Ciencia de Datos, Universidad de La Salle). Cada carpeta corresponde a una unidad y acompaña a su guía de contenido. La guía explica los conceptos; este repositorio contiene el código completo, listo para ejecutar.

Todo el código se ejecutó con **Apache Spark 3.5.3** en la imagen oficial de Docker `spark:3.5.3-python3`. Las salidas que aparecen en la guía están en `salidas_de_referencia/`.

## Unidad 3. Procesamiento de datos en la nube

| Archivo | Sección de la guía | Qué hace |
|---|---|---|
| `generar_lecturas.py` | 2.2, paso 1 | Genera dos días de lecturas de calidad del aire, siempre iguales (semilla fija), con duplicados y valores imposibles a propósito |
| `laboratorio_lotes.py` | 2.2, pasos 2 a 5 | Flujo por lotes: leer, filtrar, deduplicar, agregar y escribir particionado |
| `generar_eventos.py` | 3.2 | Fuente de eventos simulada: cinco lotes con un duplicado, un dato tardío aceptado y otro descartado |
| `flujo_ventanas.py` | 3.2 y 4.3 | Promedios por ventana con marca de agua; muestra el monitoreo de cada micro-lote |
| `flujo_deslizante_por_completar.py` | 3.2, paso por completar | Reto para el estudiante: completar la ventana deslizante consultando la documentación |
| `transformaciones.py` | 4.3 | Lógica de limpieza y resumen, separada para poder probarla |
| `test_transformaciones.py` | 4.3 | Pruebas automáticas con pytest |
| `trabajo_diario.py` | 4.3 | Versión operable: cuarentena de filas corruptas, registros y validación de volumen |
| `agregar_lote_06.py` | 4.3 | Simula un lote nuevo para comprobar la reanudación desde el checkpoint |
| `ejecutar_todo.sh` | — | Ejecuta todos los laboratorios desde cero |
| `verificar.sh` | — | Ejecuta todo y compara con las salidas de la guía |

### Cómo empezar

1. Instala [Docker Desktop](https://www.docker.com/products/docker-desktop/). No necesitas instalar Java ni Spark.
2. Descarga este repositorio y entra a la carpeta de la unidad:

   ```bash
   git clone https://github.com/hernandavidc/bigdata-i-laboratorios.git
   cd bigdata-i-laboratorios/unidad-3
   ```

3. Abre el entorno de Spark. En Linux o macOS:

   ```bash
   docker run -it --rm -u root -p 4040:4040 -v "$PWD":/work -w /work spark:3.5.3-python3 bash
   ```

   En PowerShell de Windows usa `-v "${PWD}:/work"` en lugar de `-v "$PWD":/work`.

4. Dentro del contenedor, sigue los pasos de la guía. Por ejemplo, el laboratorio por lotes:

   ```bash
   python3 generar_lecturas.py
   /opt/spark/bin/spark-submit laboratorio_lotes.py
   ```

Para ver la interfaz de Spark, abre `/opt/spark/bin/pyspark`, ejecuta el código paso a paso y visita `http://localhost:4040` mientras la sesión esté abierta.

### Comprobar que todo funciona

Dentro del contenedor:

```bash
bash verificar.sh
```

El script ejecuta los laboratorios desde cero y compara el resultado con `salidas_de_referencia/`. Cada vez que se actualiza el repositorio, GitHub Actions ejecuta la misma verificación (pestaña **Actions**).

### El paso por completar

`flujo_deslizante_por_completar.py` está incompleto a propósito: si se ejecuta tal como está, Spark se detiene con un error. El reto es completar la línea marcada con `COMPLETA` buscando en la [guía de Structured Streaming](https://spark.apache.org/docs/latest/streaming/index.html) cómo se declara una ventana deslizante. Este repositorio no incluye la solución.

## Cómo citar

Alvarez Caballero, H. D. (2026). *Laboratorios de Big Data I* (Versión 1.0) [Software]. GitHub. https://github.com/hernandavidc/bigdata-i-laboratorios

## Licencia

La licencia de uso está pendiente de definir con la Universidad de La Salle.
