# RFC-001: Data Quality Firewall para Ingesta de Productos

| Metadata | Detalles |
| :--- | :--- |
| **Autor** | Leandro Martín Gross Alarcón |
| **Estado** | MVP |
| **Fecha** | 2026-02-15 |
| **Alcance** | Back-Office Automation |

## 1. Resumen Ejecutivo
Propuesta para la implementación de un sistema de validación y normalización de datos ("Data Firewall") para la unidad de Back-Office. El objetivo es reducir la carga operativa manual mediante la automatización de controles de calidad sobre las solicitudes de alta de productos, garantizando que solo los datos íntegros (válidos) lleguen a los sistemas downstream.

## 2. Motivación y Problema
Actualmente, la unidad recibe archivos en formatos heterogéneos (CSV, JSON, TXT) con calidad variable. Esto ocasiona:
* Errores en procesos posteriores por datos corruptos (fechas inválidas, montos negativos).
* Falta de trazabilidad sobre qué registros fueron rechazados y por qué.
* Riesgo operativo alto al depender de validación visual humana.

## 3. Propuesta de Solución
Se ha diseñado una arquitectura **Batch ETL** desacoplada basada en el patrón *Pipes and Filters*.

### 3.1 Arquitectura Lógica
1.  **Ingesta:** Detección automática de archivos en `data/raw`.
2.  **Validación (Schema Enforcement):** Uso de **Pydantic** para aplicar reglas de tipado fuerte y lógica de negocio.
    * *Ventaja:* Validación declarativa, más fácil de mantener que scripts imperativos con múltiples `if/else`.
3.  **Segregación:** Enrutamiento automático:
    * Datos limpios --> `data/processed` (Normalizados).
    * Datos sucios --> `data/rejected` (Con reporte de error detallado en JSON).
    * Datos de reportes y logs --> `data/reports` (reportes de ejecución detallado en formato JSON)

### 3.2 Stack Tecnológico
* **Python 3.10+**: Lenguaje estándar.
* **Pydantic**: Para validación de esquemas y serialización. Elegido por su rendimiento y claridad sobre alternativas como `cerberus` o validación manual.
* **Pandas**: Para manipulación eficiente de estructuras tabulares.
* **Pytest**: Para asegurar la estabilidad del sistema ante cambios futuros (Regresión).

## 4. Estándares y Mantenibilidad
* **Código Modular:** Separación clara entre definición de datos (`schema.py`) y lógica de procesamiento (`processor.py`).
* **Data Lineage:** Los archivos de salida preservan el nombre del archivo origen para facilitar auditorías.
* **Logging:** Trazabilidad completa de la ejecución en `pipeline.log`.

## 5. Escalabilidad Futura (Roadmap)
Si el volumen de datos crece significativamente, este diseño permite:
1.  **Containerización:** Empaquetar el script en Docker para ejecución en Kubernetes.
2.  **Orquestación:** Migrar la ejecución de `main.py` a un DAG de Apache Airflow.
3.  **Streaming:** Reutilizar el módulo `schema.py` para validar eventos en tiempo real (Kafka) sin reescribir la lógica de negocio.
4.  **Data Governance:** Definición de KPIs y métricas de calidad:
    a. **DQI** Data Quality Index --> (Registros Validos / Total Registros) * 100 | `tgt: mantener un DQI > 98%`
    b. **PAreto de errores** Gráfico de barras horizontal identificando qué reglas de negocio se rompen con mayor frecuencia (ej. "El 60% de los rechazos se deben a emails mal formados")
    c. **Tendencia de volumen (time series)** Detección de anomalías en el volumen de ingesta diaria (picos o caídas abruptas que indiquen fallos en origen).
    d. **PII** logs no deben exponer data sensible en texto plano
5.  **Observability:** Implementación de un dashboard interactivo utilizando `Streamlit` que lea directamente de los repositorios de logs y datos rechazados (Objetivo: Proveer a los Data Stewards y Product Owners una vista en tiempo real de la salud de los datos.)
6.  **Persistencia y analytics layer** Actualmente, los datos procesados residen en archivos CSV ("Cold Storage"). Para habilitar análisis complejos y consultas ad-hoc, se propone la integración de una capa de persistencia SQL.
7.  **Integración y Despliegue Continuo (CI/CD):** Para operar en un entorno de alta criticidad, la calidad del código no puede depender de ejecuciones manuales. Se propone la implementación de un pipeline de **Integración Continua** automatizado.
    * **Herramienta:** GitHub Actions.
    * **Flujo de Trabajo (Quality Gate):** Cada vez que un desarrollador abra un *Pull Request* o haga *Push* a la rama `main`, un servidor deberá:
        1. Instalar las dependencias (`requirements.txt`).
        2. Ejecutar herramientas de análisis estático (Linters) para garantizar estándares de código.
        3. Ejecutar la suite de pruebas unitarias (`pytest`) para validar las reglas de negocio.
    * **Impacto:** Previene que código defectuoso rompa el entorno de producción (Shift-Left Testing) y facilita el proceso de *Code Review*, ya que el revisor humano se enfoca en la lógica y no en buscar errores de sintaxis.