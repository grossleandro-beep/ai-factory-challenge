import pandas as pd
import json
import logging
import os
import time
from datetime import datetime
from src.schema import SolicitudProducto
from pydantic import ValidationError

# Aseguramos que las carpetas existan
os.makedirs("data/reports", exist_ok=True)
os.makedirs("data/rejected", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Configuración de logs con formato más limpio y profesional
logging.basicConfig(
    filename='data/reports/pipeline.log', 
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)s | %(message)s'
)

class Pipeline:
    def __init__(self, input_file):
        self.input_file = input_file
        self.valid_records = []
        self.invalid_records = []
        
    def _read_data(self):
        """Estrategia de Ingesta Unificada"""
        _, ext = os.path.splitext(self.input_file)
        ext = ext.lower()
        
        try:
            if ext == '.csv':
                return pd.read_csv(self.input_file).to_dict(orient='records')
            elif ext == '.json':
                with open(self.input_file, 'r') as f:
                    data = json.load(f)
                    return data if isinstance(data, list) else [data]
            elif ext == '.txt':
                return pd.read_csv(self.input_file, sep='|').to_dict(orient='records')
            else:
                logging.warning(f"⚠️ Formato no soportado saltado: {ext}")
                return []
        except Exception as e:
            logging.error(f"❌ Error crítico leyendo archivo {self.input_file}: {e}")
            return []

    def run(self):
        # INICIO DEL CRONÓMETRO
        start_time = time.time()
        archivo_nombre = os.path.basename(self.input_file)
        
        logging.info(f"▶️ [PASO 1: INGESTA] Iniciando lectura de: {archivo_nombre}")
        raw_data = self._read_data()
        
        if not raw_data:
            logging.warning(f"⚠️ [PASO 1: INGESTA] Archivo vacío o ilegible. Abortando.")
            return

        logging.info(f"🔄 [PASO 2: VALIDACIÓN] Evaluando {len(raw_data)} registros contra Pydantic Schema...")
        for record in raw_data:
            try:
                obj = SolicitudProducto(**record)
                self.valid_records.append(obj.model_dump())
            except ValidationError as e:
                error_msg = json.loads(e.json())
                record['error_details'] = error_msg
                self.invalid_records.append(record)
            except Exception as e:
                record['error_details'] = [{'loc': ['general'], 'msg': str(e), 'type': 'fatal'}]
                self.invalid_records.append(record)

        logging.info(f"✅ [PASO 2: VALIDACIÓN] Completada. Válidos: {len(self.valid_records)} | Inválidos: {len(self.invalid_records)}")
        
        logging.info(f"💾 [PASO 3: PERSISTENCIA] Guardando resultados y generando reportes...")
        
        # CÁLCULO DEL TIEMPO TOTAL
        elapsed_time = round(time.time() - start_time, 4)
        
        # Pasamos el tiempo calculado a la función que guarda los resultados
        self._save_results(elapsed_time)
        
        logging.info(f"🏁 [FIN] Pipeline finalizado exitosamente en {elapsed_time} segundos.")
        logging.info("-" * 60) # Separador visual en el log

    def _generate_quality_report(self, source_name, timestamp, elapsed_time):
        """Genera el reporte analítico de calidad solicitado por el negocio"""
        total_procesados = len(self.valid_records) + len(self.invalid_records)
        total_validos = len(self.valid_records)
        total_invalidos = len(self.invalid_records)

        total_errores_individuales = 0
        for record in self.invalid_records:
            total_errores_individuales += len(record.get('error_details', []))

        report = {
            "archivo_origen": source_name,
            "fecha_ejecucion": timestamp,
            "tiempo_procesamiento_segundos": elapsed_time, # <--- NUEVO DATO
            "totales": {
                "procesados": total_procesados,
                "validos": total_validos,
                "invalidos": total_invalidos
            },
            "indicadores_globales": {
                "tasa_exito_global": f"{round((total_validos / total_procesados * 100), 2)}%" if total_procesados > 0 else "0%",
                "tasa_error_global": f"{round((total_invalidos / total_procesados * 100), 2)}%" if total_procesados > 0 else "0%"
            },
            "detalle_por_regla": []
        }

        if total_invalidos > 0:
            reglas_rotas = {}
            
            for record in self.invalid_records:
                for error in record.get('error_details', []):
                    campo = str(error.get('loc', ['desconocido'])[0])
                    mensaje = error.get('msg', 'Error de formato')
                    clave_regla = f"Campo '{campo}' -> {mensaje}"

                    if clave_regla not in reglas_rotas:
                        reglas_rotas[clave_regla] = {
                            "regla_infringida": clave_regla,
                            "cantidad_fallos": 0,
                            "ejemplos_valores_rechazados": []
                        }
                    
                    reglas_rotas[clave_regla]["cantidad_fallos"] += 1
                    
                    ejemplo = {
                        "id_solicitud": record.get("id_solicitud", "N/A"),
                        "valor_recibido": record.get(campo, "N/A")
                    }
                    reglas_rotas[clave_regla]["ejemplos_valores_rechazados"].append(ejemplo)

            for regla, datos in reglas_rotas.items():
                distribucion = (datos["cantidad_fallos"] / total_errores_individuales) * 100
                datos["composicion_del_error_total"] = f"{round(distribucion, 2)}%"

                tasa_fallo_regla = (datos["cantidad_fallos"] / total_procesados) * 100
                datos["tasa_cumplimiento_regla"] = f"{round(100 - tasa_fallo_regla, 2)}%"
                
                report["detalle_por_regla"].append(datos)

        report_path = f"data/reports/reporte_calidad_{source_name}_{timestamp}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
        logging.info(f"📄 Reporte de calidad JSON generado en: {report_path}")

    def _save_results(self, elapsed_time):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        source_name = os.path.basename(self.input_file).split('.')[0]

        if self.valid_records:
            df_valid = pd.DataFrame(self.valid_records)
            df_valid.to_csv(f"data/processed/validos_{source_name}_{timestamp}.csv", index=False)

        if self.invalid_records:
            df_invalid = pd.DataFrame(self.invalid_records)
            df_invalid.to_json(f"data/rejected/raw_rechazados_{source_name}_{timestamp}.json", orient='records', indent=2)

        # Pasamos el elapsed_time al reporte
        self._generate_quality_report(source_name, timestamp, elapsed_time)
        
        print(f"[{source_name}] Procesados: {len(self.valid_records)+len(self.invalid_records)} | Válidos: {len(self.valid_records)} | Rechazados: {len(self.invalid_records)} | Tiempo: {elapsed_time}s")