import pandas as pd
import json
import logging
import os
from datetime import datetime
from src.schema import SolicitudProducto
from pydantic import ValidationError

# Aseguramos que las carpetas existan
os.makedirs("data/reports", exist_ok=True)
os.makedirs("data/rejected", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

# Configuración básica de logs
logging.basicConfig(
    filename='data/reports/pipeline.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
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
                logging.warning(f"Formato no soportado saltado: {ext}")
                return []
        except Exception as e:
            logging.error(f"Error crítico leyendo archivo {self.input_file}: {e}")
            return []

    def run(self):
        logging.info(f"--- Iniciando procesamiento de {os.path.basename(self.input_file)} ---")
        
        raw_data = self._read_data()
        if not raw_data:
            return

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

        self._save_results()

    def _generate_quality_report(self, source_name, timestamp):
        """Genera el reporte analítico de calidad solicitado por el negocio"""
        total_procesados = len(self.valid_records) + len(self.invalid_records)
        total_validos = len(self.valid_records)
        total_invalidos = len(self.invalid_records)

        # Contamos cuántos errores individuales hubo en total para calcular la composición
        total_errores_individuales = 0
        for record in self.invalid_records:
            total_errores_individuales += len(record.get('error_details', []))

        report = {
            "archivo_origen": source_name,
            "fecha_ejecucion": timestamp,
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
                    
                    # Guardamos TODOS los ejemplos, sin límite
                    ejemplo = {
                        "id_solicitud": record.get("id_solicitud", "N/A"),
                        "valor_recibido": record.get(campo, "N/A")
                    }
                    reglas_rotas[clave_regla]["ejemplos_valores_rechazados"].append(ejemplo)

            for regla, datos in reglas_rotas.items():
                # 1. ¿Cuánto contribuye esta regla al total de errores? (Composición del 15%)
                distribucion = (datos["cantidad_fallos"] / total_errores_individuales) * 100
                datos["composicion_del_error_total"] = f"{round(distribucion, 2)}%"

                # 2. ¿Cuál es la tasa de cumplimiento específica de esta regla sobre el total de registros?
                tasa_fallo_regla = (datos["cantidad_fallos"] / total_procesados) * 100
                datos["tasa_cumplimiento_regla"] = f"{round(100 - tasa_fallo_regla, 2)}%"
                
                report["detalle_por_regla"].append(datos)

        report_path = f"data/reports/reporte_calidad_{source_name}_{timestamp}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=4, ensure_ascii=False)
        logging.info(f"Reporte de calidad JSON generado: {report_path}")

    def _save_results(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        source_name = os.path.basename(self.input_file).split('.')[0]

        # 1. Guardar Válidos (CSV)
        if self.valid_records:
            df_valid = pd.DataFrame(self.valid_records)
            df_valid.to_csv(f"data/processed/validos_{source_name}_{timestamp}.csv", index=False)
            logging.info(f"Guardados {len(df_valid)} registros válidos en CSV.")

        # 2. Guardar Raw Inválidos
        if self.invalid_records:
            df_invalid = pd.DataFrame(self.invalid_records)
            df_invalid.to_json(f"data/rejected/raw_rechazados_{source_name}_{timestamp}.json", orient='records', indent=2)

        # 3. Reporte de Calidad
        self._generate_quality_report(source_name, timestamp)
        
        print(f"[{source_name}] Procesados: {len(self.valid_records)+len(self.invalid_records)} | Válidos: {len(self.valid_records)} | Rechazados: {len(self.invalid_records)}")