import os
import glob
from src.processor import Pipeline

def main():
    # Buscar todos los archivos en raw
    # archivos = glob.glob("data/raw/*.*") # Soporta cualquier formato generado
    
    extensiones = ['*.csv', '*.json', '*.txt']
    archivos = []
    
    # Buscamos archivos con esas extensiones en data/raw
    for ext in extensiones:
        archivos.extend(glob.glob(os.path.join("data/raw", ext)))
    
    if not archivos:
        print("⚠️ No hay archivos para procesar en data/raw/")
        return

    print(f"🚀 Iniciando Pipeline ETL. Encontrados {len(archivos)} archivos.")
    print("-" * 50)
    
    for archivo in archivos:
        try:
            pipeline = Pipeline(archivo)
            pipeline.run()
        except Exception as e:
            print(f"❌ Error fatal procesando {archivo}: {e}")

if __name__ == "__main__":
    main()