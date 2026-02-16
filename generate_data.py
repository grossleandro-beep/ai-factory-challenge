import pandas as pd
import random
import json
import os
from faker import Faker
from datetime import datetime, timedelta

fake = Faker('es_ES')

def generar_datos_multi_formato():
    formatos = ['csv', 'json', 'txt']
    productos = ['CUENTA_CORRIENTE', 'TARJETA_CREDITO', 'PRESTAMO_PERSONAL', 'SEGURO_VIDA']
    monedas = ['ARS', 'USD', 'EUR']
    paises = ['AR', 'US', 'ES', 'BR']

    for i, fmt in enumerate(formatos):
        fecha = datetime.now() - timedelta(days=i)
        timestamp = fecha.strftime('%Y%m%d')
        base_name = f"solicitudes_{timestamp}"
        
        data = []
        for _ in range(100):
            es_valido = random.random() > 0.15 # 85% de éxito, 15% de error
            
            # Generamos un registro perfecto por defecto
            row = {
                "id_solicitud": fake.uuid4(),
                "fecha_solicitud": fecha.strftime("%Y-%m-%d"),
                "tipo_producto": random.choice(productos),
                "id_cliente": fake.random_int(min=100000, max=99999999),
                "monto_o_limite": round(random.uniform(1000, 50000), 2),
                "moneda": random.choice(monedas),
                "pais": random.choice(paises),
                "email_contacto": fake.email()
            }

            if not es_valido:
                # INYECCIÓN DIRIGIDA: Decidimos qué campo romper para lograr la estadística
                # 50% de los errores serán de fecha, 25% de monto, 25% de email
                error_type = random.choices(
                    ['fecha', 'monto', 'email'], 
                    weights=[0.50, 0.25, 0.25], 
                    k=1
                )[0]

                if error_type == 'fecha':
                    row['fecha_solicitud'] = "2099/13/32" # Invalida
                elif error_type == 'monto':
                    row['monto_o_limite'] = -500.00 # Invalida
                elif error_type == 'email':
                    row['email_contacto'] = "correo_sin_arroba.com" # Invalida

            data.append(row)

        # Lógica de guardado (igual que antes)
        ruta_archivo = f"data/raw/{base_name}.{fmt}"
        if fmt == 'csv':
            pd.DataFrame(data).to_csv(ruta_archivo, index=False)
        elif fmt == 'json':
            with open(ruta_archivo, 'w') as f:
                json.dump(data, f, indent=2)
        elif fmt == 'txt':
            pd.DataFrame(data).to_csv(ruta_archivo, sep='|', index=False)

        print(f"✅ Generado: {ruta_archivo}")

if __name__ == "__main__":
    os.makedirs("data/raw", exist_ok=True)
    generar_datos_multi_formato()