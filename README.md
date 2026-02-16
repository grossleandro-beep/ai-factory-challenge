# AI Factory Challenge - Data Quality Pipeline

## Ejecución
1. Instalar dependencias: `pip install -r requirements.txt`
2. Generar datos: `python generate_data.py`
3. Correr pipeline: `python main.py`
4. Validar QA (Reglas de Negocio): `python -m pytest tests/test_validator.py -v`