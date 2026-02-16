import pytest
from pydantic import ValidationError
from datetime import date
from src.schema import SolicitudProducto

# 1. Test de Caso de Éxito (Happy Path)
def test_solicitud_valida():
    payload = {
        "id_solicitud": "123-abc",
        "fecha_solicitud": "2026-02-13",
        "tipo_producto": " tarjeta_credito ", # Probamos el trimming/uppercase
        "id_cliente": 1001,
        "monto_o_limite": 5000.50,
        "moneda": "ARS",
        "pais": "AR",
        "email_contacto": "test@example.com"
    }
    
    solicitud = SolicitudProducto(**payload)
    
    assert solicitud.monto_o_limite == 5000.50
    assert solicitud.tipo_producto == "TARJETA_CREDITO" # Validamos que normalizó a mayúsculas
    assert solicitud.fecha_solicitud == date(2026, 2, 13)
    assert solicitud.moneda == "ARS"

# 2. Test de Regla de Negocio: Monto Negativo
def test_monto_negativo_falla():
    payload = {
        "id_solicitud": "123-abc",
        "fecha_solicitud": "2026-02-13",
        "tipo_producto": "PRESTAMO",
        "id_cliente": 1001,
        "monto_o_limite": -10.0, # INVALIDO
        "moneda": "USD",
        "pais": "AR",
        "email_contacto": "test@example.com"
    }
    
    with pytest.raises(ValidationError) as excinfo:
        SolicitudProducto(**payload)
    
    # Verificamos que el error sea exactamente sobre el límite
    errores = excinfo.value.errors()
    assert any(e['loc'] == ('monto_o_limite',) for e in errores)

# 3. Test de Regla de Negocio: Monto Negativo
def test_moneda_falla():
    payload = {
        "id_solicitud": "123-abc",
        "fecha_solicitud": "2026-02-13",
        "tipo_producto": "PRESTAMO",
        "id_cliente": 1001,
        "monto_o_limite": 10.0, 
        "moneda": "CLP", # INVALIDO
        "pais": "AR",
        "email_contacto": "test@example.com"
    }
    
    with pytest.raises(ValidationError) as excinfo:
        SolicitudProducto(**payload)
    
    # Verificamos que el error sea exactamente sobre la moneda
    errores = excinfo.value.errors()
    assert any(e['loc'] == ('moneda',) for e in errores)

# 4. Test de Regla de Formato: Email Inválido
def test_email_invalido():
    payload = {
        "id_solicitud": "123-abc",
        "fecha_solicitud": "2026-02-13",
        "tipo_producto": "PRESTAMO",
        "id_cliente": 1001,
        "monto_o_limite": 100.0, 
        "moneda": "USD",
        "pais": "AR",
        "email_contacto": "email sin arroba.com" # INVALIDO
    }
    
    with pytest.raises(ValidationError) as excinfo:
        SolicitudProducto(**payload)

# 5. Test de Regla de Formato: Fecha Inválida
def test_fecha_invalida():
    # Vamos a usar el mes "13" que obviamente no existe.
    payload = {
        "id_solicitud": "123-abc",
        "fecha_solicitud": "2026-13-01", # <--- EL ERROR INTENCIONAL (Mes 13)
        "tipo_producto": "PRESTAMO",
        "id_cliente": 1001,
        "monto_o_limite": 100.0, 
        "moneda": "USD",
        "pais": "AR",
        "email_contacto": "test@example.com"
    }
    
    # 2. ACT & ASSERT: El guardia (Pydantic) DEBE explotar y detener esto.
    with pytest.raises(ValidationError) as excinfo:
        SolicitudProducto(**payload)
    
    errores = excinfo.value.errors()
    assert any(e['loc'] == ('fecha_solicitud',) for e in errores)
