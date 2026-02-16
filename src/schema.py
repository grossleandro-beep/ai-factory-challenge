from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import Literal
from datetime import date

class SolicitudProducto(BaseModel):
    id_solicitud: str
    fecha_solicitud: date  # Pydantic parseará strings a fechas automáticamente
    tipo_producto: str
    id_cliente: int
    monto_o_limite: float = Field(gt=0) # Regla: Mayor a 0
    moneda: Literal['ARS', 'USD', 'EUR'] # Regla: Lista cerrada
    pais: str = Field(min_length=2, max_length=2)
    email_contacto: EmailStr # Regla: Formato email válido

    # NUEVOS FLAGS
    es_cliente_vip: bool
    canal_digital: bool

    # Validadores personalizados si necesitamos lógica compleja
    @field_validator('tipo_producto')
    @classmethod
    def normalizar_producto(cls, v):
        return v.upper().strip() # Regla: Normalización