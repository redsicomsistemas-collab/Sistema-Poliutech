# =========================================================
# models.py — Sistema MARWHATS / Poliutech
# =========================================================

from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

# Instancia global reutilizable
db = SQLAlchemy()


# ---------------------------------------------------------
# MODELOS
# ---------------------------------------------------------

class Cliente(db.Model):
    __tablename__ = "cliente"
    id = db.Column(db.Integer, primary_key=True)
    nombre_cliente = db.Column(db.String(120), nullable=False)
    empresa = db.Column(db.String(120))
    representante = db.Column(db.String(120))  # ✅ reemplaza al campo 'responsable'
    correo = db.Column(db.String(120))
    telefono = db.Column(db.String(50))
    direccion = db.Column(db.String(200))
    rfc = db.Column(db.String(50))

    cotizaciones = db.relationship("Cotizacion", backref="cliente", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Cliente {self.nombre_cliente}>"


class Concepto(db.Model):
    __tablename__ = "concepto"
    id = db.Column(db.Integer, primary_key=True)
    nombre_concepto = db.Column(db.String(500), nullable=False)
    unidad = db.Column(db.String(50))
    precio_unitario = db.Column(db.Float, default=0)
    descripcion = db.Column(db.String(1000))

    def __repr__(self):
        return f"<Concepto {self.nombre_concepto}>"


class Cotizacion(db.Model):
    __tablename__ = "cotizacion"
    id = db.Column(db.Integer, primary_key=True)
    folio = db.Column(db.String(40), unique=True)
    cliente_id = db.Column(db.Integer, db.ForeignKey("cliente.id"))
    fecha = db.Column(db.DateTime, default=datetime.utcnow)
    estatus = db.Column(db.String(20), default="PENDIENTE")
    subtotal = db.Column(db.Float, default=0.0)
    descuento_total = db.Column(db.Float, default=0.0)
    iva_porc = db.Column(db.Float, default=16.0)
    iva_monto = db.Column(db.Float, default=0.0)
    total = db.Column(db.Float, default=0.0)
    notas = db.Column(db.String(3000))
    last_whatsapp_at = db.Column(db.DateTime, nullable=True)
    representante = db.Column(db.String(120))

    detalles = db.relationship("CotizacionDetalle", backref="cotizacion",
                               cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Cotizacion {self.folio or self.id}>"


class CotizacionDetalle(db.Model):
    __tablename__ = "cotizacion_detalle"
    id = db.Column(db.Integer, primary_key=True)
    cotizacion_id = db.Column(db.Integer, db.ForeignKey("cotizacion.id"))
    concepto_id = db.Column(db.Integer, db.ForeignKey("concepto.id"), nullable=True)
    nombre_concepto = db.Column(db.String(500), nullable=False)
    unidad = db.Column(db.String(50))
    cantidad = db.Column(db.Float, default=1)
    precio_unitario = db.Column(db.Float, default=0)
    sistema = db.Column(db.String(200))  # campo nuevo
    descripcion = db.Column(db.String(1000))
    subtotal = db.Column(db.Float, default=0)

    concepto = db.relationship("Concepto")

    def __repr__(self):
        return f"<Detalle {self.nombre_concepto}>"
