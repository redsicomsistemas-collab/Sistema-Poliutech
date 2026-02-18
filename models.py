# =========================================================
# models.py — Sistema MARWHATS / Poliutech
# =========================================================

from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin

db = SQLAlchemy()

# ---------------------------------------------------------
# MODELOS
# ---------------------------------------------------------

class Cliente(db.Model):
    __tablename__ = "cliente"
    id = db.Column(db.Integer, primary_key=True)
    nombre_cliente = db.Column(db.String(120), nullable=False)
    empresa = db.Column(db.String(120))
    responsable = db.Column(db.String(120))  # campo correcto y único
    correo = db.Column(db.String(120))
    telefono = db.Column(db.String(50))
    direccion = db.Column(db.String(200))
    rfc = db.Column(db.String(50))  # se mantiene en BD por compatibilidad

    cotizaciones = db.relationship(
        "Cotizacion",
        backref="cliente",
        cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Cliente {self.nombre_cliente}>"


class Concepto(db.Model):
    __tablename__ = "concepto"
    id = db.Column(db.Integer, primary_key=True)
    nombre_concepto = db.Column(db.String(500), nullable=False)
    unidad = db.Column(db.String(50))
    precio_unitario = db.Column(db.Float, default=0)
    sistema = db.Column(db.String(200))  # para jalarlo automático
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
    responsable = db.Column(db.String(120))  # sustituye a “representante”

    detalles = db.relationship(
        "CotizacionDetalle",
        backref="cotizacion",
        cascade="all, delete-orphan"
    )

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
    sistema = db.Column(db.String(200))
    descripcion = db.Column(db.String(1000))
    subtotal = db.Column(db.Float, default=0)

    # ✅ ESTA RELACIÓN SÍ ES VÁLIDA porque existe concepto_id con FK
    concepto = db.relationship("Concepto")

    def __repr__(self):
        return f"<Detalle {self.nombre_concepto}>"


class Usuario(UserMixin, db.Model):
    __tablename__ = "usuario"
    id = db.Column(db.Integer, primary_key=True)

    # Solo primer nombre (ej: "Rafa")
    nombre = db.Column(db.String(60), unique=True, nullable=False)

    # ADMIN o REP
    rol = db.Column(db.String(10), default="REP", nullable=False)

    password_hash = db.Column(db.String(255), nullable=False)



    def set_password(self, raw: str):
        self.password_hash = generate_password_hash(raw)

    def check_password(self, raw: str) -> bool:
        return check_password_hash(self.password_hash, raw)

    def __repr__(self):
        return f"<Usuario {self.nombre} ({self.rol})>"

# ---------------------------------------------------------
# BITÁCORA (Audit Log)
# ---------------------------------------------------------
class ActivityLog(db.Model):
    __tablename__ = "activity_log"
    id = db.Column(db.Integer, primary_key=True)

    fecha = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    # Usuario (puede ser ANON si no está autenticado)
    usuario_id = db.Column(db.Integer, nullable=True)
    usuario = db.Column(db.String(60), nullable=False, default="ANON")
    rol = db.Column(db.String(10), nullable=True)

    # Request metadata
    metodo = db.Column(db.String(10), nullable=False)
    ruta = db.Column(db.String(300), nullable=False)
    endpoint = db.Column(db.String(120), nullable=True)
    status_code = db.Column(db.Integer, nullable=True)

    ip = db.Column(db.String(60), nullable=True)
    user_agent = db.Column(db.String(300), nullable=True)

    # Contexto (sin valores sensibles)
    query_string = db.Column(db.String(800), nullable=True)
    form_keys = db.Column(db.String(800), nullable=True)
    json_keys = db.Column(db.String(800), nullable=True)

    # Acción legible
    accion = db.Column(db.String(500), nullable=False, default="REQUEST")

    def __repr__(self):
        return f"<ActivityLog {self.fecha} {self.usuario} {self.metodo} {self.ruta}>"
