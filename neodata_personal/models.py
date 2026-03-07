
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Material(db.Model):
    __tablename__ = 'materiales'

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    unidad = db.Column(db.String(50), nullable=False)
    precio_unitario = db.Column(db.Float, default=0.0)
    proveedor = db.Column(db.String(200))
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ManoObra(db.Model):
    __tablename__ = 'mano_obra'

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    unidad = db.Column(db.String(50), nullable=False)
    precio_unitario = db.Column(db.Float, default=0.0)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Maquinaria(db.Model):
    __tablename__ = 'maquinaria'

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    unidad = db.Column(db.String(50), nullable=False)
    precio_unitario = db.Column(db.Float, default=0.0)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class APU(db.Model):
    __tablename__ = 'apu'

    id = db.Column(db.Integer, primary_key=True)
    clave = db.Column(db.String(50))
    concepto = db.Column(db.String(300), nullable=False)
    unidad = db.Column(db.String(50), nullable=False)

    indirecto_pct = db.Column(db.Float, default=0.0)
    utilidad_pct = db.Column(db.Float, default=0.0)
    financiamiento_pct = db.Column(db.Float, default=0.0)
    cargos_adicionales_pct = db.Column(db.Float, default=0.0)

    costo_materiales = db.Column(db.Float, default=0.0)
    costo_mano_obra = db.Column(db.Float, default=0.0)
    costo_maquinaria = db.Column(db.Float, default=0.0)
    costo_directo = db.Column(db.Float, default=0.0)
    precio_unitario = db.Column(db.Float, default=0.0)

    creado_en = db.Column(db.DateTime, default=datetime.utcnow)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    detalles = db.relationship("APUDetalle", backref="apu", cascade="all, delete-orphan", lazy=True)


class APUDetalle(db.Model):
    __tablename__ = 'apu_detalle'

    id = db.Column(db.Integer, primary_key=True)
    apu_id = db.Column(db.Integer, db.ForeignKey("apu.id"), nullable=False)

    tipo_insumo = db.Column(db.String(20))  # material, mano_obra, maquinaria
    descripcion = db.Column(db.String(300))

    unidad = db.Column(db.String(50))
    cantidad = db.Column(db.Float, default=0.0)
    precio_unitario = db.Column(db.Float, default=0.0)
    subtotal = db.Column(db.Float, default=0.0)
