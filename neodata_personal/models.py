
from datetime import datetime
from models import db

class Material(db.Model):
    __tablename__ = "apu_material"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    unidad = db.Column(db.String(50), nullable=False, default="kg")
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    proveedor = db.Column(db.String(200))
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Material {self.nombre}>"


class ManoObra(db.Model):
    __tablename__ = "apu_mano_obra"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    unidad = db.Column(db.String(50), nullable=False, default="jor")
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<ManoObra {self.nombre}>"


class Maquinaria(db.Model):
    __tablename__ = "apu_maquinaria"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    unidad = db.Column(db.String(50), nullable=False, default="hr")
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Maquinaria {self.nombre}>"


class APU(db.Model):
    __tablename__ = "apu"

    id = db.Column(db.Integer, primary_key=True)
    clave = db.Column(db.String(50), unique=True)
    concepto = db.Column(db.String(300), nullable=False)
    unidad = db.Column(db.String(50), nullable=False, default="m2")

    indirecto_pct = db.Column(db.Float, nullable=False, default=0.0)
    utilidad_pct = db.Column(db.Float, nullable=False, default=0.0)
    financiamiento_pct = db.Column(db.Float, nullable=False, default=0.0)
    cargos_adicionales_pct = db.Column(db.Float, nullable=False, default=0.0)

    costo_materiales = db.Column(db.Float, nullable=False, default=0.0)
    costo_mano_obra = db.Column(db.Float, nullable=False, default=0.0)
    costo_maquinaria = db.Column(db.Float, nullable=False, default=0.0)
    costo_directo = db.Column(db.Float, nullable=False, default=0.0)
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)

    creado_en = db.Column(db.DateTime, default=datetime.utcnow)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    detalles = db.relationship(
        "APUDetalle",
        backref="apu",
        cascade="all, delete-orphan",
        order_by="APUDetalle.id.asc()"
    )

    def __repr__(self):
        return f"<APU {self.concepto}>"


class APUDetalle(db.Model):
    __tablename__ = "apu_detalle"

    id = db.Column(db.Integer, primary_key=True)
    apu_id = db.Column(db.Integer, db.ForeignKey("apu.id"), nullable=False)

    tipo_insumo = db.Column(db.String(20), nullable=False)  # material, mano_obra, maquinaria
    referencia_id = db.Column(db.Integer, nullable=True)
    descripcion = db.Column(db.String(300), nullable=False)

    unidad = db.Column(db.String(50), nullable=False, default="kg")
    cantidad = db.Column(db.Float, nullable=False, default=0.0)
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    subtotal = db.Column(db.Float, nullable=False, default=0.0)

    def __repr__(self):
        return f"<APUDetalle {self.tipo_insumo} {self.descripcion}>"
