
from datetime import datetime
from models import db

class Material(db.Model):
    __tablename__ = "apu_material"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    clave = db.Column(db.String(60))
    categoria = db.Column(db.String(120))
    unidad = db.Column(db.String(50), nullable=False, default="kg")
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    proveedor = db.Column(db.String(200))
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ManoObra(db.Model):
    __tablename__ = "apu_mano_obra"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    clave = db.Column(db.String(60))
    categoria = db.Column(db.String(120))
    unidad = db.Column(db.String(50), nullable=False, default="jor")
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Maquinaria(db.Model):
    __tablename__ = "apu_maquinaria"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), unique=True, nullable=False)
    clave = db.Column(db.String(60))
    categoria = db.Column(db.String(120))
    unidad = db.Column(db.String(50), nullable=False, default="hr")
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    fecha_actualizacion = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class APU(db.Model):
    __tablename__ = "apu"

    id = db.Column(db.Integer, primary_key=True)
    clave = db.Column(db.String(50), unique=True)
    concepto = db.Column(db.String(300), nullable=False)
    descripcion = db.Column(db.Text)
    categoria = db.Column(db.String(120))
    unidad = db.Column(db.String(50), nullable=False, default="m2")
    cantidad_objetivo = db.Column(db.Float, nullable=False, default=1.0)
    rendimiento_base = db.Column(db.Float, nullable=False, default=1.0)
    jornada_horas = db.Column(db.Float, nullable=False, default=8.0)
    desperdicio_general_pct = db.Column(db.Float, nullable=False, default=0.0)
    herramienta_menor_pct = db.Column(db.Float, nullable=False, default=0.0)
    notas = db.Column(db.Text)
    es_auxiliar = db.Column(db.Boolean, nullable=False, default=False)
    capitulo = db.Column(db.String(120))
    subcapitulo = db.Column(db.String(120))
    alcance = db.Column(db.String(300))

    indirecto_pct = db.Column(db.Float, nullable=False, default=0.0)
    utilidad_pct = db.Column(db.Float, nullable=False, default=0.0)
    financiamiento_pct = db.Column(db.Float, nullable=False, default=0.0)
    cargos_adicionales_pct = db.Column(db.Float, nullable=False, default=0.0)

    costo_materiales = db.Column(db.Float, nullable=False, default=0.0)
    costo_mano_obra = db.Column(db.Float, nullable=False, default=0.0)
    costo_maquinaria = db.Column(db.Float, nullable=False, default=0.0)
    costo_herramienta = db.Column(db.Float, nullable=False, default=0.0)
    costo_directo = db.Column(db.Float, nullable=False, default=0.0)
    indirecto_monto = db.Column(db.Float, nullable=False, default=0.0)
    financiamiento_monto = db.Column(db.Float, nullable=False, default=0.0)
    utilidad_monto = db.Column(db.Float, nullable=False, default=0.0)
    cargos_adicionales_monto = db.Column(db.Float, nullable=False, default=0.0)
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    importe_partida = db.Column(db.Float, nullable=False, default=0.0)

    creado_en = db.Column(db.DateTime, default=datetime.utcnow)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    detalles = db.relationship(
        "APUDetalle",
        backref="apu",
        cascade="all, delete-orphan",
        order_by="APUDetalle.id.asc()"
    )

class APUDetalle(db.Model):
    __tablename__ = "apu_detalle"

    id = db.Column(db.Integer, primary_key=True)
    apu_id = db.Column(db.Integer, db.ForeignKey("apu.id"), nullable=False)

    tipo_insumo = db.Column(db.String(20), nullable=False)
    referencia_id = db.Column(db.Integer, nullable=True)
    descripcion = db.Column(db.String(300), nullable=False)
    codigo = db.Column(db.String(60))
    categoria = db.Column(db.String(120))

    unidad = db.Column(db.String(50), nullable=False, default="kg")
    cantidad = db.Column(db.Float, nullable=False, default=0.0)
    factor = db.Column(db.Float, nullable=False, default=1.0)
    cuadrilla = db.Column(db.Float, nullable=False, default=1.0)
    rendimiento = db.Column(db.Float, nullable=False, default=0.0)
    desperdicio_pct = db.Column(db.Float, nullable=False, default=0.0)
    comentario = db.Column(db.String(500))
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    subtotal = db.Column(db.Float, nullable=False, default=0.0)
    auxiliar_apu_id = db.Column(db.Integer, db.ForeignKey("apu.id"))

    auxiliar = db.relationship("APU", foreign_keys=[auxiliar_apu_id])


class Obra(db.Model):
    __tablename__ = "apu_obra"

    id = db.Column(db.Integer, primary_key=True)
    clave = db.Column(db.String(60), unique=True)
    nombre = db.Column(db.String(220), nullable=False)
    cliente = db.Column(db.String(160))
    descripcion = db.Column(db.Text)
    ubicacion = db.Column(db.String(220))
    unidad_venta = db.Column(db.String(50), nullable=False, default="obra")
    fecha_inicio = db.Column(db.DateTime)
    fecha_fin = db.Column(db.DateTime)
    plazo_dias = db.Column(db.Integer, default=0)
    programa_intervalo_dias = db.Column(db.Integer, default=7)
    frentes = db.Column(db.Float, nullable=False, default=1.0)
    indirecto_pct = db.Column(db.Float, nullable=False, default=0.0)
    indirecto_campo_pct = db.Column(db.Float, nullable=False, default=0.0)
    indirecto_oficina_pct = db.Column(db.Float, nullable=False, default=0.0)
    financiamiento_pct = db.Column(db.Float, nullable=False, default=0.0)
    utilidad_pct = db.Column(db.Float, nullable=False, default=0.0)
    cargos_adicionales_pct = db.Column(db.Float, nullable=False, default=0.0)
    subtotal_directo = db.Column(db.Float, nullable=False, default=0.0)
    indirecto_monto = db.Column(db.Float, nullable=False, default=0.0)
    financiamiento_monto = db.Column(db.Float, nullable=False, default=0.0)
    utilidad_monto = db.Column(db.Float, nullable=False, default=0.0)
    cargos_adicionales_monto = db.Column(db.Float, nullable=False, default=0.0)
    total_venta = db.Column(db.Float, nullable=False, default=0.0)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    partidas = db.relationship(
        "ObraPartida",
        backref="obra",
        cascade="all, delete-orphan",
        order_by="ObraPartida.orden.asc(), ObraPartida.id.asc()",
    )


class ObraPartida(db.Model):
    __tablename__ = "apu_obra_partida"

    id = db.Column(db.Integer, primary_key=True)
    obra_id = db.Column(db.Integer, db.ForeignKey("apu_obra.id"), nullable=False)
    apu_id = db.Column(db.Integer, db.ForeignKey("apu.id"), nullable=False)
    orden = db.Column(db.Integer, nullable=False, default=0)
    capitulo = db.Column(db.String(120))
    subcapitulo = db.Column(db.String(120))
    clave = db.Column(db.String(60))
    concepto = db.Column(db.String(300))
    unidad = db.Column(db.String(50), nullable=False, default="m2")
    cantidad = db.Column(db.Float, nullable=False, default=1.0)
    rendimiento = db.Column(db.Float, nullable=False, default=0.0)
    precio_unitario = db.Column(db.Float, nullable=False, default=0.0)
    importe_directo = db.Column(db.Float, nullable=False, default=0.0)
    importe_venta = db.Column(db.Float, nullable=False, default=0.0)
    comentario = db.Column(db.String(500))

    apu = db.relationship("APU", foreign_keys=[apu_id])
