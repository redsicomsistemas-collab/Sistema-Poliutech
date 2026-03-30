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
    area_total = db.Column(db.Float, default=0.0)
    memoria_tecnica = db.Column(db.Text)
    lista_materiales_json = db.Column(db.Text)

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
    capitulo = db.Column(db.String(120))
    sistema = db.Column(db.String(200))
    descripcion = db.Column(db.String(1000))
    subtotal = db.Column(db.Float, default=0)
    origen = db.Column(db.String(50))
    apu_id = db.Column(db.Integer)
    apu_clave = db.Column(db.String(80))
    apu_directo = db.Column(db.Float, default=0.0)
    apu_resumen_json = db.Column(db.Text)

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


class MobileDevice(db.Model):
    __tablename__ = "mobile_device"

    id = db.Column(db.Integer, primary_key=True)
    usuario_id = db.Column(db.Integer, db.ForeignKey("usuario.id"), nullable=False)
    token = db.Column(db.String(512), nullable=False, unique=True)
    plataforma = db.Column(db.String(30), default="android", nullable=False)
    device_name = db.Column(db.String(120))
    app_version = db.Column(db.String(40))
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_seen_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    usuario = db.relationship("Usuario", backref=db.backref("mobile_devices", lazy=True, cascade="all, delete-orphan"))

    def __repr__(self):
        return f"<MobileDevice user={self.usuario_id} platform={self.plataforma} active={self.is_active}>"


class RegistroObra(db.Model):
    __tablename__ = "registro_obra"

    id = db.Column(db.Integer, primary_key=True)
    numero = db.Column(db.Integer, nullable=False, default=1)
    obra = db.Column(db.String(220), nullable=False, default="")
    ubicacion = db.Column(db.String(220))
    encargado = db.Column(db.String(160))
    puesto = db.Column(db.String(160))
    telefono = db.Column(db.String(60))
    correo = db.Column(db.String(160))
    responsable = db.Column(db.String(120))
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<RegistroObra {self.id} {self.obra}>"

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


class PUObra(db.Model):
    __tablename__ = "pu_obra"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), nullable=False)
    descripcion = db.Column(db.String(1000))
    direccion = db.Column(db.String(220))
    colonia = db.Column(db.String(160))
    ciudad = db.Column(db.String(160))
    estado = db.Column(db.String(160))
    codigo_postal = db.Column(db.String(20))
    telefono = db.Column(db.String(60))
    correo = db.Column(db.String(160))
    observaciones = db.Column(db.Text)
    empresa = db.Column(db.String(180))
    encargado = db.Column(db.String(160))
    responsable = db.Column(db.String(120))
    fecha_inicio = db.Column(db.Date)
    fecha_terminacion = db.Column(db.Date)
    plazo_dias = db.Column(db.Integer, default=0)
    moneda = db.Column(db.String(20), default="PESOS")
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    partidas = db.relationship("PUPartida", backref="obra", cascade="all, delete-orphan", order_by="PUPartida.id.asc()")
    sobrecosto = db.relationship("PUSobrecosto", backref="obra", cascade="all, delete-orphan", uselist=False)

    def __repr__(self):
        return f"<PUObra {self.nombre}>"


class PURecurso(db.Model):
    __tablename__ = "pu_recurso"

    id = db.Column(db.Integer, primary_key=True)
    tipo = db.Column(db.String(30), nullable=False)  # material, mano_obra, maquinaria
    codigo = db.Column(db.String(60))
    descripcion = db.Column(db.String(300), nullable=False)
    unidad = db.Column(db.String(50))
    costo_base = db.Column(db.Float, default=0.0)
    familia = db.Column(db.String(120))
    gravable = db.Column(db.Boolean, default=True, nullable=False)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<PURecurso {self.tipo} {self.codigo or self.id}>"


class PUSobrecosto(db.Model):
    __tablename__ = "pu_sobrecosto"

    id = db.Column(db.Integer, primary_key=True)
    obra_id = db.Column(db.Integer, db.ForeignKey("pu_obra.id"), nullable=False, unique=True)
    porcentaje_utilidad_propuesta = db.Column(db.Float, default=10.0)
    tasa_interes_usada = db.Column(db.Float, default=0.0)
    porcentaje_puntos_banco = db.Column(db.Float, default=0.0)
    porcentaje_primer_anticipo = db.Column(db.Float, default=0.0)
    factor_sfp = db.Column(db.Float, default=0.0)
    indicador_economico = db.Column(db.String(120))
    tipo_anticipo = db.Column(db.String(120), default="Un ejercicio con un anticipo")
    libro_sobrecosto = db.Column(db.String(120), default="Sobrecosto estandar")
    programa_obra = db.Column(db.String(120), default="Programa base")
    num_veces = db.Column(db.Integer, default=1)
    libro_pie_indirectos = db.Column(db.String(120), default="Indirectos manuales")
    indirecto_campo_pct = db.Column(db.Float, default=0.0)
    indirecto_oficina_pct = db.Column(db.Float, default=0.0)
    financiamiento_pct = db.Column(db.Float, default=0.0)
    utilidad_pct = db.Column(db.Float, default=10.0)
    cargos_adicionales_pct = db.Column(db.Float, default=0.0)
    factor_pie_indirectos = db.Column(db.Float, default=1.0)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<PUSobrecosto obra={self.obra_id}>"


class PUPartida(db.Model):
    __tablename__ = "pu_partida"

    id = db.Column(db.Integer, primary_key=True)
    obra_id = db.Column(db.Integer, db.ForeignKey("pu_obra.id"), nullable=False)
    capitulo = db.Column(db.String(160), default="General")
    wbs = db.Column(db.String(40))
    codigo = db.Column(db.String(60))
    descripcion = db.Column(db.String(500), nullable=False)
    unidad = db.Column(db.String(50), default="pza")
    cantidad = db.Column(db.Float, default=1.0)
    precio_directo = db.Column(db.Float, default=0.0)
    precio_unitario = db.Column(db.Float, default=0.0)
    importe_total = db.Column(db.Float, default=0.0)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    insumos = db.relationship(
        "PUPartidaInsumo",
        backref="partida",
        cascade="all, delete-orphan",
        order_by="PUPartidaInsumo.orden.asc(), PUPartidaInsumo.id.asc()",
    )

    def __repr__(self):
        return f"<PUPartida {self.codigo or self.id}>"


class PUPartidaInsumo(db.Model):
    __tablename__ = "pu_partida_insumo"

    id = db.Column(db.Integer, primary_key=True)
    partida_id = db.Column(db.Integer, db.ForeignKey("pu_partida.id"), nullable=False)
    recurso_id = db.Column(db.Integer, db.ForeignKey("pu_recurso.id"), nullable=True)
    orden = db.Column(db.Integer, default=0)
    tipo = db.Column(db.String(30), nullable=False)  # material, mano_obra, maquinaria, porcentaje_mo, porcentaje_cd, otro
    base_tipo = db.Column(db.String(30))
    codigo = db.Column(db.String(60))
    descripcion = db.Column(db.String(300), nullable=False)
    unidad = db.Column(db.String(50))
    costo_unitario = db.Column(db.Float, default=0.0)
    cantidad = db.Column(db.Float, default=0.0)
    porcentaje = db.Column(db.Float, default=0.0)
    importe = db.Column(db.Float, default=0.0)
    gravable = db.Column(db.Boolean, default=True, nullable=False)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    recurso = db.relationship("PURecurso")

    def __repr__(self):
        return f"<PUPartidaInsumo {self.tipo} {self.codigo or self.id}>"
