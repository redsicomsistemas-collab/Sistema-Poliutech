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
    ciudad_trabajo = db.Column(db.String(120))
    area_total = db.Column(db.Float, default=0.0)
    memoria_tecnica = db.Column(db.Text)
    lista_materiales_json = db.Column(db.Text)

    detalles = db.relationship(
        "CotizacionDetalle",
        backref="cotizacion",
        cascade="all, delete-orphan"
    )
    seguimientos = db.relationship(
        "CotizacionSeguimiento",
        backref="cotizacion",
        cascade="all, delete-orphan",
        order_by="CotizacionSeguimiento.fecha_seguimiento.desc()"
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

    # ✅ ESTA RELACIÓN SÍ ES VÁLIDA porque existe concepto_id con FK
    concepto = db.relationship("Concepto")

    def __repr__(self):
        return f"<Detalle {self.nombre_concepto}>"


class CotizacionSeguimiento(db.Model):
    __tablename__ = "cotizacion_seguimiento"

    id = db.Column(db.Integer, primary_key=True)
    cotizacion_id = db.Column(db.Integer, db.ForeignKey("cotizacion.id"), nullable=False, index=True)
    usuario_id = db.Column(db.Integer, db.ForeignKey("usuario.id"), nullable=True)
    autor = db.Column(db.String(120), nullable=False)
    comentario = db.Column(db.Text, nullable=False)
    fecha_seguimiento = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    usuario = db.relationship("Usuario", backref=db.backref("seguimientos_cotizacion", lazy=True))

    def __repr__(self):
        return f"<CotizacionSeguimiento cotizacion={self.cotizacion_id} autor={self.autor}>"


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


class Prospecto(db.Model):
    __tablename__ = "prospecto"

    id = db.Column(db.Integer, primary_key=True)
    titulo = db.Column(db.String(220), nullable=False, default="")
    descripcion = db.Column(db.Text)
    contacto = db.Column(db.String(160))
    telefono = db.Column(db.String(60))
    correo = db.Column(db.String(160))
    status = db.Column(db.String(30), nullable=False, default="PENDIENTE")
    responsable = db.Column(db.String(120))
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    seguimientos = db.relationship(
        "ProspectoSeguimiento",
        backref="prospecto",
        cascade="all, delete-orphan",
        order_by="ProspectoSeguimiento.fecha_seguimiento.desc()"
    )

    def __repr__(self):
        return f"<Prospecto {self.id} {self.titulo}>"


class ProspectoSeguimiento(db.Model):
    __tablename__ = "prospecto_seguimiento"

    id = db.Column(db.Integer, primary_key=True)
    prospecto_id = db.Column(db.Integer, db.ForeignKey("prospecto.id"), nullable=False, index=True)
    usuario_id = db.Column(db.Integer, db.ForeignKey("usuario.id"), nullable=True)
    autor = db.Column(db.String(120), nullable=False)
    comentario = db.Column(db.Text, nullable=False)
    fecha_seguimiento = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    usuario = db.relationship("Usuario", backref=db.backref("seguimientos_prospecto", lazy=True))

    def __repr__(self):
        return f"<ProspectoSeguimiento prospecto={self.prospecto_id} autor={self.autor}>"

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


class APUSheet(db.Model):
    __tablename__ = "apu_sheet"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(120), nullable=False, unique=True, index=True)
    hidden = db.Column(db.Boolean, default=False, nullable=False)
    max_row = db.Column(db.Integer, default=1, nullable=False)
    max_col = db.Column(db.Integer, default=1, nullable=False)
    freeze_panes = db.Column(db.String(30))
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    celdas = db.relationship(
        "APUCell",
        backref="sheet",
        cascade="all, delete-orphan",
        lazy=True,
        order_by="APUCell.row_idx.asc(), APUCell.col_idx.asc()",
    )
    merges = db.relationship(
        "APUMerge",
        backref="sheet",
        cascade="all, delete-orphan",
        lazy=True,
        order_by="APUMerge.start_row.asc(), APUMerge.start_col.asc()",
    )

    def __repr__(self):
        return f"<APUSheet {self.nombre}>"


class APUCell(db.Model):
    __tablename__ = "apu_cell"
    __table_args__ = (
        db.UniqueConstraint("sheet_id", "coord", name="uq_apu_cell_sheet_coord"),
    )

    id = db.Column(db.Integer, primary_key=True)
    sheet_id = db.Column(db.Integer, db.ForeignKey("apu_sheet.id"), nullable=False, index=True)
    coord = db.Column(db.String(20), nullable=False)
    row_idx = db.Column(db.Integer, nullable=False, index=True)
    col_idx = db.Column(db.Integer, nullable=False, index=True)
    col_letter = db.Column(db.String(10), nullable=False)
    value = db.Column(db.Text)
    raw = db.Column(db.Text)
    formula = db.Column(db.Text)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<APUCell {self.coord}>"


class APUMerge(db.Model):
    __tablename__ = "apu_merge"

    id = db.Column(db.Integer, primary_key=True)
    sheet_id = db.Column(db.Integer, db.ForeignKey("apu_sheet.id"), nullable=False, index=True)
    rango = db.Column(db.String(40), nullable=False)
    start_row = db.Column(db.Integer, nullable=False)
    start_col = db.Column(db.Integer, nullable=False)
    end_row = db.Column(db.Integer, nullable=False)
    end_col = db.Column(db.Integer, nullable=False)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<APUMerge {self.rango}>"


class PUObra(db.Model):
    __tablename__ = "pu_obra"

    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(200), nullable=False)
    cliente = db.Column(db.String(180))
    ubicacion = db.Column(db.String(220))
    descripcion = db.Column(db.Text)
    moneda = db.Column(db.String(20), default="MXN", nullable=False)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    partidas = db.relationship("PUPartida", backref="obra", cascade="all, delete-orphan", order_by="PUPartida.id.asc()")
    sobrecosto = db.relationship("PUSobrecosto", backref="obra", cascade="all, delete-orphan", uselist=False)

    def __repr__(self):
        return f"<PUObra {self.nombre}>"


class PUSobrecosto(db.Model):
    __tablename__ = "pu_sobrecosto"

    id = db.Column(db.Integer, primary_key=True)
    obra_id = db.Column(db.Integer, db.ForeignKey("pu_obra.id"), nullable=False, unique=True)
    indirecto_campo_pct = db.Column(db.Float, default=0.0)
    indirecto_oficina_pct = db.Column(db.Float, default=0.0)
    financiamiento_pct = db.Column(db.Float, default=0.0)
    utilidad_pct = db.Column(db.Float, default=10.0)
    cargos_adicionales_pct = db.Column(db.Float, default=0.0)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<PUSobrecosto obra={self.obra_id}>"


class PURecurso(db.Model):
    __tablename__ = "pu_recurso"

    id = db.Column(db.Integer, primary_key=True)
    tipo = db.Column(db.String(30), nullable=False)  # material, mano_obra, maquinaria, basico, extra
    codigo = db.Column(db.String(60))
    descripcion = db.Column(db.String(300), nullable=False)
    unidad = db.Column(db.String(50), default="")
    costo_unitario = db.Column(db.Float, default=0.0)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<PURecurso {self.tipo} {self.descripcion}>"


class PUPartida(db.Model):
    __tablename__ = "pu_partida"

    id = db.Column(db.Integer, primary_key=True)
    obra_id = db.Column(db.Integer, db.ForeignKey("pu_obra.id"), nullable=False, index=True)
    capitulo = db.Column(db.String(160), default="General")
    clave = db.Column(db.String(80))
    descripcion = db.Column(db.String(600), nullable=False)
    unidad = db.Column(db.String(50), default="pza")
    cantidad = db.Column(db.Float, default=1.0)
    costo_directo = db.Column(db.Float, default=0.0)
    precio_unitario = db.Column(db.Float, default=0.0)
    importe = db.Column(db.Float, default=0.0)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    insumos = db.relationship("PUPartidaInsumo", backref="partida", cascade="all, delete-orphan", order_by="PUPartidaInsumo.id.asc()")

    def __repr__(self):
        return f"<PUPartida {self.clave or self.id}>"


class PUPartidaInsumo(db.Model):
    __tablename__ = "pu_partida_insumo"

    id = db.Column(db.Integer, primary_key=True)
    partida_id = db.Column(db.Integer, db.ForeignKey("pu_partida.id"), nullable=False, index=True)
    recurso_id = db.Column(db.Integer, db.ForeignKey("pu_recurso.id"), nullable=True)
    tipo = db.Column(db.String(30), nullable=False, default="material")
    codigo = db.Column(db.String(60))
    descripcion = db.Column(db.String(400), nullable=False)
    unidad = db.Column(db.String(50), default="")
    cantidad = db.Column(db.Float, default=0.0)
    costo_unitario = db.Column(db.Float, default=0.0)
    importe = db.Column(db.Float, default=0.0)
    creado_en = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    actualizado_en = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    recurso = db.relationship("PURecurso")

    def __repr__(self):
        return f"<PUPartidaInsumo {self.tipo} {self.descripcion}>"
