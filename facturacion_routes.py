from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from flask import Blueprint, Response, current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user, login_required
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from models import Cliente, Cotizacion, Factura, FacturacionConfig, FacturaPartida, db
from pac_providers import PacNotConfiguredError, get_pac_provider


facturacion_bp = Blueprint("facturacion", __name__, url_prefix="/facturacion")
MAR_BLUE = "#0C3C78"


def _money(value) -> float:
    try:
        return float(str(value or 0).replace("$", "").replace(",", "").strip())
    except Exception:
        return 0.0


def _text(value: str | None, default: str = "") -> str:
    return (value or default).strip()


def _active_config() -> FacturacionConfig | None:
    return (
        FacturacionConfig.query.filter_by(activo=True)
        .order_by(FacturacionConfig.id.desc())
        .first()
    )


def _next_folio() -> str:
    year = datetime.utcnow().year
    prefix = f"FAC-{year}-"
    last = (
        Factura.query.filter(Factura.folio.like(f"{prefix}%"))
        .order_by(Factura.id.desc())
        .first()
    )
    if not last or not last.folio:
        return f"{prefix}0001"
    try:
        return f"{prefix}{int(last.folio.rsplit('-', 1)[-1]) + 1:04d}"
    except Exception:
        return f"{prefix}{(last.id or 0) + 1:04d}"


def _recalculate(factura: Factura) -> None:
    subtotal = 0.0
    iva = 0.0
    descuento = 0.0
    for partida in factura.partidas:
        partida.importe = round(_money(partida.cantidad) * _money(partida.valor_unitario), 2)
        partida.descuento = _money(partida.descuento)
        base = max(partida.importe - partida.descuento, 0)
        partida.iva_importe = round(base * _money(partida.iva_tasa), 2)
        subtotal += partida.importe
        descuento += partida.descuento
        iva += partida.iva_importe
    factura.subtotal = round(subtotal, 2)
    factura.descuento = round(descuento, 2)
    factura.iva = round(iva, 2)
    factura.total = round(subtotal - descuento + iva, 2)


def _ensure_storage_dir() -> Path:
    storage = Path(current_app.instance_path) / "facturacion"
    storage.mkdir(parents=True, exist_ok=True)
    return storage


def _render_pdf(factura: Factura) -> bytes:
    import io

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, rightMargin=14 * mm, leftMargin=14 * mm, topMargin=14 * mm)
    styles = getSampleStyleSheet()
    story = [
        Paragraph(f"Factura {factura.folio}", styles["Title"]),
        Paragraph(f"Estado: {factura.estatus}", styles["Normal"]),
        Spacer(1, 8),
        Paragraph(f"Receptor: {factura.receptor_nombre} - {factura.receptor_rfc}", styles["Normal"]),
        Paragraph(f"Uso CFDI: {factura.uso_cfdi} | Metodo: {factura.metodo_pago} | Forma: {factura.forma_pago}", styles["Normal"]),
        Spacer(1, 10),
    ]
    rows = [["Cant.", "Unidad", "Descripcion", "P. Unitario", "Importe"]]
    for item in factura.partidas:
        rows.append([
            f"{item.cantidad:g}",
            item.unidad or item.clave_unidad,
            Paragraph(item.descripcion or "", styles["BodyText"]),
            f"${item.valor_unitario:,.2f}",
            f"${item.importe:,.2f}",
        ])
    rows.extend([
        ["", "", "", "Subtotal", f"${factura.subtotal:,.2f}"],
        ["", "", "", "IVA", f"${factura.iva:,.2f}"],
        ["", "", "", "Total", f"${factura.total:,.2f}"],
    ])
    table = Table(rows, colWidths=[18 * mm, 24 * mm, 84 * mm, 28 * mm, 28 * mm])
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(MAR_BLUE)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.lightgrey),
        ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(table)
    if factura.notas:
        story.extend([Spacer(1, 10), Paragraph(f"Notas: {factura.notas}", styles["Normal"])])
    doc.build(story)
    return buffer.getvalue()


def _save_pdf(factura: Factura) -> None:
    storage = _ensure_storage_dir()
    path = storage / f"{factura.folio}.pdf"
    path.write_bytes(_render_pdf(factura))
    factura.pdf_path = str(path)


@facturacion_bp.get("/")
@login_required
def index():
    facturas = Factura.query.order_by(Factura.fecha.desc(), Factura.id.desc()).limit(200).all()
    config = _active_config()
    total_mes = sum(f.total or 0 for f in facturas if f.fecha and f.fecha.month == datetime.utcnow().month)
    pendientes = sum(1 for f in facturas if f.estatus in {"BORRADOR", "PENDIENTE_TIMBRADO", "ERROR_TIMBRADO"})
    return render_template(
        "facturacion.html",
        facturas=facturas,
        config=config,
        total_mes=total_mes,
        pendientes=pendientes,
    )


@facturacion_bp.post("/config")
@login_required
def guardar_config():
    config = _active_config() or FacturacionConfig()
    config.rfc = _text(request.form.get("rfc")).upper()
    config.razon_social = _text(request.form.get("razon_social")).upper()
    config.regimen_fiscal = _text(request.form.get("regimen_fiscal"))
    config.codigo_postal = _text(request.form.get("codigo_postal"))
    config.pac = _text(request.form.get("pac"), "FACTURAMA").upper()
    config.pac_ambiente = _text(request.form.get("pac_ambiente"), "SANDBOX").upper()
    config.pac_usuario = _text(request.form.get("pac_usuario"))
    config.csd_cer_path = _text(request.form.get("csd_cer_path"))
    config.csd_key_path = _text(request.form.get("csd_key_path"))
    config.csd_no_certificado = _text(request.form.get("csd_no_certificado"))
    config.activo = True
    if not config.rfc or not config.razon_social or not config.regimen_fiscal or not config.codigo_postal:
        flash("Captura RFC, razon social, regimen fiscal y codigo postal.", "warning")
        return redirect(url_for("facturacion.index"))
    db.session.add(config)
    db.session.commit()
    flash("Configuracion fiscal guardada. Falta activar credenciales del PAC cuando termines el registro.", "success")
    return redirect(url_for("facturacion.index"))


@facturacion_bp.get("/nueva")
@login_required
def nueva():
    cotizacion_id = request.args.get("cotizacion_id", type=int)
    cotizacion = Cotizacion.query.get(cotizacion_id) if cotizacion_id else None
    clientes = Cliente.query.order_by(Cliente.empresa.asc(), Cliente.nombre_cliente.asc()).limit(300).all()
    return render_template("factura_form.html", factura=None, cotizacion=cotizacion, clientes=clientes)


@facturacion_bp.post("/crear")
@login_required
def crear():
    cliente_id = request.form.get("cliente_id", type=int)
    cotizacion_id = request.form.get("cotizacion_id", type=int)
    cliente = Cliente.query.get(cliente_id) if cliente_id else None
    cotizacion = Cotizacion.query.get(cotizacion_id) if cotizacion_id else None
    receptor_nombre = _text(request.form.get("receptor_nombre") or (cliente.empresa if cliente else "") or (cliente.nombre_cliente if cliente else ""))
    receptor_rfc = _text(request.form.get("receptor_rfc") or (cliente.rfc if cliente else "")).upper()
    if not receptor_nombre or not receptor_rfc:
        flash("Captura razon social/nombre y RFC del receptor.", "warning")
        return redirect(url_for("facturacion.nueva", cotizacion_id=cotizacion_id or ""))

    config = _active_config()
    factura = Factura(
        folio=_next_folio(),
        serie=request.form.get("serie") or "F",
        tipo_comprobante=request.form.get("tipo_comprobante") or "I",
        estatus="BORRADOR",
        cliente_id=cliente_id,
        cotizacion_id=cotizacion_id,
        receptor_rfc=receptor_rfc,
        receptor_nombre=receptor_nombre,
        receptor_regimen_fiscal=_text(request.form.get("receptor_regimen_fiscal")),
        receptor_codigo_postal=_text(request.form.get("receptor_codigo_postal")),
        uso_cfdi=_text(request.form.get("uso_cfdi"), "G03"),
        metodo_pago=_text(request.form.get("metodo_pago"), "PUE"),
        forma_pago=_text(request.form.get("forma_pago"), "03"),
        moneda=_text(request.form.get("moneda"), "MXN").upper(),
        notas=_text(request.form.get("notas")),
        pac=(config.pac if config else None),
        pac_ambiente=(config.pac_ambiente if config else None),
        creado_por_id=getattr(current_user, "id", None),
    )
    db.session.add(factura)
    db.session.flush()

    if cotizacion and cotizacion.detalles:
        for detalle in cotizacion.detalles:
            db.session.add(FacturaPartida(
                factura_id=factura.id,
                cantidad=detalle.cantidad or 1,
                unidad=detalle.unidad or "Servicio",
                descripcion=detalle.nombre_concepto or "Servicio",
                valor_unitario=detalle.precio_unitario or 0,
                importe=detalle.subtotal or 0,
            ))
    else:
        descripciones = request.form.getlist("descripcion[]")
        cantidades = request.form.getlist("cantidad[]")
        unidades = request.form.getlist("unidad[]")
        precios = request.form.getlist("valor_unitario[]")
        for idx, descripcion in enumerate(descripciones):
            descripcion = _text(descripcion)
            if not descripcion:
                continue
            db.session.add(FacturaPartida(
                factura_id=factura.id,
                cantidad=_money(cantidades[idx] if idx < len(cantidades) else 1) or 1,
                unidad=_text(unidades[idx] if idx < len(unidades) else "", "Servicio"),
                descripcion=descripcion,
                valor_unitario=_money(precios[idx] if idx < len(precios) else 0),
            ))

    db.session.flush()
    _recalculate(factura)
    _save_pdf(factura)
    db.session.commit()
    flash(f"Factura {factura.folio} creada como borrador.", "success")
    return redirect(url_for("facturacion.detalle", factura_id=factura.id))


@facturacion_bp.get("/<int:factura_id>")
@login_required
def detalle(factura_id: int):
    factura = Factura.query.get_or_404(factura_id)
    return render_template("factura_detalle.html", factura=factura, config=_active_config())


@facturacion_bp.post("/<int:factura_id>/timbrar")
@login_required
def timbrar(factura_id: int):
    factura = Factura.query.get_or_404(factura_id)
    config = _active_config()
    provider = get_pac_provider(config)
    try:
        result = provider.timbrar(factura)
    except PacNotConfiguredError as exc:
        factura.estatus = "PENDIENTE_TIMBRADO"
        factura.error_timbrado = str(exc)
        db.session.commit()
        flash(str(exc), "warning")
        return redirect(url_for("facturacion.detalle", factura_id=factura.id))

    if result.ok:
        factura.estatus = "TIMBRADA"
        factura.uuid = result.uuid
        factura.fecha_timbrado = datetime.utcnow()
        if result.xml:
            xml_path = _ensure_storage_dir() / f"{factura.folio}.xml"
            xml_path.write_text(result.xml, encoding="utf-8")
            factura.xml_path = str(xml_path)
        factura.error_timbrado = None
        flash(f"Factura {factura.folio} timbrada correctamente.", "success")
    else:
        factura.estatus = "PENDIENTE_TIMBRADO"
        factura.error_timbrado = result.message
        flash(result.message, "warning")
    db.session.commit()
    return redirect(url_for("facturacion.detalle", factura_id=factura.id))


@facturacion_bp.get("/<int:factura_id>/pdf")
@login_required
def descargar_pdf(factura_id: int):
    factura = Factura.query.get_or_404(factura_id)
    data = Path(factura.pdf_path).read_bytes() if factura.pdf_path and os.path.exists(factura.pdf_path) else _render_pdf(factura)
    return Response(
        data,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{factura.folio}.pdf"'},
    )
