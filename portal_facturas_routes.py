from __future__ import annotations

import os
import re
import secrets
import smtplib
import xml.etree.ElementTree as ET
from datetime import date, datetime
from email.message import EmailMessage
from functools import wraps
from html import escape
from pathlib import Path

from flask import (
    Blueprint,
    abort,
    current_app,
    flash,
    g,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask_login import current_user, login_required
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import secure_filename

from contabilidad_access import can_access_contabilidad
from models import (
    FacturaProveedor,
    FacturaProveedorMovimiento,
    InAppNotification,
    PortalProveedorUsuario,
    Usuario,
    db,
)


portal_facturas_bp = Blueprint(
    "portal_facturas",
    __name__,
    url_prefix="/portal-facturas",
)

SESSION_KEY = "portal_facturas_usuario_id"
CSRF_KEY = "portal_facturas_csrf"
MAX_XML_BYTES = 5 * 1024 * 1024
MAX_PDF_BYTES = 15 * 1024 * 1024
RFC_PATTERN = re.compile(r"^[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}$")
FINANCE_NOTIFICATION_PROFILES = (
    {
        "label": "Marco",
        "aliases": ("marco", "mescalera", "mesacalera"),
        "fallback_email": "mescalera@poliutech.com",
    },
    {
        "label": "Uriel",
        "aliases": ("uriel", "umorales"),
        "fallback_email": "umorales@poliutech.com",
    },
)

ESTATUS = {
    "RECIBIDA": {"label": "Recibida", "class": "status-blue"},
    "EN_REVISION": {"label": "En revisión", "class": "status-amber"},
    "CORRECCION_SOLICITADA": {"label": "Requiere corrección", "class": "status-orange"},
    "APROBADA": {"label": "Aprobada", "class": "status-teal"},
    "PROGRAMADA": {"label": "Pago programado", "class": "status-purple"},
    "PAGADA": {"label": "Pagada", "class": "status-green"},
    "RECHAZADA": {"label": "Rechazada", "class": "status-red"},
}


def _now() -> datetime:
    return datetime.utcnow()


def _csrf_token() -> str:
    token = session.get(CSRF_KEY)
    if not token:
        token = secrets.token_urlsafe(32)
        session[CSRF_KEY] = token
    return token


def _require_csrf() -> None:
    expected = str(session.get(CSRF_KEY) or "")
    received = str(request.form.get("csrf_token") or request.headers.get("X-CSRF-Token") or "")
    if not expected or not received or not secrets.compare_digest(expected, received):
        abort(400, "La sesión del formulario venció. Recarga la página e inténtalo de nuevo.")


@portal_facturas_bp.before_request
def _load_portal_user() -> None:
    g.portal_proveedor = None
    user_id = session.get(SESSION_KEY)
    if user_id:
        try:
            g.portal_proveedor = db.session.get(PortalProveedorUsuario, int(user_id))
        except (TypeError, ValueError):
            session.pop(SESSION_KEY, None)


@portal_facturas_bp.context_processor
def _portal_template_context():
    return {
        "portal_csrf_token": _csrf_token,
        "portal_proveedor": getattr(g, "portal_proveedor", None),
        "factura_estatus": ESTATUS,
    }


def proveedor_login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        proveedor = getattr(g, "portal_proveedor", None)
        if not proveedor:
            return redirect(url_for("portal_facturas.ingresar", next=request.path))
        if proveedor.estatus != "ACTIVO":
            session.pop(SESSION_KEY, None)
            flash("Tu cuenta no está activa. Contacta al departamento de Finanzas.", "danger")
            return redirect(url_for("portal_facturas.ingresar"))
        return view(*args, **kwargs)

    return wrapped


def finanzas_required(view):
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        if not can_access_contabilidad(current_user):
            abort(403)
        return view(*args, **kwargs)

    return wrapped


def _normalize_rfc(raw: str) -> str:
    return re.sub(r"\s+", "", (raw or "").strip().upper())


def _normalize_email(raw: str) -> str:
    return (raw or "").strip().casefold()


def _finance_notification_targets() -> tuple[list[Usuario], list[str]]:
    """Resuelve las cuentas de Marco y Uriel y conserva sus correos como respaldo."""
    users = Usuario.query.order_by(Usuario.id.asc()).all()
    selected_users: list[Usuario] = []
    recipient_emails: list[str] = []
    seen_user_ids: set[int] = set()
    seen_emails: set[str] = set()

    for profile in FINANCE_NOTIFICATION_PROFILES:
        fallback_email = _normalize_email(profile["fallback_email"])
        aliases = tuple(str(value).casefold() for value in profile["aliases"])
        matched_user = next(
            (
                user
                for user in users
                if _normalize_email(getattr(user, "correo", "")) == fallback_email
            ),
            None,
        )
        if matched_user is None:
            for user in users:
                identity_values = (
                    str(getattr(user, "nombre", "") or "").strip().casefold(),
                    str(getattr(user, "nombre_visible", "") or "").strip().casefold(),
                )
                if any(
                    value == alias or value.startswith(f"{alias} ")
                    for value in identity_values
                    if value
                    for alias in aliases
                ):
                    matched_user = user
                    break

        if matched_user is not None and matched_user.id not in seen_user_ids:
            selected_users.append(matched_user)
            seen_user_ids.add(matched_user.id)

        email = _normalize_email(getattr(matched_user, "correo", "")) if matched_user else ""
        email = email or fallback_email
        if email and email not in seen_emails:
            recipient_emails.append(email)
            seen_emails.add(email)

    return selected_users, recipient_emails


def _invoice_notification_copy(factura: FacturaProveedor, *, corrected: bool) -> tuple[str, str]:
    provider = factura.proveedor_usuario
    provider_name = (
        getattr(provider, "nombre_comercial", None)
        or getattr(provider, "razon_social", None)
        or factura.emisor_nombre
        or factura.emisor_rfc
        or "Proveedor"
    )
    action = "Factura corregida" if corrected else "Nueva factura recibida"
    title = f"{action}: {factura.folio_recepcion}"
    body = (
        f"{provider_name} envió {factura.folio_recepcion} por "
        f"${float(factura.total or 0):,.2f} {factura.moneda or 'MXN'}."
    )
    return title, body


def _send_finance_invoice_email(
    factura: FacturaProveedor,
    recipients: list[str],
    *,
    corrected: bool,
) -> None:
    if not recipients or current_app.config.get("PORTAL_FACTURAS_DISABLE_EMAIL"):
        return

    title, body = _invoice_notification_copy(factura, corrected=corrected)
    detail_url = url_for(
        "portal_facturas.finanzas_detalle",
        factura_id=factura.id,
        _external=True,
    )
    provider = factura.proveedor_usuario
    provider_name = (
        getattr(provider, "nombre_comercial", None)
        or getattr(provider, "razon_social", None)
        or factura.emisor_nombre
        or "Proveedor"
    )

    msg = EmailMessage()
    msg["Subject"] = title
    smtp_from = str(current_app.config.get("SMTP_FROM") or current_app.config.get("SMTP_USERNAME") or "").strip()
    msg["From"] = f"PORTAL DE FACTURAS POLIUTECH <{smtp_from}>"
    msg["To"] = ", ".join(recipients)
    msg.set_content(
        f"{body}\n"
        f"Proveedor: {provider_name}\n"
        f"RFC: {factura.emisor_rfc}\n"
        f"UUID: {factura.uuid_cfdi}\n"
        f"Orden de compra: {factura.orden_compra or 'Sin referencia'}\n\n"
        f"Revisar factura: {detail_url}\n"
    )
    msg.add_alternative(
        (
            "<div style='font-family:Arial,sans-serif;max-width:680px;color:#15263b'>"
            f"<h2 style='margin-bottom:8px'>{escape(title)}</h2>"
            f"<p>{escape(body)}</p>"
            "<table style='border-collapse:collapse;margin:18px 0'>"
            f"<tr><td style='padding:5px 14px 5px 0;color:#607086'>Proveedor</td><td><b>{escape(str(provider_name))}</b></td></tr>"
            f"<tr><td style='padding:5px 14px 5px 0;color:#607086'>RFC</td><td>{escape(factura.emisor_rfc or '')}</td></tr>"
            f"<tr><td style='padding:5px 14px 5px 0;color:#607086'>UUID</td><td>{escape(factura.uuid_cfdi or '')}</td></tr>"
            f"<tr><td style='padding:5px 14px 5px 0;color:#607086'>Orden de compra</td><td>{escape(factura.orden_compra or 'Sin referencia')}</td></tr>"
            "</table>"
            f"<p><a href='{escape(detail_url)}' style='display:inline-block;padding:11px 18px;background:#f97316;color:#fff;text-decoration:none;border-radius:7px'>Revisar factura</a></p>"
            "</div>"
        ),
        subtype="html",
    )

    smtp_host = str(current_app.config.get("SMTP_HOST") or "").strip()
    smtp_port = int(current_app.config.get("SMTP_PORT") or 26)
    smtp_username = str(current_app.config.get("SMTP_USERNAME") or "").strip()
    smtp_password = str(current_app.config.get("SMTP_PASSWORD") or "")
    if not smtp_host or not smtp_username or not smtp_password:
        raise RuntimeError("La configuración SMTP del portal está incompleta.")
    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as smtp:
        smtp.ehlo()
        smtp.login(smtp_username, smtp_password)
        smtp.send_message(msg, to_addrs=recipients)


def _notify_finance_invoice(factura: FacturaProveedor, *, corrected: bool = False) -> None:
    """Avisa a Marco y Uriel sin impedir que el proveedor termine su envío."""
    try:
        users, recipients = _finance_notification_targets()
        title, body = _invoice_notification_copy(factura, corrected=corrected)
        detail_url = url_for("portal_facturas.finanzas_detalle", factura_id=factura.id)
    except Exception as exc:
        current_app.logger.warning(
            "No se pudieron resolver los destinatarios de la factura %s: %s",
            factura.folio_recepcion,
            exc,
        )
        return

    try:
        for user in users:
            db.session.add(
                InAppNotification(
                    usuario_id=user.id,
                    tipo="portal_facturas",
                    titulo=title[:180],
                    mensaje=body,
                    destino_url=detail_url,
                    creada_en=_now(),
                )
            )
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        current_app.logger.warning(
            "No se pudieron guardar las notificaciones de la factura %s: %s",
            factura.folio_recepcion,
            exc,
        )

    try:
        _send_finance_invoice_email(factura, recipients, corrected=corrected)
    except Exception as exc:
        current_app.logger.warning(
            "No se pudo enviar el correo de la factura %s a %s: %s",
            factura.folio_recepcion,
            recipients,
            exc,
        )


def _parse_date(raw: str) -> date | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise ValueError("Captura una fecha válida.")


def _read_upload(uploaded, *, max_bytes: int, expected_ext: str, label: str) -> bytes:
    if not uploaded or not (uploaded.filename or "").strip():
        raise ValueError(f"Adjunta el archivo {label}.")
    filename = secure_filename(uploaded.filename or "")
    if Path(filename).suffix.lower() != expected_ext:
        raise ValueError(f"El archivo {label} debe tener extensión {expected_ext}.")
    payload = uploaded.stream.read(max_bytes + 1)
    if not payload:
        raise ValueError(f"El archivo {label} está vacío.")
    if len(payload) > max_bytes:
        max_mb = max_bytes // (1024 * 1024)
        raise ValueError(f"El archivo {label} supera el límite de {max_mb} MB.")
    return payload


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _xml_attr(node: ET.Element | None, name: str, default: str = "") -> str:
    if node is None:
        return default
    for key, value in node.attrib.items():
        if key.casefold() == name.casefold():
            return str(value or "").strip()
    return default


def _parse_cfdi(xml_bytes: bytes) -> dict:
    probe = xml_bytes[:4096].upper()
    if b"<!DOCTYPE" in probe or b"<!ENTITY" in probe:
        raise ValueError("El XML contiene una declaración no permitida.")
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise ValueError("El XML no es un CFDI válido o está dañado.") from exc
    if _xml_local_name(root.tag).casefold() != "comprobante":
        raise ValueError("El XML no corresponde a un comprobante CFDI.")

    emisor = next((node for node in root.iter() if _xml_local_name(node.tag).casefold() == "emisor"), None)
    receptor = next((node for node in root.iter() if _xml_local_name(node.tag).casefold() == "receptor"), None)
    timbre = next((node for node in root.iter() if _xml_local_name(node.tag).casefold() == "timbrefiscaldigital"), None)
    uuid = _xml_attr(timbre, "UUID").upper()
    if not uuid:
        raise ValueError("El XML no contiene el UUID del timbre fiscal.")

    fecha_raw = _xml_attr(root, "Fecha")
    try:
        fecha_emision = datetime.fromisoformat(fecha_raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError as exc:
        raise ValueError("El CFDI no contiene una fecha de emisión válida.") from exc

    try:
        subtotal = float(_xml_attr(root, "SubTotal", "0") or 0)
        total = float(_xml_attr(root, "Total", "0") or 0)
    except ValueError as exc:
        raise ValueError("Los importes del CFDI no son válidos.") from exc
    if total <= 0:
        raise ValueError("El total del CFDI debe ser mayor a cero.")

    emisor_rfc = _normalize_rfc(_xml_attr(emisor, "Rfc"))
    receptor_rfc = _normalize_rfc(_xml_attr(receptor, "Rfc"))
    if not emisor_rfc or not receptor_rfc:
        raise ValueError("El CFDI no contiene los RFC de emisor y receptor.")

    return {
        "uuid_cfdi": uuid,
        "serie": _xml_attr(root, "Serie")[:30] or None,
        "folio_cfdi": _xml_attr(root, "Folio")[:80] or None,
        "fecha_emision": fecha_emision,
        "emisor_rfc": emisor_rfc,
        "emisor_nombre": _xml_attr(emisor, "Nombre")[:220] or None,
        "receptor_rfc": receptor_rfc,
        "receptor_nombre": _xml_attr(receptor, "Nombre")[:220] or None,
        "subtotal": round(subtotal, 2),
        "total": round(total, 2),
        "moneda": (_xml_attr(root, "Moneda", "MXN") or "MXN")[:10].upper(),
    }


def _upload_root() -> Path:
    configured = (os.getenv("UPLOAD_STORAGE_ROOT") or "").strip()
    if configured:
        root = Path(configured).expanduser().resolve()
    elif Path("/data").is_dir():
        root = Path("/data/uploads").resolve()
    else:
        root = (Path(current_app.static_folder or "static").resolve() / "uploads")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _safe_upload_path(relative_path: str) -> Path:
    root = _upload_root()
    normalized = str(relative_path or "").replace("\\", "/").lstrip("/")
    candidate = (root / normalized).resolve()
    if candidate != root and root not in candidate.parents:
        abort(404)
    return candidate


def _save_invoice_files(proveedor_id: int, uuid_cfdi: str, xml_bytes: bytes, pdf_bytes: bytes) -> tuple[str, str]:
    folder = _upload_root() / "portal_facturas" / str(proveedor_id)
    folder.mkdir(parents=True, exist_ok=True)
    safe_uuid = re.sub(r"[^A-Za-z0-9-]", "", uuid_cfdi)[:60] or secrets.token_hex(16)
    nonce = secrets.token_hex(5)
    xml_name = f"{safe_uuid}_{nonce}.xml"
    pdf_name = f"{safe_uuid}_{nonce}.pdf"
    xml_path = folder / xml_name
    pdf_path = folder / pdf_name
    xml_path.write_bytes(xml_bytes)
    try:
        pdf_path.write_bytes(pdf_bytes)
    except Exception:
        xml_path.unlink(missing_ok=True)
        raise
    return (
        f"portal_facturas/{proveedor_id}/{xml_name}",
        f"portal_facturas/{proveedor_id}/{pdf_name}",
    )


def _delete_paths(*relative_paths: str) -> None:
    for relative_path in relative_paths:
        if not relative_path:
            continue
        try:
            _safe_upload_path(relative_path).unlink(missing_ok=True)
        except OSError:
            current_app.logger.warning("No se pudo eliminar el archivo reemplazado %s", relative_path)


def _add_event(
    factura: FacturaProveedor,
    *,
    old_status: str | None,
    new_status: str,
    actor_type: str,
    actor_name: str,
    comment: str = "",
    actor_user_id: int | None = None,
) -> None:
    db.session.add(
        FacturaProveedorMovimiento(
            factura=factura,
            estatus_anterior=old_status,
            estatus_nuevo=new_status,
            actor_tipo=actor_type,
            actor_usuario_id=actor_user_id,
            actor_nombre=(actor_name or "Sistema")[:180],
            comentario=(comment or "").strip() or None,
        )
    )


def _prepare_invoice_upload(proveedor: PortalProveedorUsuario) -> tuple[dict, bytes, bytes, str, str]:
    xml_upload = request.files.get("xml")
    pdf_upload = request.files.get("pdf")
    xml_bytes = _read_upload(xml_upload, max_bytes=MAX_XML_BYTES, expected_ext=".xml", label="XML")
    pdf_bytes = _read_upload(pdf_upload, max_bytes=MAX_PDF_BYTES, expected_ext=".pdf", label="PDF")
    if not pdf_bytes.startswith(b"%PDF-"):
        raise ValueError("El archivo PDF no tiene un formato válido.")
    cfdi = _parse_cfdi(xml_bytes)
    if cfdi["emisor_rfc"] != proveedor.rfc:
        raise ValueError(
            f"El RFC emisor del XML ({cfdi['emisor_rfc']}) no coincide con el RFC de tu cuenta ({proveedor.rfc})."
        )
    xml_name = secure_filename(xml_upload.filename or "factura.xml")[:260] or "factura.xml"
    pdf_name = secure_filename(pdf_upload.filename or "factura.pdf")[:260] or "factura.pdf"
    return cfdi, xml_bytes, pdf_bytes, xml_name, pdf_name


@portal_facturas_bp.get("/")
def inicio():
    if getattr(g, "portal_proveedor", None):
        return redirect(url_for("portal_facturas.mis_facturas"))
    return redirect(url_for("portal_facturas.ingresar"))


@portal_facturas_bp.route("/registro", methods=["GET", "POST"])
def registro():
    if getattr(g, "portal_proveedor", None):
        return redirect(url_for("portal_facturas.mis_facturas"))
    if request.method == "POST":
        _require_csrf()
        razon_social = (request.form.get("razon_social") or "").strip()
        nombre_comercial = (request.form.get("nombre_comercial") or "").strip()
        rfc = _normalize_rfc(request.form.get("rfc") or "")
        contacto = (request.form.get("contacto") or "").strip()
        correo = _normalize_email(request.form.get("correo") or "")
        telefono = (request.form.get("telefono") or "").strip()
        password = request.form.get("password") or ""
        confirmation = request.form.get("password_confirmation") or ""

        if not all([razon_social, rfc, contacto, correo, password]):
            flash("Completa todos los campos obligatorios.", "danger")
        elif not RFC_PATTERN.fullmatch(rfc):
            flash("Captura un RFC válido, sin espacios ni guiones.", "danger")
        elif "@" not in correo or "." not in correo.rsplit("@", 1)[-1]:
            flash("Captura un correo electrónico válido.", "danger")
        elif len(password) < 8:
            flash("La contraseña debe tener al menos 8 caracteres.", "danger")
        elif password != confirmation:
            flash("Las contraseñas no coinciden.", "danger")
        elif PortalProveedorUsuario.query.filter(
            or_(
                func.upper(PortalProveedorUsuario.rfc) == rfc,
                func.lower(PortalProveedorUsuario.correo) == correo,
            )
        ).first():
            flash("Ya existe una cuenta con ese RFC o correo electrónico.", "danger")
        else:
            proveedor = PortalProveedorUsuario(
                razon_social=razon_social[:200],
                nombre_comercial=nombre_comercial[:180] or None,
                rfc=rfc,
                contacto=contacto[:160],
                correo=correo[:180],
                telefono=telefono[:40] or None,
                estatus="ACTIVO",
            )
            proveedor.set_password(password)
            db.session.add(proveedor)
            try:
                db.session.commit()
            except IntegrityError:
                db.session.rollback()
                flash("Ya existe una cuenta con ese RFC o correo electrónico.", "danger")
            else:
                session[SESSION_KEY] = proveedor.id
                session[CSRF_KEY] = secrets.token_urlsafe(32)
                flash("Tu cuenta quedó creada. Ya puedes enviar tu primera factura.", "success")
                return redirect(url_for("portal_facturas.nueva_factura"))

    return render_template("portal_facturas/registro.html")


@portal_facturas_bp.route("/ingresar", methods=["GET", "POST"])
def ingresar():
    if getattr(g, "portal_proveedor", None):
        return redirect(url_for("portal_facturas.mis_facturas"))
    if request.method == "POST":
        _require_csrf()
        correo = _normalize_email(request.form.get("correo") or "")
        password = request.form.get("password") or ""
        proveedor = PortalProveedorUsuario.query.filter(
            func.lower(PortalProveedorUsuario.correo) == correo
        ).first()
        if not proveedor or not proveedor.check_password(password):
            flash("Correo o contraseña incorrectos.", "danger")
        elif proveedor.estatus != "ACTIVO":
            flash("Tu cuenta no está activa. Contacta al departamento de Finanzas.", "danger")
        else:
            proveedor.ultimo_acceso_en = _now()
            db.session.commit()
            session[SESSION_KEY] = proveedor.id
            session[CSRF_KEY] = secrets.token_urlsafe(32)
            return redirect(url_for("portal_facturas.mis_facturas"))
    return render_template("portal_facturas/login.html")


@portal_facturas_bp.post("/salir")
def salir():
    _require_csrf()
    session.pop(SESSION_KEY, None)
    session.pop(CSRF_KEY, None)
    flash("Sesión cerrada correctamente.", "success")
    return redirect(url_for("portal_facturas.ingresar"))


@portal_facturas_bp.get("/mis-facturas")
@proveedor_login_required
def mis_facturas():
    facturas = FacturaProveedor.query.filter_by(
        proveedor_usuario_id=g.portal_proveedor.id
    ).order_by(FacturaProveedor.recibida_en.desc()).all()
    resumen = {
        "total": len(facturas),
        "en_proceso": sum(1 for item in facturas if item.estatus not in {"PAGADA", "RECHAZADA"}),
        "pagadas": sum(1 for item in facturas if item.estatus == "PAGADA"),
        "importe": sum(float(item.total or 0) for item in facturas),
    }
    return render_template("portal_facturas/mis_facturas.html", facturas=facturas, resumen=resumen)


@portal_facturas_bp.route("/facturas/nueva", methods=["GET", "POST"])
@proveedor_login_required
def nueva_factura():
    if request.method == "POST":
        _require_csrf()
        try:
            cfdi, xml_bytes, pdf_bytes, xml_original, pdf_original = _prepare_invoice_upload(g.portal_proveedor)
            if FacturaProveedor.query.filter_by(uuid_cfdi=cfdi["uuid_cfdi"]).first():
                raise ValueError("Este UUID ya fue recibido anteriormente.")
            xml_path, pdf_path = _save_invoice_files(
                g.portal_proveedor.id,
                cfdi["uuid_cfdi"],
                xml_bytes,
                pdf_bytes,
            )
            factura = FacturaProveedor(
                folio_recepcion=f"TMP-{secrets.token_hex(10)}",
                proveedor_usuario_id=g.portal_proveedor.id,
                concepto=(request.form.get("concepto") or "").strip()[:300] or None,
                orden_compra=(request.form.get("orden_compra") or "").strip()[:120] or None,
                notas_proveedor=(request.form.get("notas_proveedor") or "").strip() or None,
                estatus="RECIBIDA",
                xml_path=xml_path,
                pdf_path=pdf_path,
                xml_nombre_original=xml_original,
                pdf_nombre_original=pdf_original,
                xml_tamano=len(xml_bytes),
                pdf_tamano=len(pdf_bytes),
                **cfdi,
            )
            db.session.add(factura)
            db.session.flush()
            factura.folio_recepcion = f"PF-{_now().strftime('%Y%m')}-{factura.id:05d}"
            _add_event(
                factura,
                old_status=None,
                new_status="RECIBIDA",
                actor_type="PROVEEDOR",
                actor_name=g.portal_proveedor.contacto,
                comment="Factura enviada para revisión.",
            )
            db.session.commit()
            _notify_finance_invoice(factura)
        except (ValueError, OSError, IntegrityError) as exc:
            db.session.rollback()
            if "xml_path" in locals() and "pdf_path" in locals():
                _delete_paths(xml_path, pdf_path)
            message = str(exc) if isinstance(exc, (ValueError, OSError)) else "No fue posible guardar la factura. Verifica que no esté duplicada."
            flash(message, "danger")
        else:
            flash(f"Factura {factura.folio_recepcion} recibida correctamente.", "success")
            return redirect(url_for("portal_facturas.detalle_factura", factura_id=factura.id))
    return render_template("portal_facturas/nueva_factura.html")


def _owned_invoice_or_404(factura_id: int) -> FacturaProveedor:
    factura = db.session.get(FacturaProveedor, factura_id)
    if not factura or factura.proveedor_usuario_id != g.portal_proveedor.id:
        abort(404)
    return factura


@portal_facturas_bp.get("/facturas/<int:factura_id>")
@proveedor_login_required
def detalle_factura(factura_id: int):
    return render_template(
        "portal_facturas/detalle_factura.html",
        factura=_owned_invoice_or_404(factura_id),
    )


@portal_facturas_bp.route("/facturas/<int:factura_id>/corregir", methods=["GET", "POST"])
@proveedor_login_required
def corregir_factura(factura_id: int):
    factura = _owned_invoice_or_404(factura_id)
    if factura.estatus != "CORRECCION_SOLICITADA":
        flash("Esta factura no tiene una corrección pendiente.", "warning")
        return redirect(url_for("portal_facturas.detalle_factura", factura_id=factura.id))
    if request.method == "POST":
        _require_csrf()
        new_xml_path = new_pdf_path = ""
        try:
            cfdi, xml_bytes, pdf_bytes, xml_original, pdf_original = _prepare_invoice_upload(g.portal_proveedor)
            duplicate = FacturaProveedor.query.filter(
                FacturaProveedor.uuid_cfdi == cfdi["uuid_cfdi"],
                FacturaProveedor.id != factura.id,
            ).first()
            if duplicate:
                raise ValueError("Este UUID ya fue recibido en otra factura.")
            new_xml_path, new_pdf_path = _save_invoice_files(
                g.portal_proveedor.id,
                cfdi["uuid_cfdi"],
                xml_bytes,
                pdf_bytes,
            )
            old_xml_path, old_pdf_path = factura.xml_path, factura.pdf_path
            old_status = factura.estatus
            for key, value in cfdi.items():
                setattr(factura, key, value)
            factura.xml_path = new_xml_path
            factura.pdf_path = new_pdf_path
            factura.xml_nombre_original = xml_original
            factura.pdf_nombre_original = pdf_original
            factura.xml_tamano = len(xml_bytes)
            factura.pdf_tamano = len(pdf_bytes)
            factura.concepto = (request.form.get("concepto") or factura.concepto or "").strip()[:300] or None
            factura.orden_compra = (request.form.get("orden_compra") or factura.orden_compra or "").strip()[:120] or None
            factura.notas_proveedor = (request.form.get("notas_proveedor") or "").strip() or factura.notas_proveedor
            factura.estatus = "RECIBIDA"
            factura.actualizada_en = _now()
            _add_event(
                factura,
                old_status=old_status,
                new_status="RECIBIDA",
                actor_type="PROVEEDOR",
                actor_name=g.portal_proveedor.contacto,
                comment="Se enviaron XML y PDF corregidos.",
            )
            db.session.commit()
            _notify_finance_invoice(factura, corrected=True)
            _delete_paths(old_xml_path, old_pdf_path)
        except (ValueError, OSError, IntegrityError) as exc:
            db.session.rollback()
            _delete_paths(new_xml_path, new_pdf_path)
            message = str(exc) if isinstance(exc, (ValueError, OSError)) else "No fue posible guardar la corrección."
            flash(message, "danger")
        else:
            flash("Los documentos corregidos se enviaron a Finanzas.", "success")
            return redirect(url_for("portal_facturas.detalle_factura", factura_id=factura.id))
    return render_template("portal_facturas/corregir_factura.html", factura=factura)


@portal_facturas_bp.get("/facturas/<int:factura_id>/archivo/<string:tipo>")
@proveedor_login_required
def descargar_archivo(factura_id: int, tipo: str):
    factura = _owned_invoice_or_404(factura_id)
    return _send_invoice_file(factura, tipo)


def _send_invoice_file(factura: FacturaProveedor, tipo: str):
    if tipo == "xml":
        relative_path, original_name, mimetype = factura.xml_path, factura.xml_nombre_original, "application/xml"
    elif tipo == "pdf":
        relative_path, original_name, mimetype = factura.pdf_path, factura.pdf_nombre_original, "application/pdf"
    else:
        abort(404)
    path = _safe_upload_path(relative_path)
    if not path.is_file():
        abort(404)
    return send_file(path, mimetype=mimetype, as_attachment=True, download_name=original_name)


@portal_facturas_bp.get("/finanzas")
@finanzas_required
def finanzas():
    status = (request.args.get("estatus") or "").strip().upper()
    query_text = (request.args.get("q") or "").strip()
    query = FacturaProveedor.query.join(PortalProveedorUsuario)
    if status in ESTATUS:
        query = query.filter(FacturaProveedor.estatus == status)
    else:
        status = ""
    if query_text:
        like = f"%{query_text}%"
        query = query.filter(
            or_(
                FacturaProveedor.folio_recepcion.ilike(like),
                FacturaProveedor.uuid_cfdi.ilike(like),
                FacturaProveedor.folio_cfdi.ilike(like),
                FacturaProveedor.orden_compra.ilike(like),
                PortalProveedorUsuario.razon_social.ilike(like),
                PortalProveedorUsuario.nombre_comercial.ilike(like),
                PortalProveedorUsuario.rfc.ilike(like),
            )
        )
    facturas = query.order_by(FacturaProveedor.recibida_en.desc()).limit(300).all()
    counts = {
        key: FacturaProveedor.query.filter_by(estatus=key).count()
        for key in ESTATUS
    }
    counts["TOTAL"] = FacturaProveedor.query.count()
    pending_statuses = ["RECIBIDA", "EN_REVISION", "CORRECCION_SOLICITADA", "APROBADA", "PROGRAMADA"]
    pending_total = db.session.query(func.coalesce(func.sum(FacturaProveedor.total), 0)).filter(
        FacturaProveedor.estatus.in_(pending_statuses)
    ).scalar() or 0
    return render_template(
        "portal_facturas/finanzas.html",
        facturas=facturas,
        counts=counts,
        pending_total=float(pending_total),
        selected_status=status,
        q=query_text,
    )


@portal_facturas_bp.get("/finanzas/<int:factura_id>")
@finanzas_required
def finanzas_detalle(factura_id: int):
    factura = db.session.get(FacturaProveedor, factura_id)
    if not factura:
        abort(404)
    return render_template("portal_facturas/finanzas_detalle.html", factura=factura)


@portal_facturas_bp.post("/finanzas/<int:factura_id>/estatus")
@finanzas_required
def finanzas_actualizar_estatus(factura_id: int):
    _require_csrf()
    factura = db.session.get(FacturaProveedor, factura_id)
    if not factura:
        abort(404)
    new_status = (request.form.get("estatus") or "").strip().upper()
    comment = (request.form.get("comentario") or "").strip()
    if new_status not in ESTATUS:
        flash("Selecciona un estatus válido.", "danger")
        return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))
    if new_status in {"CORRECCION_SOLICITADA", "RECHAZADA"} and not comment:
        flash("Escribe el motivo para que el proveedor sepa qué debe atender.", "danger")
        return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))

    try:
        scheduled_date = _parse_date(request.form.get("fecha_programada_pago") or "")
        payment_date = _parse_date(request.form.get("fecha_pago") or "")
    except ValueError as exc:
        flash(str(exc), "danger")
        return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))
    payment_reference = (request.form.get("referencia_pago") or "").strip()
    if new_status == "PROGRAMADA" and not scheduled_date:
        flash("Indica la fecha programada de pago.", "danger")
        return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))
    if new_status == "PAGADA" and (not payment_date or not payment_reference):
        flash("Para marcarla como pagada indica fecha y referencia de pago.", "danger")
        return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))

    old_status = factura.estatus
    factura.estatus = new_status
    factura.comentario_finanzas = comment or None
    factura.revisada_por_id = current_user.id
    factura.revisada_en = _now()
    factura.fecha_programada_pago = scheduled_date if new_status in {"PROGRAMADA", "PAGADA"} else None
    factura.fecha_pago = payment_date if new_status == "PAGADA" else None
    factura.referencia_pago = payment_reference[:160] if new_status == "PAGADA" else None
    factura.actualizada_en = _now()
    _add_event(
        factura,
        old_status=old_status,
        new_status=new_status,
        actor_type="FINANZAS",
        actor_name=getattr(current_user, "nombre_representante", None) or current_user.nombre,
        actor_user_id=current_user.id,
        comment=comment,
    )
    db.session.commit()
    flash(f"{factura.folio_recepcion} cambió a {ESTATUS[new_status]['label']}.", "success")
    return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))


@portal_facturas_bp.get("/finanzas/<int:factura_id>/archivo/<string:tipo>")
@finanzas_required
def finanzas_descargar_archivo(factura_id: int, tipo: str):
    factura = db.session.get(FacturaProveedor, factura_id)
    if not factura:
        abort(404)
    return _send_invoice_file(factura, tipo)

