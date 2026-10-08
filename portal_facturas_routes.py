from __future__ import annotations

import json
import mimetypes
import os
import re
import secrets
import smtplib
import xml.etree.ElementTree as ET
from datetime import date, datetime
from email.message import EmailMessage
from email.utils import formataddr, parseaddr
from functools import wraps
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

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
from sqlalchemy import func, inspect, or_, text
from sqlalchemy.exc import IntegrityError
from werkzeug.utils import secure_filename

from contabilidad_access import can_access_facturas_recibidas
from models import (
    FacturaProveedor,
    FacturaProveedorMovimiento,
    InAppNotification,
    MessengerNotificationOutbox,
    OrdenCompra,
    OrdenCompraPartida,
    PortalProveedorUsuario,
    Usuario,
    db,
)


portal_facturas_bp = Blueprint(
    "portal_facturas",
    __name__,
    url_prefix="/portal-facturas",
)


@portal_facturas_bp.record_once
def _ensure_purchase_order_portal_schema(state) -> None:
    """Migra instalaciones existentes al registrar el portal de proveedores."""
    with state.app.app_context():
        inspector = inspect(db.engine)
        table_names = inspector.get_table_names()
        try:
            if "orden_compra" in table_names:
                columns = {column["name"] for column in inspector.get_columns("orden_compra")}
                if "portal_proveedor_usuario_id" not in columns:
                    db.session.execute(text(
                        "ALTER TABLE orden_compra ADD COLUMN portal_proveedor_usuario_id INTEGER"
                    ))
                if "enviada_en" not in columns:
                    db.session.execute(text(
                        "ALTER TABLE orden_compra ADD COLUMN enviada_en TIMESTAMP"
                    ))
                db.session.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_orden_compra_portal_proveedor_usuario_id "
                    "ON orden_compra (portal_proveedor_usuario_id)"
                ))
            if "portal_proveedor_usuario" in table_names:
                provider_columns = {
                    column["name"]
                    for column in inspector.get_columns("portal_proveedor_usuario")
                }
                provider_migrations = {
                    "revision_comentario": "TEXT",
                    "revisado_por_id": "INTEGER",
                    "revisado_en": "TIMESTAMP",
                }
                for column_name, column_type in provider_migrations.items():
                    if column_name not in provider_columns:
                        db.session.execute(text(
                            f"ALTER TABLE portal_proveedor_usuario ADD COLUMN {column_name} {column_type}"
                        ))
                db.session.execute(text(
                    "CREATE INDEX IF NOT EXISTS ix_portal_proveedor_usuario_revisado_por_id "
                    "ON portal_proveedor_usuario (revisado_por_id)"
                ))
            db.session.commit()
        except Exception:
            db.session.rollback()
            state.app.logger.exception(
                "No se pudo actualizar el esquema del portal de proveedores."
            )

SESSION_KEY = "portal_facturas_usuario_id"
CSRF_KEY = "portal_facturas_csrf"
MAX_XML_BYTES = 5 * 1024 * 1024
MAX_PDF_BYTES = 15 * 1024 * 1024
MAX_PROVIDER_DOCUMENT_BYTES = 10 * 1024 * 1024
TZ_CDMX = ZoneInfo("America/Mexico_City")
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
    "RECIBIDA": {"label": "Recibida", "class": "status-amber"},
    "EN_REVISION": {"label": "En revisión", "class": "status-amber"},
    "CORRECCION_SOLICITADA": {"label": "Requiere corrección", "class": "status-amber"},
    "APROBADA": {"label": "Aprobada", "class": "status-blue"},
    "PROGRAMADA": {"label": "Pago programado", "class": "status-blue"},
    "PAGADA": {"label": "Pagada", "class": "status-green"},
    "RECHAZADA": {"label": "Rechazada", "class": "status-red"},
}

ORDEN_COMPRA_ESTADOS = {
    "BORRADOR": {"label": "Borrador", "class": "status-amber"},
    "ENVIADA": {"label": "Enviada", "class": "status-blue"},
    "PARCIALMENTE RECIBIDA": {"label": "Parcialmente recibida", "class": "status-blue"},
    "RECIBIDA COMPLETA": {"label": "Recibida completa", "class": "status-green"},
    "FACTURADA": {"label": "Facturada", "class": "status-purple"},
    "PAGADA": {"label": "Pagada", "class": "status-green"},
    "CANCELADA": {"label": "Cancelada", "class": "status-red"},
}
ORDEN_COMPRA_VISIBLES_PROVEEDOR = tuple(
    status for status in ORDEN_COMPRA_ESTADOS if status != "BORRADOR"
)


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
        "orden_compra_estatus": ORDEN_COMPRA_ESTADOS,
        "puede_revisar_proveedores": _can_review_provider(current_user),
    }


def proveedor_login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        proveedor = getattr(g, "portal_proveedor", None)
        if not proveedor:
            return redirect(url_for("portal_facturas.ingresar", next=request.path))
        if proveedor.estatus != "ACTIVO":
            session.pop(SESSION_KEY, None)
            if proveedor.estatus == "PENDIENTE":
                flash("Tu alta sigue pendiente de autorización por nuestro equipo.", "warning")
            elif proveedor.estatus == "RECHAZADO":
                flash("Tu alta fue rechazada. Contacta a nuestro equipo.", "danger")
            else:
                flash("Tu cuenta no está activa. Contacta a nuestro equipo.", "danger")
            return redirect(url_for("portal_facturas.ingresar"))
        return view(*args, **kwargs)

    return wrapped


def finanzas_required(view):
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        if not can_access_facturas_recibidas(current_user):
            abort(403)
        return view(*args, **kwargs)

    return wrapped


def _can_review_provider(user) -> bool:
    if not getattr(user, "is_authenticated", False):
        return False
    username = str(getattr(user, "nombre", "") or "").strip().casefold()
    display_name = str(getattr(user, "nombre_visible", "") or "").strip().casefold()
    email = _normalize_email(getattr(user, "correo", ""))
    return (
        username in {"admin", "marco", "mescalera"}
        or display_name in {"marco", "mescalera", "marco escalera"}
        or email == "mescalera@poliutech.com"
    )


def provider_reviewer_required(view):
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        if not _can_review_provider(current_user):
            abort(403)
        return view(*args, **kwargs)

    return wrapped


def administrador_account_required(view):
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        username = (getattr(current_user, "nombre", "") or "").strip().casefold()
        if username != "admin":
            abort(403)
        return view(*args, **kwargs)

    return wrapped


def _normalize_rfc(raw: str) -> str:
    return re.sub(r"\s+", "", (raw or "").strip().upper())


def _normalize_email(raw: str) -> str:
    return (raw or "").strip().casefold()


def _send_portal_email(msg: EmailMessage, recipients: list[str]) -> None:
    envelope = []
    seen_recipients = set()
    for value in recipients:
        email = _normalize_email(value)
        if email and email not in seen_recipients:
            envelope.append(email)
            seen_recipients.add(email)
    if not envelope:
        raise RuntimeError("No hay destinatarios válidos para el correo.")

    configured_accounts = [
        (
            str(current_app.config.get("SMTP_HOST") or "").strip(),
            int(current_app.config.get("SMTP_PORT") or 26),
            str(current_app.config.get("SMTP_USERNAME") or "").strip(),
            str(current_app.config.get("SMTP_PASSWORD") or ""),
        ),
        (
            str(current_app.config.get("REGISTRO_MAIL_HOST") or "").strip(),
            int(current_app.config.get("REGISTRO_MAIL_PORT") or 26),
            str(current_app.config.get("REGISTRO_MAIL_USERNAME") or "").strip(),
            str(current_app.config.get("REGISTRO_MAIL_PASSWORD") or ""),
        ),
    ]
    accounts = []
    seen_accounts = set()
    for account in configured_accounts:
        host, port, username, password = account
        key = (host.casefold(), port, username.casefold())
        if host and username and password and key not in seen_accounts:
            accounts.append(account)
            seen_accounts.add(key)
    if not accounts:
        raise RuntimeError("La configuración SMTP del portal está incompleta.")

    display_name, _ = parseaddr(str(msg.get("From") or ""))
    errors = []
    for index, (host, port, username, password) in enumerate(accounts):
        if "From" in msg:
            msg.replace_header(
                "From",
                formataddr((display_name or "PORTAL DE PROVEEDORES POLIUTECH", username)),
            )
        else:
            msg["From"] = formataddr(
                (display_name or "PORTAL DE PROVEEDORES POLIUTECH", username)
            )
        try:
            with smtplib.SMTP(host, port, timeout=30) as smtp:
                smtp.ehlo()
                smtp.login(username, password)
                refused = smtp.send_message(msg, to_addrs=envelope) or {}
            if isinstance(refused, dict) and refused:
                rejected = ", ".join(sorted(str(value) for value in refused))
                raise RuntimeError(f"El servidor rechazó: {rejected}.")
            return
        except Exception as exc:
            errors.append(f"{username}: {exc}")
            if index + 1 < len(accounts):
                current_app.logger.warning(
                    "Falló el correo del portal con %s; se intentará la cuenta de respaldo: %s",
                    username,
                    exc,
                )
    raise RuntimeError("; ".join(errors))


def _provider_registry_path() -> Path:
    """Usa el mismo padrón persistente que Contabilidad > Altas."""
    configured = (os.getenv("PROVIDER_NUMBERS_JSON_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    if Path("/data").is_dir():
        return Path("/data/provider_numbers.json")
    return (Path(current_app.root_path) / "provider_numbers.json").resolve()


def _sync_provider_registry(proveedor: PortalProveedorUsuario) -> None:
    """Agrega o actualiza el alta maestra al crear una cuenta del portal."""
    registry_path = _provider_registry_path()
    seed_path = (Path(current_app.root_path) / "provider_numbers.json").resolve()
    source_path = registry_path if registry_path.exists() else seed_path
    rows: list[dict] = []
    if source_path.exists():
        raw_rows = json.loads(source_path.read_text(encoding="utf-8"))
        if not isinstance(raw_rows, list):
            raise ValueError("El registro de proveedores no tiene un formato válido.")
        rows = [dict(row) for row in raw_rows if isinstance(row, dict)]

    rfc = _normalize_rfc(proveedor.rfc)
    correo = _normalize_email(proveedor.correo)
    razon_social = (proveedor.razon_social or "").strip().casefold()
    match = None
    for row in rows:
        if str(row.get("relacion") or "PROVEEDOR").strip().upper() != "PROVEEDOR":
            continue
        row_number = _normalize_rfc(str(row.get("numero") or ""))
        row_email = _normalize_email(str(row.get("correo") or ""))
        row_company = str(row.get("empresa") or "").strip().casefold()
        if (
            (rfc and row_number == rfc)
            or (correo and row_email == correo)
            or (razon_social and row_company == razon_social)
        ):
            match = row
            break

    if match is None:
        match = {
            "id": len(rows) + 1,
            "numero": rfc,
            "empresa": proveedor.razon_social,
            "razon_social_poliutech": "",
            "relacion": "PROVEEDOR",
            "contacto": proveedor.contacto,
            "telefono": proveedor.telefono or "",
            "correo": proveedor.correo,
            "credito": False,
            "monto_credito": "",
            "plazo_credito_dias": "",
        }
        rows.append(match)
    else:
        # Conserva el número interno y las condiciones de crédito capturadas por
        # Contabilidad; el portal sólo mantiene actualizados los datos de contacto.
        if not str(match.get("numero") or "").strip():
            match["numero"] = rfc
        match["empresa"] = proveedor.razon_social
        match["relacion"] = "PROVEEDOR"
        match["contacto"] = proveedor.contacto
        match["telefono"] = proveedor.telefono or ""
        match["correo"] = proveedor.correo

    for index, row in enumerate(rows, start=1):
        row["id"] = index

    registry_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = registry_path.with_name(
        f".{registry_path.name}.{secrets.token_hex(8)}.tmp"
    )
    try:
        temp_path.write_text(
            json.dumps(rows, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(registry_path)
    finally:
        temp_path.unlink(missing_ok=True)


def _provider_registration_notification_targets() -> tuple[list[Usuario], list[str]]:
    """Notifica solo a las cuentas internas autorizadas para este portal."""
    users = Usuario.query.order_by(Usuario.id.asc()).all()
    selected_users: list[Usuario] = []
    recipient_emails: list[str] = []
    seen_user_ids: set[int] = set()
    seen_emails: set[str] = set()

    for user in users:
        if not can_access_facturas_recibidas(user):
            continue
        if user.id not in seen_user_ids:
            selected_users.append(user)
            seen_user_ids.add(user.id)
        email = _normalize_email(getattr(user, "correo", ""))
        if email and email not in seen_emails:
            recipient_emails.append(email)
            seen_emails.add(email)

    for fallback_email in (
        "sistemas@poliutech.com",
        "mescalera@poliutech.com",
        "umorales@poliutech.com",
        "hjaramillo@poliutech.com",
    ):
        if fallback_email not in seen_emails:
            recipient_emails.append(fallback_email)
            seen_emails.add(fallback_email)

    return selected_users, recipient_emails


def _queue_messenger_notifications(
    users: list[Usuario],
    *,
    source_key: str,
    title: str,
    body: str,
    view_url: str,
) -> int:
    """Encola avisos para que Messenger los entregue en web, escritorio y móvil."""
    queued = 0
    deliver_at = datetime.now(TZ_CDMX)
    next_attempt = deliver_at.replace(tzinfo=None)
    for user in users:
        email = _normalize_email(getattr(user, "correo", ""))
        if not email or not getattr(user, "id", None):
            continue
        user_source_key = f"portal:{source_key}:usuario:{user.id}"[:240]
        if MessengerNotificationOutbox.query.filter_by(source_key=user_source_key).first():
            continue
        payload = {
            "email": email,
            "sourceKey": user_source_key,
            "title": title[:180],
            "body": body[:2000],
            "url": view_url,
            "deliverAt": deliver_at.isoformat(),
        }
        db.session.add(
            MessengerNotificationOutbox(
                source_key=user_source_key,
                usuario_id=user.id,
                correo=email,
                payload_json=json.dumps(payload, ensure_ascii=False),
                siguiente_intento_en=next_attempt,
            )
        )
        queued += 1
    return queued


def _queue_messenger_email_notification(
    email: str,
    *,
    source_key: str,
    title: str,
    body: str,
    view_url: str,
) -> bool:
    """Encola un aviso por correo, aunque no exista un Usuario interno asociado."""
    normalized_email = _normalize_email(email)
    if not normalized_email:
        return False
    outbox_source_key = f"portal:{source_key}:correo:{normalized_email}"[:240]
    if MessengerNotificationOutbox.query.filter_by(source_key=outbox_source_key).first():
        return False
    deliver_at = datetime.now(TZ_CDMX)
    matched_user = Usuario.query.filter(
        func.lower(Usuario.correo) == normalized_email
    ).first()
    payload = {
        "email": normalized_email,
        "sourceKey": outbox_source_key,
        "title": title[:180],
        "body": body[:2000],
        "url": view_url,
        "deliverAt": deliver_at.isoformat(),
    }
    db.session.add(
        MessengerNotificationOutbox(
            source_key=outbox_source_key,
            usuario_id=matched_user.id if matched_user else None,
            correo=normalized_email,
            payload_json=json.dumps(payload, ensure_ascii=False),
            siguiente_intento_en=deliver_at.replace(tzinfo=None),
        )
    )
    return True


def _send_provider_registration_email(
    proveedor: PortalProveedorUsuario,
    recipients: list[str],
) -> None:
    if not recipients or current_app.config.get("PORTAL_FACTURAS_DISABLE_EMAIL"):
        return

    detail_url = url_for(
        "portal_facturas.finanzas_proveedor_detalle",
        proveedor_id=proveedor.id,
        _external=True,
    )
    provider_name = proveedor.nombre_portal
    subject = f"Proveedor pendiente de autorización: {provider_name}"
    msg = EmailMessage()
    msg["Subject"] = subject
    smtp_from = str(
        current_app.config.get("SMTP_FROM")
        or current_app.config.get("SMTP_USERNAME")
        or ""
    ).strip()
    msg["From"] = f"PORTAL DE FACTURAS POLIUTECH <{smtp_from}>"
    msg["To"] = ", ".join(recipients)
    msg.set_content(
        f"Un proveedor solicitó su alta en el portal y requiere autorización del equipo responsable.\n\n"
        f"Razón social: {proveedor.razon_social}\n"
        f"RFC: {proveedor.rfc}\n"
        f"Contacto: {proveedor.contacto}\n"
        f"Correo: {proveedor.correo}\n"
        f"Teléfono: {proveedor.telefono or 'No indicado'}\n\n"
        f"La Constancia de Situación Fiscal y la carátula bancaria están disponibles aquí:\n"
        f"{detail_url}\n"
    )
    msg.add_alternative(
        (
            "<div style='font-family:Arial,sans-serif;max-width:680px;color:#15263b'>"
            "<div style='font-size:12px;font-weight:800;letter-spacing:.08em;color:#0b67b2'>ALTA PENDIENTE DE PROVEEDOR</div>"
            f"<h2 style='margin:8px 0'>{escape(subject)}</h2>"
            "<p>El proveedor completó su solicitud y adjuntó su expediente fiscal y bancario. El equipo responsable debe autorizarla antes de que pueda facturar.</p>"
            "<table style='border-collapse:collapse;margin:18px 0'>"
            f"<tr><td style='padding:5px 14px 5px 0;color:#607086'>Razón social</td><td><b>{escape(proveedor.razon_social)}</b></td></tr>"
            f"<tr><td style='padding:5px 14px 5px 0;color:#607086'>RFC</td><td>{escape(proveedor.rfc)}</td></tr>"
            f"<tr><td style='padding:5px 14px 5px 0;color:#607086'>Contacto</td><td>{escape(proveedor.contacto)}</td></tr>"
            f"<tr><td style='padding:5px 14px 5px 0;color:#607086'>Correo</td><td>{escape(proveedor.correo)}</td></tr>"
            "</table>"
            f"<p><a href='{escape(detail_url)}' style='display:inline-block;padding:11px 18px;background:#0b67b2;color:#fff;text-decoration:none;border-radius:7px'>Revisar expediente</a></p>"
            "</div>"
        ),
        subtype="html",
    )

    _send_portal_email(msg, recipients)


def _send_provider_registration_received_email(
    proveedor: PortalProveedorUsuario,
) -> bool:
    if current_app.config.get("PORTAL_FACTURAS_DISABLE_EMAIL"):
        return False
    portal_url = url_for("portal_facturas.ingresar", _external=True)
    title = "Recibimos tu solicitud de alta"
    body = (
        "Tu información, Constancia de Situación Fiscal y carátula bancaria "
        "quedaron en revisión. Nuestro equipo te avisará cuando tome una decisión."
    )
    msg = EmailMessage()
    msg["Subject"] = title
    smtp_from = str(
        current_app.config.get("SMTP_FROM")
        or current_app.config.get("SMTP_USERNAME")
        or ""
    ).strip()
    msg["From"] = f"PORTAL DE PROVEEDORES POLIUTECH <{smtp_from}>"
    msg["To"] = proveedor.correo
    msg.set_content(
        f"Hola {proveedor.contacto},\n\n{body}\n\n"
        f"Puedes consultar el portal aquí:\n{portal_url}\n"
    )
    msg.add_alternative(
        (
            "<div style='font-family:Arial,sans-serif;max-width:680px;color:#15263b'>"
            f"<h2>{escape(title)}</h2>"
            f"<p>Hola <b>{escape(proveedor.contacto)}</b>,</p>"
            f"<p>{escape(body)}</p>"
            f"<p><a href='{escape(portal_url)}' style='display:inline-block;padding:12px 18px;background:#0b67b2;color:#fff;text-decoration:none;border-radius:7px;font-weight:700'>Consultar solicitud</a></p>"
            "</div>"
        ),
        subtype="html",
    )
    _send_portal_email(msg, [proveedor.correo])
    return True


def _notify_provider_registration(proveedor: PortalProveedorUsuario) -> None:
    title = f"Alta por autorizar: {proveedor.nombre_portal}"
    body = (
        f"{proveedor.razon_social} ({proveedor.rfc}) solicita autorización para facturar; "
        "adjuntó su CSF y carátula bancaria."
    )
    detail_url = url_for(
        "portal_facturas.finanzas_proveedor_detalle",
        proveedor_id=proveedor.id,
    )
    try:
        users, recipients = _provider_registration_notification_targets()
        for user in users:
            db.session.add(
                InAppNotification(
                    usuario_id=user.id,
                    tipo="portal_proveedores",
                    titulo=title[:180],
                    mensaje=body,
                    destino_url=detail_url,
                    creada_en=_now(),
                )
            )
        _queue_messenger_notifications(
            users,
            source_key=f"proveedor:{proveedor.id}:alta-pendiente",
            title=title,
            body=body,
            view_url=url_for(
                "portal_facturas.finanzas_proveedor_detalle",
                proveedor_id=proveedor.id,
                _external=True,
            ),
        )
        _queue_messenger_email_notification(
            proveedor.correo,
            source_key=f"proveedor:{proveedor.id}:alta-pendiente:proveedor",
            title="Recibimos tu solicitud de alta",
            body=(
                "Tu información y documentos están en revisión. "
                "Nuestro equipo te avisará cuando tome una decisión."
            ),
            view_url=url_for("portal_facturas.ingresar", _external=True),
        )
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        current_app.logger.warning(
            "No se pudieron guardar los avisos del alta de proveedor %s: %s",
            proveedor.id,
            exc,
        )
        recipients = ["sistemas@poliutech.com", "mescalera@poliutech.com"]

    try:
        _send_provider_registration_email(proveedor, recipients)
    except Exception as exc:
        current_app.logger.warning(
            "No se pudo enviar el correo del alta de proveedor %s a %s: %s",
            proveedor.id,
            recipients,
            exc,
        )
    try:
        _send_provider_registration_received_email(proveedor)
    except Exception as exc:
        current_app.logger.warning(
            "No se pudo confirmar por correo el alta del proveedor %s a %s: %s",
            proveedor.id,
            proveedor.correo,
            exc,
        )


def _send_provider_review_email(
    proveedor: PortalProveedorUsuario,
    recipients: list[str],
) -> bool:
    if not recipients or current_app.config.get("PORTAL_FACTURAS_DISABLE_EMAIL"):
        return False
    approved = proveedor.estatus == "ACTIVO"
    title = (
        f"Alta autorizada: {proveedor.nombre_portal}"
        if approved
        else f"Alta rechazada: {proveedor.nombre_portal}"
    )
    portal_url = url_for("portal_facturas.ingresar", _external=True)
    reason = (proveedor.revision_comentario or "Sin comentarios adicionales.").strip()
    msg = EmailMessage()
    msg["Subject"] = title
    smtp_from = str(current_app.config.get("SMTP_FROM") or current_app.config.get("SMTP_USERNAME") or "").strip()
    msg["From"] = f"PORTAL DE PROVEEDORES POLIUTECH <{smtp_from}>"
    msg["To"] = proveedor.correo
    msg.set_content(
        f"Hola {proveedor.contacto},\n\n"
        f"Tu solicitud de alta fue {'AUTORIZADA' if approved else 'RECHAZADA'} por nuestro equipo.\n"
        f"Comentario: {reason}\n\n"
        + (f"Ya puedes iniciar sesión y subir facturas:\n{portal_url}\n" if approved else "")
    )
    action_html = (
        f"<p><a href='{escape(portal_url)}' style='display:inline-block;padding:12px 18px;background:#0b67b2;color:#fff;text-decoration:none;border-radius:7px;font-weight:700'>Entrar al portal</a></p>"
        if approved
        else ""
    )
    msg.add_alternative(
        (
            "<div style='font-family:Arial,sans-serif;max-width:680px;color:#15263b'>"
            f"<h2>{escape(title)}</h2>"
            f"<p>Hola <b>{escape(proveedor.contacto)}</b>, tu solicitud fue <b>{'AUTORIZADA' if approved else 'RECHAZADA'}</b> por nuestro equipo.</p>"
            f"<p><b>Comentario:</b> {escape(reason)}</p>"
            f"{action_html}</div>"
        ),
        subtype="html",
    )
    _send_portal_email(msg, recipients)
    return True


def _notify_provider_review(proveedor: PortalProveedorUsuario) -> bool:
    users, internal_emails = _provider_registration_notification_targets()
    approved = proveedor.estatus == "ACTIVO"
    reviewer_name = (
        getattr(proveedor.revisado_por, "nombre_representante", None)
        or getattr(proveedor.revisado_por, "nombre", None)
        or "Equipo de revisión"
    )
    title = f"Alta {'autorizada' if approved else 'rechazada'}: {proveedor.nombre_portal}"
    body = f"{reviewer_name} {'autorizó' if approved else 'rechazó'} a {proveedor.razon_social} ({proveedor.rfc})."
    internal_url = url_for(
        "portal_facturas.finanzas_proveedor_detalle",
        proveedor_id=proveedor.id,
    )
    for user in users:
        db.session.add(
            InAppNotification(
                usuario_id=user.id,
                tipo="portal_proveedores_revision",
                titulo=title[:180],
                mensaje=body,
                destino_url=internal_url,
                creada_en=_now(),
            )
        )
    review_stamp = int((proveedor.revisado_en or _now()).timestamp())
    _queue_messenger_notifications(
        users,
        source_key=f"proveedor:{proveedor.id}:revision:{proveedor.estatus.lower()}:{review_stamp}",
        title=title,
        body=body,
        view_url=url_for(
            "portal_facturas.finanzas_proveedor_detalle",
            proveedor_id=proveedor.id,
            _external=True,
        ),
    )
    provider_title = f"Solicitud {'autorizada' if approved else 'rechazada'}"
    provider_body = (
        "Tu alta fue autorizada. Ya puedes ingresar y enviar facturas."
        if approved
        else f"Tu alta fue rechazada. Motivo: {proveedor.revision_comentario or 'Sin comentario adicional.'}"
    )
    _queue_messenger_email_notification(
        proveedor.correo,
        source_key=f"proveedor:{proveedor.id}:revision:{proveedor.estatus.lower()}:{review_stamp}:proveedor",
        title=provider_title,
        body=provider_body,
        view_url=url_for("portal_facturas.ingresar", _external=True),
    )
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        current_app.logger.warning("No se guardaron los avisos de revisión de %s: %s", proveedor.id, exc)

    recipients = []
    seen = set()
    for email in [proveedor.correo, *internal_emails]:
        normalized = _normalize_email(email)
        if normalized and normalized not in seen:
            recipients.append(normalized)
            seen.add(normalized)
    try:
        return _send_provider_review_email(proveedor, recipients)
    except Exception as exc:
        current_app.logger.exception("No se envió el resultado del alta %s: %s", proveedor.id, exc)
        return False


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
        event_suffix = "corregida" if corrected else "recibida"
        if corrected:
            event_suffix = f"{event_suffix}:{int(_now().timestamp())}"
        _queue_messenger_notifications(
            users,
            source_key=f"factura:{factura.id}:{event_suffix}",
            title=title,
            body=body,
            view_url=url_for(
                "portal_facturas.finanzas_detalle",
                factura_id=factura.id,
                _external=True,
            ),
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


def _invoice_status_notification_copy(factura: FacturaProveedor) -> tuple[str, str]:
    label = ESTATUS[factura.estatus]["label"]
    status_messages = {
        "RECIBIDA": "Recibimos tu factura y quedó registrada para revisión.",
        "EN_REVISION": "Nuestro equipo está revisando la información y los documentos de tu factura.",
        "CORRECCION_SOLICITADA": "Necesitamos que corrijas la factura antes de continuar.",
        "APROBADA": "Tu factura fue aprobada y continuará con el proceso de pago.",
        "PROGRAMADA": "El pago de tu factura ya fue programado.",
        "PAGADA": "El pago de tu factura fue registrado.",
        "RECHAZADA": "Tu factura fue rechazada.",
    }
    details = [status_messages.get(factura.estatus, f"Tu factura cambió a {label}.")]
    if factura.estatus == "PROGRAMADA" and factura.fecha_programada_pago:
        details.append(
            f"Fecha programada: {factura.fecha_programada_pago.strftime('%d/%m/%Y')}."
        )
    if factura.estatus == "PAGADA":
        if factura.fecha_pago:
            details.append(f"Fecha de pago: {factura.fecha_pago.strftime('%d/%m/%Y')}.")
        if factura.referencia_pago:
            details.append(f"Referencia: {factura.referencia_pago}.")
    if factura.comentario_finanzas:
        details.append(f"Comentario: {factura.comentario_finanzas.strip()}")
    return f"Factura {factura.folio_recepcion}: {label}", " ".join(details)


def _send_provider_invoice_status_email(
    factura: FacturaProveedor,
    recipients: list[str],
) -> bool:
    if not recipients or current_app.config.get("PORTAL_FACTURAS_DISABLE_EMAIL"):
        return False
    title, body = _invoice_status_notification_copy(factura)
    provider = factura.proveedor_usuario
    portal_url = url_for(
        "portal_facturas.detalle_factura",
        factura_id=factura.id,
        _external=True,
    )
    msg = EmailMessage()
    msg["Subject"] = title
    smtp_from = str(
        current_app.config.get("SMTP_FROM")
        or current_app.config.get("SMTP_USERNAME")
        or ""
    ).strip()
    msg["From"] = f"PORTAL DE FACTURAS POLIUTECH <{smtp_from}>"
    msg["To"] = provider.correo
    msg.set_content(
        f"Hola {provider.contacto},\n\n"
        f"{body}\n\n"
        f"Total: ${float(factura.total or 0):,.2f} {factura.moneda or 'MXN'}\n"
        f"Consulta el detalle y el historial aquí:\n{portal_url}\n"
    )
    msg.add_alternative(
        (
            "<div style='font-family:Arial,sans-serif;max-width:680px;color:#15263b'>"
            f"<div style='font-size:12px;font-weight:800;letter-spacing:.08em;color:#0b67b2'>ACTUALIZACIÓN DE FACTURA</div>"
            f"<h2 style='margin:8px 0'>{escape(title)}</h2>"
            f"<p>Hola <b>{escape(provider.contacto)}</b>,</p>"
            f"<p>{escape(body)}</p>"
            f"<p><b>Total:</b> ${float(factura.total or 0):,.2f} {escape(factura.moneda or 'MXN')}</p>"
            f"<p><a href='{escape(portal_url)}' style='display:inline-block;padding:12px 18px;background:#0b67b2;color:#fff;text-decoration:none;border-radius:7px;font-weight:700'>Consultar factura</a></p>"
            "</div>"
        ),
        subtype="html",
    )
    _send_portal_email(msg, recipients)
    return True


def _send_internal_invoice_status_email(
    factura: FacturaProveedor,
    recipients: list[str],
) -> None:
    if not recipients or current_app.config.get("PORTAL_FACTURAS_DISABLE_EMAIL"):
        return
    title, provider_body = _invoice_status_notification_copy(factura)
    provider = factura.proveedor_usuario
    finance_url = url_for(
        "portal_facturas.finanzas_detalle",
        factura_id=factura.id,
        _external=True,
    )
    msg = EmailMessage()
    msg["Subject"] = title
    smtp_from = str(
        current_app.config.get("SMTP_FROM")
        or current_app.config.get("SMTP_USERNAME")
        or ""
    ).strip()
    msg["From"] = f"PORTAL DE FACTURAS POLIUTECH <{smtp_from}>"
    msg["To"] = ", ".join(recipients)
    msg.set_content(
        f"{factura.folio_recepcion} de {provider.razon_social} cambió a "
        f"{ESTATUS[factura.estatus]['label']}.\n\n"
        f"Detalle comunicado al proveedor: {provider_body}\n\n"
        f"Revisar en MAR:\n{finance_url}\n"
    )
    msg.add_alternative(
        (
            "<div style='font-family:Arial,sans-serif;max-width:680px;color:#15263b'>"
            f"<h2>{escape(title)}</h2>"
            f"<p><b>{escape(provider.razon_social)}</b> · {escape(provider.rfc)}</p>"
            f"<p>{escape(provider_body)}</p>"
            f"<p><a href='{escape(finance_url)}' style='display:inline-block;padding:12px 18px;background:#0b67b2;color:#fff;text-decoration:none;border-radius:7px;font-weight:700'>Revisar en MAR</a></p>"
            "</div>"
        ),
        subtype="html",
    )
    _send_portal_email(msg, recipients)


def _notify_invoice_status_change(
    factura: FacturaProveedor,
    *,
    event_key: str,
    notify_internal: bool,
    provider_email_already_sent: bool = False,
) -> dict[str, object]:
    title, provider_body = _invoice_status_notification_copy(factura)
    provider = factura.proveedor_usuario
    provider_url = url_for(
        "portal_facturas.detalle_factura",
        factura_id=factura.id,
        _external=True,
    )
    internal_users: list[Usuario] = []
    internal_emails: list[str] = []
    queued = 0
    if notify_internal:
        internal_users, internal_emails = _provider_registration_notification_targets()
        internal_url = url_for(
            "portal_facturas.finanzas_detalle",
            factura_id=factura.id,
        )
        internal_body = (
            f"{factura.folio_recepcion} de {provider.nombre_portal} cambió a "
            f"{ESTATUS[factura.estatus]['label']}."
        )
        for user in internal_users:
            db.session.add(
                InAppNotification(
                    usuario_id=user.id,
                    tipo="portal_facturas_estatus",
                    titulo=title[:180],
                    mensaje=internal_body,
                    destino_url=internal_url,
                    creada_en=_now(),
                )
            )
        queued += _queue_messenger_notifications(
            internal_users,
            source_key=f"factura:{factura.id}:estatus:{event_key}",
            title=title,
            body=internal_body,
            view_url=url_for(
                "portal_facturas.finanzas_detalle",
                factura_id=factura.id,
                _external=True,
            ),
        )
    if _queue_messenger_email_notification(
        provider.correo,
        source_key=f"factura:{factura.id}:estatus:{event_key}:proveedor",
        title=title,
        body=provider_body,
        view_url=provider_url,
    ):
        queued += 1
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        queued = 0
        current_app.logger.exception(
            "No se guardaron las notificaciones del estatus de %s: %s",
            factura.folio_recepcion,
            exc,
        )

    email_sent = provider_email_already_sent
    if not provider_email_already_sent:
        try:
            email_sent = _send_provider_invoice_status_email(
                factura,
                [_normalize_email(provider.correo)],
            )
        except Exception as exc:
            current_app.logger.exception(
                "No se envió el cambio de estatus de %s a %s: %s",
                factura.folio_recepcion,
                provider.correo,
                exc,
            )
    if notify_internal:
        try:
            _send_internal_invoice_status_email(factura, internal_emails)
        except Exception as exc:
            current_app.logger.exception(
                "No se envió la copia interna del estatus de %s a %s: %s",
                factura.folio_recepcion,
                internal_emails,
                exc,
            )
    return {"email_sent": email_sent, "messenger_queued": queued}


def _parse_date(raw: str) -> date | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        raise ValueError("Captura una fecha válida.")


def _parse_nonnegative_float(raw: str, *, label: str, default: float = 0.0) -> float:
    text = str(raw or "").strip().replace(",", "")
    if not text:
        return default
    try:
        value = float(text)
    except ValueError as exc:
        raise ValueError(f"{label} debe ser un número válido.") from exc
    if value < 0:
        raise ValueError(f"{label} no puede ser negativo.")
    return round(value, 2)


def _purchase_order_totals(orden: OrdenCompra) -> None:
    subtotal = 0.0
    for partida in orden.partidas:
        partida.cantidad = round(float(partida.cantidad or 0), 4)
        partida.precio_unitario = round(float(partida.precio_unitario or 0), 2)
        partida.subtotal = round(partida.cantidad * partida.precio_unitario, 2)
        subtotal += partida.subtotal
    orden.subtotal = round(subtotal, 2)
    discount_percentage = min(100.0, max(0.0, float(orden.descuento_total or 0)))
    orden.descuento_total = round(discount_percentage, 2)
    taxable = max(0.0, orden.subtotal * (1 - discount_percentage / 100.0))
    orden.iva_porc = round(max(0.0, float(orden.iva_porc or 0)), 2)
    orden.iva_monto = round(taxable * orden.iva_porc / 100.0, 2)
    orden.total = round(taxable + orden.iva_monto, 2)
    orden.actualizado_en = _now()


def _purchase_order_lines_from_form() -> list[OrdenCompraPartida]:
    descriptions = request.form.getlist("descripcion[]")
    units = request.form.getlist("unidad[]")
    quantities = request.form.getlist("cantidad[]")
    prices = request.form.getlist("precio_unitario[]")
    observations = request.form.getlist("observaciones[]")
    total_rows = max(len(descriptions), len(units), len(quantities), len(prices), 0)
    lines: list[OrdenCompraPartida] = []
    for index in range(total_rows):
        description = (descriptions[index] if index < len(descriptions) else "").strip()
        if not description:
            continue
        quantity = _parse_nonnegative_float(
            quantities[index] if index < len(quantities) else "",
            label=f"La cantidad de la partida {index + 1}",
        )
        if quantity <= 0:
            raise ValueError(f"La cantidad de la partida {index + 1} debe ser mayor a cero.")
        unit_price = _parse_nonnegative_float(
            prices[index] if index < len(prices) else "",
            label=f"El precio de la partida {index + 1}",
        )
        lines.append(
            OrdenCompraPartida(
                descripcion=description[:320],
                unidad=(units[index] if index < len(units) else "pieza").strip()[:50] or "pieza",
                cantidad=quantity,
                cantidad_recibida=0.0,
                precio_unitario=unit_price,
                observaciones=(observations[index] if index < len(observations) else "").strip() or None,
            )
        )
    if not lines:
        raise ValueError("Agrega al menos un producto o servicio a la orden.")
    return lines


def _provider_purchase_orders(proveedor_id: int, *, invoiceable_only: bool = False):
    query = OrdenCompra.query.filter(
        OrdenCompra.portal_proveedor_usuario_id == proveedor_id,
        OrdenCompra.estatus.in_(ORDEN_COMPRA_VISIBLES_PROVEEDOR),
    )
    if invoiceable_only:
        query = query.filter(
            OrdenCompra.estatus.in_(("ENVIADA", "PARCIALMENTE RECIBIDA", "RECIBIDA COMPLETA", "FACTURADA"))
        )
    return query.order_by(OrdenCompra.fecha.desc(), OrdenCompra.id.desc()).all()


def _selected_provider_purchase_order(proveedor_id: int, folio: str) -> OrdenCompra | None:
    normalized = (folio or "").strip()
    if not normalized:
        return None
    orden = OrdenCompra.query.filter(
        OrdenCompra.portal_proveedor_usuario_id == proveedor_id,
        func.upper(OrdenCompra.folio) == normalized.upper(),
        OrdenCompra.estatus.in_(("ENVIADA", "PARCIALMENTE RECIBIDA", "RECIBIDA COMPLETA", "FACTURADA")),
    ).first()
    if not orden:
        raise ValueError("Selecciona una orden de compra válida de tu cuenta.")
    return orden


def _linked_purchase_order(factura: FacturaProveedor) -> OrdenCompra | None:
    if not factura.orden_compra:
        return None
    return OrdenCompra.query.filter(
        OrdenCompra.portal_proveedor_usuario_id == factura.proveedor_usuario_id,
        func.upper(OrdenCompra.folio) == factura.orden_compra.strip().upper(),
    ).first()


def _restore_purchase_order_after_invoice_removal(orden: OrdenCompra) -> None:
    ordered = sum(float(item.cantidad or 0) for item in orden.partidas)
    received = sum(float(item.cantidad_recibida or 0) for item in orden.partidas)
    if ordered > 0 and received + 0.0001 >= ordered:
        orden.estatus = "RECIBIDA COMPLETA"
    elif received > 0:
        orden.estatus = "PARCIALMENTE RECIBIDA"
    else:
        orden.estatus = "ENVIADA"
    orden.factura_folio = None
    orden.factura_monto = 0.0
    orden.pago_referencia = None
    orden.pago_monto = 0.0
    orden.actualizado_en = _now()


def _send_purchase_order_email(orden: OrdenCompra, recipients: list[str]) -> None:
    if not recipients or current_app.config.get("PORTAL_FACTURAS_DISABLE_EMAIL"):
        return
    proveedor = orden.portal_proveedor_usuario
    detail_url = url_for(
        "portal_facturas.orden_compra_proveedor_detalle",
        orden_id=orden.id,
        _external=True,
    )
    subject = f"Orden de compra {orden.folio} · Poliutech"
    msg = EmailMessage()
    msg["Subject"] = subject
    smtp_from = str(current_app.config.get("SMTP_FROM") or current_app.config.get("SMTP_USERNAME") or "").strip()
    msg["From"] = f"COMPRAS POLIUTECH <{smtp_from}>"
    msg["To"] = proveedor.correo
    msg.set_content(
        f"Hola {proveedor.contacto},\n\n"
        f"Poliutech emitió la orden de compra {orden.folio} para {proveedor.razon_social}.\n"
        f"Importe total: ${float(orden.total or 0):,.2f} MXN\n"
        f"Entrega estimada: {orden.fecha_entrega.strftime('%d/%m/%Y') if orden.fecha_entrega else 'Por acordar'}\n\n"
        f"Consulta los productos o servicios y las condiciones en tu portal:\n{detail_url}\n"
    )
    msg.add_alternative(
        (
            "<div style='font-family:Arial,sans-serif;max-width:680px;color:#15263b'>"
            "<div style='font-size:12px;font-weight:800;letter-spacing:.08em;color:#0b67b2'>ORDEN DE COMPRA POLIUTECH</div>"
            f"<h2 style='margin:8px 0'>{escape(orden.folio or '')}</h2>"
            f"<p>Hola <b>{escape(proveedor.contacto)}</b>, Poliutech emitió esta orden para <b>{escape(proveedor.razon_social)}</b>.</p>"
            f"<p style='font-size:24px;font-weight:800;color:#0b67b2'>${float(orden.total or 0):,.2f} MXN</p>"
            f"<p>Entrega estimada: {orden.fecha_entrega.strftime('%d/%m/%Y') if orden.fecha_entrega else 'Por acordar'}</p>"
            f"<p><a href='{escape(detail_url)}' style='display:inline-block;padding:12px 18px;background:#f36c21;color:#fff;text-decoration:none;border-radius:7px;font-weight:700'>Ver orden de compra</a></p>"
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


def _notify_purchase_order_sent(orden: OrdenCompra) -> bool:
    users, internal_emails = _provider_registration_notification_targets()
    internal_url = url_for("portal_facturas.finanzas_orden_detalle", orden_id=orden.id)
    title = f"Orden enviada: {orden.folio}"
    body = f"{orden.proveedor} · ${float(orden.total or 0):,.2f} MXN"
    for user in users:
        db.session.add(
            InAppNotification(
                usuario_id=user.id,
                tipo="portal_ordenes_compra",
                titulo=title[:180],
                mensaje=body,
                destino_url=internal_url,
                creada_en=_now(),
            )
        )
    sent_stamp = int((orden.enviada_en or _now()).timestamp())
    _queue_messenger_notifications(
        users,
        source_key=f"orden-compra:{orden.id}:enviada:{sent_stamp}",
        title=title,
        body=body,
        view_url=url_for(
            "portal_facturas.finanzas_orden_detalle",
            orden_id=orden.id,
            _external=True,
        ),
    )
    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        current_app.logger.warning("No se guardaron los avisos de %s: %s", orden.folio, exc)

    recipients = []
    seen = set()
    for email in [orden.portal_proveedor_usuario.correo, *internal_emails]:
        normalized = _normalize_email(email)
        if normalized and normalized not in seen:
            recipients.append(normalized)
            seen.add(normalized)
    try:
        _send_purchase_order_email(orden, recipients)
        return True
    except Exception as exc:
        current_app.logger.warning("No se pudo enviar por correo %s: %s", orden.folio, exc)
        return False


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


def _read_provider_document(
    uploaded,
    *,
    label: str,
    allowed_extensions: set[str],
) -> tuple[bytes, str, str]:
    if not uploaded or not (uploaded.filename or "").strip():
        raise ValueError(f"Adjunta {label}.")
    original_name = secure_filename(uploaded.filename or "")[:260]
    extension = Path(original_name).suffix.lower()
    if extension not in allowed_extensions:
        readable = ", ".join(sorted(allowed_extensions))
        raise ValueError(f"{label} debe tener uno de estos formatos: {readable}.")
    payload = uploaded.stream.read(MAX_PROVIDER_DOCUMENT_BYTES + 1)
    if not payload:
        raise ValueError(f"{label} está vacía.")
    if len(payload) > MAX_PROVIDER_DOCUMENT_BYTES:
        raise ValueError(f"{label} supera el límite de 10 MB.")

    signatures_ok = {
        ".pdf": payload[:1024].lstrip().startswith(b"%PDF-"),
        ".png": payload.startswith(b"\x89PNG\r\n\x1a\n"),
        ".jpg": payload.startswith(b"\xff\xd8\xff"),
        ".jpeg": payload.startswith(b"\xff\xd8\xff"),
    }
    if not signatures_ok.get(extension, False):
        raise ValueError(f"{label} no coincide con el formato indicado.")
    return payload, original_name, extension


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


def _save_provider_documents(
    proveedor_id: int,
    csf_bytes: bytes,
    csf_extension: str,
    caratula_bytes: bytes,
    caratula_extension: str,
) -> tuple[str, str]:
    folder = _upload_root() / "portal_facturas" / "proveedores" / str(proveedor_id)
    folder.mkdir(parents=True, exist_ok=True)
    nonce = secrets.token_hex(6)
    csf_name = f"csf_{nonce}{csf_extension}"
    caratula_name = f"caratula_bancaria_{nonce}{caratula_extension}"
    csf_path = folder / csf_name
    caratula_path = folder / caratula_name
    csf_path.write_bytes(csf_bytes)
    try:
        caratula_path.write_bytes(caratula_bytes)
    except Exception:
        csf_path.unlink(missing_ok=True)
        raise
    return (
        f"portal_facturas/proveedores/{proveedor_id}/{csf_name}",
        f"portal_facturas/proveedores/{proveedor_id}/{caratula_name}",
    )


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
) -> FacturaProveedorMovimiento:
    event = FacturaProveedorMovimiento(
        factura=factura,
        estatus_anterior=old_status,
        estatus_nuevo=new_status,
        actor_tipo=actor_type,
        actor_usuario_id=actor_user_id,
        actor_nombre=(actor_name or "Sistema")[:180],
        comentario=(comment or "").strip() or None,
    )
    db.session.add(event)
    return event


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
                estatus="PENDIENTE",
            )
            proveedor.set_password(password)
            db.session.add(proveedor)
            saved_document_paths: tuple[str, ...] = ()
            try:
                csf_bytes, csf_original, csf_extension = _read_provider_document(
                    request.files.get("csf"),
                    label="la Constancia de Situación Fiscal",
                    allowed_extensions={".pdf"},
                )
                caratula_bytes, caratula_original, caratula_extension = _read_provider_document(
                    request.files.get("caratula_bancaria"),
                    label="la carátula del estado de cuenta",
                    allowed_extensions={".pdf", ".png", ".jpg", ".jpeg"},
                )
                db.session.flush()
                csf_path, caratula_path = _save_provider_documents(
                    proveedor.id,
                    csf_bytes,
                    csf_extension,
                    caratula_bytes,
                    caratula_extension,
                )
                saved_document_paths = (csf_path, caratula_path)
                proveedor.csf_path = csf_path
                proveedor.csf_nombre_original = csf_original
                proveedor.csf_tamano = len(csf_bytes)
                proveedor.caratula_bancaria_path = caratula_path
                proveedor.caratula_bancaria_nombre_original = caratula_original
                proveedor.caratula_bancaria_tamano = len(caratula_bytes)
                db.session.commit()
            except (IntegrityError, OSError, ValueError) as exc:
                db.session.rollback()
                _delete_paths(*saved_document_paths)
                if isinstance(exc, IntegrityError):
                    flash("Ya existe una cuenta con ese RFC o correo electrónico.", "danger")
                elif isinstance(exc, ValueError):
                    flash(str(exc), "danger")
                else:
                    current_app.logger.exception(
                        "No se pudo registrar al proveedor %s en el padrón maestro.",
                        rfc,
                    )
                    flash(
                        "No se pudo guardar el alta en el registro de proveedores. Inténtalo nuevamente.",
                        "danger",
                    )
            else:
                _notify_provider_registration(proveedor)
                flash(
                    "Recibimos tu solicitud. Nuestro equipo revisará tu información y documentos antes de autorizarte para facturar.",
                    "success",
                )
                return redirect(url_for("portal_facturas.ingresar"))

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
        elif proveedor.estatus == "PENDIENTE":
            flash("Tu alta sigue pendiente de autorización por nuestro equipo.", "warning")
        elif proveedor.estatus == "RECHAZADO":
            reason = (proveedor.revision_comentario or "Contacta a nuestro equipo para conocer el motivo.").strip()
            flash(f"Tu alta fue rechazada: {reason}", "danger")
        elif proveedor.estatus != "ACTIVO":
            flash("Tu cuenta no está activa. Contacta a nuestro equipo.", "danger")
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


@portal_facturas_bp.get("/ordenes-compra")
@proveedor_login_required
def ordenes_compra_proveedor():
    ordenes = _provider_purchase_orders(g.portal_proveedor.id)
    return render_template(
        "portal_facturas/ordenes_compra_proveedor.html",
        ordenes=ordenes,
    )


def _owned_purchase_order_or_404(orden_id: int) -> OrdenCompra:
    orden = db.session.get(OrdenCompra, orden_id)
    if (
        not orden
        or orden.portal_proveedor_usuario_id != g.portal_proveedor.id
        or orden.estatus == "BORRADOR"
    ):
        abort(404)
    return orden


@portal_facturas_bp.get("/ordenes-compra/<int:orden_id>")
@proveedor_login_required
def orden_compra_proveedor_detalle(orden_id: int):
    return render_template(
        "portal_facturas/orden_compra_proveedor_detalle.html",
        orden=_owned_purchase_order_or_404(orden_id),
    )


@portal_facturas_bp.route("/facturas/nueva", methods=["GET", "POST"])
@proveedor_login_required
def nueva_factura():
    ordenes_disponibles = _provider_purchase_orders(
        g.portal_proveedor.id,
        invoiceable_only=True,
    )
    if request.method == "POST":
        _require_csrf()
        try:
            orden = _selected_provider_purchase_order(
                g.portal_proveedor.id,
                request.form.get("orden_compra") or "",
            )
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
                orden_compra=orden.folio if orden else None,
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
            if orden:
                orden.factura_folio = factura.folio_recepcion
                orden.factura_monto = factura.total
                if orden.estatus not in {"PAGADA", "CANCELADA"}:
                    orden.estatus = "FACTURADA"
                orden.actualizado_en = _now()
            event = _add_event(
                factura,
                old_status=None,
                new_status="RECIBIDA",
                actor_type="PROVEEDOR",
                actor_name=g.portal_proveedor.contacto,
                comment="Factura enviada para revisión.",
            )
            db.session.commit()
            _notify_finance_invoice(factura)
            _notify_invoice_status_change(
                factura,
                event_key=f"movimiento-{event.id}",
                notify_internal=False,
            )
        except (ValueError, OSError, IntegrityError) as exc:
            db.session.rollback()
            if "xml_path" in locals() and "pdf_path" in locals():
                _delete_paths(xml_path, pdf_path)
            message = str(exc) if isinstance(exc, (ValueError, OSError)) else "No fue posible guardar la factura. Verifica que no esté duplicada."
            flash(message, "danger")
        else:
            flash(f"Factura {factura.folio_recepcion} recibida correctamente.", "success")
            return redirect(url_for("portal_facturas.detalle_factura", factura_id=factura.id))
    return render_template(
        "portal_facturas/nueva_factura.html",
        ordenes_disponibles=ordenes_disponibles,
        selected_order_id=request.args.get("orden_id", type=int),
    )


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
            event = _add_event(
                factura,
                old_status=old_status,
                new_status="RECIBIDA",
                actor_type="PROVEEDOR",
                actor_name=g.portal_proveedor.contacto,
                comment="Se enviaron XML y PDF corregidos.",
            )
            db.session.commit()
            _notify_finance_invoice(factura, corrected=True)
            _notify_invoice_status_change(
                factura,
                event_key=f"movimiento-{event.id}",
                notify_internal=False,
            )
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


@portal_facturas_bp.get("/finanzas/proveedores")
@finanzas_required
def finanzas_proveedores():
    query_text = (request.args.get("q") or "").strip()
    selected_status = (request.args.get("estatus") or "").strip().upper()
    query = PortalProveedorUsuario.query
    if query_text:
        like = f"%{query_text}%"
        query = query.filter(
            or_(
                PortalProveedorUsuario.razon_social.ilike(like),
                PortalProveedorUsuario.nombre_comercial.ilike(like),
                PortalProveedorUsuario.rfc.ilike(like),
                PortalProveedorUsuario.contacto.ilike(like),
                PortalProveedorUsuario.correo.ilike(like),
            )
        )
    if selected_status in {"PENDIENTE", "ACTIVO", "RECHAZADO"}:
        query = query.filter(PortalProveedorUsuario.estatus == selected_status)
    else:
        selected_status = ""
    proveedores = query.order_by(
        (PortalProveedorUsuario.estatus == "PENDIENTE").desc(),
        PortalProveedorUsuario.creado_en.desc(),
    ).limit(500).all()
    provider_ids = [item.id for item in proveedores]
    invoice_counts = {}
    if provider_ids:
        invoice_counts = dict(
            db.session.query(
                FacturaProveedor.proveedor_usuario_id,
                func.count(FacturaProveedor.id),
            )
            .filter(FacturaProveedor.proveedor_usuario_id.in_(provider_ids))
            .group_by(FacturaProveedor.proveedor_usuario_id)
            .all()
        )
    total_proveedores = PortalProveedorUsuario.query.count()
    pendientes_revision = PortalProveedorUsuario.query.filter_by(estatus="PENDIENTE").count()
    expedientes_completos = PortalProveedorUsuario.query.filter(
        PortalProveedorUsuario.csf_path.isnot(None),
        PortalProveedorUsuario.caratula_bancaria_path.isnot(None),
    ).count()
    return render_template(
        "portal_facturas/finanzas_proveedores.html",
        proveedores=proveedores,
        invoice_counts=invoice_counts,
        q=query_text,
        total_proveedores=total_proveedores,
        expedientes_completos=expedientes_completos,
        expedientes_pendientes=max(total_proveedores - expedientes_completos, 0),
        pendientes_revision=pendientes_revision,
        selected_status=selected_status,
    )


@portal_facturas_bp.get("/finanzas/ordenes-compra")
@finanzas_required
def finanzas_ordenes_compra():
    query_text = (request.args.get("q") or "").strip()
    status = (request.args.get("estatus") or "").strip().upper()
    query = OrdenCompra.query.filter(OrdenCompra.portal_proveedor_usuario_id.isnot(None))
    if query_text:
        like = f"%{query_text}%"
        query = query.filter(
            or_(
                OrdenCompra.folio.ilike(like),
                OrdenCompra.proveedor.ilike(like),
                OrdenCompra.notas.ilike(like),
            )
        )
    if status in ORDEN_COMPRA_ESTADOS:
        query = query.filter(OrdenCompra.estatus == status)
    else:
        status = ""
    ordenes = query.order_by(OrdenCompra.fecha.desc(), OrdenCompra.id.desc()).limit(500).all()
    counts = {
        key: OrdenCompra.query.filter(
            OrdenCompra.portal_proveedor_usuario_id.isnot(None),
            OrdenCompra.estatus == key,
        ).count()
        for key in ORDEN_COMPRA_ESTADOS
    }
    counts["TOTAL"] = OrdenCompra.query.filter(
        OrdenCompra.portal_proveedor_usuario_id.isnot(None)
    ).count()
    return render_template(
        "portal_facturas/finanzas_ordenes.html",
        ordenes=ordenes,
        counts=counts,
        q=query_text,
        selected_status=status,
    )


@portal_facturas_bp.route("/finanzas/ordenes-compra/nueva", methods=["GET", "POST"])
@finanzas_required
def finanzas_orden_nueva():
    proveedores = PortalProveedorUsuario.query.filter_by(estatus="ACTIVO").order_by(
        PortalProveedorUsuario.razon_social.asc()
    ).all()
    selected_provider_id = request.args.get("proveedor_id", type=int) or request.form.get(
        "proveedor_id", type=int
    )
    if request.method == "POST":
        _require_csrf()
        proveedor = db.session.get(PortalProveedorUsuario, selected_provider_id)
        if not proveedor or proveedor.estatus != "ACTIVO":
            flash("Selecciona un proveedor activo del portal.", "danger")
        else:
            try:
                delivery_date = _parse_date(request.form.get("fecha_entrega") or "")
                lines = _purchase_order_lines_from_form()
                order = OrdenCompra(
                    folio=f"TMP-{secrets.token_hex(10)}",
                    portal_proveedor_usuario_id=proveedor.id,
                    proveedor=proveedor.razon_social,
                    contacto=proveedor.contacto,
                    telefono=proveedor.telefono,
                    correo=proveedor.correo,
                    fecha=_now(),
                    fecha_entrega=(
                        datetime.combine(delivery_date, datetime.min.time())
                        if delivery_date
                        else None
                    ),
                    forma_pago=(request.form.get("forma_pago") or "CONTADO").strip().upper()[:20],
                    estatus="BORRADOR",
                    descuento_total=_parse_nonnegative_float(
                        request.form.get("descuento_total") or "0",
                        label="El descuento",
                    ),
                    iva_porc=_parse_nonnegative_float(
                        request.form.get("iva_porc") or "16",
                        label="El IVA",
                        default=16.0,
                    ),
                    condiciones=(request.form.get("condiciones") or "").strip() or None,
                    notas=(request.form.get("notas") or "").strip() or None,
                    responsable=(
                        getattr(current_user, "nombre_representante", None)
                        or getattr(current_user, "nombre", "")
                    )[:120]
                    or None,
                    usuario_id=current_user.id,
                )
                order.partidas.extend(lines)
                _purchase_order_totals(order)
                db.session.add(order)
                db.session.flush()
                order.folio = f"OC-{_now().year}-{order.id:04d}"
                db.session.commit()
            except (ValueError, IntegrityError) as exc:
                db.session.rollback()
                message = str(exc) if isinstance(exc, ValueError) else "No se pudo generar el folio de la orden."
                flash(message, "danger")
            else:
                flash(f"Orden {order.folio} creada como borrador. Revísala y envíala al proveedor.", "success")
                return redirect(
                    url_for("portal_facturas.finanzas_orden_detalle", orden_id=order.id)
                )
    return render_template(
        "portal_facturas/finanzas_orden_nueva.html",
        proveedores=proveedores,
        selected_provider_id=selected_provider_id,
    )


def _finance_purchase_order_or_404(orden_id: int) -> OrdenCompra:
    orden = db.session.get(OrdenCompra, orden_id)
    if not orden or not orden.portal_proveedor_usuario_id:
        abort(404)
    return orden


@portal_facturas_bp.get("/finanzas/ordenes-compra/<int:orden_id>")
@finanzas_required
def finanzas_orden_detalle(orden_id: int):
    return render_template(
        "portal_facturas/finanzas_orden_detalle.html",
        orden=_finance_purchase_order_or_404(orden_id),
    )


@portal_facturas_bp.post("/finanzas/ordenes-compra/<int:orden_id>/enviar")
@finanzas_required
def finanzas_orden_enviar(orden_id: int):
    _require_csrf()
    orden = _finance_purchase_order_or_404(orden_id)
    if orden.estatus in {"CANCELADA", "PAGADA"}:
        flash("Esta orden ya no puede enviarse al proveedor.", "danger")
        return redirect(url_for("portal_facturas.finanzas_orden_detalle", orden_id=orden.id))
    if orden.estatus == "BORRADOR":
        orden.estatus = "ENVIADA"
    orden.enviada_en = _now()
    orden.actualizado_en = _now()
    db.session.commit()
    email_sent = _notify_purchase_order_sent(orden)
    if email_sent:
        flash(f"{orden.folio} fue enviada al proveedor y notificada por correo.", "success")
    else:
        flash(
            f"{orden.folio} ya está visible en el portal, pero el correo no pudo enviarse.",
            "warning",
        )
    return redirect(url_for("portal_facturas.finanzas_orden_detalle", orden_id=orden.id))


@portal_facturas_bp.get("/finanzas/proveedores/<int:proveedor_id>")
@finanzas_required
def finanzas_proveedor_detalle(proveedor_id: int):
    proveedor = db.session.get(PortalProveedorUsuario, proveedor_id)
    if not proveedor:
        abort(404)
    facturas = FacturaProveedor.query.filter_by(
        proveedor_usuario_id=proveedor.id
    ).order_by(FacturaProveedor.recibida_en.desc()).all()
    ordenes = OrdenCompra.query.filter_by(
        portal_proveedor_usuario_id=proveedor.id
    ).order_by(OrdenCompra.fecha.desc(), OrdenCompra.id.desc()).all()
    return render_template(
        "portal_facturas/finanzas_proveedor_detalle.html",
        proveedor=proveedor,
        facturas=facturas,
        ordenes=ordenes,
    )


@portal_facturas_bp.post("/finanzas/proveedores/<int:proveedor_id>/revision")
@provider_reviewer_required
def finanzas_proveedor_revision(proveedor_id: int):
    _require_csrf()
    proveedor = db.session.get(PortalProveedorUsuario, proveedor_id)
    if not proveedor:
        abort(404)
    decision = (request.form.get("decision") or "").strip().upper()
    comment = (request.form.get("comentario") or "").strip()
    if decision not in {"AUTORIZAR", "RECHAZAR"}:
        flash("Selecciona Autorizar o Rechazar.", "danger")
        return redirect(url_for("portal_facturas.finanzas_proveedor_detalle", proveedor_id=proveedor.id))
    if decision == "RECHAZAR" and not comment:
        flash("Escribe el motivo del rechazo para informar al proveedor.", "danger")
        return redirect(url_for("portal_facturas.finanzas_proveedor_detalle", proveedor_id=proveedor.id))

    proveedor.estatus = "ACTIVO" if decision == "AUTORIZAR" else "RECHAZADO"
    proveedor.revision_comentario = comment or "Alta autorizada por el equipo responsable."
    proveedor.revisado_por_id = current_user.id
    proveedor.revisado_en = _now()
    try:
        if decision == "AUTORIZAR":
            _sync_provider_registry(proveedor)
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        current_app.logger.exception("No se pudo revisar el alta %s", proveedor.id)
        flash(f"No se pudo guardar la decisión: {exc}", "danger")
        return redirect(url_for("portal_facturas.finanzas_proveedor_detalle", proveedor_id=proveedor.id))

    email_sent = _notify_provider_review(proveedor)
    result = "autorizado para facturar" if decision == "AUTORIZAR" else "rechazado"
    if email_sent:
        flash(
            f"{proveedor.nombre_portal} fue {result}. Correo de {'autorización' if decision == 'AUTORIZAR' else 'rechazo'} enviado a {proveedor.correo}.",
            "success" if decision == "AUTORIZAR" else "warning",
        )
    else:
        flash(
            f"{proveedor.nombre_portal} fue {result}, pero no se pudo enviar el correo a {proveedor.correo}. Usa 'Reenviar correo al proveedor'.",
            "warning",
        )
    return redirect(url_for("portal_facturas.finanzas_proveedor_detalle", proveedor_id=proveedor.id))


@portal_facturas_bp.post("/finanzas/proveedores/<int:proveedor_id>/revision/correo")
@provider_reviewer_required
def finanzas_proveedor_revision_correo(proveedor_id: int):
    _require_csrf()
    proveedor = db.session.get(PortalProveedorUsuario, proveedor_id)
    if not proveedor:
        abort(404)
    if proveedor.estatus not in {"ACTIVO", "RECHAZADO"}:
        flash("Primero autoriza o rechaza el alta del proveedor.", "warning")
        return redirect(url_for("portal_facturas.finanzas_proveedor_detalle", proveedor_id=proveedor.id))
    try:
        sent = _send_provider_review_email(proveedor, [proveedor.correo])
        if not sent:
            raise RuntimeError("El envío de correos está deshabilitado.")
    except Exception as exc:
        current_app.logger.exception(
            "No se pudo reenviar el resultado del alta %s: %s", proveedor.id, exc
        )
        flash(f"No se pudo enviar el correo a {proveedor.correo}. Intenta nuevamente.", "danger")
    else:
        flash(f"Correo reenviado correctamente a {proveedor.correo}.", "success")
    return redirect(url_for("portal_facturas.finanzas_proveedor_detalle", proveedor_id=proveedor.id))


@portal_facturas_bp.get("/finanzas/proveedores/<int:proveedor_id>/documento/<string:tipo>")
@finanzas_required
def finanzas_proveedor_documento(proveedor_id: int, tipo: str):
    proveedor = db.session.get(PortalProveedorUsuario, proveedor_id)
    if not proveedor:
        abort(404)
    if tipo == "csf":
        relative_path = proveedor.csf_path
        original_name = proveedor.csf_nombre_original or "constancia_situacion_fiscal.pdf"
    elif tipo == "caratula-bancaria":
        relative_path = proveedor.caratula_bancaria_path
        original_name = proveedor.caratula_bancaria_nombre_original or "caratula_bancaria.pdf"
    else:
        abort(404)
    if not relative_path:
        abort(404)
    path = _safe_upload_path(relative_path)
    if not path.is_file():
        abort(404)
    mimetype = mimetypes.guess_type(original_name)[0] or "application/octet-stream"
    return send_file(
        path,
        mimetype=mimetype,
        as_attachment=False,
        download_name=original_name,
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
    linked_order = _linked_purchase_order(factura)
    if linked_order and new_status == "PAGADA":
        linked_order.estatus = "PAGADA"
        linked_order.pago_referencia = factura.referencia_pago
        linked_order.pago_monto = factura.total
        linked_order.actualizado_en = _now()
    event = _add_event(
        factura,
        old_status=old_status,
        new_status=new_status,
        actor_type="FINANZAS",
        actor_name=getattr(current_user, "nombre_representante", None) or current_user.nombre,
        actor_user_id=current_user.id,
        comment=comment,
    )
    db.session.flush()
    try:
        email_sent = _send_provider_invoice_status_email(
            factura,
            [_normalize_email(factura.proveedor_usuario.correo)],
        )
        if not email_sent:
            raise RuntimeError("El envío de correo está deshabilitado.")
    except Exception as exc:
        db.session.rollback()
        error_text = str(exc).casefold()
        if "user unknown" in error_text or "5.1.1" in error_text:
            reason = (
                f"el correo registrado {factura.proveedor_usuario.correo} no existe "
                "en el servidor de correo"
            )
        else:
            reason = (
                f"el servidor no aceptó el correo para "
                f"{factura.proveedor_usuario.correo}"
            )
        current_app.logger.exception(
            "No se cambió el estatus de %s porque falló el correo automático a %s: %s",
            factura.folio_recepcion,
            factura.proveedor_usuario.correo,
            exc,
        )
        flash(
            f"No se cambió el estatus de {factura.folio_recepcion}: {reason}. "
            "Corrige el correo del proveedor e intenta guardar la decisión nuevamente.",
            "danger",
        )
        return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))

    db.session.commit()
    _notify_invoice_status_change(
        factura,
        event_key=f"movimiento-{event.id}",
        notify_internal=True,
        provider_email_already_sent=True,
    )
    flash(
        f"{factura.folio_recepcion} cambió a {ESTATUS[new_status]['label']} y se notificó automáticamente a {factura.proveedor_usuario.correo}.",
        "success",
    )
    return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))


@portal_facturas_bp.post("/finanzas/<int:factura_id>/eliminar")
@administrador_account_required
def finanzas_eliminar_factura(factura_id: int):
    _require_csrf()
    factura = db.session.get(FacturaProveedor, factura_id)
    if not factura:
        abort(404)
    if (request.form.get("confirmacion") or "").strip().upper() != "ELIMINAR":
        flash("Escribe ELIMINAR para confirmar la eliminación definitiva.", "danger")
        return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura.id))

    folio_recepcion = factura.folio_recepcion
    stored_paths = (factura.xml_path, factura.pdf_path)
    linked_order = _linked_purchase_order(factura)
    if linked_order and linked_order.factura_folio == factura.folio_recepcion:
        _restore_purchase_order_after_invoice_removal(linked_order)
    db.session.delete(factura)
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("No se pudo eliminar la factura %s", factura_id)
        flash("No se pudo eliminar la factura. Inténtalo nuevamente.", "danger")
        return redirect(url_for("portal_facturas.finanzas_detalle", factura_id=factura_id))

    _delete_paths(*stored_paths)
    flash(f"{folio_recepcion} fue eliminada definitivamente.", "success")
    return redirect(url_for("portal_facturas.finanzas"))


@portal_facturas_bp.get("/finanzas/<int:factura_id>/archivo/<string:tipo>")
@finanzas_required
def finanzas_descargar_archivo(factura_id: int, tipo: str):
    factura = db.session.get(FacturaProveedor, factura_id)
    if not factura:
        abort(404)
    return _send_invoice_file(factura, tipo)

