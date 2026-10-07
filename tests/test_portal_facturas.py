import io
import json
import os
import re
import tempfile
import unittest
from unittest.mock import MagicMock, patch


_upload_dir = tempfile.TemporaryDirectory(prefix="mar_portal_facturas_")
_provider_registry_dir = tempfile.TemporaryDirectory(prefix="mar_portal_proveedores_")
os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["UPLOAD_STORAGE_ROOT"] = _upload_dir.name
os.environ["PROVIDER_NUMBERS_JSON_PATH"] = os.path.join(
    _provider_registry_dir.name,
    "provider_numbers.json",
)
os.environ["DISABLE_BACKGROUND_SCHEDULER"] = "1"

try:
    from app import app  # noqa: E402
    from models import (  # noqa: E402
        FacturaProveedor,
        InAppNotification,
        MessengerNotificationOutbox,
        OrdenCompra,
        PortalProveedorUsuario,
        Usuario,
        db,
    )
    from portal_facturas_routes import ESTATUS  # noqa: E402
    _IMPORT_ERROR = ""
except ModuleNotFoundError as exc:  # El CI ligero no instala dependencias web.
    app = None
    _IMPORT_ERROR = str(exc)


def _csrf(response) -> str:
    match = re.search(rb'name="csrf_token" value="([^"]+)"', response.data)
    if not match:
        raise AssertionError("No se encontró el token CSRF en la respuesta.")
    return match.group(1).decode()


@unittest.skipIf(app is None, f"Dependencias de integración no disponibles: {_IMPORT_ERROR}")
class PortalFacturasFlowTest(unittest.TestCase):
    def setUp(self):
        app.config.update(TESTING=True, PORTAL_FACTURAS_DISABLE_EMAIL=False)

    def test_login_no_muestra_acceso_al_portal_de_proveedores(self):
        response = app.test_client().get("/login")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("¿Eres proveedor?".encode("utf-8"), response.data)
        self.assertNotIn("Sube tu factura aquí".encode("utf-8"), response.data)
        self.assertNotIn(b'href="/portal-facturas/ingresar"', response.data)

    def test_registro_carga_revision_y_pago(self):
        xml = b'''<?xml version="1.0" encoding="UTF-8"?>
<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Version="4.0" Serie="A" Folio="1001" Fecha="2026-10-02T10:15:00" SubTotal="1000.00" Total="1160.00" Moneda="MXN">
  <cfdi:Emisor Rfc="AAA010101AAA" Nombre="Proveedor Prueba SA de CV"/>
  <cfdi:Receptor Rfc="POL010101AAA" Nombre="Poliutech SA de CV"/>
  <cfdi:Complemento><tfd:TimbreFiscalDigital xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital" UUID="123E4567-E89B-12D3-A456-426614174000"/></cfdi:Complemento>
</cfdi:Comprobante>'''
        pdf = b"%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\n%%EOF"
        bank_cover = b"\xff\xd8\xff\xe0JFIF\x00\x01\xff\xd9"

        with app.app_context():
            marco = Usuario(
                nombre="mescalera",
                nombre_visible="Marco Escalera",
                correo="mescalera@poliutech.com",
                rol="USER",
            )
            marco.set_password("MarcoTest123")
            uriel = Usuario(
                nombre="umorales",
                nombre_visible="Uriel Morales",
                correo="umorales@poliutech.com",
                rol="USER",
            )
            uriel.set_password("UrielTest123")
            admin = Usuario(
                nombre="admin",
                nombre_visible="Administrador",
                correo="finanzas@example.com",
                telefono="5511111111",
                rol="ADMIN",
            )
            admin.set_password("AdminTest123")
            other_admin = Usuario(
                nombre="otro_admin_portal",
                nombre_visible="Otro administrador",
                correo="otro-admin@example.com",
                rol="ADMIN",
            )
            other_admin.set_password("OtroAdminTest123")
            db.session.add_all([marco, uriel, admin, other_admin])
            db.session.commit()

        provider = app.test_client()
        response = provider.get("/portal-facturas/registro")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'name="csf"', response.data)
        self.assertIn(b'name="caratula_bancaria"', response.data)
        with patch("portal_facturas_routes.smtplib.SMTP") as registration_smtp_class:
            response = provider.post(
                "/portal-facturas/registro",
                data={
                    "csrf_token": _csrf(response),
                    "razon_social": "Proveedor Prueba SA de CV",
                    "nombre_comercial": "Proveedor Prueba",
                    "rfc": "AAA010101AAA",
                    "contacto": "Ana Proveedor",
                    "correo": "ana@example.com",
                    "telefono": "5512345678",
                    "password": "Segura1234",
                    "password_confirmation": "Segura1234",
                    "csf": (io.BytesIO(pdf), "constancia_fiscal.pdf"),
                    "caratula_bancaria": (io.BytesIO(bank_cover), "caratula_bancaria.jpg"),
                },
                content_type="multipart/form-data",
            )
            registration_smtp = registration_smtp_class.return_value.__enter__.return_value
            self.assertEqual(registration_smtp.send_message.call_count, 2)
            registration_recipients = set(
                registration_smtp.send_message.call_args_list[0].kwargs["to_addrs"]
            )
            self.assertIn("finanzas@example.com", registration_recipients)
            self.assertIn("mescalera@poliutech.com", registration_recipients)
            self.assertIn("sistemas@poliutech.com", registration_recipients)
            self.assertEqual(
                ["ana@example.com"],
                registration_smtp.send_message.call_args_list[1].kwargs["to_addrs"],
            )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/ingresar", response.headers["Location"])

        with app.app_context():
            proveedor_registrado = PortalProveedorUsuario.query.filter_by(
                rfc="AAA010101AAA"
            ).one()
            provider_id = proveedor_registrado.id
            self.assertEqual(proveedor_registrado.estatus, "PENDIENTE")
            self.assertTrue(proveedor_registrado.csf_path)
            self.assertTrue(proveedor_registrado.caratula_bancaria_path)
            self.assertEqual(proveedor_registrado.csf_nombre_original, "constancia_fiscal.pdf")
            self.assertEqual(
                proveedor_registrado.caratula_bancaria_nombre_original,
                "caratula_bancaria.jpg",
            )
            registration_notices = InAppNotification.query.filter_by(
                tipo="portal_proveedores"
            ).all()
            registration_notice_emails = {
                notice.usuario.correo for notice in registration_notices
            }
            self.assertIn("finanzas@example.com", registration_notice_emails)
            self.assertIn("mescalera@poliutech.com", registration_notice_emails)
            registration_messenger = MessengerNotificationOutbox.query.filter(
                MessengerNotificationOutbox.source_key.like("portal:proveedor:%:alta-pendiente:%")
            ).all()
            self.assertIn(
                "mescalera@poliutech.com",
                {notice.correo for notice in registration_messenger},
            )
            self.assertIn(
                "ana@example.com",
                {notice.correo for notice in registration_messenger},
            )

        response = provider.get("/portal-facturas/ingresar")
        response = provider.post(
            "/portal-facturas/ingresar",
            data={
                "csrf_token": _csrf(response),
                "correo": "ana@example.com",
                "password": "Segura1234",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn("pendiente de autorización".encode("utf-8"), response.data)
        self.assertIn(b"nuestro equipo", response.data)
        self.assertNotIn(b"Marco/Mescalera", response.data)

        finance = app.test_client()
        response = finance.post(
            "/login",
            data={"nombre": "admin", "password": "AdminTest123"},
        )
        self.assertEqual(response.status_code, 302)
        response = finance.get(f"/portal-facturas/finanzas/proveedores/{provider_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Autorizar alta", response.data)
        self.assertIn(b"purchase-order-cta", response.data)
        self.assertIn(
            b'href="/portal-facturas/finanzas/ordenes-compra"', response.data
        )
        primary_actions_start = response.data.index(
            b'<div class="topbar-primary-actions'
        )
        primary_actions_end = response.data.index(b"</div>", primary_actions_start)
        purchase_order_position = response.data.index(
            b"purchase-order-cta", primary_actions_start
        )
        support_position = response.data.index(
            b"support-ticket-cta", primary_actions_start
        )
        self.assertLess(purchase_order_position, support_position)
        self.assertLess(support_position, primary_actions_end)

        other_admin_finance = app.test_client()
        response = other_admin_finance.post(
            "/login",
            data={"nombre": "otro_admin_portal", "password": "OtroAdminTest123"},
        )
        self.assertEqual(response.status_code, 302)
        response = other_admin_finance.get(
            f"/portal-facturas/finanzas/proveedores/{provider_id}"
        )
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Autorizar alta", response.data)
        response = other_admin_finance.post(
            f"/portal-facturas/finanzas/proveedores/{provider_id}/revision",
            data={"csrf_token": "forged", "decision": "AUTORIZAR"},
        )
        self.assertEqual(response.status_code, 403)

        marco_finance = app.test_client()
        response = marco_finance.post(
            "/login",
            data={"nombre": "mescalera", "password": "MarcoTest123"},
        )
        self.assertEqual(response.status_code, 302)
        response = marco_finance.get(
            f"/portal-facturas/finanzas/proveedores/{provider_id}"
        )
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Autorizar alta", response.data)
        with patch("portal_facturas_routes.smtplib.SMTP") as review_smtp_class:
            response = finance.post(
                f"/portal-facturas/finanzas/proveedores/{provider_id}/revision",
                data={
                    "csrf_token": _csrf(
                        finance.get(
                            f"/portal-facturas/finanzas/proveedores/{provider_id}"
                        )
                    ),
                    "decision": "AUTORIZAR",
                    "comentario": "Expediente fiscal y bancario validado.",
                },
            )
            review_smtp = review_smtp_class.return_value.__enter__.return_value
            review_smtp.send_message.assert_called_once()
            self.assertIn(
                "ana@example.com",
                set(review_smtp.send_message.call_args.kwargs["to_addrs"]),
            )
        self.assertEqual(response.status_code, 302)
        response = finance.get(response.headers["Location"])
        self.assertIn(
            "Correo de autorización enviado a ana@example.com".encode("utf-8"),
            response.data,
        )
        self.assertIn("Reenviar correo al proveedor".encode("utf-8"), response.data)

        primary_connection = MagicMock()
        primary_connection.__enter__.return_value.send_message.side_effect = RuntimeError(
            "SMTP principal no disponible"
        )
        backup_connection = MagicMock()
        with patch(
            "portal_facturas_routes.smtplib.SMTP",
            side_effect=[primary_connection, backup_connection],
        ) as resend_smtp_class:
            response = finance.post(
                f"/portal-facturas/finanzas/proveedores/{provider_id}/revision/correo",
                data={"csrf_token": _csrf(response)},
            )
            self.assertEqual(resend_smtp_class.call_count, 2)
            resend_smtp = backup_connection.__enter__.return_value
            resend_smtp.send_message.assert_called_once()
            self.assertEqual(
                ["ana@example.com"],
                resend_smtp.send_message.call_args.kwargs["to_addrs"],
            )
        self.assertEqual(response.status_code, 302)

        with app.app_context():
            proveedor_registrado = db.session.get(PortalProveedorUsuario, provider_id)
            self.assertEqual(proveedor_registrado.estatus, "ACTIVO")
            self.assertEqual(proveedor_registrado.revisado_por.nombre, "admin")
            review_messenger = MessengerNotificationOutbox.query.filter(
                MessengerNotificationOutbox.source_key.like("portal:proveedor:%:revision:%")
            ).all()
            self.assertIn(
                "mescalera@poliutech.com",
                {notice.correo for notice in review_messenger},
            )
            self.assertIn(
                "ana@example.com",
                {notice.correo for notice in review_messenger},
            )

        with open(os.environ["PROVIDER_NUMBERS_JSON_PATH"], encoding="utf-8") as registry_file:
            provider_rows = json.load(registry_file)
        portal_row = next(row for row in provider_rows if row.get("numero") == "AAA010101AAA")
        self.assertEqual(portal_row["empresa"], "Proveedor Prueba SA de CV")
        self.assertEqual(portal_row["relacion"], "PROVEEDOR")
        self.assertEqual(portal_row["contacto"], "Ana Proveedor")
        self.assertEqual(portal_row["correo"], "ana@example.com")

        response = provider.get("/portal-facturas/ingresar")
        response = provider.post(
            "/portal-facturas/ingresar",
            data={
                "csrf_token": _csrf(response),
                "correo": "ana@example.com",
                "password": "Segura1234",
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/mis-facturas", response.headers["Location"])

        self.assertEqual(ESTATUS["RECIBIDA"]["class"], "status-amber")
        self.assertEqual(ESTATUS["CORRECCION_SOLICITADA"]["class"], "status-amber")
        self.assertEqual(ESTATUS["APROBADA"]["class"], "status-blue")
        self.assertEqual(ESTATUS["PROGRAMADA"]["class"], "status-blue")
        self.assertEqual(ESTATUS["RECHAZADA"]["class"], "status-red")
        self.assertEqual(ESTATUS["PAGADA"]["class"], "status-green")

        response = finance.get(
            f"/portal-facturas/finanzas/ordenes-compra/nueva?proveedor_id={provider_id}"
        )
        self.assertEqual(response.status_code, 200)
        response = finance.post(
            "/portal-facturas/finanzas/ordenes-compra/nueva",
            data={
                "csrf_token": _csrf(response),
                "proveedor_id": str(provider_id),
                "fecha_entrega": "2026-10-20",
                "forma_pago": "CREDITO",
                "descuento_total": "0",
                "iva_porc": "16",
                "condiciones": "Crédito a 30 días. Entrega en almacén.",
                "notas": "Presentar esta orden al entregar.",
                "descripcion[]": ["Material de reforzamiento", "Servicio de instalación"],
                "unidad[]": ["lote", "servicio"],
                "cantidad[]": ["1", "1"],
                "precio_unitario[]": ["800", "200"],
                "observaciones[]": ["Según especificación", "Incluye herramienta"],
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertRegex(response.headers["Location"], r"/ordenes-compra/\d+$")
        with app.app_context():
            purchase_order = OrdenCompra.query.one()
            order_id = purchase_order.id
            order_folio = purchase_order.folio
            self.assertEqual(purchase_order.portal_proveedor_usuario_id, provider_id)
            self.assertEqual(purchase_order.estatus, "BORRADOR")
            self.assertEqual(purchase_order.total, 1160.0)
            self.assertEqual(len(purchase_order.partidas), 2)

        response = finance.get("/portal-facturas/finanzas/ordenes-compra")
        self.assertEqual(response.status_code, 200)
        self.assertIn(order_folio.encode(), response.data)

        response = provider.get("/portal-facturas/ordenes-compra")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(order_folio.encode(), response.data)

        response = finance.get(f"/portal-facturas/finanzas/ordenes-compra/{order_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(order_folio.encode(), response.data)
        with patch("portal_facturas_routes.smtplib.SMTP") as purchase_order_smtp_class:
            response = finance.post(
                f"/portal-facturas/finanzas/ordenes-compra/{order_id}/enviar",
                data={"csrf_token": _csrf(response)},
            )
            purchase_order_smtp = purchase_order_smtp_class.return_value.__enter__.return_value
            purchase_order_smtp.send_message.assert_called_once()
            order_recipients = set(
                purchase_order_smtp.send_message.call_args.kwargs["to_addrs"]
            )
            self.assertIn("ana@example.com", order_recipients)
            self.assertIn("finanzas@example.com", order_recipients)
            self.assertIn("mescalera@poliutech.com", order_recipients)
        self.assertEqual(response.status_code, 302)
        with app.app_context():
            purchase_order = db.session.get(OrdenCompra, order_id)
            self.assertEqual(purchase_order.estatus, "ENVIADA")
            self.assertIsNotNone(purchase_order.enviada_en)
            order_notices = InAppNotification.query.filter_by(
                tipo="portal_ordenes_compra"
            ).all()
            self.assertIn(
                "mescalera@poliutech.com",
                {notice.usuario.correo for notice in order_notices},
            )
            order_messenger = MessengerNotificationOutbox.query.filter(
                MessengerNotificationOutbox.source_key.like("portal:orden-compra:%")
            ).all()
            self.assertIn(
                "mescalera@poliutech.com",
                {notice.correo for notice in order_messenger},
            )

        response = provider.get("/portal-facturas/ordenes-compra")
        self.assertEqual(response.status_code, 200)
        self.assertIn(order_folio.encode(), response.data)
        response = provider.get(f"/portal-facturas/ordenes-compra/{order_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Material de reforzamiento", response.data)

        with app.app_context():
            unrelated_provider = PortalProveedorUsuario(
                razon_social="Proveedor Ajeno SA de CV",
                rfc="BBB010101BBB",
                contacto="Otro Proveedor",
                correo="otro-proveedor@example.com",
                estatus="ACTIVO",
            )
            unrelated_provider.set_password("Ajena1234")
            db.session.add(unrelated_provider)
            db.session.commit()
            unrelated_provider_id = unrelated_provider.id
        unrelated_client = app.test_client()
        with unrelated_client.session_transaction() as unrelated_session:
            unrelated_session["portal_facturas_usuario_id"] = unrelated_provider_id
            unrelated_session["portal_facturas_csrf"] = "test-csrf"
        response = unrelated_client.get(
            f"/portal-facturas/ordenes-compra/{order_id}"
        )
        self.assertEqual(response.status_code, 404)

        response = provider.get(f"/portal-facturas/facturas/nueva?orden_id={order_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(order_folio.encode(), response.data)

        response = provider.get("/portal-facturas/facturas/nueva")
        with patch("portal_facturas_routes.smtplib.SMTP") as smtp_class:
            response = provider.post(
                "/portal-facturas/facturas/nueva",
                data={
                    "csrf_token": _csrf(response),
                    "orden_compra": order_folio,
                    "concepto": "Material de reforzamiento",
                    "xml": (io.BytesIO(xml), "factura.xml"),
                    "pdf": (io.BytesIO(pdf), "factura.pdf"),
                },
                content_type="multipart/form-data",
            )
            smtp = smtp_class.return_value.__enter__.return_value
            self.assertEqual(smtp.send_message.call_count, 2)
            self.assertEqual(
                smtp.send_message.call_args_list[0].kwargs["to_addrs"],
                ["mescalera@poliutech.com", "umorales@poliutech.com"],
            )
            self.assertEqual(
                smtp.send_message.call_args_list[1].kwargs["to_addrs"],
                ["ana@example.com"],
            )
        self.assertEqual(response.status_code, 302)
        self.assertRegex(response.headers["Location"], r"/facturas/\d+$")

        with app.app_context():
            factura = FacturaProveedor.query.one()
            invoice_id = factura.id
            xml_path = os.path.join(_upload_dir.name, *factura.xml_path.split("/"))
            pdf_path = os.path.join(_upload_dir.name, *factura.pdf_path.split("/"))
            self.assertEqual(factura.estatus, "RECIBIDA")
            self.assertEqual(factura.total, 1160.0)
            self.assertEqual(factura.orden_compra, order_folio)
            purchase_order = db.session.get(OrdenCompra, order_id)
            self.assertEqual(purchase_order.estatus, "FACTURADA")
            self.assertEqual(purchase_order.factura_folio, factura.folio_recepcion)
            self.assertEqual(purchase_order.factura_monto, 1160.0)
            notices = InAppNotification.query.filter_by(tipo="portal_facturas").all()
            self.assertEqual(len(notices), 2)
            self.assertEqual(
                {notice.usuario.correo for notice in notices},
                {"mescalera@poliutech.com", "umorales@poliutech.com"},
            )
            self.assertTrue(all(notice.destino_url.endswith("/finanzas/1") for notice in notices))
            invoice_messenger = MessengerNotificationOutbox.query.filter(
                MessengerNotificationOutbox.source_key.like("portal:factura:%")
            ).all()
            self.assertEqual(
                {notice.correo for notice in invoice_messenger},
                {
                    "ana@example.com",
                    "mescalera@poliutech.com",
                    "umorales@poliutech.com",
                },
            )

        response = marco_finance.get(f"/portal-facturas/finanzas/{invoice_id}")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Eliminar factura definitivamente", response.data)
        response = marco_finance.post(
            f"/portal-facturas/finanzas/{invoice_id}/eliminar",
            data={"csrf_token": _csrf(response), "confirmacion": "ELIMINAR"},
        )
        self.assertEqual(response.status_code, 403)
        with app.app_context():
            self.assertIsNotNone(db.session.get(FacturaProveedor, invoice_id))

        other_admin_finance = app.test_client()
        response = other_admin_finance.post(
            "/login",
            data={"nombre": "otro_admin_portal", "password": "OtroAdminTest123"},
        )
        self.assertEqual(response.status_code, 302)
        response = other_admin_finance.get(f"/portal-facturas/finanzas/{invoice_id}")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn(b"Eliminar factura definitivamente", response.data)
        response = other_admin_finance.post(
            f"/portal-facturas/finanzas/{invoice_id}/eliminar",
            data={"csrf_token": _csrf(response), "confirmacion": "ELIMINAR"},
        )
        self.assertEqual(response.status_code, 403)
        with app.app_context():
            self.assertIsNotNone(db.session.get(FacturaProveedor, invoice_id))

        response = finance.get("/portal-facturas/finanzas")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"PF-2026", response.data)

        response = finance.get("/portal-facturas/finanzas/proveedores")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Proveedor Prueba SA de CV".encode("utf-8"), response.data)
        response = finance.get(f"/portal-facturas/finanzas/proveedores/{provider_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Constancia de Situación Fiscal".encode("utf-8"), response.data)
        self.assertIn("Crear orden de compra".encode("utf-8"), response.data)
        response = finance.get(
            f"/portal-facturas/finanzas/proveedores/{provider_id}/documento/csf"
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data.startswith(b"%PDF-"))
        response.close()
        response = finance.get(
            f"/portal-facturas/finanzas/proveedores/{provider_id}/documento/caratula-bancaria"
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.data.startswith(b"\xff\xd8\xff"))
        response.close()

        response = finance.get(f"/portal-facturas/finanzas/{invoice_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Eliminar factura definitivamente", response.data)
        with patch("portal_facturas_routes.smtplib.SMTP") as status_smtp_class:
            response = finance.post(
                f"/portal-facturas/finanzas/{invoice_id}/estatus",
                data={
                    "csrf_token": _csrf(response),
                    "estatus": "PAGADA",
                    "comentario": "Pago confirmado.",
                    "fecha_pago": "2026-10-02",
                    "referencia_pago": "SPEI-7788",
                },
            )
            status_smtp = status_smtp_class.return_value.__enter__.return_value
            self.assertEqual(status_smtp.send_message.call_count, 2)
            self.assertEqual(
                ["ana@example.com"],
                status_smtp.send_message.call_args_list[0].kwargs["to_addrs"],
            )
            status_recipients = set(
                status_smtp.send_message.call_args_list[1].kwargs["to_addrs"]
            )
            self.assertIn("finanzas@example.com", status_recipients)
            self.assertIn("mescalera@poliutech.com", status_recipients)
            status_message = status_smtp.send_message.call_args_list[0].args[0]
            self.assertIn("Pagada", status_message["Subject"])
            self.assertIn("SPEI-7788", status_message.get_body(preferencelist=("plain",)).get_content())
        self.assertEqual(response.status_code, 302)

        with app.app_context():
            factura = db.session.get(FacturaProveedor, invoice_id)
            self.assertEqual(factura.estatus, "PAGADA")
            self.assertEqual(factura.referencia_pago, "SPEI-7788")
            purchase_order = db.session.get(OrdenCompra, order_id)
            self.assertEqual(purchase_order.estatus, "PAGADA")
            self.assertEqual(purchase_order.pago_referencia, "SPEI-7788")
            status_notices = InAppNotification.query.filter_by(
                tipo="portal_facturas_estatus"
            ).all()
            self.assertIn(
                "finanzas@example.com",
                {notice.usuario.correo for notice in status_notices},
            )
            status_messenger = MessengerNotificationOutbox.query.filter(
                MessengerNotificationOutbox.source_key.like(
                    f"portal:factura:{invoice_id}:estatus:%"
                )
            ).all()
            self.assertIn(
                "ana@example.com",
                {notice.correo for notice in status_messenger},
            )

        response = provider.get(f"/portal-facturas/facturas/{invoice_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"SPEI-7788", response.data)

        response = finance.get(f"/portal-facturas/finanzas/{invoice_id}")
        self.assertIn("Reenviar notificación al proveedor".encode("utf-8"), response.data)
        with patch("portal_facturas_routes.smtplib.SMTP") as resend_status_smtp_class:
            response = finance.post(
                f"/portal-facturas/finanzas/{invoice_id}/estatus/notificar",
                data={"csrf_token": _csrf(response)},
            )
            resend_status_smtp = (
                resend_status_smtp_class.return_value.__enter__.return_value
            )
            resend_status_smtp.send_message.assert_called_once()
            self.assertEqual(
                ["ana@example.com"],
                resend_status_smtp.send_message.call_args.kwargs["to_addrs"],
            )
        self.assertEqual(response.status_code, 302)

        response = finance.get(f"/portal-facturas/finanzas/{invoice_id}")
        response = finance.post(
            f"/portal-facturas/finanzas/{invoice_id}/eliminar",
            data={"csrf_token": _csrf(response), "confirmacion": "NO"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn(f"/finanzas/{invoice_id}", response.headers["Location"])
        with app.app_context():
            self.assertIsNotNone(db.session.get(FacturaProveedor, invoice_id))

        response = finance.get(f"/portal-facturas/finanzas/{invoice_id}")
        response = finance.post(
            f"/portal-facturas/finanzas/{invoice_id}/eliminar",
            data={"csrf_token": _csrf(response), "confirmacion": "ELIMINAR"},
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/portal-facturas/finanzas"))
        with app.app_context():
            self.assertIsNone(db.session.get(FacturaProveedor, invoice_id))
            purchase_order = db.session.get(OrdenCompra, order_id)
            self.assertEqual(purchase_order.estatus, "ENVIADA")
            self.assertIsNone(purchase_order.factura_folio)
            self.assertEqual(purchase_order.factura_monto, 0.0)
        self.assertFalse(os.path.exists(xml_path))
        self.assertFalse(os.path.exists(pdf_path))


if __name__ == "__main__":
    unittest.main()

