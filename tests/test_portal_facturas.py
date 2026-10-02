import io
import os
import re
import tempfile
import unittest


_upload_dir = tempfile.TemporaryDirectory(prefix="mar_portal_facturas_")
os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["UPLOAD_STORAGE_ROOT"] = _upload_dir.name
os.environ["DISABLE_BACKGROUND_SCHEDULER"] = "1"

from app import app  # noqa: E402
from models import FacturaProveedor, Usuario, db  # noqa: E402


def _csrf(response) -> str:
    match = re.search(rb'name="csrf_token" value="([^"]+)"', response.data)
    if not match:
        raise AssertionError("No se encontró el token CSRF en la respuesta.")
    return match.group(1).decode()


class PortalFacturasFlowTest(unittest.TestCase):
    def setUp(self):
        app.config.update(TESTING=True)

    def test_registro_carga_revision_y_pago(self):
        xml = b'''<?xml version="1.0" encoding="UTF-8"?>
<cfdi:Comprobante xmlns:cfdi="http://www.sat.gob.mx/cfd/4" Version="4.0" Serie="A" Folio="1001" Fecha="2026-10-02T10:15:00" SubTotal="1000.00" Total="1160.00" Moneda="MXN">
  <cfdi:Emisor Rfc="AAA010101AAA" Nombre="Proveedor Prueba SA de CV"/>
  <cfdi:Receptor Rfc="POL010101AAA" Nombre="Poliutech SA de CV"/>
  <cfdi:Complemento><tfd:TimbreFiscalDigital xmlns:tfd="http://www.sat.gob.mx/TimbreFiscalDigital" UUID="123E4567-E89B-12D3-A456-426614174000"/></cfdi:Complemento>
</cfdi:Comprobante>'''
        pdf = b"%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\n%%EOF"

        provider = app.test_client()
        response = provider.get("/portal-facturas/registro")
        self.assertEqual(response.status_code, 200)
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
            },
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn("/facturas/nueva", response.headers["Location"])

        response = provider.get("/portal-facturas/facturas/nueva")
        response = provider.post(
            "/portal-facturas/facturas/nueva",
            data={
                "csrf_token": _csrf(response),
                "orden_compra": "OC-2026-77",
                "concepto": "Material de reforzamiento",
                "xml": (io.BytesIO(xml), "factura.xml"),
                "pdf": (io.BytesIO(pdf), "factura.pdf"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(response.status_code, 302)
        self.assertRegex(response.headers["Location"], r"/facturas/\d+$")

        with app.app_context():
            factura = FacturaProveedor.query.one()
            invoice_id = factura.id
            self.assertEqual(factura.estatus, "RECIBIDA")
            self.assertEqual(factura.total, 1160.0)
            admin = Usuario(
                nombre="portal_finanzas_test",
                nombre_visible="Finanzas Prueba",
                correo="finanzas@example.com",
                telefono="5511111111",
                rol="ADMIN",
            )
            admin.set_password("AdminTest123")
            db.session.add(admin)
            db.session.commit()

        finance = app.test_client()
        response = finance.post(
            "/login",
            data={"nombre": "portal_finanzas_test", "password": "AdminTest123"},
        )
        self.assertEqual(response.status_code, 302)
        response = finance.get("/portal-facturas/finanzas")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"PF-2026", response.data)

        response = finance.get(f"/portal-facturas/finanzas/{invoice_id}")
        self.assertEqual(response.status_code, 200)
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
        self.assertEqual(response.status_code, 302)

        with app.app_context():
            factura = db.session.get(FacturaProveedor, invoice_id)
            self.assertEqual(factura.estatus, "PAGADA")
            self.assertEqual(factura.referencia_pago, "SPEI-7788")

        response = provider.get(f"/portal-facturas/facturas/{invoice_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"SPEI-7788", response.data)


if __name__ == "__main__":
    unittest.main()

