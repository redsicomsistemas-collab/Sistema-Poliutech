import os
import unittest
from datetime import datetime
from types import SimpleNamespace


os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["DISABLE_BACKGROUND_SCHEDULER"] = "1"

try:
    from app import _build_cotizacion_pdf_response, app
    from models import Usuario
    _IMPORT_ERROR = ""
except ModuleNotFoundError as exc:
    app = None
    _IMPORT_ERROR = str(exc)


@unittest.skipIf(app is None, f"Dependencias de integración no disponibles: {_IMPORT_ERROR}")
class CotizacionPdfLayoutTest(unittest.TestCase):
    def test_concepto_mas_alto_que_una_pagina_se_divide(self):
        detalle = SimpleNamespace(
            capitulo="1",
            nombre_concepto=(
                "Descripción extensa para comprobar el salto correcto entre páginas. "
                * 220
            ),
            unidad="m2",
            cantidad=1,
            sistema="Sistema",
            precio_unitario=100,
            subtotal=100,
        )
        cotizacion = SimpleNamespace(
            folio="PTCH-TEST",
            fecha=datetime(2026, 10, 21, 12, 0),
            responsable=Usuario(nombre="test", nombre_visible="Prueba PDF"),
            cliente=SimpleNamespace(
                nombre_cliente="Cliente",
                empresa="Empresa",
                correo="cliente@example.com",
                telefono="5555555555",
            ),
            ciudad_trabajo="CDMX",
            region="MÉXICO",
            moneda="MXN",
            detalles=[detalle],
            subtotal=100,
            descuento_total=0,
            iva_porc=16,
            iva_monto=16,
            total=116,
            notas="",
        )

        with app.app_context():
            response = _build_cotizacion_pdf_response(cotizacion)

        pdf = response.get_data()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, "application/pdf")
        self.assertTrue(pdf.startswith(b"%PDF-"))
        self.assertGreater(len(pdf), 1_000)


if __name__ == "__main__":
    unittest.main()
