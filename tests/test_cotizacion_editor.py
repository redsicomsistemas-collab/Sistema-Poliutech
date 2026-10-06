import os
import unittest
from unittest.mock import patch

os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["DISABLE_BACKGROUND_SCHEDULER"] = "1"

try:
    from werkzeug.datastructures import MultiDict
    from app import app
    from models import Cliente, Cotizacion, CotizacionDetalle, Usuario, db
    _IMPORT_ERROR = ""
except ModuleNotFoundError as exc:  # El CI ligero puede omitir dependencias web.
    app = None
    _IMPORT_ERROR = str(exc)


@unittest.skipIf(app is None, f"Dependencias de integración no disponibles: {_IMPORT_ERROR}")
class CotizacionEditorTest(unittest.TestCase):
    def setUp(self):
        app.config.update(TESTING=True)
        with app.app_context():
            db.drop_all()
            db.create_all()
            user = Usuario(nombre="editor_test", nombre_visible="Editor Test", rol="ADMIN")
            user.set_password("Editor123")
            client = Cliente(nombre_cliente="Cliente editor", empresa="Empresa editor")
            db.session.add_all([user, client])
            db.session.flush()
            quote = Cotizacion(
                folio="PTCH-TEST-EDITOR",
                cliente_id=client.id,
                responsable=user.nombre,
                especialidad="Construcción",
                region="MÉXICO",
                estatus="0%",
                estatus_aprobacion="EN REVISIÓN",
                moneda="MXN",
                iva_porc=16,
            )
            db.session.add(quote)
            db.session.flush()
            db.session.add_all([
                CotizacionDetalle(
                    cotizacion_id=quote.id,
                    nombre_concepto="Concepto primero",
                    unidad="m2",
                    cantidad=1,
                    precio_unitario=100,
                    capitulo="Capítulo A",
                    subtotal=100,
                ),
                CotizacionDetalle(
                    cotizacion_id=quote.id,
                    nombre_concepto="Concepto segundo",
                    unidad="ml",
                    cantidad=2,
                    precio_unitario=200,
                    capitulo="Capítulo B",
                    subtotal=400,
                ),
            ])
            db.session.commit()
            self.user_id = user.id
            self.quote_id = quote.id

        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["_user_id"] = str(self.user_id)
            session["_fresh"] = True

    def test_editor_muestra_tarjetas_y_control_de_arrastre(self):
        response = self.client.get(f"/cotizaciones/{self.quote_id}/editar")
        self.assertEqual(response.status_code, 200, dict(response.headers))
        self.assertIn(b"quote-edit-item", response.data)
        self.assertIn(b"quote-drag-handle", response.data)
        self.assertIn(b"pointerdown", response.data)
        self.assertIn(b'name="fecha"', response.data)
        self.assertIn("Arrastra cada tarjeta".encode("utf-8"), response.data)

    def test_actualizacion_persiste_el_orden_enviado_por_el_editor(self):
        payload = MultiDict([
            ("cliente", "Cliente editor"),
            ("empresa", "Empresa editor"),
            ("fecha", "2026-10-06T12:00"),
            ("especialidad", "Construcción"),
            ("region", "MÉXICO"),
            ("moneda", "MXN"),
            ("estatus", "0%"),
            ("estatus_aprobacion", "EN REVISIÓN"),
            ("iva_porc", "16"),
            ("descuento_total", "0"),
            ("motivo_cambio", "Reordenar conceptos con arrastre"),
            ("item_nombre_concepto[]", "Concepto segundo"),
            ("item_nombre_concepto[]", "Concepto primero"),
            ("item_unidad[]", "ml"),
            ("item_unidad[]", "m2"),
            ("item_cantidad[]", "2"),
            ("item_cantidad[]", "1"),
            ("item_precio[]", "200"),
            ("item_precio[]", "100"),
            ("item_capitulo[]", "Capítulo B"),
            ("item_capitulo[]", "Capítulo A"),
            ("item_sistema[]", ""),
            ("item_sistema[]", ""),
            ("item_descripcion[]", ""),
            ("item_descripcion[]", ""),
        ])
        with (
            patch("app._send_quote_updated_email"),
            patch("app._send_quote_updated_push"),
            patch("app.send_whatsapp_multi"),
        ):
            response = self.client.post(
                f"/cotizaciones/{self.quote_id}/actualizar",
                data=payload,
            )
        with self.client.session_transaction() as session:
            flashes = session.get("_flashes", [])
        self.assertEqual(response.status_code, 200, flashes)

        with app.app_context():
            quote = db.session.get(Cotizacion, self.quote_id)
            self.assertEqual(
                [detail.nombre_concepto for detail in quote.detalles],
                ["Concepto segundo", "Concepto primero"],
            )


if __name__ == "__main__":
    unittest.main()
