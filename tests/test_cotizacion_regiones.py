import os
import unittest
from unittest.mock import patch


os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["DISABLE_BACKGROUND_SCHEDULER"] = "1"

try:
    from app import (
        REGIONES_COTIZACION,
        _build_dashboard_cotizaciones_query,
        app,
        ensure_schema,
        normalize_region,
    )
    from models import Cliente, Cotizacion, CotizacionVersion, Usuario, db
    from sqlalchemy import text
    _IMPORT_ERROR = ""
except ModuleNotFoundError as exc:
    app = None
    _IMPORT_ERROR = str(exc)


@unittest.skipIf(app is None, f"Dependencias de integración no disponibles: {_IMPORT_ERROR}")
class CotizacionRegionesTest(unittest.TestCase):
    def setUp(self):
        app.config.update(TESTING=True)
        with app.app_context():
            db.drop_all()
            db.create_all()
            user = Usuario(
                nombre="regiones_test",
                nombre_visible="Regiones Test",
                correo="regiones@example.com",
                rol="ADMIN",
            )
            user.set_password("Regiones123")
            db.session.add(user)
            db.session.commit()
            self.user_id = user.id

        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["_user_id"] = str(self.user_id)
            session["_fresh"] = True

    def _create_quote(self, region="PANAMÁ"):
        payload = {
            "cliente": "Cliente regional",
            "empresa": "Empresa regional",
            "especialidad": "Construcción",
            "region": region,
            "ciudad_trabajo": "PANAMÁ",
            "moneda": "USD",
            "estatus": "0%",
            "estatus_aprobacion": "EN REVISIÓN",
            "iva_porc": "16",
            "descuento_total": "0",
            "item_nombre_concepto[]": "Servicio regional",
            "item_unidad[]": "servicio",
            "item_cantidad[]": "1",
            "item_precio[]": "100",
            "item_capitulo[]": "General",
            "item_sistema[]": "",
            "item_descripcion[]": "",
        }
        with (
            patch("app._send_quote_created_notification"),
            patch("app._send_quote_review_email_safely"),
        ):
            response = self.client.post("/cotizaciones/crear", data=payload)
        self.assertEqual(response.status_code, 200)
        with app.app_context():
            return Cotizacion.query.one().id

    def test_catalogo_y_normalizacion(self):
        self.assertEqual(REGIONES_COTIZACION, ["USA", "MÉXICO", "PANAMÁ"])
        self.assertEqual(normalize_region("mexico"), "MÉXICO")
        self.assertEqual(normalize_region("Panama"), "PANAMÁ")
        self.assertEqual(normalize_region("Estados Unidos"), "USA")
        self.assertEqual(normalize_region("Europa", default=""), "")

        response = self.client.get("/cotizador")
        self.assertEqual(response.status_code, 200)
        for region in REGIONES_COTIZACION:
            self.assertIn(region.encode("utf-8"), response.data)

    def test_crear_editar_filtrar_y_exportar_por_region(self):
        quote_id = self._create_quote()
        with app.app_context():
            quote = db.session.get(Cotizacion, quote_id)
            self.assertEqual(quote.region, "PANAMÁ")
            with patch("app.is_admin", return_value=True):
                self.assertEqual(_build_dashboard_cotizaciones_query(region="PANAMA").count(), 1)
                self.assertEqual(_build_dashboard_cotizaciones_query(region="USA").count(), 0)

        response = self.client.get("/?region=PANAMA")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"PTCH-", response.data)
        self.assertIn("PANAMÁ".encode("utf-8"), response.data)

        response = self.client.get(f"/cotizaciones/{quote_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("PANAMÁ".encode("utf-8"), response.data)

        update_payload = {
            "cliente": "Cliente regional",
            "empresa": "Empresa regional",
            "fecha": "2026-10-02T10:00",
            "especialidad": "Construcción",
            "region": "USA",
            "ciudad_trabajo": "MIAMI",
            "moneda": "USD",
            "estatus": "0%",
            "estatus_aprobacion": "EN REVISIÓN",
            "iva_porc": "16",
            "descuento_total": "0",
            "motivo_cambio": "Cambio de región solicitado por el cliente",
            "item_nombre_concepto[]": "Servicio regional",
            "item_unidad[]": "servicio",
            "item_cantidad[]": "1",
            "item_precio[]": "100",
            "item_capitulo[]": "General",
            "item_sistema[]": "",
            "item_descripcion[]": "",
        }
        with (
            patch("app._send_quote_updated_email"),
            patch("app._send_quote_updated_push"),
            patch("app.send_whatsapp_multi"),
        ):
            response = self.client.post(f"/cotizaciones/{quote_id}/actualizar", data=update_payload)
        self.assertEqual(response.status_code, 200)

        with app.app_context():
            quote = db.session.get(Cotizacion, quote_id)
            self.assertEqual(quote.region, "USA")
            versions = (
                CotizacionVersion.query.filter_by(cotizacion_id=quote_id)
                .order_by(CotizacionVersion.numero_version)
                .all()
            )
            self.assertEqual(len(versions), 2)
            self.assertIn('"region": "USA"', versions[-1].snapshot_json)

        response = self.client.get(f"/cotizaciones/{quote_id}/export.csv")
        self.assertEqual(response.status_code, 200)
        self.assertIn(b"USA", response.data)

        response = self.client.get("/cotizaciones/export/dashboard.xlsx?region=USA")
        self.assertEqual(response.status_code, 200)
        self.assertGreater(len(response.data), 1000)

    def test_region_es_obligatoria_en_alta(self):
        response = self.client.post(
            "/cotizaciones/crear",
            data={"cliente": "Sin región", "especialidad": "Construcción"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.headers["Location"].endswith("/cotizador"))
        with app.app_context():
            self.assertEqual(Cotizacion.query.count(), 0)
            self.assertEqual(Cliente.query.count(), 0)

    def test_migracion_clasifica_historicas_como_mexico(self):
        with app.app_context():
            db.session.execute(text(
                "INSERT INTO cotizacion (folio, fecha, estatus, region) "
                "VALUES ('HIST-001', CURRENT_TIMESTAMP, '0%', 'MÉXICO')"
            ))
            db.session.commit()
            db.session.execute(text("DROP INDEX IF EXISTS ix_cotizacion_region"))
            db.session.execute(text("ALTER TABLE cotizacion DROP COLUMN region"))
            db.session.commit()

            ensure_schema()

            region = db.session.execute(
                text("SELECT region FROM cotizacion WHERE folio = 'HIST-001'")
            ).scalar_one()
            self.assertEqual(region, "MÉXICO")


if __name__ == "__main__":
    unittest.main()
