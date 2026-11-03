import os
import unittest
from unittest.mock import patch

os.environ["DATABASE_URL"] = "sqlite:///:memory:"
os.environ["DISABLE_BACKGROUND_SCHEDULER"] = "1"

try:
    import app as app_module
    from app import app
    from models import RegistroObra, Usuario, db

    _IMPORT_ERROR = ""
except ModuleNotFoundError as exc:  # El CI ligero puede omitir dependencias web.
    app = None
    _IMPORT_ERROR = str(exc)


@unittest.skipIf(app is None, f"Dependencias de integración no disponibles: {_IMPORT_ERROR}")
class RegistroObrasTest(unittest.TestCase):
    def setUp(self):
        app.config.update(TESTING=True)
        with app.app_context():
            db.drop_all()
            db.create_all()
            user = Usuario(nombre="admin_obras", nombre_visible="Admin Obras", rol="ADMIN")
            user.set_password("Admin123")
            db.session.add(user)
            db.session.add_all(
                [
                    RegistroObra(numero=1, obra="Obra Norte", encargado="Contacto 1", responsable="Zoé Ramos"),
                    RegistroObra(numero=2, obra="Obra Centro", encargado="Contacto 2", responsable="Ana López"),
                    RegistroObra(numero=3, obra="Obra Sur", encargado="Contacto 3", responsable="Ana López"),
                ]
            )
            db.session.commit()
            self.user_id = user.id

        self.client = app.test_client()
        with self.client.session_transaction() as session:
            session["_user_id"] = str(self.user_id)
            session["_fresh"] = True

    def test_filtro_responsable_es_dropdown_con_nombres_unicos(self):
        response = self.client.get(
            "/registro-obras",
            query_string={"responsable": "Ana López"},
        )

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('<select id="filtro-responsable"', html)
        self.assertIn('<option value="Ana López" selected>Ana López</option>', html)
        self.assertEqual(html.count('<option value="Ana López"'), 1)
        self.assertIn('<option value="Zoé Ramos"', html)
        self.assertIn("Total de registros: <strong>2</strong>", html)

    def test_correo_usa_nuevo_asunto_y_portafolio(self):
        with patch("app._send_smtp_message") as send_message:
            app_module._send_registro_obra_email({"correo": "cliente@example.com"})

        message = send_message.call_args.args[0]
        attachments = list(message.iter_attachments())
        self.assertEqual(message["Subject"], "En poliutech queremos trabajar a tu lado")
        self.assertEqual(len(attachments), 1)
        self.assertEqual(attachments[0].get_filename(), "portafolio.pdf")
        self.assertEqual(attachments[0].get_payload(decode=True), app_module.REGISTRO_MAIL_ATTACHMENT.read_bytes())


if __name__ == "__main__":
    unittest.main()
