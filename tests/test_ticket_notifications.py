from types import SimpleNamespace

try:
    from jinja2 import Environment, FileSystemLoader

    import app as app_module
except ModuleNotFoundError:
    # The lightweight GitHub workflow intentionally installs only the PDF test
    # dependencies. Pytest runs these application tests when the full app
    # environment is available.
    Environment = FileSystemLoader = app_module = None


def _ticket():
    return SimpleNamespace(
        id=17,
        folio="TCK-000017",
        asunto="Acceso al sistema",
        estado="EN REVISION",
        prioridad="ALTA",
        categoria="SISTEMA",
        responsable="Soporte",
        correo="solicitante@example.com",
        creado_por=SimpleNamespace(correo="creador@example.com"),
    )


def test_ticket_update_email_reaches_requester_creator_and_support(monkeypatch):
    if app_module is None:
        return
    sent = {}
    monkeypatch.setattr(
        app_module,
        "notification_targets",
        lambda event, channel: ["soporte@example.com"],
    )
    monkeypatch.setattr(
        app_module,
        "_send_smtp_message",
        lambda message, to_addrs, **kwargs: sent.update(
            {"message": message, "recipients": to_addrs}
        ),
    )

    with app_module.app.test_request_context("/", base_url="https://mar.example"):
        recipients = app_module._send_support_ticket_update_email(
            _ticket(),
            author="Administrador",
            changes=["Estado: En revisión → Resuelto"],
        )

    assert recipients == [
        "solicitante@example.com",
        "creador@example.com",
        "soporte@example.com",
    ]
    assert sent["recipients"] == recipients
    assert "TCK-000017" in sent["message"]["Subject"]


def test_internal_ticket_comment_only_reaches_support(monkeypatch):
    if app_module is None:
        return
    monkeypatch.setattr(
        app_module,
        "notification_targets",
        lambda event, channel: ["soporte@example.com"],
    )

    recipients = app_module._support_ticket_update_recipients(
        _ticket(), include_requester=False
    )

    assert recipients == ["soporte@example.com"]


def test_modified_templates_compile():
    if app_module is None:
        return
    environment = Environment(loader=FileSystemLoader("templates"))
    for template_name in (
        "dashboard.html",
        "admin_bitacora.html",
        "soporte_tickets.html",
        "soporte_ticket_detalle.html",
    ):
        environment.get_template(template_name)


def test_audit_parser_identifies_windows_chrome_computer():
    if app_module is None:
        return
    context = app_module._audit_parse_user_agent(
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"
    )
    assert context["device"] == "Computadora"
    assert context["os"] == "Windows 10/11"
    assert context["browser"] == "Google Chrome 140.0.0.0"


def test_audit_query_hides_sensitive_values():
    if app_module is None:
        return
    with app_module.app.test_request_context("/admin/bitacora?q=COT-25&token=secreto"):
        query = app_module._audit_safe_query_string()
    assert "q=COT-25" in query
    assert "token=<hidden>" in query
    assert "secreto" not in query


def test_audit_request_records_network_and_device_context():
    if app_module is None:
        return
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/140.0.0.0 Safari/537.36"
    )
    with app_module.app.test_client() as client:
        response = client.get(
            "/login?q=auditoria&token=no-guardar",
            headers={
                "User-Agent": user_agent,
                "X-Forwarded-For": "203.0.113.24, 10.0.0.5",
                "X-Device-Name": "PC-Pruebas",
            },
        )
    request_id = response.headers.get("X-Request-ID")
    assert response.status_code == 200
    assert request_id
    assert "mar_device_id=" in response.headers.get("Set-Cookie", "")

    with app_module.app.app_context():
        log = app_module.ActivityLog.query.filter_by(request_id=request_id).one()
        try:
            assert log.ip == "203.0.113.24"
            assert log.dispositivo == "PC-Pruebas · Computadora"
            assert log.sistema_operativo == "Windows 10/11"
            assert log.navegador == "Google Chrome 140.0.0.0"
            assert log.duracion_ms is not None
            assert "token=<hidden>" in (log.query_string or "")
            assert "no-guardar" not in (log.query_string or "")
        finally:
            app_module.db.session.delete(log)
            app_module.db.session.commit()
