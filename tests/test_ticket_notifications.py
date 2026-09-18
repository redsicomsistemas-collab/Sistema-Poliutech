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
        "soporte_tickets.html",
        "soporte_ticket_detalle.html",
    ):
        environment.get_template(template_name)
