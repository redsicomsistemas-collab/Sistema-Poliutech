from __future__ import annotations

import math
import textwrap
from pathlib import Path


OUT = Path("Presentacion_MAR_Poliutech.pdf")
W, H = 842, 595  # A4 landscape in points
LOGO = Path("static/logo.jpg")
NAVY = (8, 31, 61)
DARK = (9, 45, 58)
BLUE = (23, 85, 166)
BLUE_2 = (46, 123, 205)
SKY = (226, 240, 253)
PALE = (246, 250, 254)
LINE = (183, 205, 228)
GREEN = (13, 121, 119)
ORANGE = (242, 125, 32)


def pdf_text(value: str) -> str:
    value = value.replace("\u2013", "-").replace("\u2014", "-").replace("\u201c", '"').replace("\u201d", '"')
    raw = value.encode("latin-1", "replace")
    return raw.decode("latin-1").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


class SimplePDF:
    def __init__(self):
        self.pages: list[str] = []
        self.logo_bytes: bytes | None = None
        self.logo_size: tuple[int, int] | None = None
        if LOGO.exists():
            self.logo_bytes = LOGO.read_bytes()
            self.logo_size = jpeg_size(self.logo_bytes)

    def add_page(self, content: str):
        self.pages.append(content)

    def rect(self, x, y, w, h, stroke=(13, 118, 110), fill=None, width=1):
        ops = [f"{width} w"]
        if fill:
            ops.append(rgb(*fill))
            ops.append(f"{x:.2f} {y:.2f} {w:.2f} {h:.2f} re f")
        ops.append(rgb(*stroke))
        ops.append(f"{x:.2f} {y:.2f} {w:.2f} {h:.2f} re S")
        return "\n".join(ops) + "\n"

    def line(self, x1, y1, x2, y2, color=(20, 58, 87), width=1.5, arrow=False):
        s = f"{width} w\n{rgb(*color)}\n{x1:.2f} {y1:.2f} m {x2:.2f} {y2:.2f} l S\n"
        if arrow:
            angle = math.atan2(y2 - y1, x2 - x1)
            size = 8
            a1 = angle + math.pi * 0.82
            a2 = angle - math.pi * 0.82
            p1 = (x2 + math.cos(a1) * size, y2 + math.sin(a1) * size)
            p2 = (x2 + math.cos(a2) * size, y2 + math.sin(a2) * size)
            s += f"{rgb(*color)}\n{x2:.2f} {y2:.2f} m {p1[0]:.2f} {p1[1]:.2f} l {p2[0]:.2f} {p2[1]:.2f} l f\n"
        return s

    def text(self, x, y, value, size=18, color=(23, 32, 51), bold=False, align="left"):
        font = "F2" if bold else "F1"
        width = approx_width(value, size)
        if align == "center":
            x -= width / 2
        if align == "right":
            x -= width
        return f"BT\n{rgb(*color)}\n/{font} {size:.2f} Tf\n{x:.2f} {y:.2f} Td\n({pdf_text(value)}) Tj\nET\n"

    def image(self, x, y, w, h):
        if not self.logo_bytes:
            return ""
        return f"q\n{w:.2f} 0 0 {h:.2f} {x:.2f} {y:.2f} cm\n/Im1 Do\nQ\n"

    def save(self, path: Path):
        objects: list[bytes] = []
        has_logo = bool(self.logo_bytes and self.logo_size)
        first_page_obj = 4 if has_logo else 3
        objects.append(b"<< /Type /Catalog /Pages 2 0 R >>")
        kids = " ".join(f"{first_page_obj + i * 2} 0 R" for i in range(len(self.pages)))
        objects.append(f"<< /Type /Pages /Count {len(self.pages)} /Kids [{kids}] >>".encode("latin-1"))
        if has_logo:
            iw, ih = self.logo_size or (1, 1)
            data = self.logo_bytes or b""
            objects.append(
                f"<< /Type /XObject /Subtype /Image /Width {iw} /Height {ih} /ColorSpace /DeviceRGB "
                f"/BitsPerComponent 8 /Filter /DCTDecode /Length {len(data)} >>\nstream\n".encode("latin-1")
                + data
                + b"\nendstream"
            )
        for idx, stream in enumerate(self.pages):
            page_id = first_page_obj + idx * 2
            stream_id = page_id + 1
            xobject = f"/XObject << /Im1 3 0 R >> " if has_logo else ""
            page = (
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {W} {H}] "
                f"/Resources << /Font << /F1 << /Type /Font /Subtype /Type1 /BaseFont /Helvetica /Encoding /WinAnsiEncoding >> "
                f"/F2 << /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold /Encoding /WinAnsiEncoding >> >> {xobject}>> "
                f"/Contents {stream_id} 0 R >>"
            )
            data = stream.encode("latin-1", "replace")
            objects.append(page.encode("latin-1"))
            objects.append(f"<< /Length {len(data)} >>\nstream\n".encode("latin-1") + data + b"\nendstream")

        output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
        offsets = [0]
        for i, obj in enumerate(objects, start=1):
            offsets.append(len(output))
            output.extend(f"{i} 0 obj\n".encode("latin-1"))
            output.extend(obj)
            output.extend(b"\nendobj\n")
        xref = len(output)
        output.extend(f"xref\n0 {len(objects)+1}\n0000000000 65535 f \n".encode("latin-1"))
        for off in offsets[1:]:
            output.extend(f"{off:010d} 00000 n \n".encode("latin-1"))
        output.extend(
            f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode("latin-1")
        )
        path.write_bytes(output)


def rgb(r, g, b):
    return f"{r/255:.4f} {g/255:.4f} {b/255:.4f} rg {r/255:.4f} {g/255:.4f} {b/255:.4f} RG"


def jpeg_size(data: bytes) -> tuple[int, int] | None:
    i = 2
    while i < len(data) - 9:
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        i += 2
        if marker in (0xD8, 0xD9):
            continue
        length = int.from_bytes(data[i:i + 2], "big")
        if marker in {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}:
            h = int.from_bytes(data[i + 3:i + 5], "big")
            w = int.from_bytes(data[i + 5:i + 7], "big")
            return w, h
        i += length
    return None


def approx_width(text: str, size: float) -> float:
    return len(text) * size * 0.47


def wrap(text: str, size: int, max_width: int):
    chars = max(28, int(max_width / (size * 0.47)))
    return textwrap.wrap(text, width=chars, break_long_words=False)


def base_page(pdf: SimplePDF, title: str, subtitle: str | None = None):
    c = ""
    c += f"{rgb(*PALE)}\n0 0 {W} {H} re f\n"
    c += f"{rgb(255,255,255)}\n34 34 {W-68} {H-68} re f\n"
    c += f"{rgb(*NAVY)}\n0 0 {W} 28 re f\n"
    c += f"{rgb(*BLUE)}\n34 {H-99} 5 58 re f\n"
    c += f"{rgb(*BLUE)}\n252 {H-74} {W-420} 1.5 re f\n"
    c += f"{rgb(*ORANGE)}\n34 {H-108} 100 3 re f\n"
    c += pdf.image(55, H - 83, 82, 70)
    c += pdf.text(154, H - 59, title, 26, NAVY, True)
    c += pdf.text(W - 55, 10, "Sistema MAR - Poliutech Recubrimientos Especializados", 8.5, (255, 255, 255), False, "right")
    y = H - 126
    if subtitle:
        for line in wrap(subtitle, 15, 690):
            c += pdf.text(60, y, line, 15, (42, 55, 74))
            y -= 24
    return c, y - 6


def add_bullets(pdf: SimplePDF, c: str, items: list[str], x: int, y: int, width: int, size=15, gap=8):
    for item in items:
        lines = wrap(item, size, width - 25)
        c += pdf.text(x, y, "-", size, BLUE, True)
        c += pdf.text(x + 18, y, lines[0], size, (23, 32, 51))
        y -= size + 6
        for line in lines[1:]:
            c += pdf.text(x + 18, y, line, size, (23, 32, 51))
            y -= size + 6
        y -= gap
    return c, y


def add_numbered(pdf: SimplePDF, c: str, items: list[str], x: int, y: int, width: int, size=15):
    for idx, item in enumerate(items, start=1):
        lines = wrap(item, size, width - 35)
        c += pdf.text(x, y, f"{idx}.", size, BLUE, True)
        c += pdf.text(x + 28, y, lines[0], size, (23, 32, 51))
        y -= size + 6
        for line in lines[1:]:
            c += pdf.text(x + 28, y, line, size, (23, 32, 51))
            y -= size + 6
        y -= 5
    return c, y


def add_note(pdf: SimplePDF, c: str, text: str, y: int):
    c += pdf.rect(60, y - 36, 720, 48, stroke=SKY, fill=SKY, width=0)
    c += f"{rgb(*BLUE)}\n60 {y-36:.2f} 6 48 re f\n"
    c += pdf.text(78, y - 7, text, 14, NAVY, True)
    return c


def add_simple_slide(pdf: SimplePDF, title: str, subtitle: str, left: list[str], right: list[str] | None = None, note: str | None = None):
    c, y = base_page(pdf, title, subtitle)
    if right is None:
        c, y2 = add_bullets(pdf, c, left, 82, y, 660, 15)
    else:
        c, _ = add_bullets(pdf, c, left, 82, y, 330, 15)
        c, _ = add_bullets(pdf, c, right, 455, y, 330, 15)
        y2 = 125
    if note:
        c = add_note(pdf, c, note, max(75, y2))
    pdf.add_page(c)


def add_box_grid(pdf: SimplePDF, title: str, subtitle: str, boxes: list[tuple[str, str]]):
    c, y = base_page(pdf, title, subtitle)
    x0, y0 = 60, y
    bw, bh, gx, gy = 225, 86, 20, 22
    for i, (head, body) in enumerate(boxes):
        col, row = i % 3, i // 3
        x = x0 + col * (bw + gx)
        yy = y0 - row * (bh + gy)
        c += pdf.rect(x, yy - bh, bw, bh, stroke=LINE, fill=(255, 255, 255), width=.8)
        accent = [BLUE, BLUE_2, GREEN, ORANGE][i % 4]
        c += f"{rgb(*accent)}\n{x+12:.2f} {yy-22:.2f} 24 24 re f\n"
        c += pdf.text(x + 46, yy - 23, head, 13, NAVY, True)
        ty = yy - 49
        for line in wrap(body, 11, bw - 30)[:3]:
            c += pdf.text(x + 16, ty, line, 11, (23, 32, 51))
            ty -= 15
    pdf.add_page(c)


def add_cover(pdf: SimplePDF):
    c = f"{rgb(*PALE)}\n0 0 {W} {H} re f\n"
    c += f"{rgb(255,255,255)}\n34 34 {W-68} {H-68} re f\n"
    c += f"{rgb(*NAVY)}\n0 0 {W} 34 re f\n"
    c += f"{rgb(*SKY)}\n0 {H-126} {W} 126 re f\n"
    c += f"{rgb(*BLUE)}\n58 166 6 280 re f\n"
    c += f"{rgb(*ORANGE)}\n66 166 3 155 re f\n"
    c += f"{rgb(*NAVY)}\n600 34 208 527 re f\n"
    c += f"{rgb(*BLUE)}\n600 382 208 88 re f\n"
    c += f"{rgb(*BLUE_2)}\n600 470 208 91 re f\n"
    c += f"{rgb(*GREEN)}\n600 322 104 60 re f\n"
    c += f"{rgb(*ORANGE)}\n704 322 104 60 re f\n"
    c += pdf.image(72, 462, 122, 104)
    c += pdf.text(72, 365, "SISTEMA", 24, NAVY, True)
    c += pdf.text(72, 318, "MAR", 52, BLUE, True)
    c += pdf.text(72, 278, "POLIUTECH", 32, NAVY, True)
    c += pdf.text(74, 230, "Plataforma integral para gestion comercial,", 18, (42, 55, 74))
    c += pdf.text(74, 204, "operativa y administrativa.", 18, (42, 55, 74))
    c += pdf.text(74, 145, "Presentacion ejecutiva del sistema", 15, BLUE, True)
    c += pdf.text(622, 110, "RECUBRIMIENTOS", 17, (255, 255, 255), True)
    c += pdf.text(622, 84, "ESPECIALIZADOS", 17, (255, 255, 255), True)
    c += pdf.text(W - 58, 12, "Sistema MAR - Poliutech Recubrimientos Especializados", 9, (255, 255, 255), False, "right")
    pdf.add_page(c)


def add_flow_diagram(pdf: SimplePDF):
    c, y = base_page(pdf, "Diagrama De Flujo Del Funcionamiento", "El sistema concentra la informacion en una base central y distribuye valor hacia ventas, campo, compras, finanzas, inventario y direccion.")

    def node(x, y, w, h, title, sub="", fill=(248, 251, 253)):
        nonlocal c
        c += pdf.rect(x, y, w, h, stroke=BLUE, fill=fill, width=1.5)
        color = (255, 255, 255) if fill == BLUE else (23, 32, 51)
        c += pdf.text(x + w / 2, y + h - 28, title, 13, color, True, "center")
        if sub:
            c += pdf.text(x + w / 2, y + 18, sub, 10, (78, 91, 104) if fill != BLUE else (235, 250, 247), False, "center")

    top_y = 405
    node(60, top_y, 150, 58, "Prospecto / Cliente", "Datos de contacto")
    node(270, top_y, 150, 58, "Cotizador", "Conceptos y totales")
    node(480, top_y, 150, 58, "Documento", "PDF / Excel / CSV")
    node(690, top_y, 150, 58, "Seguimiento", "Comentarios y estatus")
    c += pdf.line(210, top_y + 29, 270, top_y + 29, arrow=True)
    c += pdf.line(420, top_y + 29, 480, top_y + 29, arrow=True)
    c += pdf.line(630, top_y + 29, 690, top_y + 29, arrow=True)

    node(315, 250, 220, 78, "Base Central MAR", "Clientes, cotizaciones, obras, finanzas", BLUE)
    c += pdf.line(345, top_y, 360, 328, arrow=True)
    c += pdf.line(555, top_y, 500, 328, arrow=True)
    c += pdf.line(765, top_y, 535, 295, arrow=True)

    node(60, 100, 155, 62, "Registro de Obras", "Campo y seguimiento")
    node(255, 100, 155, 62, "Compras / Inventario", "Ordenes, stock, kardex")
    node(450, 100, 155, 62, "Finanzas", "Saldos, pagos, vencimientos")
    node(645, 100, 155, 62, "Dashboard", "Indicadores y reportes")
    node(660, 240, 145, 62, "App Movil", "Consulta y captura")

    c += pdf.line(355, 250, 150, 162, arrow=True)
    c += pdf.line(395, 250, 330, 162, arrow=True)
    c += pdf.line(475, 250, 525, 162, arrow=True)
    c += pdf.line(535, 290, 715, 162, arrow=True)
    c += pdf.line(660, 270, 535, 282, arrow=True)
    c += pdf.line(535, 300, 660, 285, arrow=True)

    c = add_note(pdf, c, "Flujo: oportunidad -> cotizacion -> documento -> seguimiento -> control operativo y decision.", 60)
    pdf.add_page(c)


def build():
    pdf = SimplePDF()
    add_cover(pdf)
    add_simple_slide(
        pdf,
        "1. Problema Detectado",
        "Antes del sistema, parte de la informacion podia quedar dispersa entre archivos, mensajes, hojas de calculo y registros manuales.",
        ["Informacion duplicada o dificil de rastrear.", "Seguimiento comercial dependiente de mensajes o memoria.", "Cotizaciones sin visibilidad clara de avance.", "Datos de clientes y obras en fuentes separadas."],
        ["Menor control sobre pendientes y responsables.", "Dificultad para consultar informacion desde campo.", "Reportes manuales y poco inmediatos.", "Riesgo de perder historial operativo."],
        "El reto principal era transformar informacion dispersa en control, seguimiento y capacidad de decision.",
    )
    add_box_grid(pdf, "2. Objetivo Del Sistema", "MAR centraliza, ordena y automatiza procesos clave de Poliutech para mejorar la operacion diaria.", [
        ("Centralizar", "Reunir clientes, cotizaciones, prospectos, obras, finanzas e inventario en un solo entorno."),
        ("Dar Seguimiento", "Registrar historial, comentarios, estatus y responsables en cada proceso."),
        ("Agilizar", "Reducir captura repetitiva y generar documentos profesionales en PDF y Excel."),
        ("Controlar", "Ordenar la informacion por modulo, usuario, proyecto, cliente y estado."),
        ("Movilizar", "Permitir consulta y captura desde campo mediante app Android."),
        ("Decidir", "Dar visibilidad a direccion mediante metricas, reportes y trazabilidad."),
    ])
    add_simple_slide(
        pdf,
        "3. Descripcion General",
        "MAR / Poliutech es una plataforma web y movil que conecta las areas comercial, operativa y administrativa.",
        ["Dashboard ejecutivo", "Gestion de clientes", "Catalogo de conceptos", "Cotizador", "Seguimiento de cotizaciones", "Prospectos"],
        ["Registro de obras", "Finanzas", "Inventario", "Ordenes de compra", "Precios unitarios / APU", "App movil Android"],
        "No es solo un cotizador: es una plataforma para controlar el ciclo de trabajo de Poliutech.",
    )
    c, y = base_page(pdf, "4. Flujo General De Trabajo", "El sistema acompana el proceso desde el primer contacto hasta el control operativo.")
    c, y = add_numbered(pdf, c, [
        "Se registra un prospecto o cliente.",
        "Se genera una cotizacion con conceptos, cantidades, precios e impuestos.",
        "Se da seguimiento comercial con comentarios, fechas y responsables.",
        "Se actualiza el estatus de la oportunidad.",
        "Si avanza, se relaciona con proyecto u obra.",
        "Se generan documentos PDF, Excel o reportes internos.",
        "Se controlan compras, finanzas e inventario cuando aplica.",
        "El equipo puede consultar y actualizar informacion desde la app movil.",
    ], 82, y, 690, 14)
    c = add_note(pdf, c, "El valor esta en que la informacion acompana el proceso completo, no se queda aislada en un archivo.", 70)
    pdf.add_page(c)

    modules = [
        ("Modulo 1: Dashboard Ejecutivo", "Concentra indicadores para consultar rapidamente el estado comercial y operativo.", ["Total de cotizaciones registradas.", "Cotizaciones por estatus.", "Montos acumulados.", "Pendientes por atender.", "Filtros para analisis rapido.", "Vista ejecutiva para direccion."], "Permite saber como va la operacion sin revisar registro por registro."),
        ("Modulo 2: Clientes", "Administra la base de clientes y la relacion de cada cliente con sus cotizaciones.", ["Registro de nombre, empresa, correo, telefono, direccion y responsable.", "Relacion directa con cotizaciones generadas.", "Consulta ordenada de informacion de contacto.", "Evita depender de agendas, mensajes o archivos externos."], "Crea una base confiable para el seguimiento comercial."),
        ("Modulo 3: Catalogo De Conceptos", "Sirve como base para construir cotizaciones de manera mas rapida, uniforme y profesional.", ["Nombre del concepto.", "Unidad de medida.", "Precio unitario.", "Sistema asociado.", "Descripcion tecnica.", "Edicion, importacion y sugerencias al cotizar."], "Reduce errores y mantiene consistencia en las propuestas comerciales."),
        ("Modulo 4: Cotizador", "Modulo central para generar propuestas comerciales estructuradas.", ["Alta de cotizaciones.", "Seleccion o captura de cliente.", "Agregado de conceptos.", "Cantidades y precios unitarios.", "Subtotal, IVA, descuento y total.", "Proyecto, ciudad de trabajo, moneda, notas y condiciones comerciales.", "Generacion de folio, edicion y actualizacion posterior."], None),
        ("Modulo 5: Exportacion De Cotizaciones", "La informacion capturada puede salir del sistema como documentos utiles para clientes o administracion.", ["PDF formal de cotizacion.", "Excel para analisis interno.", "CSV para intercambio de datos.", "Reportes de seguimiento.", "Exportacion del dashboard a Excel."], "Permite presentar informacion profesional y conservar respaldo documental."),
        ("Modulo 6: Seguimiento De Cotizaciones", "Cada cotizacion conserva un historial de comentarios, fechas, responsables y cambios de estatus.", ["Comentarios de seguimiento.", "Fecha de registro y autor.", "Edicion y eliminacion controlada.", "Estatus: enviada, pendiente, en curso, terminada, finalizada, ganada o perdida.", "Historial de contacto con cliente."], "Evita perder contexto despues de enviar una propuesta."),
        ("Modulo 7: Captura Por Voz", "Integra funciones para convertir audio en informacion util para cotizaciones.", ["Transcripcion de audio en espanol.", "Identificacion de datos como cliente, empresa, telefono, ciudad y conceptos.", "Vista previa antes de registrar informacion.", "Apoyo a captura rapida desde web o movil."], "Funcion diferenciadora para usuarios que trabajan en campo o necesitan capturar rapido."),
        ("Modulo 8: Prospectos", "Controla oportunidades comerciales antes de convertirse en clientes o cotizaciones formales.", ["Titulo y descripcion.", "Contacto, telefono y correo.", "Responsable asignado.", "Seguimiento por comentarios.", "Estatus: pendiente, contactado, cotizado, finalizado o rechazado."], "Ayuda a que ninguna oportunidad comercial quede sin seguimiento."),
        ("Modulo 9: Registro De Obras", "Organiza informacion de obras detectadas, visitadas o en seguimiento.", ["Numero de registro y nombre de obra.", "Ubicacion.", "Encargado, puesto, telefono y correo.", "Responsable interno.", "Historial de seguimiento.", "Exportacion de registros."], "Conecta el trabajo de campo con la administracion central."),
        ("Modulo 10: App Movil Android", "Extiende el sistema al equipo que trabaja fuera de oficina.", ["Login contra el servidor.", "Consulta, alta y edicion de registros de obra.", "Eliminacion masiva cuando aplica.", "Consulta de cotizaciones pendientes.", "Cambio de estatus y visualizacion de PDF.", "Registro para notificaciones push."], "Permite actualizar informacion en campo sin esperar a volver a oficina."),
        ("Modulo 11: Finanzas", "Ayuda a controlar movimientos, saldos, vencimientos y pagos.", ["Movimientos financieros por categoria.", "Contraparte, concepto, proyecto y referencia.", "Monto, saldo, moneda, fecha y vencimiento.", "Dias de credito.", "Registro de abonos o pagos.", "Exportacion a Excel."], "Relaciona informacion economica con proyectos y responsables."),
        ("Modulo 12: Ordenes De Compra", "Formaliza y controla solicitudes de compra desde su creacion hasta la recepcion.", ["Creacion de ordenes de compra.", "Consulta de detalle.", "Actualizacion de informacion.", "Cambio de estatus.", "Registro de recepcion.", "Exportacion a Excel y generacion de PDF."], None),
        ("Modulo 13: Inventario", "Permite controlar productos, existencias y movimientos internos.", ["Alta y actualizacion de productos.", "Registro de entradas y salidas.", "Consulta de kardex por producto.", "Historial de movimientos.", "Exportacion a Excel."], "Aporta visibilidad sobre materiales y existencias disponibles."),
        ("Modulo 14: Precios Unitarios / APU", "Estructura analisis de costos por obra, partida e insumo.", ["Obras y partidas.", "Materiales, mano de obra y maquinaria.", "Basicos y extras.", "Costo directo.", "Sobrecostos, financiamiento y utilidad.", "Precio unitario e importe."], "Ayuda a construir precios mas justificados y controlados."),
        ("Modulo 15: Administracion De Usuarios", "Contempla control de acceso mediante usuarios y roles.", ["Creacion, edicion y eliminacion de usuarios.", "Rol administrador o representante.", "Responsables asignados por registros.", "Proteccion de rutas internas."], "Los roles ayudan a mantener orden y seguridad."),
        ("Modulo 16: Bitacora Y Trazabilidad", "Conserva informacion historica para saber que paso, cuando paso y quien lo registro.", ["Seguimientos con autor y fecha.", "Fechas de creacion y actualizacion.", "Cambios de estatus.", "Usuarios responsables.", "Historial comercial y operativo."], "Mejora la continuidad del trabajo y reduce perdida de contexto."),
        ("Modulo 17: Importacion De Cotizaciones Externas", "Integra cotizaciones generadas fuera de MAR para darles seguimiento dentro de la plataforma.", ["Validacion previa sin escribir en la base de datos.", "Importacion de cliente y conceptos.", "Registro de folio externo o generacion de folio interno.", "Integracion al flujo de seguimiento comercial."], "Recupera informacion externa y la convierte en informacion operable dentro del sistema."),
    ]
    for title, subtitle, items, note in modules:
        add_simple_slide(pdf, title, subtitle, items, None, note)

    add_box_grid(pdf, "Beneficios Para Poliutech", "La plataforma convierte informacion diaria en control operativo.", [
        ("Orden", "La informacion queda centralizada y clasificada."),
        ("Velocidad", "Las cotizaciones y reportes se generan con menos trabajo manual."),
        ("Seguimiento", "Las oportunidades no dependen de memoria o mensajes aislados."),
        ("Control", "Se identifican responsables, estatus y fechas."),
        ("Movilidad", "El equipo puede trabajar desde campo con app Android."),
        ("Decision", "Direccion cuenta con datos mas claros para evaluar avance."),
    ])
    add_simple_slide(pdf, "Impacto Operativo", "El sistema impacta varias areas y conecta informacion que antes podia estar separada.", ["Ventas: cotizacion, seguimiento y estatus.", "Administracion: reportes, usuarios y documentos.", "Campo: app movil y registro de obras.", "Direccion: dashboard e indicadores."], ["Finanzas: movimientos, saldos y pagos.", "Compras: ordenes y recepciones.", "Inventario: productos y kardex.", "Presupuestos: precios unitarios y costos."])
    add_simple_slide(pdf, "Seguridad Y Control", "MAR contempla medidas basicas para proteger y ordenar el acceso a la informacion.", ["Inicio de sesion.", "Usuarios con roles.", "Separacion entre administradores y representantes.", "Control por responsable.", "Acceso movil autenticado.", "Registro de dispositivos para notificaciones."])
    add_simple_slide(pdf, "Tecnologia Utilizada", "El sistema combina aplicacion web, app movil y servicios de integracion.", ["Backend web en Python / Flask.", "Base de datos SQL.", "Interfaz web con HTML, CSS y JavaScript.", "Generacion de PDF, Excel y CSV."], ["App Android nativa.", "API movil para comunicacion con el backend.", "Firebase para notificaciones push.", "Transcripcion de voz para captura asistida."])
    c, y = base_page(pdf, "Diferenciador Principal", "MAR une en una sola plataforma lo comercial, operativo y administrativo.")
    for line in wrap("No es solo un cotizador. No es solo una base de datos. No es solo una app movil. Es una herramienta integral para controlar el ciclo de trabajo de Poliutech.", 23, 700):
        c += pdf.text(70, y, line, 23, (18, 63, 72), True)
        y -= 34
    pdf.add_page(c)
    add_simple_slide(pdf, "Estado Actual Del Sistema", "Actualmente el sistema cuenta con una base amplia de modulos funcionales.", ["Cotizaciones", "Clientes", "Catalogos", "Seguimientos", "Prospectos", "Registro de obras"], ["Finanzas", "Inventario", "Ordenes de compra", "Precios unitarios", "Administracion", "App movil Android"], "Se presenta como una plataforma operativa, no como una idea futura.")
    add_simple_slide(pdf, "Mejoras Futuras", "Una vez centralizada la informacion, el siguiente paso es convertirla en inteligencia operativa.", ["Reportes ejecutivos mas avanzados.", "Indicadores por vendedor o responsable.", "Automatizacion de recordatorios.", "Integracion mas profunda con correo o WhatsApp.", "Firma o autorizacion digital.", "Control documental por proyecto.", "Metricas financieras comparativas.", "Paneles por periodo, cliente o area."])
    add_simple_slide(pdf, "Cierre", "MAR / Poliutech representa un paso importante hacia la digitalizacion y control integral de la empresa.", ["Ordena informacion clave.", "Mejora tiempos de respuesta.", "Fortalece el seguimiento comercial.", "Conecta oficina y campo.", "Apoya decisiones con datos.", "Prepara a la empresa para seguir creciendo."], None, "MAR no solo digitaliza procesos: convierte la informacion diaria de Poliutech en control, seguimiento y capacidad de decision.")
    add_flow_diagram(pdf)
    pdf.save(OUT)


if __name__ == "__main__":
    build()
    print(OUT.resolve())
