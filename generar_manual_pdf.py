from __future__ import annotations

import math
import textwrap
from pathlib import Path


OUT = Path("Manual_Sistema_MAR_Poliutech.pdf")
W, H = 595, 842  # A4 portrait
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
    value = (
        value.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u00a0", " ")
    )
    raw = value.encode("latin-1", "replace")
    return raw.decode("latin-1").replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


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

    def text(self, x, y, value, size=11, color=(23, 32, 51), bold=False, align="left"):
        font = "F2" if bold else "F1"
        width = approx_width(value, size)
        if align == "center":
            x -= width / 2
        if align == "right":
            x -= width
        return f"BT\n{rgb(*color)}\n/{font} {size:.2f} Tf\n{x:.2f} {y:.2f} Td\n({pdf_text(value)}) Tj\nET\n"

    def rect(self, x, y, w, h, stroke=LINE, fill=None, width=0.8):
        ops = [f"{width} w"]
        if fill:
            ops.append(rgb(*fill))
            ops.append(f"{x:.2f} {y:.2f} {w:.2f} {h:.2f} re f")
        ops.append(rgb(*stroke))
        ops.append(f"{x:.2f} {y:.2f} {w:.2f} {h:.2f} re S")
        return "\n".join(ops) + "\n"

    def line(self, x1, y1, x2, y2, color=BLUE, width=1.2, arrow=False):
        s = f"{width} w\n{rgb(*color)}\n{x1:.2f} {y1:.2f} m {x2:.2f} {y2:.2f} l S\n"
        if arrow:
            angle = math.atan2(y2 - y1, x2 - x1)
            size = 7
            a1 = angle + math.pi * 0.82
            a2 = angle - math.pi * 0.82
            p1 = (x2 + math.cos(a1) * size, y2 + math.sin(a1) * size)
            p2 = (x2 + math.cos(a2) * size, y2 + math.sin(a2) * size)
            s += f"{rgb(*color)}\n{x2:.2f} {y2:.2f} m {p1[0]:.2f} {p1[1]:.2f} l {p2[0]:.2f} {p2[1]:.2f} l f\n"
        return s

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


class Manual:
    def __init__(self):
        self.pdf = SimplePDF()
        self.page_no = 0

    def header(self, title: str):
        self.page_no += 1
        c = f"{rgb(*PALE)}\n0 0 {W} {H} re f\n"
        c += f"{rgb(255,255,255)}\n32 36 {W-64} {H-72} re f\n"
        c += f"{rgb(*NAVY)}\n0 0 {W} 28 re f\n"
        c += f"{rgb(*BLUE)}\n48 {H-88} 5 48 re f\n"
        c += f"{rgb(*ORANGE)}\n48 {H-96} 86 3 re f\n"
        c += self.pdf.image(62, H - 82, 70, 58)
        c += self.pdf.text(145, H - 62, title, 19, NAVY, True)
        c += self.pdf.text(W - 46, 10, f"Sistema MAR - Manual de Usuario | Pag. {self.page_no}", 8, (255, 255, 255), False, "right")
        return c, H - 120

    def cover(self):
        self.page_no += 1
        c = f"{rgb(*PALE)}\n0 0 {W} {H} re f\n"
        c += f"{rgb(255,255,255)}\n36 36 {W-72} {H-72} re f\n"
        c += f"{rgb(*NAVY)}\n0 0 {W} 34 re f\n"
        c += f"{rgb(*SKY)}\n0 {H-160} {W} 160 re f\n"
        c += f"{rgb(*BLUE)}\n64 165 6 415 re f\n"
        c += f"{rgb(*ORANGE)}\n73 165 3 230 re f\n"
        c += f"{rgb(*NAVY)}\n405 36 154 650 re f\n"
        c += f"{rgb(*BLUE)}\n405 472 154 96 re f\n"
        c += f"{rgb(*BLUE_2)}\n405 568 154 118 re f\n"
        c += f"{rgb(*GREEN)}\n405 400 77 72 re f\n"
        c += f"{rgb(*ORANGE)}\n482 400 77 72 re f\n"
        c += self.pdf.image(78, 668, 128, 108)
        c += self.pdf.text(78, 560, "MANUAL DE USUARIO", 24, NAVY, True)
        c += self.pdf.text(78, 508, "SISTEMA MAR", 38, BLUE, True)
        c += self.pdf.text(78, 466, "POLIUTECH", 25, NAVY, True)
        c += self.pdf.text(80, 405, "Guia operativa para uso del sistema", 15, (42, 55, 74))
        c += self.pdf.text(80, 382, "comercial, administrativo y de campo.", 15, (42, 55, 74))
        c += self.pdf.text(426, 118, "RECUBRIMIENTOS", 13, (255, 255, 255), True)
        c += self.pdf.text(426, 96, "ESPECIALIZADOS", 13, (255, 255, 255), True)
        c += self.pdf.text(W - 44, 12, "Sistema MAR - Poliutech Recubrimientos Especializados", 8, (255, 255, 255), False, "right")
        self.pdf.add_page(c)

    def paragraph(self, c: str, text: str, x: int, y: int, width: int, size=11, color=(33, 43, 58), leading=16):
        for line in wrap(text, size, width):
            c += self.pdf.text(x, y, line, size, color)
            y -= leading
        return c, y

    def bullets(self, c: str, items: list[str], x: int, y: int, width: int, size=10.5):
        for item in items:
            lines = wrap(item, size, width - 18)
            c += self.pdf.text(x, y, "-", size, BLUE, True)
            c += self.pdf.text(x + 14, y, lines[0], size, (33, 43, 58))
            y -= 15
            for line in lines[1:]:
                c += self.pdf.text(x + 14, y, line, size, (33, 43, 58))
                y -= 15
            y -= 4
        return c, y

    def steps(self, c: str, items: list[str], x: int, y: int, width: int):
        for idx, item in enumerate(items, 1):
            lines = wrap(item, 10.5, width - 28)
            c += self.pdf.text(x, y, f"{idx}.", 10.5, BLUE, True)
            c += self.pdf.text(x + 23, y, lines[0], 10.5, (33, 43, 58))
            y -= 15
            for line in lines[1:]:
                c += self.pdf.text(x + 23, y, line, 10.5, (33, 43, 58))
                y -= 15
            y -= 5
        return c, y

    def section(self, title: str, intro: str, blocks: list[tuple[str, list[str], str]]):
        c, y = self.header(title)
        c, y = self.paragraph(c, intro, 56, y, 480, 11.5, (33, 43, 58), 17)
        y -= 10
        for heading, items, mode in blocks:
            if y < 145:
                self.pdf.add_page(c)
                c, y = self.header(title)
            c += self.pdf.text(56, y, heading, 13, BLUE, True)
            c += f"{rgb(*BLUE)}\n56 {y-8:.2f} 96 1.2 re f\n"
            y -= 24
            if mode == "steps":
                c, y = self.steps(c, items, 66, y, 455)
            else:
                c, y = self.bullets(c, items, 66, y, 455)
            y -= 10
        self.pdf.add_page(c)

    def flow_diagram(self):
        c, y = self.header("Diagrama De Flujo Del Funcionamiento")
        c, y = self.paragraph(
            c,
            "El siguiente diagrama resume el funcionamiento general del sistema MAR, desde la captura de oportunidades hasta la generacion de documentos, seguimiento, control operativo y reportes.",
            56,
            y,
            480,
            11.5,
        )

        def node(x, y, w, h, title, sub="", fill=(255, 255, 255)):
            nonlocal c
            c += self.pdf.rect(x, y, w, h, stroke=BLUE, fill=fill, width=1)
            color = (255, 255, 255) if fill == BLUE else NAVY
            c += self.pdf.text(x + w / 2, y + h - 22, title, 10.5, color, True, "center")
            if sub:
                c += self.pdf.text(x + w / 2, y + 13, sub, 8, (72, 84, 100) if fill != BLUE else (232, 240, 254), False, "center")

        top = 610
        node(56, top, 110, 48, "Prospecto", "Contacto inicial")
        node(206, top, 110, 48, "Cliente", "Datos completos")
        node(356, top, 110, 48, "Cotizacion", "Conceptos y totales")
        c += self.pdf.line(166, top + 24, 206, top + 24, arrow=True)
        c += self.pdf.line(316, top + 24, 356, top + 24, arrow=True)

        node(180, 480, 190, 62, "Base Central MAR", "Informacion operativa", BLUE)
        c += self.pdf.line(410, top, 330, 542, arrow=True)

        node(56, 360, 118, 52, "PDF / Excel", "Documentos")
        node(216, 360, 118, 52, "Seguimiento", "Comentarios y estatus")
        node(376, 360, 118, 52, "Obras / Campo", "App movil")
        c += self.pdf.line(220, 480, 115, 412, arrow=True)
        c += self.pdf.line(275, 480, 275, 412, arrow=True)
        c += self.pdf.line(330, 480, 435, 412, arrow=True)

        node(56, 235, 118, 52, "Compras", "Ordenes")
        node(216, 235, 118, 52, "Inventario", "Kardex")
        node(376, 235, 118, 52, "Finanzas", "Saldos y pagos")
        c += self.pdf.line(435, 360, 435, 287, arrow=True)
        c += self.pdf.line(115, 360, 115, 287, arrow=True)
        c += self.pdf.line(275, 360, 275, 287, arrow=True)

        node(180, 115, 190, 58, "Dashboard / Reportes", "Indicadores para decision")
        c += self.pdf.line(115, 235, 180, 173, arrow=True)
        c += self.pdf.line(275, 235, 275, 173, arrow=True)
        c += self.pdf.line(435, 235, 370, 173, arrow=True)

        c += self.pdf.rect(56, 70, 480, 28, stroke=SKY, fill=SKY, width=0)
        c += f"{rgb(*BLUE)}\n56 70 5 28 re f\n"
        c += self.pdf.text(70, 80, "Flujo principal: oportunidad -> cliente -> cotizacion -> seguimiento -> operacion -> reportes.", 9.5, NAVY, True)
        self.pdf.add_page(c)

    def build(self):
        self.cover()
        self.section(
            "1. Introduccion",
            "Este manual explica el uso general del Sistema MAR de Poliutech. Su objetivo es ayudar a los usuarios a operar los modulos principales de forma ordenada, desde el registro de clientes hasta el seguimiento de cotizaciones, obras, finanzas, inventario y reportes.",
            [
                ("Objetivo del sistema", [
                    "Centralizar informacion comercial, operativa y administrativa en una sola plataforma.",
                    "Reducir registros manuales dispersos y mejorar la trazabilidad.",
                    "Facilitar la generacion de cotizaciones, reportes y documentos.",
                    "Permitir consulta y captura desde oficina y campo.",
                ], "bullets"),
                ("Usuarios principales", [
                    "Administradores: gestionan usuarios, catalogos, reportes y operacion general.",
                    "Representantes: registran clientes, cotizaciones, seguimientos, prospectos y obras asignadas.",
                    "Personal de campo: consulta y actualiza informacion mediante la app movil cuando aplica.",
                ], "bullets"),
            ],
        )
        self.section(
            "2. Acceso Al Sistema",
            "El acceso se realiza mediante usuario y contrasena. Cada usuario debe ingresar con sus propias credenciales para conservar trazabilidad de las operaciones.",
            [
                ("Inicio de sesion", [
                    "Abrir la direccion web del sistema MAR.",
                    "Capturar nombre de usuario.",
                    "Capturar contrasena.",
                    "Presionar el boton de ingreso.",
                    "Verificar que el sistema muestre el menu principal.",
                ], "steps"),
                ("Recomendaciones", [
                    "No compartir contrasenas entre usuarios.",
                    "Cerrar sesion al terminar de trabajar en equipos compartidos.",
                    "Reportar al administrador cualquier acceso incorrecto o usuario bloqueado.",
                ], "bullets"),
            ],
        )
        self.section(
            "3. Dashboard Ejecutivo",
            "El dashboard es la vista de consulta rapida del sistema. Resume informacion clave para conocer el avance comercial y operativo.",
            [
                ("Que permite consultar", [
                    "Numero de cotizaciones registradas.",
                    "Cotizaciones agrupadas por estatus.",
                    "Montos acumulados.",
                    "Pendientes y oportunidades en seguimiento.",
                    "Indicadores utiles para direccion y administracion.",
                ], "bullets"),
                ("Uso recomendado", [
                    "Revisar diariamente las cotizaciones pendientes.",
                    "Filtrar por periodo, responsable o estatus cuando aplique.",
                    "Exportar informacion cuando se requiera analisis externo.",
                ], "bullets"),
            ],
        )
        self.section(
            "4. Clientes",
            "El modulo de clientes concentra los datos de contacto y permite relacionarlos con cotizaciones y seguimientos.",
            [
                ("Alta de cliente", [
                    "Entrar al modulo de clientes o altas.",
                    "Capturar nombre del cliente.",
                    "Agregar empresa, correo, telefono y direccion cuando existan.",
                    "Asignar responsable interno.",
                    "Guardar el registro.",
                ], "steps"),
                ("Buenas practicas", [
                    "Verificar que el cliente no exista antes de duplicarlo.",
                    "Mantener correo y telefono actualizados.",
                    "Usar nombres claros para facilitar busquedas.",
                ], "bullets"),
            ],
        )
        self.section(
            "5. Catalogo De Conceptos",
            "El catalogo de conceptos permite estandarizar los servicios, unidades, precios y descripciones utilizados en las cotizaciones.",
            [
                ("Datos principales", [
                    "Nombre del concepto.",
                    "Unidad de medida.",
                    "Precio unitario.",
                    "Sistema asociado.",
                    "Descripcion tecnica o comercial.",
                ], "bullets"),
                ("Uso dentro del cotizador", [
                    "Buscar el concepto por nombre o palabra clave.",
                    "Seleccionarlo para traer unidad, precio y descripcion.",
                    "Ajustar cantidad o precio cuando el proyecto lo requiera.",
                ], "steps"),
            ],
        )
        self.section(
            "6. Cotizador",
            "El cotizador permite crear propuestas comerciales completas con cliente, conceptos, cantidades, precios, impuestos y condiciones.",
            [
                ("Crear una cotizacion", [
                    "Ingresar al modulo Cotizador.",
                    "Seleccionar o registrar el cliente.",
                    "Capturar proyecto, ciudad y responsable.",
                    "Agregar conceptos desde catalogo o manualmente.",
                    "Indicar unidad, cantidad y precio unitario.",
                    "Revisar subtotal, descuento, IVA y total.",
                    "Agregar notas o condiciones comerciales si aplica.",
                    "Guardar la cotizacion.",
                ], "steps"),
                ("Editar una cotizacion", [
                    "Abrir la cotizacion desde el listado.",
                    "Seleccionar la opcion de editar.",
                    "Modificar datos generales o conceptos.",
                    "Guardar cambios y verificar totales.",
                ], "steps"),
            ],
        )
        self.section(
            "7. Documentos Y Exportaciones",
            "El sistema genera archivos para compartir con clientes o respaldar informacion interna.",
            [
                ("Formatos disponibles", [
                    "PDF formal de cotizacion.",
                    "Excel para analisis o respaldo.",
                    "CSV para intercambio de datos.",
                    "Reportes de seguimiento en PDF.",
                    "Exportacion de dashboard y registros.",
                ], "bullets"),
                ("Recomendaciones", [
                    "Revisar datos del cliente antes de generar PDF.",
                    "Verificar totales, moneda, notas y condiciones.",
                    "Guardar el documento final en la carpeta correspondiente del proyecto.",
                ], "bullets"),
            ],
        )
        self.section(
            "8. Seguimiento De Cotizaciones",
            "El seguimiento registra el historial comercial posterior a la creacion o envio de una cotizacion.",
            [
                ("Agregar seguimiento", [
                    "Abrir la cotizacion.",
                    "Entrar a la seccion de seguimiento.",
                    "Capturar comentario claro y concreto.",
                    "Guardar el seguimiento.",
                    "Actualizar estatus si corresponde.",
                ], "steps"),
                ("Estatus comunes", [
                    "Enviada: la propuesta ya fue enviada al cliente.",
                    "Pendiente: requiere respuesta o accion.",
                    "En curso: se esta trabajando o negociando.",
                    "Ganada: el cliente acepto la propuesta.",
                    "Perdida: la oportunidad no continuo.",
                    "Finalizada u obra terminada: el proceso concluyo.",
                ], "bullets"),
            ],
        )
        self.section(
            "9. Prospectos",
            "El modulo de prospectos controla oportunidades antes de que se conviertan en clientes o cotizaciones formales.",
            [
                ("Registrar prospecto", [
                    "Ingresar a Prospectos.",
                    "Capturar titulo o nombre de oportunidad.",
                    "Agregar descripcion, contacto, telefono y correo.",
                    "Asignar responsable.",
                    "Seleccionar estatus inicial.",
                    "Guardar.",
                ], "steps"),
                ("Seguimiento", [
                    "Registrar llamadas, visitas, acuerdos o pendientes.",
                    "Cambiar estatus a contactado, cotizado, finalizado o rechazado.",
                    "Convertir la oportunidad en cotizacion cuando exista informacion suficiente.",
                ], "bullets"),
            ],
        )
        self.section(
            "10. Registro De Obras",
            "Este modulo permite controlar obras detectadas, visitadas o en seguimiento, especialmente para informacion levantada en campo.",
            [
                ("Alta de obra", [
                    "Entrar a Registro de Obras.",
                    "Capturar nombre de obra y ubicacion.",
                    "Registrar encargado, puesto, telefono y correo.",
                    "Asignar responsable.",
                    "Guardar registro.",
                ], "steps"),
                ("Seguimiento de obra", [
                    "Abrir el registro de obra.",
                    "Agregar comentario de visita, avance o pendiente.",
                    "Actualizar datos de contacto si cambian.",
                    "Exportar informacion cuando sea necesario.",
                ], "steps"),
            ],
        )
        self.section(
            "11. Aplicacion Movil Android",
            "La aplicacion movil permite consultar y actualizar informacion desde campo, conectandose al mismo sistema central.",
            [
                ("Funciones principales", [
                    "Inicio de sesion con usuario del sistema.",
                    "Consulta de registros de obra.",
                    "Alta y edicion de obras.",
                    "Consulta de cotizaciones pendientes.",
                    "Cambio de estatus de cotizaciones.",
                    "Apertura de PDF de cotizacion.",
                    "Registro de dispositivo para notificaciones.",
                ], "bullets"),
                ("Uso recomendado", [
                    "Confirmar conexion a internet antes de sincronizar.",
                    "Actualizar registros en el momento de la visita.",
                    "Verificar que los cambios aparezcan correctamente en el sistema web.",
                ], "bullets"),
            ],
        )
        self.section(
            "12. Finanzas",
            "El modulo de finanzas ayuda a controlar movimientos economicos, saldos, vencimientos, pagos y responsables.",
            [
                ("Registrar movimiento", [
                    "Entrar a Finanzas.",
                    "Capturar categoria, contraparte y concepto.",
                    "Relacionar proyecto si aplica.",
                    "Capturar fecha, monto, saldo, moneda y vencimiento.",
                    "Guardar el movimiento.",
                ], "steps"),
                ("Registrar abono o pago", [
                    "Abrir el movimiento financiero.",
                    "Capturar monto, fecha, referencia y notas.",
                    "Guardar pago.",
                    "Verificar que el saldo se actualice.",
                ], "steps"),
            ],
        )
        self.section(
            "13. Ordenes De Compra",
            "Las ordenes de compra formalizan solicitudes de adquisicion y permiten dar seguimiento a su estatus y recepcion.",
            [
                ("Proceso general", [
                    "Crear orden de compra.",
                    "Capturar proveedor o contraparte.",
                    "Agregar conceptos o productos requeridos.",
                    "Guardar y revisar detalle.",
                    "Cambiar estatus segun avance.",
                    "Registrar recepcion cuando llegue el material o servicio.",
                    "Exportar PDF o Excel si se requiere.",
                ], "steps"),
            ],
        )
        self.section(
            "14. Inventario",
            "El inventario permite controlar productos, movimientos y kardex por producto.",
            [
                ("Productos", [
                    "Crear nuevos productos.",
                    "Actualizar descripcion, unidad o datos de control.",
                    "Consultar existencia cuando aplique.",
                ], "bullets"),
                ("Movimientos", [
                    "Registrar entradas.",
                    "Registrar salidas.",
                    "Consultar kardex por producto.",
                    "Exportar informacion para revision administrativa.",
                ], "bullets"),
            ],
        )
        self.section(
            "15. Precios Unitarios / APU",
            "El modulo de precios unitarios permite construir analisis de costos por obra, partida e insumos.",
            [
                ("Componentes", [
                    "Obras.",
                    "Partidas.",
                    "Materiales.",
                    "Mano de obra.",
                    "Maquinaria.",
                    "Basicos y extras.",
                    "Sobrecostos, financiamiento y utilidad.",
                ], "bullets"),
                ("Uso general", [
                    "Crear obra de precios unitarios.",
                    "Agregar partidas.",
                    "Capturar insumos y costos.",
                    "Revisar costo directo, sobrecostos y precio unitario.",
                    "Actualizar cantidades o rendimientos cuando cambie el proyecto.",
                ], "steps"),
            ],
        )
        self.section(
            "16. Administracion Y Seguridad",
            "La administracion permite controlar usuarios, roles y acceso al sistema.",
            [
                ("Usuarios", [
                    "Crear usuarios nuevos.",
                    "Editar nombre, rol o contrasena.",
                    "Eliminar usuarios que ya no deban acceder.",
                    "Asignar rol administrador o representante.",
                ], "bullets"),
                ("Seguridad operativa", [
                    "Usar cuentas individuales.",
                    "Evitar compartir credenciales.",
                    "Revisar responsables de registros importantes.",
                    "Cerrar sesion al terminar.",
                ], "bullets"),
            ],
        )
        self.section(
            "17. Recomendaciones Generales",
            "Para obtener mejores resultados, el sistema debe usarse de forma constante y con informacion completa.",
            [
                ("Buenas practicas", [
                    "Capturar datos completos desde el primer registro.",
                    "Actualizar estatus despues de cada contacto con cliente.",
                    "Registrar comentarios de seguimiento claros.",
                    "Evitar duplicar clientes, conceptos u obras.",
                    "Exportar documentos despues de revisar importes y datos.",
                    "Mantener catalogos actualizados.",
                ], "bullets"),
                ("Errores comunes a evitar", [
                    "Crear varias veces el mismo cliente.",
                    "Cotizar sin revisar unidad, cantidad o precio.",
                    "No registrar seguimiento despues de enviar una cotizacion.",
                    "Usar usuarios compartidos.",
                    "Dejar movimientos financieros sin saldo actualizado.",
                ], "bullets"),
            ],
        )
        self.flow_diagram()
        self.pdf.save(OUT)


if __name__ == "__main__":
    manual = Manual()
    manual.build()
    print(OUT.resolve())
