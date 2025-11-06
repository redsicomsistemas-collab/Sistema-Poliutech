# =========================================================
# catalogos_routes.py — Sistema MARWHATS / Poliutech
# Versión estable con paginación, importación y exportación.
# =========================================================

import io, csv, traceback
from flask import (
    Blueprint, request, redirect, url_for,
    render_template, flash, jsonify, Response
)
from sqlalchemy import text
from models import db, Cliente, Concepto

bp = Blueprint("catalogos", __name__)

# ---------------------------------------------------------
# Vista principal del módulo de catálogos (con paginación)
# ---------------------------------------------------------
@bp.route("/")
def catalogos_index():
    page_clientes = int(request.args.get("page_clientes", 1))
    page_conceptos = int(request.args.get("page_conceptos", 1))

    clientes_pag = Cliente.query.order_by(Cliente.id.desc()).paginate(page=page_clientes, per_page=10)
    conceptos_pag = Concepto.query.order_by(Concepto.id.desc()).paginate(page=page_conceptos, per_page=10)

    return render_template(
        "admin_catalogos.html",
        title="Catálogos",
        clientes=clientes_pag.items,
        conceptos=conceptos_pag.items,
        clientes_pag=clientes_pag,
        conceptos_pag=conceptos_pag
    )

# ---------------------------------------------------------
# Exportar catálogo de clientes a CSV
# ---------------------------------------------------------
@bp.route("/clientes/export.csv")
def export_clientes_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Nombre", "Empresa", "Responsable", "Correo", "Teléfono", "Dirección", "RFC"])
    for c in Cliente.query.order_by(Cliente.id.asc()).all():
        writer.writerow([
            c.id, c.nombre_cliente, c.empresa or "", c.responsable or "",
            c.correo or "", c.telefono or "", c.direccion or "", c.rfc or ""
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=clientes_catalogo.csv"}
    )

# ---------------------------------------------------------
# Exportar catálogo de conceptos a CSV
# ---------------------------------------------------------
@bp.route("/conceptos/export.csv")
def export_conceptos_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Nombre", "Unidad", "Precio Unitario", "Sistema", "Descripción"])
    for c in Concepto.query.order_by(Concepto.id.asc()).all():
        writer.writerow([
            c.id, c.nombre_concepto, c.unidad or "",
            f"{c.precio_unitario:.2f}", c.sistema or "", c.descripcion or ""
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=conceptos_catalogo.csv"}
    )

# ---------------------------------------------------------
# Importar catálogos (Clientes o Conceptos)
# ---------------------------------------------------------
@bp.route("/import", methods=["GET", "POST"])
def import_catalogo():
    if request.method == "POST":
        tipo = request.form.get("tipo")
        file = request.files.get("archivo")

        if not tipo or tipo.lower() not in ["clientes", "conceptos"]:
            flash("Selecciona un tipo de catálogo válido.", "warning")
            return redirect(url_for("catalogos.catalogos_index"))

        if not file:
            flash("Selecciona un archivo para importar.", "danger")
            return redirect(url_for("catalogos.catalogos_index"))

        try:
            # Intentar decodificar de forma robusta
            try:
                data = file.read().decode("utf-8").splitlines()
            except UnicodeDecodeError:
                file.seek(0)
                data = file.read().decode("latin-1").splitlines()

            reader = csv.DictReader(data)
            count = 0

            if tipo.lower() == "clientes":
                for row in reader:
                    nombre = (row.get("Nombre") or row.get("nombre_cliente") or "").strip()
                    if not nombre:
                        continue
                    cliente = Cliente.query.filter_by(nombre_cliente=nombre).first()
                    if not cliente:
                        cliente = Cliente(
                            nombre_cliente=nombre,
                            empresa=row.get("Empresa") or row.get("empresa"),
                            responsable=row.get("Responsable") or row.get("responsable"),
                            correo=row.get("Correo") or row.get("correo"),
                            telefono=row.get("Teléfono") or row.get("telefono"),
                            direccion=row.get("Dirección") or row.get("direccion"),
                            rfc=row.get("RFC") or row.get("rfc")
                        )
                        db.session.add(cliente)
                        count += 1

            elif tipo.lower() == "conceptos":
                def _get_key(d, *candidatos):
                    for k in d.keys():
                        for c in candidatos:
                            if k.strip().lower() == c.lower():
                                return k
                    return None

                for row in reader:
                    k_nombre = _get_key(row, "Nombre", "NOMBRE_CONCEPTO", "nombre_concepto", "concepto")
                    k_unidad = _get_key(row, "Unidad", "unidad")
                    k_precio = _get_key(row, "Precio Unitario", "PRECIO_UNITARIO", "precio_unitario", "precio")
                    k_desc   = _get_key(row, "Descripción", "DESCRIPCION", "descripcion", "descripción")
                    k_sis    = _get_key(row, "Sistema", "SISTEMA", "sistema")

                    nombre = (row.get(k_nombre) or "").strip()
                    if not nombre:
                        continue
                    unidad = (row.get(k_unidad) or "").strip() or None
                    precio = float((str(row.get(k_precio) or "0").replace("$","").replace(",","").strip() or "0"))
                    descripcion = (row.get(k_desc) or "").strip() or None
                    sistema = (row.get(k_sis) or "").strip() or None

                    concepto = Concepto.query.filter_by(nombre_concepto=nombre).first()
                    if not concepto:
                        concepto = Concepto(
                            nombre_concepto=nombre,
                            unidad=unidad,
                            precio_unitario=precio,
                            sistema=sistema,  # 👈 nuevo campo soportado
                            descripcion=descripcion
                        )
                        db.session.add(concepto)
                        count += 1
                    else:
                        # Si viene sistema y no lo tiene en BD, se actualiza
                        if sistema and not concepto.sistema:
                            concepto.sistema = sistema

            db.session.commit()
            flash(f"Catálogo '{tipo}' importado correctamente ({count} nuevos registros).", "success")
        except Exception as e:
            db.session.rollback()
            print("[IMPORT ERROR]", e)
            traceback.print_exc()
            flash(f"Error al importar catálogo: {e}", "danger")

        return redirect(url_for("catalogos.catalogos_index"))

    return render_template("import_catalogo.html", title="Importar Catálogo")

# ---------------------------------------------------------
# Eliminar registro de catálogo
# ---------------------------------------------------------
@bp.route("/eliminar/<tipo>/<int:item_id>")
def eliminar_catalogo(tipo, item_id):
    if tipo == "clientes":
        obj = Cliente.query.get_or_404(item_id)
    elif tipo == "conceptos":
        obj = Concepto.query.get_or_404(item_id)
    else:
        flash("Tipo de catálogo inválido.", "warning")
        return redirect(url_for("catalogos.catalogos_index"))

    try:
        db.session.delete(obj)
        db.session.commit()
        flash(f"Registro eliminado del catálogo '{tipo}'.", "success")
    except Exception as e:
        db.session.rollback()
        flash(f"Error al eliminar: {e}", "danger")

    return redirect(url_for("catalogos.catalogos_index"))
