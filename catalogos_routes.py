# =========================================================
# catalogos_routes.py — Sistema MARWHATS / Poliutech
# =========================================================

import io, csv, traceback
from flask import (
    Blueprint, request, redirect, url_for,
    render_template, flash, jsonify, Response
)
from models import db, Cliente, Concepto  # Importa desde models.py

bp = Blueprint("catalogos", __name__)

# ---------------------------------------------------------
# Vista principal con paginación numerada
# ---------------------------------------------------------
@bp.route("/")
def catalogos_index():
    # Paginación
    page_clientes = int(request.args.get("page_clientes", 1))
    page_conceptos = int(request.args.get("page_conceptos", 1))
    per_page = 20

    clientes_pag = Cliente.query.order_by(Cliente.id.desc()).paginate(
        page=page_clientes, per_page=per_page, error_out=False
    )
    conceptos_pag = Concepto.query.order_by(Concepto.id.desc()).paginate(
        page=page_conceptos, per_page=per_page, error_out=False
    )

    return render_template(
        "admin_catalogos.html",
        title="Catálogos",
        clientes=clientes_pag.items,
        conceptos=conceptos_pag.items,
        clientes_pag=clientes_pag,
        conceptos_pag=conceptos_pag,
    )

# ---------------------------------------------------------
# Exportar catálogos a CSV
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

@bp.route("/conceptos/export.csv")
def export_conceptos_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Nombre", "Unidad", "Precio Unitario", "Descripción"])
    for c in Concepto.query.order_by(Concepto.id.asc()).all():
        writer.writerow([
            c.id, c.nombre_concepto, c.unidad or "",
            f"{c.precio_unitario:.2f}", c.descripcion or ""
        ])
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=conceptos_catalogo.csv"}
    )

# ---------------------------------------------------------
# Importar catálogos
# ---------------------------------------------------------
@bp.route("/import", methods=["POST"])
def import_catalogo():
    tipo = request.form.get("tipo")
    file = request.files.get("archivo")

    if not tipo or tipo.lower() not in ["clientes", "conceptos"]:
        flash("Selecciona un tipo de catálogo válido.", "warning")
        return redirect(url_for("catalogos.catalogos_index"))

    if not file:
        flash("Selecciona un archivo para importar.", "danger")
        return redirect(url_for("catalogos.catalogos_index"))

    try:
        data = file.read().decode("utf-8").splitlines()
        reader = csv.DictReader(data)
        count = 0

        if tipo.lower() == "clientes":
            for row in reader:
                nombre = (row.get("Nombre") or row.get("nombre_cliente") or "").strip()
                if not nombre:
                    continue
                existe = Cliente.query.filter_by(nombre_cliente=nombre).first()
                if not existe:
                    cliente = Cliente(
                        nombre_cliente=nombre,
                        empresa=row.get("Empresa") or row.get("empresa"),
                        responsable=row.get("Responsable") or row.get("responsable"),
                        correo=row.get("Correo") or row.get("correo"),
                        telefono=row.get("Teléfono") or row.get("telefono"),
                        direccion=row.get("Dirección") or row.get("direccion"),
                        rfc=row.get("RFC") or row.get("rfc"),
                    )
                    db.session.add(cliente)
                    count += 1
        else:
            for row in reader:
                nombre = (row.get("Nombre") or row.get("nombre_concepto") or "").strip()
                if not nombre:
                    continue
                existe = Concepto.query.filter_by(nombre_concepto=nombre).first()
                if not existe:
                    concepto = Concepto(
                        nombre_concepto=nombre,
                        unidad=row.get("Unidad") or row.get("unidad"),
                        precio_unitario=float(row.get("Precio Unitario") or 0),
                        descripcion=row.get("Descripción") or row.get("descripcion"),
                    )
                    db.session.add(concepto)
                    count += 1

        db.session.commit()
        flash(f"Catálogo '{tipo}' importado correctamente ({count} nuevos registros).", "success")

    except Exception as e:
        db.session.rollback()
        traceback.print_exc()
        flash(f"Error al importar catálogo: {e}", "danger")

    return redirect(url_for("catalogos.catalogos_index"))

# ---------------------------------------------------------
# Eliminar registros
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
