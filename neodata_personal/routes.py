
from flask import Blueprint, render_template, request, redirect, url_for
from models import db
from .models import APU
from .calc import recalcular_apu

apu_bp = Blueprint("apu", __name__, url_prefix="/apu")

@apu_bp.route("/")
def lista_apu():
    apus = APU.query.all()
    return render_template("apu_list.html", apus=apus)


@apu_bp.route("/crear", methods=["GET", "POST"])
def crear_apu():
    if request.method == "POST":
        concepto = request.form["concepto"]
        unidad = request.form["unidad"]

        apu = APU(concepto=concepto, unidad=unidad)
        db.session.add(apu)
        db.session.commit()

        return redirect(url_for("apu.lista_apu"))

    return render_template("apu_create.html")
