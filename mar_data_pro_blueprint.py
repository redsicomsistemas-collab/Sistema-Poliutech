
from flask import Blueprint, request, jsonify, render_template
from area_calculator import calculate_area
from materials_list_generator import generate_materials_list
from technical_memory_generator import generate_technical_memory

mar_data_pro_bp = Blueprint("mar_data_pro", __name__, url_prefix="/mar-data", template_folder="templates", static_folder="static")

@mar_data_pro_bp.route("/fase2")
def fase2_home():
    return render_template("mar_data_pro_fase2.html")

@mar_data_pro_bp.route("/area/calculate", methods=["POST"])
def area_calculate():
    data = request.get_json(silent=True) or {}
    rows = data.get("rows", [])
    waste_pct = data.get("waste_pct", 0)
    return jsonify(calculate_area(rows, waste_pct))

@mar_data_pro_bp.route("/materials/generate", methods=["POST"])
def materials_generate():
    data = request.get_json(silent=True) or {}
    area_total = data.get("area_total", 0)
    materials = data.get("materials", [])
    return jsonify(generate_materials_list(area_total, materials))

@mar_data_pro_bp.route("/memory/generate", methods=["POST"])
def memory_generate():
    data = request.get_json(silent=True) or {}
    return jsonify(generate_technical_memory(data))
