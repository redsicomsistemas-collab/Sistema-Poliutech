
import json
from pathlib import Path
from flask import Blueprint, render_template, request, jsonify

mar_data_advanced_bp = Blueprint("mar_data_advanced", __name__, url_prefix="/mar-data/advanced", template_folder="templates", static_folder="static")
LIB_PATH = Path(__file__).resolve().parent / "mar_data_system_library.json"

def _load_library():
    with open(LIB_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def _get_system(slug):
    lib = _load_library()
    for s in lib.get('systems', []):
        if s.get('slug') == slug:
            return s
    return None

def _calc_parametric(system, area_total, espesor_mm, merma_pct=None, rendimiento_m2_dia=None):
    area_total = float(area_total or 0)
    espesor_mm = float(espesor_mm or 0)
    merma_pct = float(system.get('merma_pct_default', 0) if merma_pct in (None, '', False) else merma_pct)
    rendimiento = float(system.get('rendimiento_base_m2_dia', 0) if rendimiento_m2_dia in (None, '', False) else rendimiento_m2_dia)
    area_con_merma = area_total * (1 + merma_pct/100.0)
    jornadas_estimadas = round(area_con_merma / rendimiento, 4) if rendimiento else 0.0
    items = []
    total_directo = 0.0
    indirecto_pct = 0.0
    for c in system.get('componentes', []):
        tipo = c.get('tipo')
        if tipo == 'indirecto':
            indirecto_pct += float(c.get('porcentaje', 0) or 0)
            continue
        consumo = 0.0
        if c.get('consumo_por_mm') is not None:
            consumo = float(c.get('consumo_por_mm') or 0) * espesor_mm
            cantidad = area_con_merma * consumo
        else:
            consumo = float(c.get('consumo_fijo') or 0)
            cantidad = area_con_merma * consumo
        costo_total = cantidad * float(c.get('precio_unitario') or 0)
        total_directo += costo_total
        items.append({
            'tipo': tipo,
            'nombre': c.get('nombre'),
            'unidad': c.get('unidad'),
            'consumo_unitario': round(consumo, 4),
            'cantidad_total': round(cantidad, 4),
            'precio_unitario': round(float(c.get('precio_unitario') or 0), 4),
            'costo_total': round(costo_total, 4)
        })
    indirecto = total_directo * (indirecto_pct/100.0)
    venta = total_directo + indirecto
    memoria = f"""SISTEMA: {system.get('nombre')}
ESPESOR: {espesor_mm} mm
ÁREA TOTAL: {round(area_con_merma,4)} m²
PREPARACIÓN: {system.get('preparacion')}
CAPAS:
""" + "
".join([f"- {x}" for x in system.get('capas', [])]) + f"
RENDIMIENTO ESTIMADO: {rendimiento} m²/día
OBSERVACIONES: {system.get('observaciones')}"
    return {
        'system': system,
        'area_total': round(area_total,4),
        'area_con_merma': round(area_con_merma,4),
        'espesor_mm': espesor_mm,
        'merma_pct': merma_pct,
        'rendimiento_m2_dia': rendimiento,
        'jornadas_estimadas': jornadas_estimadas,
        'items': items,
        'total_directo': round(total_directo,4),
        'indirecto_pct': indirecto_pct,
        'indirecto_total': round(indirecto,4),
        'precio_venta_estimado': round(venta,4),
        'memoria_tecnica': memoria
    }

@mar_data_advanced_bp.route('/')
def home():
    lib = _load_library()
    return render_template('mar_data_advanced_home.html', systems=lib.get('systems', []))

@mar_data_advanced_bp.route('/library')
def library():
    lib = _load_library()
    return render_template('mar_data_library.html', systems=lib.get('systems', []))

@mar_data_advanced_bp.route('/propuesta')
def propuesta():
    lib = _load_library()
    return render_template('mar_data_propuesta.html', systems=lib.get('systems', []))

@mar_data_advanced_bp.route('/costeo')
def costeo():
    lib = _load_library()
    return render_template('mar_data_costeo.html', systems=lib.get('systems', []))

@mar_data_advanced_bp.route('/api/library')
def api_library():
    return jsonify(_load_library())

@mar_data_advanced_bp.route('/api/parametric', methods=['POST'])
def api_parametric():
    data = request.get_json(silent=True) or {}
    slug = data.get('slug')
    system = _get_system(slug)
    if not system:
        return jsonify({'error':'Sistema no encontrado'}), 404
    return jsonify(_calc_parametric(system, data.get('area_total'), data.get('espesor_mm'), data.get('merma_pct'), data.get('rendimiento_m2_dia')))

@mar_data_advanced_bp.route('/api/propuesta', methods=['POST'])
def api_propuesta():
    data = request.get_json(silent=True) or {}
    slug = data.get('slug')
    system = _get_system(slug)
    if not system:
        return jsonify({'error':'Sistema no encontrado'}), 404
    calc = _calc_parametric(system, data.get('area_total'), data.get('espesor_mm'), data.get('merma_pct'), data.get('rendimiento_m2_dia'))
    propuesta = f"""PROPUESTA TÉCNICA Y ECONÓMICA

Sistema propuesto: {system.get('nombre')}
Área considerada: {calc['area_con_merma']} m²
Espesor considerado: {calc['espesor_mm']} mm
Rendimiento estimado: {calc['rendimiento_m2_dia']} m²/día
Jornadas estimadas: {calc['jornadas_estimadas']}

Preparación de superficie:
{system.get('preparacion')}

Capas del sistema:
""" + "
".join([f"- {x}" for x in system.get('capas', [])]) + f"

Precio de venta estimado: ${calc['precio_venta_estimado']:,.2f}

Observaciones:
{system.get('observaciones')}"
    return jsonify({'propuesta_texto': propuesta, 'calculo': calc})

@mar_data_advanced_bp.route('/api/costeo', methods=['POST'])
def api_costeo():
    data = request.get_json(silent=True) or {}
    venta = float(data.get('venta', 0) or 0)
    costo_real = float(data.get('costo_real', 0) or 0)
    utilidad = venta - costo_real
    margen = (utilidad / venta * 100.0) if venta else 0.0
    return jsonify({
        'venta': round(venta,4),
        'costo_real': round(costo_real,4),
        'utilidad_real': round(utilidad,4),
        'margen_real_pct': round(margen,4)
    })
