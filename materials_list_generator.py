
def generate_materials_list(area_total, materials):
    output = []
    total_cost = 0.0
    for m in materials:
        consumo_unitario = float(m.get("consumo_unitario", 0) or 0)
        precio_unitario = float(m.get("precio_unitario", 0) or 0)
        cantidad_total = float(area_total or 0) * consumo_unitario
        costo_total = cantidad_total * precio_unitario
        total_cost += costo_total
        output.append({
            "nombre": m.get("nombre"),
            "unidad": m.get("unidad"),
            "consumo_unitario": round(consumo_unitario, 4),
            "cantidad_total": round(cantidad_total, 4),
            "precio_unitario": round(precio_unitario, 4),
            "costo_total": round(costo_total, 4)
        })
    return {
        "area_total": round(float(area_total or 0), 4),
        "items": output,
        "total_cost": round(total_cost, 4)
    }
