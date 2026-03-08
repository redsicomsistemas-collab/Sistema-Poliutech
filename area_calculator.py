
def calculate_area(rows, waste_pct=0):
    base_area = 0.0
    details = []
    for row in rows:
        largo = float(row.get("largo", 0) or 0)
        ancho = float(row.get("ancho", 0) or 0)
        piezas = float(row.get("piezas", 1) or 1)
        area = largo * ancho * piezas
        base_area += area
        details.append({
            "largo": largo,
            "ancho": ancho,
            "piezas": piezas,
            "area": round(area, 4)
        })
    waste_factor = 1 + (float(waste_pct or 0) / 100.0)
    total_area = base_area * waste_factor
    return {
        "details": details,
        "base_area": round(base_area, 4),
        "waste_pct": float(waste_pct or 0),
        "total_area": round(total_area, 4)
    }
