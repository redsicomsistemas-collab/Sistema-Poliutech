
def round4(n):
    try:
        return round(float(n or 0), 4)
    except Exception:
        return 0.0

def recalcular_apu(apu):
    costo_materiales = 0.0
    costo_mano_obra = 0.0
    costo_maquinaria = 0.0

    for d in apu.detalles:
        d.subtotal = round4((d.cantidad or 0) * (d.precio_unitario or 0))
        if d.tipo_insumo == "material":
            costo_materiales += d.subtotal
        elif d.tipo_insumo == "mano_obra":
            costo_mano_obra += d.subtotal
        elif d.tipo_insumo == "maquinaria":
            costo_maquinaria += d.subtotal

    apu.costo_materiales = round4(costo_materiales)
    apu.costo_mano_obra = round4(costo_mano_obra)
    apu.costo_maquinaria = round4(costo_maquinaria)

    apu.costo_directo = round4(costo_materiales + costo_mano_obra + costo_maquinaria)

    factor = (
        (apu.indirecto_pct or 0)
        + (apu.utilidad_pct or 0)
        + (apu.financiamiento_pct or 0)
        + (apu.cargos_adicionales_pct or 0)
    ) / 100.0

    apu.precio_unitario = round4(apu.costo_directo * (1 + factor))
    return apu
