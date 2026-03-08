
def generate_technical_memory(data):
    sistema = data.get("sistema", "Sistema no especificado")
    espesor = data.get("espesor", "N/D")
    preparacion = data.get("preparacion", "Preparación mecánica de superficie según requerimiento del sistema.")
    capas = data.get("capas", [])
    observaciones = data.get("observaciones", "Aplicación sujeta a condiciones adecuadas de temperatura, humedad y estado del sustrato.")
    rendimiento = data.get("rendimiento", "N/D")
    capas_text = "\n".join([f"- {c}" for c in capas]) if capas else "- Capa base\n- Capa intermedia\n- Capa final"
    texto = f'''
MEMORIA TÉCNICA

Sistema:
{sistema}

Espesor estimado:
{espesor}

Preparación de superficie:
{preparacion}

Capas del sistema:
{capas_text}

Rendimiento estimado:
{rendimiento}

Observaciones:
{observaciones}
'''.strip()
    return {"memory_text": texto}
