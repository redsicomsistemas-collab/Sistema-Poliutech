import os
from datetime import datetime
from models import Cotizacion, CotizacionItem

def exportar_cotizacion(cot_id: int):
    cot = Cotizacion.query.get(cot_id)
    items = CotizacionItem.query.filter_by(cotizacion_id=cot_id).all()
    app_root = os.path.dirname(os.path.abspath(__file__))
    app_root = os.path.dirname(app_root)  # back to project root
    out_dir = os.path.join(app_root, "exports")
    os.makedirs(out_dir, exist_ok=True)

    pdf_name = f"cotizacion_{cot_id}.pdf"
    xlsx_name = f"cotizacion_{cot_id}.xlsx"
    pdf_path = os.path.join(out_dir, pdf_name)
    xlsx_path = os.path.join(out_dir, xlsx_name)

    # PDF via reportlab (fallback placeholder if not installed)
    try:
        from reportlab.lib.pagesizes import LETTER
        from reportlab.pdfgen import canvas
        c = canvas.Canvas(pdf_path, pagesize=LETTER)
        width, height = LETTER
        y = height - 50
        c.setFont("Helvetica-Bold", 14)
        c.drawString(50, y, f"Cotización {cot.folio}")
        y -= 20
        c.setFont("Helvetica", 10)
        c.drawString(50, y, f"Cliente: {cot.cliente}  Empresa: {cot.empresa or ''}  Estatus: {cot.estatus}")
        y -= 15
        c.drawString(50, y, f"Tel: {cot.telefono or ''}  Correo: {cot.correo or ''}")
        y -= 25
        c.setFont("Helvetica-Bold", 10)
        c.drawString(50, y, "Cant"); c.drawString(90, y, "Unidad"); c.drawString(150, y, "Concepto")
        c.drawString(400, y, "P.Unit"); c.drawString(470, y, "Importe")
        y -= 12; c.setFont("Helvetica", 10)
        for it in items:
            if y < 80: c.showPage(); y = height - 50
            c.drawString(50, y, str(it.cantidad))
            c.drawString(90, y, (it.unidad or "")[:8])
            c.drawString(150, y, (it.concepto or "")[:50])
            c.drawRightString(450, y, f"{it.precio_unitario:,.2f}")
            c.drawRightString(520, y, f"{it.importe:,.2f}")
            y -= 14
        y -= 10; c.setFont("Helvetica-Bold", 12)
        c.drawRightString(520, y, f"TOTAL: {cot.total:,.2f}")
        c.save()
    except Exception:
        with open(pdf_path, "wb") as f:
            f.write(b"%PDF-1.1\n% Placeholder. Instala reportlab para PDF reales.\n")

    # XLSX via pandas + openpyxl (fallback to CSV content with .xlsx extension)
    try:
        import pandas as pd
        df = pd.DataFrame([{
            "Cantidad": it.cantidad,
            "Unidad": it.unidad,
            "Concepto": it.concepto,
            "Precio Unitario": it.precio_unitario,
            "Importe": it.importe
        } for it in items])
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Partidas")
    except Exception:
        with open(xlsx_path, "w", encoding="utf-8") as f:
            f.write("Cantidad,Unidad,Concepto,Precio Unitario,Importe\n")
            for it in items:
                f.write(f"{it.cantidad},{it.unidad or ''},{(it.concepto or '').replace(',', ' ')},{it.precio_unitario},{it.importe}\n")

    return {"pdf": f"/exports/{pdf_name}", "xlsx": f"/exports/{xlsx_name}"}
