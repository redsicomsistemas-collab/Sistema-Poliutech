# Importador de cotizaciones externas

## Archivos
- `importar_cotizacion_externa.py`: importa una cotizacion desde JSON hacia `mar3.db`
- `example_quote_import.json`: plantilla base para capturar datos

## Validar sin escribir en la base
```powershell
python importar_cotizacion_externa.py --json example_quote_import.json --dry-run
```

## Importar de verdad
```powershell
python importar_cotizacion_externa.py --json mi_cotizacion.json --source-label "C:\Users\x\Downloads\Cotización Tremproof Jardin-Oracle Guadalajara  COT-2026-02-026-2.pdf"
```

## Campos esperados en el JSON
- `folio`: folio externo; si ya existe, el script genera un `PTCH-xxxx`
- `fecha`: `YYYY-MM-DD` o `DD/MM/YYYY`
- `estatus`: por ejemplo `PENDIENTE`
- `responsable`: opcional
- `cliente.nombre_cliente`: obligatorio
- `items`: lista obligatoria de conceptos

Cada item acepta:
- `nombre_concepto`
- `unidad`
- `cantidad`
- `precio_unitario`
- `sistema`
- `descripcion`
