# MAROK — Fix de importación de conceptos (CSV/XLSX/XLS)

**Qué cambia:** Solo la lógica de importación de conceptos en `catalogos_routes.py`. No se toca diseño ni otras funcionalidades.

## Formatos aceptados
- **CSV** (coma/;/**|**/tab) en UTF-8/UTF-8-SIG/CP1252/Latin-1 (autodetección).
- **Excel .xlsx** (instala `openpyxl`).
- **Excel .xls (legacy)** requiere `xlrd==1.2.0`. Si no está instalado, se mostrará un error claro (puedes guardar como .xlsx).

## Encabezados admitidos (flexibles)
Puedes usar cualquiera de estos nombres de columna (se aceptan acentos y mayúsculas/minúsculas):

- **nombre**: `nombre`, `concepto`, `clave concepto`, `producto`
- **descripcion**: `descripcion`, `descripción`, `detalle`, `observaciones`
- **unidad**: `unidad`, `u`, `um`, `unidad de medida`
- **precio**: `precio`, `precio unitario`, `p.u.`, `importe unitario`
- **anio**: `anio`, `año`, `year`

> La columna **nombre/concepto** es obligatoria. Las demás son opcionales.

## Dependencias (si usas Excel)
```bash
pip install openpyxl
# opcional para .xls (legacy)
pip install xlrd==1.2.0
```

## Cómo instalar
1. Copia **`catalogos_routes.py`** de este ZIP y reemplaza tu archivo actual.
2. Reinicia tu servidor Flask.

## Notas
- Los precios se normalizan: soporta `$`, separadores de miles (`.`, `,`) y decimales tanto `.` como `,`.
- La respuesta JSON indica cuántos registros se **insertaron** y cuántos se **saltaron** con una muestra de errores.
