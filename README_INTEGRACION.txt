
PAQUETE INTEGRADO COMPLETO – SISTEMA MAR + MAR DATA

Este paquete ya viene integrado para TU app actual.

Incluye:
- app.py corregido
- templates/base.html con menú MAR DATA
- templates/dashboard.html con accesos rápidos a MAR DATA
- carpeta neodata_personal completa con:
  - Materiales
  - Mano de obra
  - Maquinaria
  - APU
  - Plantillas
  - Generador automático
  - Duplicar APU
  - Mandar APU a Concepto

========================================
CÓMO SUBIRLO A TU PROYECTO EN RENDER
========================================

1) DESCARGA ESTE ZIP Y DESCOMPRÍMELO

2) REEMPLAZA EN TU PROYECTO:
   - app.py
   - templates/base.html
   - templates/dashboard.html
   - toda la carpeta neodata_personal/

3) VERIFICA QUE TU PROYECTO QUEDE ASÍ:
   app.py
   models.py
   templates/
      base.html
      dashboard.html
   static/
   neodata_personal/

4) REINICIA TU SERVICIO EN RENDER

5) ENTRA A:
   /apu
   /apu/plantillas
   /apu/generador

========================================
NOTAS
========================================
- Este módulo usa el MISMO db del sistema:
  from models import db
- No crea otro SQLAlchemy.
- El cotizador actual ya consume Concepto; por eso "Mandar a catálogo"
  deja el APU utilizable en el flujo actual del sistema.
