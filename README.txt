PAQUETE COMPLETO CORREGIDO

Incluye:
- app.py corregido para el bloque PDF
- templates/base.html
- templates/dashboard.html
- templates/cotizador.html
- templates/cotizacion_edit.html
- templates/cotizacion_view.html
- templates/partials/mar_data_nav.html
- neodata_personal/routes.py
- templates/apu_cotizador_rapido.html
- static/js/cotizador_apu_bridge.js
- static/js/cotizacion_edit_apu_bridge.js

Este paquete elimina referencias rotas a:
- mar_data_advanced.propuesta
- otros endpoints avanzados no registrados

Y deja solo endpoints seguros:
- apu.index
- apu.apu_list
- apu.apu_new
- apu.plantillas
- apu.generador
- apu.apu_cotizador_rapido
