# Registro Obras Android

Proyecto Android Studio con app nativa para:

- login contra tu servidor Render
- alta de registro de obras
- listado de registros
- edición
- borrado masivo

## Requisito de backend

La app consume estos endpoints del sistema Flask:

- `POST /api/mobile/login`
- `GET /api/mobile/registro-obras`
- `POST /api/mobile/registro-obras`
- `PUT /api/mobile/registro-obras/<id>`
- `POST /api/mobile/registro-obras/bulk-delete`

## Cómo usarlo

1. Abre esta carpeta en Android Studio.
2. Deja que sincronice Gradle.
3. Compila el APK.
4. En el primer arranque configura la URL de Render.
5. Inicia sesión.
6. Usa el formulario nativo.
