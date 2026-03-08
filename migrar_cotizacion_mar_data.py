from sqlalchemy import text
from app import app, db

SQLS = [
    "ALTER TABLE cotizacion ADD COLUMN area_total FLOAT",
    "ALTER TABLE cotizacion ADD COLUMN memoria_tecnica TEXT",
    "ALTER TABLE cotizacion ADD COLUMN lista_materiales_json TEXT",
]

with app.app_context():
    for sql in SQLS:
        try:
            db.session.execute(text(sql))
            db.session.commit()
            print(f"OK: {sql}")
        except Exception as e:
            db.session.rollback()
            msg = str(e).lower()
            if "duplicate column name" in msg or "already exists" in msg:
                print(f"YA EXISTE: {sql}")
            else:
                print(f"ERROR: {sql}\n{e}")
                raise

    print("Migración terminada.")
