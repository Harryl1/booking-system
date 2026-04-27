import os
import sqlite3


SQLITE_DB_PATH = os.environ.get("SQLITE_DB_PATH", "bookings.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

TABLES = [
    "organisations",
    "users",
    "branch_territories",
    "leads",
    "lead_notes",
    "lead_tasks",
    "audit_logs",
    "service_referrals",
    "bookings",
]

SERIAL_TABLES = [
    "organisations",
    "users",
    "branch_territories",
    "leads",
    "lead_notes",
    "lead_tasks",
    "audit_logs",
    "service_referrals",
]


def sqlite_columns(connection, table_name):
    return [row["name"] for row in connection.execute(f"PRAGMA table_info({table_name})")]


def migrate_table(sqlite_db, postgres_db, table_name):
    columns = sqlite_columns(sqlite_db, table_name)
    if not columns:
        print(f"SKIPPED {table_name}: table not found in SQLite")
        return

    rows = sqlite_db.execute(f"SELECT {', '.join(columns)} FROM {table_name}").fetchall()
    if not rows:
        print(f"SKIPPED {table_name}: no rows")
        return

    column_sql = ", ".join(columns)
    placeholders = ", ".join("?" for _ in columns)
    conflict_column = "id" if "id" in columns else columns[0]
    sql = f"""
        INSERT INTO {table_name} ({column_sql})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_column}) DO NOTHING
    """

    for row in rows:
        postgres_db.execute(sql, [row[column] for column in columns])

    print(f"MIGRATED {table_name}: {len(rows)} row(s)")


def reset_sequence(postgres_db, table_name):
    postgres_db.execute(f"""
        SELECT setval(
            pg_get_serial_sequence('{table_name}', 'id'),
            COALESCE((SELECT MAX(id) FROM {table_name}), 1),
            (SELECT COUNT(*) FROM {table_name}) > 0
        )
    """)


def main():
    if not DATABASE_URL:
        raise SystemExit("Set DATABASE_URL to your Render Postgres internal database URL first.")

    import main as app_module

    sqlite_db = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_db.row_factory = sqlite3.Row

    with app_module.app.app_context():
        postgres_db = app_module.get_db()
        app_module.init_db()
        app_module.ensure_lead_action_columns()

        for table_name in TABLES:
            migrate_table(sqlite_db, postgres_db, table_name)

        for table_name in SERIAL_TABLES:
            reset_sequence(postgres_db, table_name)

        postgres_db.commit()

    sqlite_db.close()
    print("DONE")


if __name__ == "__main__":
    main()
