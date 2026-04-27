# Postgres Migration Plan

The app currently uses SQLite. Before multi-agent production use, move to Render Postgres.

## Recommended Render steps

1. Create a Render Postgres database.
2. Copy its internal database URL for the Render web service.
3. Add the internal URL to the web service as `DATABASE_URL`.
4. Deploy the app update that adds Postgres support.
5. Export existing SQLite data.
6. Import the data into Postgres with `migrate_sqlite_to_postgres.py`.
7. Run a staging deploy and test:
   - login
   - lead capture
   - PDF generation
   - referrals
   - tasks
   - CSV export
8. Switch production once staging is verified.

## Migration command

Run this locally or from a secure shell where the SQLite file is available. If running locally, use Render's external Postgres URL for the one-off migration:

```bash
DATABASE_URL="postgres://..." SQLITE_DB_PATH="bookings.db" python3 migrate_sqlite_to_postgres.py
```

Use the Render internal database URL for app-to-database traffic inside Render after the migration. Keep both URLs private.

## Current readiness

The system readiness page checks whether `DATABASE_URL` is present:

`/admin/system`

The application now uses SQLite when `DATABASE_URL` is not set and Postgres when `DATABASE_URL` is set.
