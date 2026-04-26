# Postgres Migration Plan

The app currently uses SQLite. Before multi-agent production use, move to Render Postgres.

## Recommended Render steps

1. Create a Render Postgres database.
2. Copy its internal database URL.
3. Add it to the web service as `DATABASE_URL`.
4. Add a Python Postgres driver to `requirements.txt` when the app is updated to use Postgres.
5. Export existing SQLite data.
6. Import the data into Postgres.
7. Run a staging deploy and test:
   - login
   - lead capture
   - PDF generation
   - referrals
   - tasks
   - CSV export
8. Switch production once staging is verified.

## Current readiness

The system readiness page checks whether `DATABASE_URL` is present:

`/admin/system`

The application still needs a database adapter update before it can use Postgres directly.
