# Go-Live Checklist

## Render environment

- Set `SECRET_KEY` to a long random value.
- Set `INTERNAL_API_TOKEN` to a long random value.
- Set `SESSION_COOKIE_SECURE=1`.
- Set `FRONTEND_ORIGIN` to the live website origin, for example `https://example.com`.
- Set `PRIVACY_NOTICE_URL` to the public privacy notice URL.
- Leave `ENABLE_CSV_EXPORTS` unset or set to `0` unless bulk exports are specifically needed.
- If CSV exports are enabled, set `CSV_EXPORT_MAX_ROWS` to the smallest practical number.
- Optional email settings:
  - `SMTP_HOST`
  - `SMTP_PORT`
  - `SMTP_USERNAME`
  - `SMTP_PASSWORD`
  - `SMTP_USE_TLS=1`
  - `CUSTOMER_EMAIL_FROM`
  - `LEAD_NOTIFICATION_EMAIL`
- Retention settings:
  - `REPORT_RETENTION_DAYS=30`
  - `LEAD_RETENTION_DAYS=365`

## Database

- Move production data from SQLite to a managed database before meaningful traffic.
- Recommended Render path: create a Render Postgres database, then migrate the app to use it.
- Until that migration is done, enable regular backups of `bookings.db`.

## Website checks

- Check `/privacy` and `/agent-terms`, then replace draft wording with solicitor-approved text.
- Complete the full user flow on desktop and mobile.
- Confirm every submitted lead appears in `/leads`.
- Confirm the PDF link opens and expires according to retention policy.
- Confirm marketing consent is optional and privacy notice acceptance is required.
- Confirm UTM parameters are passed from the frontend if running paid campaigns.

## Operational checks

- Confirm new lead email notifications are received if SMTP is configured.
- Confirm agents can update statuses, add notes, and complete tasks.
- Confirm CSV export is disabled in production, or works only for approved users if deliberately enabled.
- Review the report disclaimer before launch.
- Confirm `ENABLE_TEST_TOOLS` is unset or set to `0` in production.
- Confirm `/admin/system` has no critical missing production settings except planned Postgres migration during pilot.
- Run 5-10 dummy leads through the live site before public launch.
