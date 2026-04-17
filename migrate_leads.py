import sqlite3

conn = sqlite3.connect("bookings.db")
cursor = conn.cursor()

commands = [
    "ALTER TABLE leads ADD COLUMN source TEXT DEFAULT 'website'",
    "ALTER TABLE leads ADD COLUMN created_at TEXT",
    "ALTER TABLE leads ADD COLUMN contacted_at TEXT",
    "ALTER TABLE leads ADD COLUMN valuation_booked_at TEXT",
    "ALTER TABLE leads ADD COLUMN notes TEXT",
]

for command in commands:
    try:
        cursor.execute(command)
        print("SUCCESS:", command)
    except Exception as e:
        print("SKIPPED:", command, "->", e)

conn.commit()
conn.close()

print("DONE")