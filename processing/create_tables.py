import sqlite3

conn = sqlite3.connect('stats.sqlite')
c = conn.cursor()

c.execute('''
        CREATE TABLE IF NOT EXISTS stats
        (id INTEGER PRIMARY KEY ASC,
        num_ci_readings INTEGER NOT NULL,
        num_bc_readings INTEGER NOT NULL,
        max_numPeople INTEGER,
        max_numNights INTEGER,
        last_updated VARCHAR(100) NOT NULL)
''')


conn.commit()
conn.close()
