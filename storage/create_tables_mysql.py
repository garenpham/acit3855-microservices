import mysql.connector
import yaml

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

db_conn = mysql.connector.connect(host=app_config["datastore"]["hostname"], user=app_config["datastore"]["user"],
                                  password=app_config["datastore"]["password"])

db_conn.autocommit = True

db_cursor = db_conn.cursor()

db_cursor.execute('''
    CREATE DATABASE IF NOT EXISTS events;
''')

db_cursor.execute('''
    USE events;
''')

db_cursor.execute('''
    CREATE TABLE check_in
    (id INT NOT NULL AUTO_INCREMENT,
    reservationCode VARCHAR(250) NOT NULL,
    name VARCHAR(250) NOT NULL,
    initialDeposit INTEGER NOT NULL,
    numPeople INTEGER NOT NULL,
    date_created VARCHAR(100) NOT NULL,
    trace_id VARCHAR(100),
    CONSTRAINT check_in_pk PRIMARY KEY (id))
''')
db_cursor.execute('''
    CREATE TABLE booking_confirm
    (id INT NOT NULL AUTO_INCREMENT,
    confirmationCode VARCHAR(250) NOT NULL,
    name VARCHAR(250) NOT NULL,
    roomNum INTEGER NOT NULL,
    nights INTEGER NOT NULL,
    arriveDate VARCHAR(250),
    date_created VARCHAR(100) NOT NULL,
    trace_id VARCHAR(250),
    CONSTRAINT booking_confirm_pk PRIMARY KEY (id))
''')
db_conn.commit()
db_conn.close()
