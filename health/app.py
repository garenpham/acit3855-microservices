import datetime
import json
import connexion
from connexion import NoContent
import requests
import yaml
import logging
import logging.config
import uuid
import sqlite3
import os
import socket
from apscheduler.schedulers.background import BackgroundScheduler
from healths import Healths
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from flask_cors import CORS, cross_origin

# Your functions here


def create_table(sql_path):
    conn = sqlite3.connect(sql_path)
    c = conn.cursor()

    c.execute('''
            CREATE TABLE IF NOT EXISTS healths
            (id INTEGER PRIMARY KEY ASC,
            receiver VARCHAR(25) NOT NULL,
            storage VARCHAR(25) NOT NULL,
            processing VARCHAR(25) NOT NULL,
            audit VARCHAR(25) NOT NULL,
            last_updated VARCHAR(100) NOT NULL)
    ''')

    conn.commit()
    conn.close()


def get_health():
    '''Get all the Stats objects from the database in descending order'''
    logger.info("Requesting healths has started")

    session = DB_SESSION()
    results = session.query(Healths).order_by(
        Healths.last_updated.desc()).all()
    session.close()

    results_list = []

    if results != []:
        for reading in results:
            results_list.append(reading.to_dict())
        logger.debug(results_list)
        logger.info("Requesting healths has ended")
        return results_list[0], 200

    static_list = {
        "receiver": "Down",
        "storage": "Down",
        "processing": "Down",
        "audit": "Down",
        "last_updated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    }

    logger.error("Healths do not exist")
    logger.info("Requesting healths has ended")
    return static_list, 404


def create_healths(body):
    '''write a new Healths object to the database'''

    session = DB_SESSION()

    healths = Healths(body["receiver"],
                      body["storage"],
                      body["processing"],
                      body["audit"],
                      datetime.datetime.strptime(body["last_updated"],
                                                 "%Y-%m-%dT%H:%M:%S"))
    session.add(healths)
    session.commit()
    session.close()

    return NoContent, 201


def populate_healths():
    """ Periodically update healths """
    logger.info("Start Health Check")

    currentStat = get_health()

    body = currentStat[0]

    current_timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        res = sock.connect_ex(('localhost', 8080))
    if res == 0:
        body["receiver"] = "Down"
    else:
        body["receiver"] = "Up"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        res = sock.connect_ex(('localhost', 8090))
    if res == 0:
        body["storage"] = "Down"
    else:
        body["storage"] = "Up"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        res = sock.connect_ex(('localhost', 8100))
    if res == 0:
        body["processing"] = "Down"
    else:
        body["processing"] = "Up"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        res = sock.connect_ex(('localhost', 8110))
    if res == 0:
        body["audit"] = "Down"
    else:
        body["audit"] = "Up"
    create_healths(body)

    logger.debug("Updated info: " + json.dumps(body))
    logger.info("Health Check is ended")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_healths,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir="")
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


sqliteUrl = "sqlite:///%s" % app_config["datastore"]["filename"]
logger.info(sqliteUrl)
DB_ENGINE = create_engine(sqliteUrl)
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

if __name__ == "__main__":
    sql_path = app_config["datastore"]["filename"]
    if not os.path.isfile(sql_path):
        create_table(sql_path)
    init_scheduler()
    app.run(port=8120, use_reloader=False)
