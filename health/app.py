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
import time
from apscheduler.schedulers.background import BackgroundScheduler
from healths import Healths
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from flask_cors import CORS, cross_origin


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


def get_status(body):
    for sec in range(app_config["scheduler"]["max_tries"]):
        receiver_body = {
            "confirmationCode": "Test",
            "name": "test",
            "roomNum": 111,
            "nights": 1,
            "arriveDate": "2022-01-01"
        }
        receiver_check = requests.post(
            app_config["receiver_url"],
            headers={"Content-Type": "application/json"},
            data=json.dumps(receiver_body),
        )
        if receiver_check.status_code == 200:
            body["receiver"] = "Up"
            break
        else:
            body["receiver"] = "Down"
            time.sleep(app_config["scheduler"]['sleep'])
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        #     for sec in range(app_config["scheduler"]["max_tries"]):
        #         res = sock.connect_ex(('localhost', 8080))
        #         if res == 0:
        #             body["receiver"] = "Down"
        #             time.sleep(app_config["scheduler"]['sleep'])
        #         else:
        #             body["receiver"] = "Up"
        #             break

        for sec in range(app_config["scheduler"]["max_tries"]):
            res = sock.connect_ex(('localhost', 8090))
            if res == 0:
                body["storage"] = "Down"
                time.sleep(app_config["scheduler"]['sleep'])
            else:
                body["storage"] = "Up"
                break

        for sec in range(app_config["scheduler"]["max_tries"]):
            res = sock.connect_ex(('localhost', 8100))
            if res == 0:
                body["processing"] = "Down"
                time.sleep(app_config["scheduler"]['sleep'])
            else:
                body["processing"] = "Up"
                break

        for sec in range(app_config["scheduler"]["max_tries"]):
            res = sock.connect_ex(('localhost', 8110))
            if res == 0:
                body["audit"] = "Down"
                time.sleep(app_config["scheduler"]['sleep'])
            else:
                body["audit"] = "Up"
                break
    return body


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

    static_list = get_status(static_list)

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

    body = get_status(body)

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
    create_table(app_config["datastore"]["filename"])
    init_scheduler()
    app.run(port=8120, use_reloader=False)
