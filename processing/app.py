import datetime
import json
import connexion
from connexion import NoContent
import requests
import yaml
import logging
import logging.config
import uuid
from apscheduler.schedulers.background import BackgroundScheduler
from stats import Stats
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from flask_cors import CORS, cross_origin

# Your functions here


def get_stats():
    '''Get all the Stats objects from the database in descending order'''
    logger.info("Requesting process has started")

    session = DB_SESSION()
    results = session.query(Stats).order_by(Stats.last_updated.desc()).all()
    session.close()

    results_list = []

    if results != []:
        for reading in results:
            results_list.append(reading.to_dict())
        logger.debug(results_list)
        logger.info("Requesting process has ended")
        return results_list[0], 200

    static_list = {
        "num_ci_readings": 0,
        "num_bc_readings": 0,
        "max_numPeople": 0,
        "max_numNights": 0,
        "last_updated": datetime.datetime.strptime(datetime.datetime.now(),
                                                   "%Y-%m-%dT%H:%M:%S")
    }

    logger.error("Statistics do not exist")
    logger.info("Requesting process has ended")
    return static_list, 404


def create_stats(body):
    '''write a new Stats object to the database'''

    session = DB_SESSION()

    stats = Stats(body["num_ci_readings"],
                  body["num_bc_readings"],
                  body["max_numPeople"],
                  body["max_numNights"],
                  datetime.datetime.strptime(body["last_updated"],
                                             "%Y-%m-%dT%H:%M:%S"))
    session.add(stats)
    session.commit()
    session.close()

    return NoContent, 201


def populate_stats():
    """ Periodically update stats """
    logger.info("Start Periodic Processing")

    currentStat = get_stats()

    body = currentStat[0]

    current_timestamp = datetime.datetime.now()

    # checkinUrl = app_config['eventstoreCheckIn']['url']
    # bookingUrl = app_config['eventstoreBookingConfirm']['url']

    # getCheckinResponse = requests.get(
    #     checkinUrl,
    #     headers={"Content-Type": "application/json"},
    #     params={"timestamp":
    #             currentTime.strftime("%Y-%m-%dT%H:%M:%S")}
    # )

    getCheckinResponse = requests.get(
        app_config['eventstore']['url']+"/checkIn?start_timestamp=" +
        body["last_updated"] + "&end_timestamp=" +
        current_timestamp)

    trace = str(uuid.uuid4())

    checkinList = []
    bookingConfirmList = []

    if getCheckinResponse.status_code == 200:
        checkinList = getCheckinResponse.json()
        logger.info("Number of events received: %s" % (len(checkinList)))
        logger.debug(
            'Stored event check in request with a trace id of ' + trace)
    else:
        logger.error("Request data failed")

    # getBookingConfirmResponse = requests.get(
    #     bookingUrl,
    #     headers={"Content-Type": "application/json"},
    #     params={"timestamp":
    #             currentTime.strftime("%Y-%m-%dT%H:%M:%S")}
    # )
    getBookingConfirmResponse = requests.get(
        app_config['eventstore']['url']+"/bookingConfim?start_timestamp=" +
        body["last_updated"] + "&end_timestamp=" +
        current_timestamp)

    trace = str(uuid.uuid4())
    if getBookingConfirmResponse.status_code == 200:
        bookingConfirmList = getBookingConfirmResponse.json()
        logger.info("Number of events received: %s" %
                    (len(bookingConfirmList)))
        logger.debug(
            'Stored event check in request with a trace id of ' + trace)
    else:
        logger.error("Request data failed")

    body['last_updated'] = current_timestamp
    if checkinList != []:
        body["num_ci_readings"] += len(checkinList)

        body["max_numPeople"] = max(
            item["numPeople"] for item in checkinList)

    if bookingConfirmList != []:
        body["num_bc_readings"] += len(bookingConfirmList)
        body["max_numNights"] = max(item["nights"]
                                    for item in bookingConfirmList)

    create_stats(body)

    logger.debug("Updated info: " + json.dumps(body))
    logger.info("Periodic Processing is ended")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=app_config['scheduler']['period_sec'])
    sched.start()


app = connexion.FlaskApp(__name__, specification_dir="")
CORS(app.app)
app.app.config['CORS_HEADERS'] = 'Content-Type'
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

DB_ENGINE = create_engine("sqlite:///%s" %
                          app_config["datastore"]["filename"])
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

print(app_config)

if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100, use_reloader=False)
