import yaml
import logging
import logging.config
import time
import datetime
import json
import os
import connexion
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from base import Base
from check_in import CheckIn
from booking_confirm import BookingConfirm
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
from connexion import NoContent

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

urlCreate = "mysql+pymysql://" + \
    app_config["datastore"]["user"]+":"+app_config["datastore"]["password"] + \
    "@"+app_config["datastore"]["hostname"]+":" + \
    str(app_config["datastore"]["port"])+"/"+app_config["datastore"]["db"]

DB_ENGINE = create_engine(urlCreate)
Base.metadata.bind = DB_ENGINE
DB_SESSION = sessionmaker(bind=DB_ENGINE)

# Your functions here


def get_health():
    return NoContent, 200


def get_checkin_info(start_timestamp, end_timestamp):
    """ Gets new check in info after the timestamp """
    session = DB_SESSION()
    start_timestamp_datetime = datetime.datetime.strptime(
        start_timestamp, "%Y-%m-%dT%H:%M:%S")
    end_timestamp_datetime = datetime.datetime.strptime(
        end_timestamp, "%Y-%m-%dT%H:%M:%S")

    readings = session.query(CheckIn).filter(and_(CheckIn.date_created >= start_timestamp_datetime,
                                                  CheckIn.date_created < end_timestamp_datetime)).all()
    results_list = []
    for reading in readings:
        results_list.append(reading.to_dict())

    session.close()

    logger.info("Query for check in info after %s returns %d results" %
                (end_timestamp, len(results_list)))
    return results_list, 200


def checkIn(body):
    """Report a check in reading"""

    session = DB_SESSION()

    print(app_config)

    ci = CheckIn(
        body["reservationCode"],
        body["name"],
        body["initialDeposit"],
        body["numPeople"],
        body["trace_id"]
    )

    session.add(ci)
    session.commit()
    session.close()

    logger.debug('Stored event check in request with a trace id of ' +
                 body["trace_id"])

    return NoContent, 201


def get_bookingConfirm_info(start_timestamp, end_timestamp):
    """ Gets new booking confirm after the timestamp """
    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(
        start_timestamp, "%Y-%m-%dT%H:%M:%S")
    end_timestamp_datetime = datetime.datetime.strptime(
        end_timestamp, "%Y-%m-%dT%H:%M:%S")

    readings = session.query(BookingConfirm).filter(and_(BookingConfirm.date_created >= start_timestamp_datetime,
                                                         BookingConfirm.date_created < end_timestamp_datetime)).all()
    results_list = []
    for reading in readings:
        results_list.append(reading.to_dict())

    session.close()

    logger.info("Query for booking confirmations info after %s returns %d results" %
                (end_timestamp, len(results_list)))
    return results_list, 200


def bookingConfirm(body):
    """Report a booking confirmation"""

    session = DB_SESSION()

    bc = BookingConfirm(
        body["confirmationCode"],
        body["name"],
        body["roomNum"],
        body["nights"],
        body["arriveDate"],
        body["trace_id"]
    )

    session.add(bc)
    session.commit()
    session.close()

    logger.debug('Stored event booking confirm request with a trace id of ' +
                 body["trace_id"])

    return NoContent, 201


def process_messages():
    """ Process event messages """
    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])

    for connecting in range(app_config["max_tries"]):
        try:
            client = KafkaClient(hosts=hostname)
            topic = client.topics[str.encode(app_config["events"]["topic"])]
            break
        except Exception:
            time.sleep(app_config["sleep"])
            continue

    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).
    consumer = topic.get_simple_consumer(consumer_group=b'event_group', reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.LATEST)
    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)
        payload = msg["payload"]
        if msg["type"] == "ci":  # Change this to your event type
            # Store the event1 (i.e., the payload) to the DB
            checkIn(payload)
        elif msg["type"] == "bc":  # Change this to your event type
            # Store the event2 (i.e., the payload) to the DB
            bookingConfirm(payload)
        # Commit the new message as being read
        consumer.commit_offsets()


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info("Connecting to DB. Hostname: %s, Port:%d" % (
    app_config["datastore"]["hostname"], app_config["datastore"]["port"]))


if __name__ == "__main__":
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)
