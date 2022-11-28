import yaml
import logging
import logging.config
import json
import os
import connexion
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
from flask_cors import CORS, cross_origin
#


def get_health():
    return 200


def get_check_in_reading(index):
    """ Get CI Reading in History """
    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    # Here we reset the offset on start so that we retrieve
    # messages at the beginning of the message queue.
    # To prevent the for loop from blocking, we set the timeout to
    # 100ms. There is a risk that this loop never stops if the
    # index is large and messages are constantly being received!
    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True,
        consumer_timeout_ms=1000)

    logger.info("Retrieving CI at index %d" % index)

    try:
        idx = -1
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            logger.info("Message: %s" % msg)
            payload = msg["payload"]
            if msg["type"] == "ci":
                idx += 1
                if idx == index:
                    return payload, 200
    except:
        logger.error("No more messages found")
    logger.error("Could not find CI at index %d" % index)
    return {"message": "Not Found"}, 404


def get_booking_confirm_reading(index):
    """ Get BC Reading in History """
    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
    # Here we reset the offset on start so that we retrieve
    # messages at the beginning of the message queue.
    # To prevent the for loop from blocking, we set the timeout to
    # 100ms. There is a risk that this loop never stops if the
    # index is large and messages are constantly being received!
    consumer = topic.get_simple_consumer(
        reset_offset_on_start=True,
        consumer_timeout_ms=1000)

    logger.info("Retrieving BC at index %d" % index)

    try:
        idx = -1
        for msg in consumer:
            msg_str = msg.value.decode('utf-8')
            msg = json.loads(msg_str)
            logger.info("Message: %s" % msg)
            payload = msg["payload"]
            if msg["type"] == 'bc':
                idx += 1
                if idx == index:
                    return payload, 200
    except:
        logger.error("No more messages found")

    logger.error("Could not find BC at index %d" % index)
    return {"message": "Not Found"}, 404


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", base_path="/audit_log",
            strict_validation=True, validate_responses=True)

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"
    CORS(app.app)
    app.app.config['CORS_HEADERS'] = 'Content-Type'

with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


if __name__ == "__main__":
    app.run(port=8110)
