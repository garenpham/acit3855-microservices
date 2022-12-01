import datetime
import json
import swagger_ui_bundle
import requests
import yaml
import os
import time
import logging
import logging.config
import uuid
import connexion
from pykafka import KafkaClient
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


# Your functions here


def get_health():
    return NoContent, 200


def checkIn(body):
    trace = str(uuid.uuid4())
    body['trace_id'] = trace
    logger.info('Received event check in request with a trace id of '+trace)

    producer = topic.get_sync_producer()
    msg = {"type": "ci",
           "datetime":
           datetime.datetime.now().strftime(
               "%Y-%m-%dT%H:%M:%S"),
           "payload": body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    return NoContent, 201


def bookingConfirm(body):
    trace = str(uuid.uuid4())
    body['trace_id'] = trace
    logger.info(
        'Received event booking confirm request with a trace id of '+trace)

    producer = topic.get_sync_producer()
    msg = {"type": "bc",
           "datetime":
           datetime.datetime.now().strftime(
               "%Y-%m-%dT%H:%M:%S"),
           "payload": body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir="")
app.add_api("openapi.yaml", base_path="/receiver",
            strict_validation=True, validate_responses=True)

with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

print(app_config)

if __name__ == "__main__":
    app.run(port=8080)
