import datetime
import json
import swagger_ui_bundle
import connexion
from connexion import NoContent
import requests
import yaml
import logging
import logging.config
import uuid
from pykafka import KafkaClient

# Your functions here


def checkIn(body):
    # received_timestamp = datetime.datetime.now()
    # request_data = body
    # current_event = {"received_timestamp": received_timestamp,
    #                  "request_data": request_data}

    trace = str(uuid.uuid4())
    body['trace_id'] = trace

    # url = app_config["eventstoreCheckIn"]["url"]

    # response = requests.post(
    #     url,
    #     headers={"Content-Type": "application/json"},
    #     data=json.dumps(body),
    # )

    logger.info('Received event check in request with a trace id of '+trace)
    # return response.text, response.status_code
    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
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
    # received_timestamp = datetime.datetime.now()
    # request_data = body
    # current_event = {"received_timestamp": received_timestamp,
    #                  "request_data": request_data}

    trace = str(uuid.uuid4())
    body['trace_id'] = trace

    # url = app_config["eventstoreBookingConfirm"]["url"]

    # response = requests.post(
    #     url,
    #     headers={"Content-Type": "application/json"},
    #     data=json.dumps(body),
    # )

    logger.info(
        'Received event booking confirm request with a trace id of '+trace)

    # return response.text, response.status_code
    hostname = "%s:%d" % (
        app_config["events"]["hostname"], app_config["events"]["port"])
    client = KafkaClient(hosts=hostname)
    topic = client.topics[str.encode(app_config["events"]["topic"])]
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
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

print(app_config)

if __name__ == "__main__":
    app.run(port=8080)
