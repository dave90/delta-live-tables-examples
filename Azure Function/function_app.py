import azure.functions as func

import logging
import os
import random

from utils import read_keys, read_config, produce_kafka
from constants import FILE_JSON,RETURN_SIZE,AKV_CONF,KAFKA_CONF,TOPIC

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="KafkaProducer")
def KafkaProducer(req: func.HttpRequest) -> func.HttpResponse:
    script_dir = os.path.dirname(__file__)

    logging.info('Python HTTP trigger function processed a request.')

    logging.info("Reading akv config")
    akv_conf = read_config(os.path.join(script_dir,AKV_CONF))
    KAFKA_ID = akv_conf["KAFKA_ID"]
    KAFKA_KEY = akv_conf["KAFKA_KEY"]
    logging.info("Reading keys")
    secrets = read_keys(
        (akv_conf["KAFKA_ID"], akv_conf["KAFKA_KEY"]),akv_conf["KEY_VAULT_URL"],akv_conf["client_id"]
    )
    logging.info(f"Kafka id: {secrets[KAFKA_ID]}")
    logging.info(f"Kafka key len: {len(secrets[KAFKA_KEY])}")

    logging.info(f"Reading json data")
    f = open(os.path.join(script_dir,FILE_JSON),"r")
    contents = f.readlines()
    logging.info(f"Extract random slice from json data")
    index = random.randint(0,len(contents)-RETURN_SIZE)
    logging.info(f"Index {index}, of {len(contents)}")
    contents_random = contents[ index : index+RETURN_SIZE]

    logging.info(f"Reading kafka config")
    kafka_conf_path = os.path.join(script_dir,KAFKA_CONF)
    kafka_conf = read_config(kafka_conf_path)
    kafka_conf["sasl.username"]=secrets[KAFKA_ID]
    kafka_conf["sasl.password"]=secrets[KAFKA_KEY]

    logging.info(f"Sending to kafka")
    produce_kafka(contents_random,kafka_conf, TOPIC)

    return func.HttpResponse(f"Contents extracted:\n {contents_random}")

