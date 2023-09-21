
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from confluent_kafka import Producer

import logging
import time

def read_keys(secrets,key_url,client_id):
    logging.info(f"client_id [{client_id}]")
    ret = {}
    try:
        with SecretClient(
            vault_url=key_url,
            credential=DefaultAzureCredential(managed_identity_client_id=client_id),
            logging_enable=True,
        ) as secret_client:
            for secret in secrets:
                ret[secret] = (secret_client.get_secret(secret)).value
    except Exception as excep:
        logging.error("%s", excep)
    return ret

def read_config(config_file):
    logging.info(f"Reading {config_file}")
    conf = {}
    with open(config_file,"r") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                logging.info(f"Reading {line}")
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

def produce_kafka(contents,confs, topic):
    producer = Producer(confs)
    logging.info("Send to kafka")
    for content in contents:
        logging.info(f"Send to topic {topic}: {str(content)}")
        producer.produce(topic, key=str(time.time()), value=str(content))
    producer.flush()
