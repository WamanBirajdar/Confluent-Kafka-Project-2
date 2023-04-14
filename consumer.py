import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer


API_KEY = 'INA5FB7VZYKJVALB'
ENDPOINT_SCHEMA_URL  = 'https://psrc-ko92v.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'et0cKVbTz1nR2lz0KIse8aqnDx9m0iZOgfhpyPbLmgVQBJrgtI89hf5RP7kePcmc'
BOOTSTRAP_SERVER = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = '4ZNY6VULLPOP357G'
SCHEMA_REGISTRY_API_SECRET = 'e0FcYesb4L7hmuNlO2yuQIPBlv5K6X0NRqLD4uezt6JafpIxn0PICTSb9VX+jVu8'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Order:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):
    schema_str="""
        {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "http://example.com/myURI.schema.json",
    "title": "SampleRecord",
    "description": "Sample schema to help you get started.",
    "type": "object",
    "additionalProperties": false,
    "properties": {
        "Order_Number": {
        "type": "number",
        "description": "The integer type is used for integral numbers."
        },
        "Order_Date": {
        "type": "string",
        "description": "The number type is used for any numeric type, either integers or floating point numbers."
        },
        "Item_Name": {
        "type": "string",
        "description": "The string type is used for strings of text."
        },
        "Quantity": {
        "type": "number",
        "description": "The number type is used for any numeric type, either integers or floating point numbers."
        },
        "Product_Price": {
        "type": "number",
        "description": "The number type is used for any numeric type, either integers or floating point numbers."
        },
        "Total_products": {
        "type": "number",
        "description": "The number type is used for any numeric type, either integers or floating point numbers."
        }
    }
    }
  
    """  
    json_deserializer = JSONDeserializer(schema_str,from_dict=Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if car is not None:
                print("User record {}: order: {}\n"
                      .format(msg.key(), car))
        except KeyboardInterrupt:
            break

    consumer.close()
main("restaurent-take-away-data")