import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer,SerializationContext,MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from datetime import datetime

import pandas as pd
from typing import List


FILE_PATH="D:/ineron_DataEngineer/Kafka/Confluent-Kafka-Project-2/restaurant_orders.csv"
columns=['Order_Number','Order_Date','Item_Name','Quantity','Product_Price','Total_products']


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
    def __init__(self,record:dict) -> None:
        for k,v in record.items():
            setattr(self,k,v)
        self.record=record

    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Order(record=data)
    
    def __str__(self) -> str:
        return f"{self.record}"
    

def get_order_instance(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,:]
    orders:List[Order]=[]

    for data in df.values:
        order=Order(dict(zip(columns,data)))
        orders.append(order)
        yield order


def order_to_dict(order:Order,ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """
    return order.record


def delivery_report(err,msg):
    if err is not None:
        print("Delivery failed for user record {}:{}".format(msg.key(),err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    topic='restaurent-take-away-data'
    schema_str=schema_registry_client.get_latest_version(topic+'-value').schema.schema_str
    print(schema_str)
    print(type(schema_str))
        # subjects = sr.get_subjects()
    # for subject in subjects:
    #     schema = sr.get_latest_version(subject)
    #     print(schema.version)
    #     print(schema.schema_id)
    #     print(schema.schema.schema_str)

    #serializing key
    string_serializer = StringSerializer('utf_8')

    #serializing data
    json_serializer = JSONSerializer(schema_str, schema_registry_client, order_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for order in get_order_instance(file_path=FILE_PATH):

            print(order)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), order_to_dict),
                            value=json_serializer(order, SerializationContext(topic,MessageField.VALUE)),
                            on_delivery=delivery_report)
            break
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("restaurent-take-away-data")
