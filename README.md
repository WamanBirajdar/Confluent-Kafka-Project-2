![download](https://user-images.githubusercontent.com/34875169/169837256-b5cce5b4-0b10-4a5b-82b7-926f10690437.png)


To use confluent kafka we need following details from Confluent dashboard.

```
confluentClusterName = ""
confluentBootstrapServers = ""
confluentTopicName = ""
confluentApiKey = ""
confluentSecret = ""
```

Add below library in requirements.txt
```
confluent-kafka[avro,json,protobuf]
pyspark==3.2.1
```

# download restaurent data from below mentioned link

Download Data Link -> https://github.com/WamanBirajdar/Confluent-Kafka-Project-2/blob/main/restaurant_orders.csv

Complete the given below task to finish this assignment.

1. Setup Confluent Kafka Account
2. Create one kafka topic named as "restaurent-take-away-data" with 3 partitions
3. Setup key (string) & value (json) schema in the confluent schema registry
4. Write a kafka producer program (python or any other language) to read data records from restaurent data csv file, 
   make sure schema is not hardcoded in the producer code, read the latest version of schema and schema_str from schema registry and use it for
   data serialization.
5. From producer code, publish data in Kafka Topic one by one and use dynamic key while publishing the records into the Kafka Topic
6. Write kafka consumer code and create two copies of same consumer code and save it with different names (kafka_consumer_1.py & kafka_consumer_2.py), 
   again make sure lates schema version and schema_str is not hardcoded in the consumer code, read it automatically from the schema registry to desrialize the data. 
   Now test two scenarios with your consumer code:
    a.) Use "group.id" property in consumer config for both consumers and mention different group_ids in kafka_consumer_1.py & kafka_consumer_2.py,
        apply "earliest" offset property in both consumers and run these two consumers from two different terminals. Calculate how many records each consumer
        consumed and printed on the terminal
    b.) Use "group.id" property in consumer config for both consumers and mention same group_ids in kafka_consumer_1.py & kafka_consumer_2.py,
        apply "earliest" offset property in both consumers and run these two consumers from two different terminals. Calculate how many records each consumer
        consumed and printed on the terminal
        
7. Once above questions are done, write another kafka consumer to read data from kafka topic and from the consumer code create one csv file "output.csv"
   and append consumed records output.csv file
