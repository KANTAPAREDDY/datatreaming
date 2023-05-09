# datastreaming
# .env
RABBIT_USER="Fib17"
RABBIT_PASSWORD="1597"
DB_NAME="DBstreaming"
DB_USER="Nombredor"
DB_PASSWORD="Fibonacci"
DB_ROOT_PASSWORD="1597"

# topic-producer.py
import time

from server import channel

QUEUES = [
    {
        "name": "queue-a",
        "topic": "a"
    },
    {
        "name": "queue-b",
        "topic": "b"
    },
    {
        "name": "queue-c",
        "topic": "c"
    }
]

EVENTS = [
    {
        "topic": "a",
        "body": "event 1"
    },
    {
        "topic": "b",
        "body": "event 1"
    },
    {
        "topic": "c",
        "body": "event 1"
    },
    {
        "topic": "a",
        "body": "event 2"
    },
    {
        "topic": "b",
        "body": "event 2"
    }
]

EXCHANGE_NAME = "topic-exchange-hello-Pascal"

# create exchange
channel.exchange_declare(EXCHANGE_NAME, durable=True, exchange_type='topic')

# create queues
for queue in QUEUES:
    channel.queue_declare(queue=queue['name'])
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue['name'], routing_key=queue['topic'])


# publish event
for event in EVENTS:
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=event['topic'], body=event['body'])
    time.sleep(2)
    print(f"[x] published event `{event['body']}` in topic `{event['topic']}`")


# server.py
import pika

from config import CONFIG

RABBIT_MQ_SERVER = "localhost"

# establish connection with rabbit mq
credentials = pika.PlainCredentials(CONFIG['RABBIT_USER'], CONFIG['RABBIT_PASSWORD'])
parameters = pika.ConnectionParameters(RABBIT_MQ_SERVER, credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()


# consumer.py

import time

from server import channel
logs_files= open("assets\web-server-nginx.log")

QUEUE= [
    {
        "name":"queue-data-lake",
        "routing_key":"logs"
    },
    {
        "name":"queue-data-clean",
        "routing_key":"logs"

    }
]
EXCHANGE_NAME = "topic-exchange-logs"
# create exchange
channel.exchange_declare(EXCHANGE_NAME, durable=True, exchange_type='topic')
# create queue
for queue in QUEUE:
    channel.queue_declare(queue=queue['name'])
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue['name'],routing_key=queue['routing_key'])

# publish event
for line in logs_files:
    #channel.basic_publish(exchange=EXCHANGE_NAME, routing_key=QUEUE_LOGS, body=line)
    #time.sleep(5)
    time.sleep(0.05)
    channel.basic_publish(exchange=EXCHANGE_NAME, routing_key="logs", body=line)



    print(f"[x] published event `{line}` in topic `{queue['routing_key']}`")




import time

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
from pika.spec import BasicProperties
from server import channel


def process_msg(chan: BlockingChannel, method: Basic.Deliver, properties: BasicProperties, body):
    print(f"[{method.routing_key}] event consumed from exchange `{method.exchange}` body `{body}`")


# consume messages from queues
channel.basic_consume(queue="queue-data-clean", on_message_callback=process_msg, auto_ack=True)

channel.basic_consume(queue="queue-a", on_message_callback=process_msg, auto_ack=True)
channel.basic_consume(queue="queue-b", on_message_callback=process_msg, auto_ack=True)
channel.basic_consume(queue="queue-c", on_message_callback=process_msg, auto_ack=True)

channel.start_consuming()

