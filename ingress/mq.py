import collections

from tornado.ioloop import IOLoop
from kafkahelpers import ReconnectingClient

import metrics
import config
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from kafka.errors import KafkaError

kafka_consumer = AIOKafkaConsumer(config.VALIDATION_QUEUE, loop=IOLoop.current().asyncio_loop,
                                  bootstrap_servers=config.MQ, group_id=config.MQ_GROUP_ID)
kafka_producer = AIOKafkaProducer(loop=IOLoop.current().asyncio_loop, bootstrap_servers=config.MQ,
                                  request_timeout_ms=10000, connections_max_idle_ms=None)
CONSUMER = ReconnectingClient(kafka_consumer, "consumer")
PRODUCER = ReconnectingClient(kafka_producer, "producer")

produce_queue = collections.deque()
current_archives = []
metrics.uploads_produce_queue_size.set_function(lambda: len(produce_queue))

class Message(object):

    def __init__(self, topic, data):

        self.topic = topic
        self.id = data.get('id')
        self.url = data.get('url')
        self.service = data.get('service')
        self.request_id = data.get('request_id')
        self.account = data.get('account')
        self.principal = data.get('principal')
        self.b64_identity = data.get('b64_identity')
        self.satellite_managed = data.get('satellite_managed')

    def cleanup_empties(self, contents):

        for k, v in contents['msg']:
            if v is None:
                contents['msg'].pop(k)

        return contents

    def msg(self):

        contents = {"topic": self.topic,
                    "msg": {
                        "id": self.id,
                        "url": self.url,
                        "service": self.service,
                        "request_id": self.request_id,
                        "payload_id": self.request_id,
                        "account": self.account,
                        "principal": self.principal,
                        "b64_identity": self.b64_identity,
                        "satellite_managed": self.satellite_managed,
                    }}

        return self.cleanup_empties(contents)
