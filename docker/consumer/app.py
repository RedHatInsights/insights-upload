import json
import logging

from time import sleep
from confluent_kafka import Consumer, Producer, KafkaError

# configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('test-consumer')

logger.info('connecting...')

c = Consumer({
    'bootstrap.servers': 'kafka:29092',
    'group.id': 'testgroup',
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})

c.subscribe(['testareno'])

p = Producer({'bootstrap.servers': 'kafka:29092'})


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        logger.info('Message delivery failed: {}'.format(err))
    else:
        logger.info('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


logger.info("Entering message processing loop.")


while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break

    logger.info('Received message: {}'.format(msg.value().decode('utf-8')))

    result = json.loads(msg.value().decode('utf-8'))

    validation = {
        'hash': result['hash'],
        'validation': 'failure'
    }

    sleep(10)  # Mock file verification and validation
    logger.info('Replying with: {}'.format(json.dumps(validation)))

    p.poll(0)
    p.produce('uploadvalidation', json.dumps(validation), callback=delivery_report)
    p.flush()
