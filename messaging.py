import asyncio
import json
import logging
import collections
from utils import mnm, config
from ctx import get_extra
from defer import defer
from kafka.errors import KafkaError
from importlib import import_module
from prometheus_async.aio import time as prom_time
from kafkahelpers import ReconnectingClient
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
logger = logging.getLogger('upload-service')

storage = import_module("utils.storage.{}".format(config.STORAGE_DRIVER))

# local queue for pushing items into kafka, this queue fills up if kafka goes down
produce_queue = collections.deque()
mnm.uploads_produce_queue_size.set_function(lambda: len(produce_queue))


@prom_time(mnm.uploads_handle_file_seconds)
async def handle_file(msg):
    """Determine which bucket to put a payload in based on the message
       returned from the validating service.

    storage.copy operations are not async so we offload to the executor

    Arguments:
        msgs -- list of kafka messages consumed on validation topic
    """
    extra = get_extra()

    try:
        with mnm.uploads_json_loads.labels(key="handle_file").time():
            data = json.loads(msg.value)
        logger.debug("handling_data: %s", data, extra=extra)
    except Exception:
        logger.error("handle_file(): unable to decode msg as json: {}".format(msg.value), extra=extra)
        return

    if 'payload_id' not in data and 'hash' not in data:
        logger.error("payload_id or hash not in message. Payload not removed from permanent.", extra=extra)
        return

    # get the payload_id. Getting the hash is temporary until consumers update
    payload_id = data['payload_id'] if 'payload_id' in data else data.get('hash')
    extra["request_id"] = payload_id
    extra["account"] = account = data.get('account', 'unknown')
    result = data.get('validation', 'unknown')

    logger.info('processing message: payload [%s] - %s', payload_id, result, extra=extra)

    r = await defer(storage.ls, storage.PERM, payload_id)

    if r['ResponseMetadata']['HTTPStatusCode'] == 200:
        if result.lower() == 'success':
            mnm.uploads_validated.inc()

            url = await defer(storage.get_url, storage.PERM, payload_id)
            data = {
                'topic': 'platform.upload.available',
                'msg': {
                    'id': data.get('id'),
                    'url': url,
                    'service': data.get('service'),
                    'payload_id': payload_id,
                    'account': account,
                    'principal': data.get('principal'),
                    'b64_identity': data.get('b64_identity'),
                    'satellite_managed': data.get('satellite_managed'),
                    'rh_account': account,  # deprecated key, temp for backward compatibility
                    'rh_principal': data.get('principal'),  # deprecated key, temp for backward compatibility
                }
            }
            mnm.uploads_produced_to_topic.labels(topic="platform.upload.available").inc()
            produce_queue.append(data)
            logger.info(
                "data for topic [%s], payload_id [%s], inv_id [%s] put on produce queue (qsize now: %d)",
                data['topic'], payload_id, data["msg"].get("id"), len(produce_queue), extra=extra)
            logger.debug("payload_id [%s] data: %s", payload_id, data, extra=extra)
        elif result.lower() == 'failure':
            mnm.uploads_invalidated.inc()
            logger.info('payload_id [%s] rejected', payload_id, extra=extra)
            url = await defer(storage.copy, storage.PERM, storage.REJECT, payload_id, account)
        elif result.lower() == 'handoff':
            mnm.uploads_handed_off.inc()
            logger.info('payload_id [%s] handed off', payload_id, extra=extra)
        else:
            logger.info('Unrecognized result: %s', result.lower(), extra=extra)
    else:
        logger.info('payload_id [%s] no longer in permanent bucket', payload_id, extra=extra)


async def handle_validation(client):
    data = await client.getmany(timeout_ms=1000, max_records=30)
    for tp, msgs in data.items():
        if tp.topic == config.VALIDATION_QUEUE:
            logger.info("Processing %s messages from topic [%s]", len(msgs), tp.topic, extra={
                "topic": tp.topic
            })
            # TODO: Figure out how to properly handle failures
            await asyncio.gather(*[handle_file(msg) for msg in msgs])


def make_preprocessor(queue=None):
    queue = produce_queue if queue is None else queue

    async def send_to_preprocessors(client):
        extra = get_extra()
        if not queue:
            await asyncio.sleep(0.1)
        else:
            try:
                item = queue.popleft()
            except Exception:
                logger.exception("Failed to popleft", extra=extra)
                return

            try:
                topic, msg, payload_id = item['topic'], item['msg'], item['msg'].get('payload_id')
                extra["account"] = msg["account"]
                extra["request_id"] = payload_id
                mnm.uploads_popped_to_topic.labels(topic=topic).inc()
            except Exception:
                logger.exception("Bad data from produce_queue.", extra={"item": item, **extra})
                return

            logger.info(
                "Popped data from produce queue (qsize now: %d) for topic [%s], payload_id [%s]: %s",
                len(queue), topic, payload_id, msg, extra=extra)
            try:
                with mnm.uploads_json_dumps.labels(key="send_to_preprocessors").time():
                    data = json.dumps(msg)
                with mnm.uploads_send_and_wait_seconds.time():
                    await client.send_and_wait(topic, data.encode("utf-8"))
                logger.info("send data for topic [%s] with payload_id [%s] succeeded", topic, payload_id, extra=extra)
            except KafkaError:
                queue.append(item)
                logger.error(
                    "send data for topic [%s] with payload_id [%s] failed, put back on queue (qsize now: %d)",
                    topic, payload_id, len(queue), extra=extra)
                raise
            except Exception:
                logger.exception("Failure to send_and_wait. Did *not* put item back on queue.",
                                 extra={"queue_msg": msg, **extra})

    return send_to_preprocessors


def start(loop):
    kafka_consumer = AIOKafkaConsumer(config.VALIDATION_QUEUE, loop=loop.asyncio_loop,
                                      bootstrap_servers=config.MQ, group_id=config.MQ_GROUP_ID)
    kafka_producer = AIOKafkaProducer(loop=loop.asyncio_loop, bootstrap_servers=config.MQ,
                                      request_timeout_ms=10000, connections_max_idle_ms=None)
    CONSUMER = ReconnectingClient(kafka_consumer, "consumer")
    PRODUCER = ReconnectingClient(kafka_producer, "producer")
    loop.spawn_callback(CONSUMER.get_callback(handle_validation))
    loop.spawn_callback(PRODUCER.get_callback(make_preprocessor(produce_queue)))
