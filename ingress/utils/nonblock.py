import asyncio
import logging
import json
from concurrent.futures import ThreadPoolExecutor

from tornado.ioloop import IOLoop
from prometheus_async.aio import time as prom_time
from kafka.errors import KafkaError

from mq import Message, produce_queue, current_archives
from __init__ import storage
import metrics
import config
import log

logger = logging.getLogger("ingress")

thread_pool_executor = ThreadPoolExecutor(max_workers=config.MAX_WORKERS)
metrics.uploads_executor_qsize.set_function(lambda: thread_pool_executor._work_queue.qsize())

async def defer(*args):
    try:
        name = args[0].__name__
    except Exception:
        name = "unknown"

    with metrics.uploads_run_in_executor.labels(function=name).time():
        return await IOLoop.current().run_in_executor(None, *args)

async def decode_msg(func, msg, extra):
    try:
        with metrics.uploads_json_loads.labels(key="process_file").time():
            data = json.loads(msg.value)
        logger.debug("decoding json", extra=extra)
        return data
    except Exception:
        logger.error(f"{func}(): unable to decode msg as json", extra=extra)


async def succeed_msg(topic, data, extra):

    metrics.uploads_validated.inc()
    try:
        message = Message(topic, data)
        metrics.uploads_produced_to_topic.labels(topic="platform.upload.available")
        produce_queue.append(message.msg())
        current_archives.append(data["request_id"])
        logger.info(
            f"data for topic [{message.topic}], request_id [{data['request_id']}], inv_id [{message.id}, put on produce queue (qsize now: {len(produce_queue)})",
            extra=extra
        )
        logger.debug(f"request_id [{data['request_id']}] data: {message.produce()}", extra=extra)
    except Exception:
        logger.exception("Failure while handling success.", extra=extra)


async def fail_msg(topic, data, extra):

    metrics.uploads_invalidated.inc()
    logger.info(f"request_id [{data['request_id']} rejected", extra=extra)
    try:
        await defer(storage.copy, storage.PERM, storage.REJECT, data["request_id"], extra["account"])
    except Exception:
        logger.exception("Failure while handling failure (aw shucks).", extra=extra)


async def consume_validation(client):
    data = await client.getmany(timeout_ms=1000, max_records=config.MAX_WORKERS)
    for tp, msgs in data.items():
        logger.info(f"Processing {len(msgs)} messages from topic [{tp.topic}]",
                    extra={"topic": tp.topic})
        await asyncio.gather(*[process_file(msg) for msg in msgs])


def make_preprocessor(queue=None):
    queue = produce_queue if queue is None else queue

    @prom_time(metrics.uploads_send_and_wait_seconds)
    async def send(client, topic, data, extra, request_id, item, msg):
        try:
            await client.send_and_wait(topic, data.encode("utf-8"))
            logger.info(f"send data for topic [{topic}] with request_id [{request_id}] succeeded",
                        extra=extra)
        except KafkaError:
            queue.append(item)
            current_archives.append(request_id)
            logger.error(
                f"send data for topic [{topic}] with request_id [{request_id}] failed, put back on queue (qsize now: {len(queue)}",
                extra=extra)
            raise
        except Exception:
            logger.exception("Failure to send_and_wait. Did *not* put item back on queue.",
                             extra={"queue_msg": msg, **extra})

    async def send_to_preprocessors(client):
        extra = log.get_extra()
        if not queue:
            await asyncio.sleep(0.1)
        else:
            try:
                items = list(queue)
                queue.clear()
                current_archives.clear()
            except Exception:
                logger.exception("Failed to popleft", extra=extra)
                return

            async def prepare_message(item):
                try:
                    topic, msg, request_id = item["topic"], item["msg"], item["msg"].get("request_id")
                    extra["account"] = msg["account"]
                    extra["request_id"] = request_id
                    metrics.uploads_popped_to_topic.labels(topic=topic).inc()
                except Exception:
                    logger.exception("Bad data from produce_queue.", extra={"item": item, **extra})
                    return

                logger.info(
                    f"Popped data from produce queue (qsize now: {len(queue)} for topic {topic}, request_id [{request_id}]",
                    extra=extra
                )

                try:
                    with metrics.uploads_json_dumps.labels(key="send_to_preprocessors").time():
                        data = json.dumps(msg)
                except Exception:
                    logger.exception("Failure to send_and_wait. Did *not* put item back on queue",
                                     extra={"queue_msg": msg, **extra})
                    return

                await send(client, topic, data, extra, request_id, item, msg)

            await asyncio.gather(*[asyncio.ensure_future(prepare_message(item)) for item in items])

    return send_to_preprocessors


@prom_time(metrics.uploads_process_file_seconds)
async def process_file(msg):
    """Determine which bucket to put a payload in based on the message
       returned from the validating service.

       storage.copy operations are not async so we offload to the executor

       Arguments:
           msg -- list of kafka messages consumed on the validation topic
    """
    extra = log.get_extra()

    data = await decode_msg("process_file", msg, extra)

    if "request_id" not in data and "payload_id" not in data:
        logger.error("request_id or payload_id not in message. Payload not removed from permanent",
                     extra=extra)
        return

    extra["request_id"] = request_id = data["request_id"] if data.get("request_id") else data["payload_id"]
    extra["account"] = account = data.get("account", "unknown")
    result = data.get("validation", "unknown")

    logger.info(f"processing message: payload [{request_id}] - {result}",
                extra=extra)

    if result.lower() == "success":
        await succeed_msg("platform.upload.available", data, extra)

    elif result.lower() == "failure":
        await fail_msg(data, extra)

    elif result.lower() == "handoff":
        metrics.uploads_handed_off.inc()
        logger.info("request_id [{request_id}] handed off", extra=extra)
    else:
        logger.info(f"Unrecognized result: {result.lower()}", extra=extra)
