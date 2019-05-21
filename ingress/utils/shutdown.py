import logging

from time import sleep
from tornado.ioloop import IOLoop

import server
import mq

logger = logging.getLogger("ingress")

def signal_handler(signal, frame):
    loop = IOLoop.current()
    logger.info(f"Recieved Exit Signal: {signal}")
    loop.spawn_callback(shutdown)

def shutdown():
    loop = IOLoop.current()
    logger.debug("Stopping Server")
    server.LOOPS["consumer"].stop()
    logger.debug("Consumer Stopped")
    while len(mq.current_archives) > 0:
        logger.debug(f"Remaining archives: {len(mq.current_archives)}")
        sleep(1)
    loop.stop()
    logger.info("Ingress Shutdown")
    logging.shutdown()
