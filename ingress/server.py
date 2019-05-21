import signal

import tornado.web
from tornado.ioloop import IOLoop

import log
import handlers
import config
import utils
import kafka
import mq
from utils import nonblock, shutdown

logger = log.initialize_logging()
LOOPS = {"consumer": None,
         "producer": None}

endpoints = [
    (config.API_PREFIX, handlers.RootHandler),
    (f"{config.API_PREFIX}/v1/version", handlers.VersionHandler,
     dict(build_id=config.BUILD_ID, build_date=config.get_commit_date(config.BUILD_ID))),
    (f"{config.API_PREFIX}/v1/upload", handlers.UploadHandler, dict(valid_topics=config.get_valid_topics())),
    (f"{config.API_PREFIX}/v1/openapi.json", handlers.SpecHandler),
    (r"/metrics", handlers.MetricsHandler),
    # Legacy Endpoints
    (r"/r/insights/platform/ingress", handlers.RootHandler),
    (r"/r/insights/platform/ingress/v1/version", handlers.VersionHandler,
     dict(build_id=config.BUILD_ID, build_date=config.get_commit_date(config.BUILD_ID))),
    (r"/r/insights/platform/ingress/v1/upload", handlers.UploadHandler,
     dict(valid_topics=config.get_valid_topics())),
    (r"/insights/platform/ingress/v1/openapi.json", handlers.SpecHandler),
]

for urlSpec in endpoints:
    config.spec.path(urlspec=urlSpec)

app = tornado.web.Application(endpoints, max_body_size=config.MAX_LENGTH)

def main():

    signal.signal(signal.SIGTERM, utils.shutdown.signal_handler)
    signal.signal(signal.SIGINT, utils.shutdown.signal_handler)

    app.listen(config.LISTEN_PORT)
    logger.info(f"Web server listening on port {config.LISTEN_PORT}")
    loop = IOLoop.current()
    loop.set_default_executor(utils.nonblock.thread_pool_executor)
    LOOPS["consumer"] = IOLoop(make_current=False).instance()
    LOOPS["producer"] = IOLoop(make_current=False).instance()
    LOOPS["consumer"].add_callback(mq.CONSUMER.get_callback(nonblock.consume_validation))
    LOOPS["producer"].add_callback(mq.PRODUCER.get_callback(nonblock.make_preprocessor(mq.produce_queue)))
    for k, v in LOOPS.items():
        v.start()
    loop.start()

if __name__ == "__main__":
    main()
