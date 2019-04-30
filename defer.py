from utils import mnm, config
from concurrent.futures import ThreadPoolExecutor
from tornado.ioloop import IOLoop
# Executor used to run non-async/blocking tasks
thread_pool_executor = ThreadPoolExecutor(max_workers=config.MAX_WORKERS)
mnm.uploads_executor_qsize.set_function(lambda: thread_pool_executor._work_queue.qsize())

IOLoop.current().set_default_executor(thread_pool_executor)


async def defer(*args):
    try:
        name = args[0].__name__
    except Exception:
        name = "unknown"

    with mnm.uploads_run_in_executor.labels(function=name).time():
        return await IOLoop.current().run_in_executor(None, *args)
