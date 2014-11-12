import logging
import sys

import nsq
from tornado import gen
from tornado import ioloop
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor


class ThreadWorker:
    def __init__(self, message_handler=None, exception_handler=None, concurrency=1, timeout=60, **kwargs):
        self.io_loop = ioloop.IOLoop.instance()
        self.executor = ThreadPoolExecutor(concurrency)
        self.concurrency = concurrency
        self.kwargs = kwargs
        self.message_handler = message_handler
        self.exception_handler = exception_handler
        self.timeout = timeout

        self.logger = logging.getLogger("ThreadWorker")
        if not self.logger.handlers:
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

            handler = logging.StreamHandler(stream=sys.stdout)
            handler.setFormatter(formatter)
            handler.setLevel(logging.DEBUG)

            self.logger.addHandler(handler)
            self.logger.setLevel(logging.DEBUG)
            self.logger.propagate = 0

    @run_on_executor
    def _run_threaded_handler(self, message):
        self.message_handler(message)

    @gen.coroutine
    def _message_handler(self, message):
        self.logger.debug("Received message %s", message.id)
        message.enable_async()

        def touch():
            self.logger.debug("Sending touch event for message %s", message.id)
            message.touch()

        p = ioloop.PeriodicCallback(touch, 30000)
        p.start()

        try:
            result = yield self._run_threaded_handler(message)
        except Exception as e:
            self.logger.debug("Message handler for message %s raised an exception", message.id)
            if self.exception_handler is not None:
                self.exception_handler(message, e)

        p.stop()

        if not message.has_responded():
            message.finish()

        self.logger.debug("Finished handling message %s", message.id)

    def start(self):
        self.logger.info("Listening for NSQD messages, handling message with %d threads", self.concurrency)
        kwargs = self.kwargs
        kwargs["message_handler"] = self._message_handler
        kwargs["max_in_flight"] = self.concurrency

        nsq.Reader(**kwargs)

        nsq.run()