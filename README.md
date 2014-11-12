nsqworker - an async task worker for NSQ
----------------------------------------

Currently only a threaded worker is supported.
it handles NSQ messaging with the official Python/Tornado library and executes a blocking message handler function in an executor thread pool.

Usage:
```
import time
import nsqworker


def process_message(message):
  print "start", message.id
  time.sleep(2)
  print "end", message.id

def handle_exc(message, e):
  traceback.print_exc()
  message.requeue()

w.nsqworker.ThreadWorker(message_handler=process_message,
                         exception_handler=handle_exc, concurrency=5, ...)

w.start()
```

The arguments for the `ThreadWorker` constructor are a synchronous, blocking function that handles messages, concurrency, an optional exception_handler and all other arguments for the official [NSQ](http://nsq.io) Python library - [pynsq](https://pynsq.readthedocs.org).

* The worker will explicitly call `message.finish()` in case the handler function didn't call `message.finish()` or `message.requeue()`.

* The worker will periodically call `message.touch()` every 30s for long running tasks so they won't timeout by nsqd.

* The exception handler is called with a message and an exception as the arguments in case it was given during the worker's initialization and an exception is raised while processing a message.

* TODO - add definable timeout for message handling.

* TODO - message de-duping.
