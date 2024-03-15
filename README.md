# CourseMapper Worker

This package is to be used with CourseMapper.

Its purpose it to provide a simple method for interacting with the worker queue of CourseMapper.

Its handles reading from the 'waiting' queue, pushing results to the 'done', sending periodic 'live' updates, and sending log messages.

## Installation
```
pip install git+https://github.com/jeanqussa/cm-worker
```

## Usage
This is a simple program that watches the 'addition' job queue, adds two numbers, and returns the result.

```
from cm_worker import Worker
from config import Config

worker = Worker(Config.REDIS_HOST, Config.REDIS_PORT, Config.REDIS_DB, Config.REDIS_PASSWORD)
worker.log_to_console = True

def execute_job(job):
    value_a = job.get('a')
    value_b = job.get('b')
    return value_a + value+b

worker.add_pipeline('addition', execute_job)

worker.start()
```

Take a look at the public methods of the `Worker` class for more use cases.
