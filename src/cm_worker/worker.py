from redis import Redis
import json
from threading import Thread
from queue import Queue, Empty
import time
import random
import sys
import io
import traceback


class LockError(Exception):
    pass


class Worker:
    def __init__(self, redis_host, redis_port, redis_db, redis_password):
        self.redis = Redis(host=redis_host, port=redis_port, db=redis_db, password=redis_password)

        # Generate a random worker id
        random.seed()
        self.worker_id = str(random.randint(0, 1000000))

        self.log_queue = Queue()
        self.is_exiting = [False]
        self.job_id = None
        self.pipelines = {}

    def check_lock(self):
        if self.job_id is None:
            return

        lock = self.redis.hget(f'locks', self.job_id)
        assert(type(lock) == bytes)
        lock = lock.decode('utf-8')

        if lock != self.worker_id:
            # TODO Find a way to stop main thread
            raise LockError()

    def start_updater_thread(self, job_id):
        def status_updater(self):
            while not self.is_exiting[0]:
                # Make sure we still have the lock
                self.check_lock()

                # Update the status
                timestamp = str(int(time.time()))
                self.redis.hset(f'last_updates', job_id, timestamp)

                # Wait a bit
                time.sleep(5)

        status_thread = Thread(target=status_updater, daemon=True)
        status_thread.start()

    def start_log_thread(self):
        def log_generator():
            while not self.is_exiting[0]:
                try:
                    log_message = self.log_queue.get(timeout=5)
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                    msg = json.dumps({
                        "worker_id": self.worker_id,
                        "job_id": self.job_id,
                        "timestamp": timestamp,
                        "message": log_message
                    })
                    self.redis.rpush('log', msg)
                except Empty:
                    pass

        log_thread = Thread(target=log_generator, daemon=True)
        log_thread.start()

    def send_result(self, data):
        # Push result to queue:done
        self.redis.rpush(f'queue:done', data)

    def clean_up(self):
        if self.job_id is None:
            return

        # Remove job from queue:processing
        self.redis.lrem(f'queue:concept-map:processing', 0, self.job_id)

        # Delete status
        self.redis.hdel(f'last_updates', self.job_id)

        # Delete lock
        self.redis.hdel(f'locks', self.job_id)

        # Delete job
        self.redis.hdel(f'jobs', self.job_id)

        # Delete file
        self.redis.hdel('files', self.job_id)

        # Reset job_id
        self.job_id = None

    def add_pipeline(self, pipeline, function):
        self.pipelines[pipeline] = function

    def get_file(self, file_id):
        file = self.redis.hget('files', file_id)
        assert(type(file) == bytes)
        return io.BytesIO(file)

    def start(self):
        print('Starting worker...')

        # Make sure we are connected to Redis
        if not self.redis.ping():
            raise Exception('Could not connect to Redis')

        print(f'Worker {self.worker_id} ready to accept jobs')

        pipelines = list(self.pipelines.keys())
        queues = [f'queue:{pipeline}:pending' for pipeline in pipelines]

        # Create a queue to print log messages
        self.start_log_thread()

        # Spawn a thread to send status updates
        self.start_updater_thread(self.job_id)

        while True:
            # Wait for a hash to be pushed to queue:pending, then pop it and push it to queue:processing
            from_queue, job_id_queue = self.redis.brpop(queues, 0)
            assert(type(job_id_queue) == bytes and type(from_queue) == bytes)
            pipeline = from_queue.decode('utf-8').split(':')[1]
            self.job_id = job_id_queue.decode('utf-8')
            self.redis.lpush(f'queue:{pipeline}:processing', self.job_id)

            self.log_queue.put(f'CourseMapper Worker: Received concept-map job for {self.job_id}...')

            # Get the job arguments
            job = self.redis.hget(f'jobs', self.job_id)
            assert(type(job) == bytes)
            job = job.decode('utf-8')
            job = json.loads(job)

            # Lock the job
            self.redis.hset(f'locks', self.job_id, self.worker_id)

            self.log_queue.put(f'CourseMapper Worker: Processing concept-map job {self.job_id}...')

            try:
                # Run the pipeline
                result = self.pipelines[pipeline](job)

                # Make sure we still have the lock
                self.check_lock()

                # Send the result
                data = json.dumps({
                    "job_id": self.job_id,
                    "result": result
                })
                self.send_result(data)

                # Clean up
                self.clean_up()

                # Print a message
                self.log_queue.put(f'CourseMapper Worker: Finished processing concept-map job {self.job_id}')
            except LockError:
                # Print the error
                self.log_queue.put(f'CourseMapper Worker: Lost lock for job {self.job_id}')

                # No need to clean up, another worker will do it
            except KeyboardInterrupt:
                # Quit
                sys.exit()
            except Exception as e:
                # Send the error
                data = json.dumps({
                    "job_id": self.job_id,
                    "error": str(e)
                })
                self.send_result(data)

                # Print a message
                self.log_queue.put(f'CourseMapper Worker: Error processing {pipeline} job {self.job_id}')

                # Print the error
                self.log_queue.put(traceback.format_exc())

                # Clean up
                self.clean_up()

    def stop(self):
        self.is_exiting[0] = True
