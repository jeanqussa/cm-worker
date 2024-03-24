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

        self._log_queue = Queue()
        self._is_exiting = [False]
        self.job_id = None
        self._pipelines = {}
        self.log_to_console = False

    def _check_lock(self):
        if self.job_id is None:
            return

        lock = self.redis.hget(f'locks', self.job_id)
        assert(type(lock) == bytes)
        lock = lock.decode('utf-8')

        if lock != self.worker_id:
            # TODO Find a way to stop main thread
            raise LockError()

    def _start_updater_thread(self):
        def status_updater():
            while not self._is_exiting[0]:
                if self.job_id is None:
                    time.sleep(5)
                    continue

                # Make sure we still have the lock
                self._check_lock()

                # Update the status
                timestamp = str(int(time.time()))
                self.redis.hset(f'last_updates', self.job_id, timestamp)

                # Wait a bit
                time.sleep(5)

        status_thread = Thread(target=status_updater, daemon=True)
        status_thread.start()

    def _start_log_thread(self):
        def log_generator():
            while not self._is_exiting[0]:
                try:
                    log_message = self._log_queue.get(timeout=5)
                    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                    if self.log_to_console:
                        print(f"{timestamp} [{self.worker_id}] {self.job_id}: {log_message}")
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

    def _send_result(self, data):
        # Push result to queue:done
        self.redis.rpush(f'queue:done', data)

    def _clean_up(self):
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
        """
        Add a pipeline to the worker

        :param pipeline: The name of the pipeline
        :param function: The function to run. Must accept a single argument, which is a dictionary
        """
        self._pipelines[pipeline] = function

    def get_file(self, file_id):
        """
        Get a file from Redis

        :param file_id: The file id
        :return: An io.BytesIO object
        """
        file = self.redis.hget('files', file_id)
        assert(type(file) == bytes)
        return io.BytesIO(file)

    def push_log_message(self, message):
        """
        Push a log message to the log queue

        :param message: The message
        """
        self._log_queue.put(message)

    def start(self):
        """
        Start the worker
        """
        print('Starting worker...')

        # Make sure we are connected to Redis
        if not self.redis.ping():
            raise Exception('Could not connect to Redis')

        print(f'Worker {self.worker_id} ready to accept jobs')

        _pipelines = list(self._pipelines.keys())
        queues = [f'queue:{pipeline}:pending' for pipeline in _pipelines]

        # Create a queue to print log messages
        self._start_log_thread()

        # Spawn a thread to send status updates
        self._start_updater_thread()

        while not self._is_exiting[0]:
            # Wait for a hash to be pushed to queue:pending, then pop it and push it to queue:processing
            pop_result = self.redis.brpop(queues, 5)
            if pop_result is None:
                continue
            from_queue, job_id_queue = pop_result
            assert(type(job_id_queue) == bytes and type(from_queue) == bytes)
            pipeline = from_queue.decode('utf-8').split(':')[1]
            self.job_id = job_id_queue.decode('utf-8')
            self.redis.lpush(f'queue:{pipeline}:processing', self.job_id)

            self._log_queue.put(f'CourseMapper Worker: Received concept-map job for {self.job_id}...')

            # Get the job arguments
            job = self.redis.hget(f'jobs', self.job_id)
            assert(type(job) == bytes)
            job = job.decode('utf-8')
            job = json.loads(job)

            # Lock the job
            self.redis.hset(f'locks', self.job_id, self.worker_id)

            # Update the status
            timestamp = str(int(time.time()))
            self.redis.hset(f'last_updates', self.job_id, timestamp)

            self._log_queue.put(f'CourseMapper Worker: Processing concept-map job {self.job_id}...')

            try:
                # Run the pipeline
                result = self._pipelines[pipeline](job)

                # Make sure we still have the lock
                self._check_lock()

                # Send the result
                data = json.dumps({
                    "job_id": self.job_id,
                    "result": result
                })
                self._send_result(data)

                # Print a message
                self._log_queue.put(f'CourseMapper Worker: Finished processing concept-map job {self.job_id}')

                # Clean up
                self._clean_up()
            except LockError:
                # Print the error
                self._log_queue.put(f'CourseMapper Worker: Lost lock for job {self.job_id}')

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
                self._send_result(data)

                # Print a message
                self._log_queue.put(f'CourseMapper Worker: Error processing {pipeline} job {self.job_id}')

                # Print the error
                self._log_queue.put(traceback.format_exc())

                # Clean up
                self._clean_up()

    def stop(self):
        """
        Stop the worker

        It might take a few seconds to stop all threads
        """
        self._is_exiting[0] = True
