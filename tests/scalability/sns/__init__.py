import time

from .sns_publisher import SnsPublisher


class SnsClient():
    def __init__(self):
        self.publisher = SnsPublisher()
        self.publisher.start()

    def start_test_run(self, run_id: str):
        self.publisher.send_start_run(run_id)

    def start_test_execution(self, run_id: str, execution_id: str):
        self.execution_id = execution_id
        self.start_time = time.time()
        msg = {"run_id": run_id, "execution_id": self.execution_id}
        self.publisher.enqueue_message(msg)

    def stop(self):
        self.publisher.stop()
