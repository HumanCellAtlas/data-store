#!/usr/bin/env python
# coding: utf-8

import datetime
import os
import sys
import time
import uuid

import argparse

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'sns')))  # noqa
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))  # noqa

from sns import SnsClient

UNIT_OF_TIME = 1 * 1000 * 1000  # 1 sec in microseconds

class ScaleTestRunner:
    def __init__(self, rps: int, duration_seconds: int) -> None:
        self.rps = rps
        self.duration_seconds = duration_seconds
        self.run_id = str(uuid.uuid4())
        self.sns_client = SnsClient()
        self.loading = '.' * duration_seconds

    def run(self):
        self.end_time = datetime.datetime.now() + datetime.timedelta(seconds=self.duration_seconds)
        self.sns_client.start_test_run(self.run_id)
        for cnt in range(self.duration_seconds):
            self.generate_load_rps()
            self.loading = self.loading[:cnt] + '#' + self.loading[cnt + 1:]
            print('\r%s Sending at %3d percent!' % (self.loading, (cnt + 1) * 100 / self.duration_seconds), end='')
        self.sns_client.stop()

    def generate_load_rps(self):
        start = datetime.datetime.now()
        for cnt in range(self.rps):
            self.sns_client.start_test_execution(self.run_id, str(uuid.uuid4()))
        duration = datetime.datetime.now() - start
        elapsed_time = duration.microseconds / UNIT_OF_TIME

        if elapsed_time < 1.0:
            time.sleep(1.0 - elapsed_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DSS scalability test runner')
    parser.add_argument('-r', '--rps',
                        help='requests generated per second',
                        default='10')
    parser.add_argument('-d', '--duration',
                        help='duration of the test',
                        default='20')
    results = parser.parse_args(sys.argv[1:])

    rps = int(results.rps)
    duration = int(results.duration)

    print(f"Test configuration rps: {rps} duration: {duration}")

    runner = ScaleTestRunner(rps, duration)
    runner.run()
