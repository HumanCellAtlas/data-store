import uuid

import time
from locust import events, Locust


from dss import stepfunctions

sfn_template = "dss-scalability-test-{stage}"
type = 'sfn'
class SfnClient():
    execution_id = None
    start_time = None

    def start_execution(self):
        self.execution_id = str(uuid.uuid4())
        test_input = {"test": "test"}
        stepfunctions.step_functions_invoke(sfn_template, self.execution_id, test_input)
        self.start_time = time.time()

    def check_execution(self):

        try:
            assert self.execution_id is not None
            for i in range(10):
                response = stepfunctions.step_functions_describe_execution(sfn_template, self.execution_id)
                print(f"response: {response}")
                status = response["status"]
                if status == 'RUNNING':
                    time.sleep(10)
                elif status == 'SUCCEEDED':
                    start_date = response["startDate"]
                    stop_date = response["stopDate"]
                    total_time = (stop_date - start_date).total_seconds()
                    events.request_success.fire(request_type=type, name=sfn_template, response_time=total_time,
                                                response_length=0)
                    break
                else:
                    start_date = response["startDate"]
                    stop_date = response["stopDate"]
                    total_time = (stop_date - start_date).total_seconds()
                    events.request_failure.fire(request_type=type, name=sfn_template, response_time=total_time,
                                                exception=None)
                    break
        except Exception as e:
            print(f"Failed to to start step function execution {sfn_template}: {str(e)}")
            total_time = int((time.time() - self.start_time) * 1000)
            events.request_failure.fire(request_type=type, name=sfn_template, response_time=total_time, exception=e)

class SfnLocust(Locust):
     def __init__(self, *args, **kwargs):
        super(SfnLocust, self).__init__(*args, **kwargs)
        self.client = SfnClient()