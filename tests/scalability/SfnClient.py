import uuid
import boto3


import time
from locust import events, Locust


from dss import stepfunctions

type = 'sfn'
sfn_template = "dss-scalability-test-{stage}"

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('scalability_test')

class SfnClient():
    def start_test_execution(self, test_run_id: str):
        self.execution_id = str(uuid.uuid4())
        test_input = {"execution_id": self.execution_id, "test_run_id": test_run_id}
        stepfunctions.step_functions_invoke(sfn_template, self.execution_id, test_input)
        self.start_time = time.time()

    def check_execution(self):
        assert self.execution_id is not None
        time.sleep(30)
        try:
            for i in range(10):
                time.sleep(10)
                response = table.get_item(
                    Key={
                        'execution_id': self.execution_id
                    }
                )
                item = response.get('Item')
                print(f"Got stuff from dynamo {self.execution_id}")
                print(str(item))
                if item is None:
                    pass
                elif item["status"] == 'SUCCEEDED':
                    total_time = item["duration"]
                    events.request_success.fire(request_type=type, name=sfn_template, response_time=total_time,
                                                response_length=0)
                    break
                else:
                    total_time = item["duration"]
                    events.request_failure.fire(request_type=type, name=sfn_template, response_time=total_time,
                                                exception=None)
                    break
        except Exception as e:
            print(f"Failed to get execution results  {sfn_template}: {str(e)}")
            total_time = int((time.time() - self.start_time) * 1000)
            events.request_failure.fire(request_type=type, name=sfn_template, response_time=total_time, exception=e)

class SfnLocust(Locust):
     def __init__(self, *args, **kwargs):
        super(SfnLocust, self).__init__(*args, **kwargs)
        self.client = SfnClient()