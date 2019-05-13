from threading import Thread
from concurrent.futures import Future
import typing

from flask import make_response, jsonify

from fusillade import User, directory
from fusillade.utils.authorize import assert_authorized, evaluate_policy


def evaluate_policy_api(token_info, body):
    with AuthorizeThread(token_info['https://auth.data.humancellatlas.org/email'],
                         ['fus:Evaluate'],
                         ['arn:hca:fus:*:*:user']):
        policies = User(directory, body['principal']).lookup_policies()
        result = evaluate_policy(body['principal'], body['action'], body['resource'], policies)
    return make_response(jsonify(**body, result=result), 200)


class AuthorizeThread:
    """
    Authorize the requester in a separate thread while executing the request. This is safe only when performing
    read operations. If authorization fails a 403 is returned and the original request results are discarded.
    """
    def __init__(
            self,
            user: str,
            actions: typing.List[str],
            resources: typing.List[str]
    ):
        self.args = (user, actions, resources)

    def __enter__(self):
        self.future = self.evaluate()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.future.result()

    @staticmethod
    def _call_with_future(fn, future, args):
        """
        Returns the result of the wrapped threaded function.
        """
        try:
            result = fn(*args)
            future.set_result(result)
        except Exception as exc:
            future.set_exception(exc)

    def evaluate(self):
        future = Future()
        Thread(target=self._call_with_future, args=(assert_authorized, future, self.args)).start()
        return future
