import logging

from dss import Config
from dss.error import DSSForbiddenException, DSSException
from dss.util.authorize import fusillade

logger = logging.getLogger(__name__)


class Authorize:
    def __new__(cls, *args, **kwargs):
        auth_backend = Config.get_auth_backend()
        if auth_backend == 'FUSILLADE':
            return fusillade.Fusillade(*args, **kwargs)
        elif auth_backend == 'AUTH0':
            # return class for auth0
            pass
        else:
            raise DSSException(500, 'Error with Security Handler, unable to locate Auth Handler')

    # name this better
    def security_flow(self, authz_methods, *args, **kwargs):
        """
        This function maps out flow for a given security config
        """

        # def process_keys(self):
        #     raise NotImplementedError()
        #
        # def __call__(self, argv: typing.List[str], args: argparse.Namespace):
        #     self.process_keys()

        # there has to be a way to replicate this to call the applicable security flow, perhaps the
        # hca blob-store has more of an example on how to perform this
        raise NotImplementedError()

    def assert_required_parameters(self, provided_params: dict, required_params: list):
        """
        Ensures existence of parameters in dictionary passed
        :param provided_params: dictionary that contains arbitrary parameters
        :param required_params: list of parameters that we want to ensure exist

        """
        for param in required_params:
            if param not in provided_params:
                raise Exception('unable to use Authorization method, missing required parameters')
        return
