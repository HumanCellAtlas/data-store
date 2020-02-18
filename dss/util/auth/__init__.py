import logging

from dss import Config
from dss.error import DSSException
from . import fusillade

logger = logging.getLogger(__name__)


class AuthHandler:
    def __new__(cls, *args, **kwargs):
        auth_backend = Config.get_auth_backend()
        if auth_backend == 'FUSILLADE':
            return fusillade.Fusillade(*args, **kwargs)
        elif auth_backend == 'AUTH0':
            # return class for auth0
            pass
        else:
            raise DSSException(500, 'Error with Security Handler, unable to locate Auth Handler')
