import ipaddress
import logging
import socket
from typing import Optional, Mapping, Any, NamedTuple, MutableMapping
from urllib.parse import urlparse


import requests
from requests_http_signature import HTTPSignatureAuth
import urllib3

from dss import DeploymentStage
from dss.util import require
from dss.util.types import JSON

logger = logging.getLogger(__name__)


class Notification(NamedTuple):
    notification_id: str
    subscription_id: str
    url: str
    method: str
    encoding: str
    body: JSON
    hmac_key: Optional[bytes] = None
    hmac_key_id: Optional[str] = None

    @classmethod
    def create(cls,
               notification_id: str,
               subscription_id: str,
               url: str,
               method: str,
               encoding: str,
               body: JSON,
               hmac_key: Optional[bytes] = None,
               hmac_key_id: Optional[str] = None) -> 'Notification':

        allowed_schemes = {'https'} if DeploymentStage.IS_PROD() else {'https', 'http'}
        scheme = urlparse(url).scheme
        require(scheme in allowed_schemes,
                f"The scheme '{scheme}' of URL '{url}' is prohibited. Allowed schemes are {allowed_schemes}.")

        if DeploymentStage.IS_PROD():
            hostname = urlparse(url).hostname
            for family, socktype, proto, canonname, sockaddr in socket.getaddrinfo(hostname, port=None):
                require(ipaddress.ip_address(sockaddr[0]).is_global,
                        f"The hostname in URL '{url}' resolves to a private IP")

        return cls(notification_id=notification_id,
                   subscription_id=subscription_id,
                   url=url,
                   method=method,
                   encoding=encoding,
                   body=body,
                   hmac_key=hmac_key,
                   hmac_key_id=hmac_key_id)

    def deliver_or_raise(self, timeout: Optional[float] = None):
        request = self._prepare_request(timeout)
        response = requests.request(**request)
        response.raise_for_status()

    def deliver(self, timeout: Optional[float] = None) -> bool:
        request = self._prepare_request(timeout)
        try:
            response = requests.request(**request)
        except BaseException as e:
            logger.warning("Exception raised during notification delivery:", exc_info=e)
            return False
        else:
            if 200 <= response.status_code < 300:
                logger.info("Successfully delivered %s: HTTP status %i", self, response.status_code)
                return True
            else:
                logger.warning("Failed delivering %s: HTTP status %i", self, response.status_code)
                return False

    def _prepare_request(self, timeout) -> Mapping[str, Any]:
        if self.hmac_key:
            auth = HTTPSignatureAuth(key=self.hmac_key, key_id=self.hmac_key_id)
        else:
            auth = None
        headers: MutableMapping[str, str] = {}
        request = dict(method=self.method,
                       url=self.url,
                       auth=auth,
                       allow_redirects=False,
                       headers=headers,
                       timeout=timeout)
        body = self.body
        if self.encoding == 'application/json':
            request['json'] = body
        elif self.encoding == 'multipart/form-data':
            # The requests.request() method can encode this content type for us (using the files= keyword argument)
            # but it is awkward to use if the field values are strings or bytes and not streams.
            data, content_type = urllib3.encode_multipart_formdata(body)
            request['data'] = data
            headers['Content-Type'] = content_type
        else:
            raise ValueError(f'Encoding {self.encoding} is not supported')
        return request

    def __str__(self) -> str:
        # Don't log body because it may be too big or the HMAC key because it is secret
        return (f"{self.__class__.__name__}("
                f"notification_id='{self.notification_id}', "
                f"subscription_id='{self.subscription_id}', "
                f"url='{self.url}', "
                f"method='{self.method}', "
                f"encoding='{self.encoding}', "
                f"hmac_key_id='{self.hmac_key_id}')")


class Endpoint(NamedTuple):
    """
    A convenience holder for the subscription attributes that describe the HTTP endpoint to send notifications to.

    Be sure to keep the defaults in sync with the Swagger API definition.
    """
    callback_url: Optional[str]
    method: str = 'POST'
    encoding: str = 'application/json'
    form_fields: Mapping[str, str] = {}  # noqa, False positive: E701 multiple statements on one line (colon)
    payload_form_field: str = 'payload'

    @classmethod
    def from_subscription(cls, subscription):
        return Endpoint(**{k: subscription[k] for k in cls._fields if k in subscription})

    def to_dict(self):
        return self._asdict()

    def extend(self, **kwargs):
        return self._replace(**kwargs)
