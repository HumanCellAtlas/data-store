import gzip
import ipaddress
import json
import logging
import socket
from typing import Optional, Mapping, Any
from urllib.parse import urlparse

import base64
from collections import namedtuple
import time

import requests
from requests_http_signature import HTTPSignatureAuth

from dss import DeploymentStage, Config
from dss.util import require
from dss.util.types import JSON

logger = logging.getLogger(__name__)


class Notification(namedtuple("Notification", ("notification_id", "subscription_id", "url", "payload",
                                               "attempts", "hmac_key", "hmac_key_id", "queued_at"))):
    @classmethod
    def from_scratch(cls,
                     notification_id: str,
                     subscription_id: str,
                     url: str,
                     payload: JSON,
                     attempts: Optional[int] = None,
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

        if attempts is None:
            attempts = Config.notification_attempts()

        return cls(notification_id=notification_id,
                   subscription_id=subscription_id,
                   url=url,
                   payload=cls._bin2sqs(json.dumps(payload).encode()),
                   attempts=attempts,
                   hmac_key=None if hmac_key is None else cls._bin2sqs(hmac_key),
                   hmac_key_id=hmac_key_id,
                   queued_at=None)  # this field will be set when the message comes out of the queue

    @classmethod
    def from_sqs_message(cls, message) -> 'Notification':
        def v(d):
            return None if d is None else d['StringValue']

        attributes = message.attributes
        message_attributes = message.message_attributes
        return cls(notification_id=attributes['MessageDeduplicationId'],
                   subscription_id=attributes['MessageGroupId'],
                   url=v(message_attributes.get('url')),
                   payload=message.body,
                   attempts=int(v(message_attributes.get('attempts'))),
                   hmac_key=v(message_attributes.get('hmac_key')),
                   hmac_key_id=v(message_attributes.get('hmac_key_id')),
                   queued_at=float(v(message_attributes.get('queued_at'))))

    def to_sqs_message(self):
        assert self.attempts is not None

        # Boto3's receive_messages returns Message instances while send_message() expects keyword arguments.
        def v(s):
            return None if s is None else dict(StringValue=s, DataType='String')

        # Removing the entries with a None value is more concise than conditionally adding them
        def f(d):
            return {k: v for k, v in d.items() if v is not None}

        return dict(MessageBody=self.payload,
                    MessageDeduplicationId=self.notification_id,
                    MessageGroupId=self.subscription_id,
                    MessageAttributes=f(dict(url=v(self.url),
                                             attempts=v(str(self.attempts)),
                                             hmac_key=v(self.hmac_key),
                                             hmac_key_id=v(self.hmac_key_id),
                                             queued_at=v(str(time.time())))))

    def deliver_or_raise(self, timeout: Optional[float] = None, attempt: Optional[int] = None):
        request = self._prepare_post(timeout, attempt)
        response = requests.post(**request)
        response.raise_for_status()

    def deliver(self, timeout: Optional[float] = None, attempt: Optional[int] = None) -> bool:
        request = self._prepare_post(timeout, attempt)
        try:
            response = requests.post(**request)
        except BaseException as e:
            logger.warning("Exception raised during notification delivery attempt:", exc_info=e)
            return False
        else:
            if 200 <= response.status_code < 300:
                logger.info("Successfully delivered %s: HTTP status %i", self, response.status_code)
                return True
            else:
                logger.warning("Failed delivering %s: HTTP status %i", self, response.status_code)
                return False

    attempt_header_name = 'X-dss-notify-attempt'

    def _prepare_post(self, timeout, attempt) -> Mapping[str, Any]:
        if self.hmac_key:
            auth = HTTPSignatureAuth(key=self._sqs2bin(self.hmac_key), key_id=self.hmac_key_id)
        else:
            auth = None
        headers = {}
        if attempt is not None:
            headers[self.attempt_header_name] = str(attempt)
        return dict(url=self.url,
                    json=json.loads(self._sqs2bin(self.payload)),
                    auth=auth,
                    allow_redirects=False,
                    headers=headers,
                    timeout=timeout)

    def spend_attempt(self):
        assert self.attempts > 0
        return self._replace(attempts=self.attempts - 1)

    @classmethod
    def _bin2sqs(cls, payload: bytes):
        # SQS supports #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF in message bodies.
        # The base85 alphabet is a subset of that and of ASCII. It is more space efficient than base64.
        return base64.b85encode(gzip.compress(payload)).decode('ascii')

    @classmethod
    def _sqs2bin(cls, payload: str):
        return gzip.decompress(base64.b85decode(payload.encode('ascii')))

    def __str__(self) -> str:
        # Don't log payload or HMAC key
        return (f"{self.__class__.__name__}("
                f"notification_id='{self.notification_id}', "
                f"subscription_id='{self.subscription_id}', "
                f"url='{self.url}', "
                f"attempts={self.attempts}, "
                f"hmac_key_id='{self.hmac_key_id}')")
