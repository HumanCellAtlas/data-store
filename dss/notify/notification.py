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

from dss import DeploymentStage
from dss.util import require
from dss.util.types import JSON

logger = logging.getLogger(__name__)


class Notification(namedtuple("Notification", ("notification_id", "subscription_id", "url", "payload",
                                               "hmac_key", "hmac_key_id"))):
    @classmethod
    def from_scratch(cls,
                     notification_id: str,
                     subscription_id: str,
                     url: str,
                     payload: JSON,
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
                   payload=payload,
                   hmac_key=hmac_key,
                   hmac_key_id=hmac_key_id)

    def deliver_or_raise(self, timeout: Optional[float] = None, attempt: Optional[int] = None):
        request = self._prepare_post(timeout, attempt)
        response = requests.post(**request)
        response.raise_for_status()

    def deliver(self, timeout: Optional[float] = None, attempt: Optional[int] = None) -> bool:
        request = self._prepare_post(timeout, attempt)
        try:
            response = requests.post(**request)
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

    attempt_header_name = 'X-dss-notify-attempt'

    def _prepare_post(self, timeout, attempt) -> Mapping[str, Any]:
        if self.hmac_key:
            auth = HTTPSignatureAuth(key=self.hmac_key, key_id=self.hmac_key_id)
        else:
            auth = None
        headers = {}
        if attempt is not None:
            headers[self.attempt_header_name] = str(attempt)
        return dict(url=self.url,
                    json=self.payload,
                    auth=auth,
                    allow_redirects=False,
                    headers=headers,
                    timeout=timeout)

    def __str__(self) -> str:
        # Don't log payload or HMAC key
        return (f"{self.__class__.__name__}("
                f"notification_id='{self.notification_id}', "
                f"subscription_id='{self.subscription_id}', "
                f"url='{self.url}', "
                f"hmac_key_id='{self.hmac_key_id}')")
