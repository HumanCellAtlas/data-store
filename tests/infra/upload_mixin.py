import datetime
import time
import typing
import uuid

import requests

from dss.config import Replica
from dss.util import UrlBuilder
from dss.util.version import datetime_to_version_format
from tests import get_auth_header
from .assert_mixin import DSSAssertResponse


class DSSUploadMixin:
    def upload_file_wait(
            self: typing.Any,
            source_url: str,
            replica: Replica,
            file_uuid: str=None,
            file_version: str=None,
            bundle_uuid: str=None,
            timeout_seconds: int=120,
            expect_async: typing.Optional[bool]=None,
    ) -> DSSAssertResponse:
        """
        Upload a file.  If the request is being handled asynchronously, wait until the file has landed in the data
        store.
        """
        file_uuid = str(uuid.uuid4()) if file_uuid is None else file_uuid
        bundle_uuid = str(uuid.uuid4()) if bundle_uuid is None else bundle_uuid
        if expect_async is True:
            expected_codes = requests.codes.accepted
        elif expect_async is False:
            expected_codes = requests.codes.created
        else:
            expected_codes = requests.codes.created, requests.codes.accepted

        if file_version is None:
            timestamp = datetime.datetime.utcnow()
            file_version = datetime_to_version_format(timestamp)
        url = UrlBuilder().set(path=f"/v1/files/{file_uuid}")
        url.add_query("version", file_version)

        resp_obj = self.assertPutResponse(
            str(url),
            expected_codes,
            json_request_body=dict(
                bundle_uuid=bundle_uuid,
                creator_uid=0,
                source_url=source_url,
            ),
            headers=get_auth_header()
        )

        if resp_obj.response.status_code == requests.codes.accepted:
            # hit the GET /files endpoint until we succeed.
            start_time = time.time()
            timeout_time = start_time + timeout_seconds

            while time.time() < timeout_time:
                try:
                    self.assertHeadResponse(
                        f"/v1/files/{file_uuid}?replica={replica.name}",
                        requests.codes.ok)
                    break
                except AssertionError:
                    pass

                time.sleep(1)
            else:
                self.fail("Could not find the output file")

        return resp_obj
