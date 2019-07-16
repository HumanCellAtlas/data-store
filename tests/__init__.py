import datetime
import json
import time
import uuid
import logging
import unittest
import jwt
import functools
import io
import os

from dss import Config
from dss.api import bundles
from dss.util.version import datetime_to_version_format
from dss.util import get_gcp_credentials_file
from dss.storage.identifiers import BundleFQID, FileFQID, CollectionFQID

logger = logging.getLogger(__name__)

# These GCP credentials point to a valid service account that is not associated with HCA. They can be used to test code
# paths that require Google authentication, but then require further permissions validation once they reach data-store.
UNAUTHORIZED_GCP_CREDENTIALS = {
    'type': 'service_account',
    'project_id': 'cool-project-188401',
    'private_key_id': 'e9db2f96adb67a91b1e10f8014159ca13eaf16ee',
    'private_key': '-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDF2ywdURP1louy\nR8nHVjdI+s/CiQ5IKH8uzMbehDdBAt1hXQRvLOWle+f4sN5CTfrG/TcZ1dSLShvu\nt71YrFCjqvgw7EsXp/S7MidtrGyHweWQELJ4mnZTSC1iYcVvJXWGdBfgJl8Qgt27\nztrRVzBvJJiuTk1zAviDGbqZRIBNkCn6NOIPjrfZ8KHx7j9nyamPie3V7Kzx+cgo\nkxiiQ36lvhfAdD/U3aV/RdRYxpZPKHgCcX1eZkFGKWR1HrjiTsvJ8fgtgwTB5fux\nw4MbUwwELsSEWbGIXr5ZTZeeYa7kfAwDlL9yam/XaD3gAv0dBsxhW8ElBGqXQLgB\nfBz8A2vjAgMBAAECggEASIW7t8s+O6jA78oshepuPUvh13aRS5W8eJUK2Akyj5vT\nNZr4wx971ZqDPr7l2PvoTgQGrGuaiqvRbUDgIJ6YuEU00AnsxHEF3Y/Wr/ahmWlT\nEels4ZQMhx5PtF4OFl1upKftEHZAJjcxu2NpBY6l7DdH16xP6zZNjRBjO0bGmKb4\nBJlvcNPcQGptuBRo0pbYGNIo614ML5pzeHf95TDFaUw1GMyHG+oVuLZK/os1RmT0\n3tNLKMBPSwhDsT3o9xuWWi2uKw3Z1thwmLgXJj4kX2uqepFnYO1PZ+wJYbTEoHxi\n+LWRpWcTzIH+6cRGrZzoa+HHa+5npCvfc96SC1uYsQKBgQDpJZBf7du1dL8zwx5K\niaOnImtQhc35iVG7nFTi5wc5RNxYio6Cx7fTr8E/kvYjqknYxMFdTf8hmmFQnfXz\nfSbfiBzKtZWp6P7cdtcsR5EtY9tvjagwK7awJPbJyJXKlQeiDRsVTS4JB/XGTAAF\nLg4/Y1uGVNGP5Z29bwBWTkD1PQKBgQDZQAx0jUx6ft50UNleGsourWDBBMPuHFik\nyE+ej7SfpEJAZ29HAsbJBVXHeCUF3NZRRwWNFWrPfttWYyx/sCyz8tfsY74oF3om\nh0iAXT0kXqLP5i0rJORpoKPyMwAFltLnXIqQDAkLOri+rvtwonrn0rtUZJZPGoxe\nbhnBPpc3nwKBgDtvUw3Rcjgg6flFHXy899ZMpPTjF24svoRIRy+M27+SuWVs9QWL\n6mXxoR8W1N6ks6yqA+1IS+kCFRrbGe8XkYhch5J5lgy5k/cZ6KKmH/FlSnR2tVCK\nZEklMzCfjOgW89ow4x2cDkdJGzOQ/lRTuFgaeSOWjdHUJFE9ceWOj2q1AoGBAJKp\nhX8NgMrVaTIW/pdj+IgIbeAAapENu94KiI2fsC1xw3QdH+dNfYtpuZ3+gufxTRHz\no1C6W7AWkNZB/2F4OsWEtLYWI+KG7uShwZU+3K734Gv/lRCiSDzywJsaSPJ8/oZI\nWBakuVpGW0AHeyFv3w8vmV2AxmRCpO5+344wxf87AoGBAIr614y4YEHfXO/TUiwb\nLau6KYp5FYtCHHWv8RujV9mRoOnNUIGVYDLk6jJVkeZWJUwVjWBetYkRVdlbvDwq\nXuL5G+lNa3GKQnqgdCDtzPyuv77x33/XcbkmRiAfUb1ePtn3ufIBXcjTXwkGLh0i\ng/hD0R3CD4EtI95IWxnTU8BF\n-----END PRIVATE KEY-----\n',  # noqa
    'client_email': 'project-viewer@cool-project-188401.iam.gserviceaccount.com',
    'client_id': '112492781067934988519',
    'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
    'token_uri': 'https://accounts.google.com/o/oauth2/token',
    'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
    'client_x509_cert_url': 'https://www.googleapis.com/robot/v1/metadata/x509/project-viewer%40cool-project-188401.iam.gserviceaccount.com',  # noqa
}

if not os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = get_gcp_credentials_file().name
with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS'], "r") as fh:
    gcp_service_email = json.loads(fh.read())['client_email']
bundles.ADMIN_USER_EMAILS = set([gcp_service_email])

def get_collection_fqid() -> CollectionFQID:
    return CollectionFQID(uuid=str(uuid.uuid4()), version=get_version())

def get_bundle_fqid() -> BundleFQID:
    return BundleFQID(uuid=str(uuid.uuid4()), version=get_version())


def get_file_fqid() -> FileFQID:
    return FileFQID(uuid=str(uuid.uuid4()), version=get_version())


def get_version():
    return datetime_to_version_format(datetime.datetime.utcnow())


def eventually(timeout: float, interval: float, errors: set = {AssertionError}):
    """
    @eventually runs a test until all assertions are satisfied or a timeout is reached.
    :param timeout: time until the test fails
    :param interval: time between attempts of the test
    :param errors: the exceptions to catch and retry on
    :return: the result of the function or a raised assertion error
    """
    def decorate(func):
        @functools.wraps(func)
        def call(*args, **kwargs):
            timeout_time = time.time() + timeout
            error_tuple = tuple(errors)
            while True:
                try:
                    return func(*args, **kwargs)
                except error_tuple as e:
                    if time.time() >= timeout_time:
                        raise
                    logger.debug("Error in %s: %s. Retrying after %s s...", func, e, interval)
                    time.sleep(interval)

        return call

    return decorate


def get_service_jwt(service_credentials, group: str = None, email=True, email_claim=False, audience=None):
    iat = time.time()
    exp = iat + 3600
    payload = {'iss': service_credentials["client_email"],
               'sub': service_credentials["client_email"],
               'aud': audience or Config.get_audience(),
               'iat': iat,
               'exp': exp,
               'scope': ['email', 'openid', 'offline_access']
               }
    if group:
        payload[Config.get_OIDC_group_claim()] = group
    if email:
        payload['email'] = service_credentials["client_email"]
    if email_claim:
        payload[Config.get_OIDC_email_claim()] = service_credentials["client_email"]
    additional_headers = {'kid': service_credentials["private_key_id"]}
    signed_jwt = jwt.encode(payload, service_credentials["private_key"], headers=additional_headers,
                            algorithm='RS256').decode()
    return signed_jwt


def get_auth_header(real_header=True, authorized=True, group='hca', email=True, email_claim=False):
    if authorized:
        credential_file = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        with io.open(credential_file) as fh:
            info = json.load(fh)
    else:
        info = UNAUTHORIZED_GCP_CREDENTIALS
    token = get_service_jwt(info, group, email=email, email_claim=email_claim) if real_header else str(uuid.uuid4())
    return {"Authorization": f"Bearer {token}"}


def skip_on_travis(test_item):
    if os.environ.get('TRAVIS') == 'true':
        return unittest.skip("Test doesn't run on travis.")(test_item)
    else:
        return test_item
