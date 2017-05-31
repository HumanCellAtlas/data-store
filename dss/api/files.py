import binascii
import hashlib

from flask import redirect, request, make_response
from werkzeug.exceptions import BadRequest

from .. import get_logger

def get(uuid: str, replica: str=None, timestamp: str=None):
    if request.method == "GET" and replica is None:
        # replica must be set when it's a GET request.
        raise BadRequest()
    get_logger().info("This is a log message.")

    if request.method == "GET":
        response = redirect("http://example.com")
    else:
        response = make_response('', 200)

    headers = response.headers
    headers['X-DSS-BUNDLE_UUID'] = uuid
    headers['X-DSS-CREATOR_UID'] = 123
    headers['X-DSS-TIMESTAMP'] = 5353
    headers['X-DSS-CONTENT-TYPE'] = "abcde"
    headers['X-DSS-CRC32C'] = "%08X" % (binascii.crc32(b"abcde"),)
    headers['X-DSS-S3_ETAG'] = hashlib.md5().hexdigest()
    headers['X-DSS-SHA1'] = hashlib.sha1().hexdigest()
    headers['X-DSS-SHA256'] = hashlib.sha256().hexdigest()

    return response

def list():
    return dict(files=[dict(uuid="", name="", versions=[])])

def post():
    pass
