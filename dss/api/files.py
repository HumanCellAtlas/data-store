import binascii

import hashlib
import requests

from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from .. import get_logger


def head(uuid: str, replica: str=None, timestamp: str=None):
    # NOTE: THIS IS NEVER ACTUALLY CALLED DUE TO A BUG IN CONNEXION.
    # HEAD requests always calls the same endpoint as get, even if we tell it to
    # go to a different method.  However, connexion freaks out if:
    # 1) there is no head() function defined in code.  *or*
    # 2) we tell the head() function to hit the same method using operationId.
    #
    # So in short, do not expect that this function actually gets called.  This
    # is only here to keep connexion from freaking out.
    return get(uuid, replica, timestamp)


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
    headers['X-DSS-BUNDLE-UUID'] = uuid
    headers['X-DSS-CREATOR-UID'] = 123
    headers['X-DSS-TIMESTAMP'] = 5353
    headers['X-DSS-CONTENT-TYPE'] = "abcde"
    headers['X-DSS-CRC32C'] = "%08X" % (binascii.crc32(b"abcde"),)
    headers['X-DSS-S3-ETAG'] = hashlib.md5().hexdigest()
    headers['X-DSS-SHA1'] = hashlib.sha1().hexdigest()
    headers['X-DSS-SHA256'] = hashlib.sha256().hexdigest()

    return response

def list():
    return dict(files=[dict(uuid="", name="", versions=[])])

def put(uuid: str):
    return jsonify(dict(timestamp="2017-06-01T19:21:17.068Z")), requests.codes.created
