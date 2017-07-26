import datetime
import io
import json
import re
import typing

import iso8601
import requests

from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from .. import get_logger
from .. import DSS_ELASTICSEARCH_INDEX_NAME, DSS_ELASTICSEARCH_DOC_TYPE, DSS_ELASTICSEARCH_QUERY_TYPE
from .. import DSS_ELASTICSEARCH_SUBSCRIPTION_INDEX_NAME, DSS_ELASTICSEARCH_SUBSCRIPTION_TYPE
from ..blobstore import BlobNotFoundError
from ..config import Config
from ..hcablobstore import FileMetadata, HCABlobStore
from ..util.es import ElasticsearchClient, get_elasticsearch_index

logger = get_logger()


def get(uuid: str, replica: str):
    pass


def find(replica: str):
    pass


def put(extras: dict, replica: str):
    pass


def delete(uuid: str, replica: str):
    pass
