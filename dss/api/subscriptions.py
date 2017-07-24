import datetime
import io
import json
import re
import typing

import iso8601
import requests

from flask import jsonify, make_response, redirect, request
from werkzeug.exceptions import BadRequest

from ..blobstore import BlobNotFoundError
from ..config import Config
from ..hcablobstore import FileMetadata, HCABlobStore
from ..util.es import ElasticsearchClient, get_elasticsearch_index


def get(uuid: str, replica: str):
    pass


def find(replica: str):
    pass


def put(extras: dict, replica: str):
    pass


def delete(uuid: str, replica: str):
    pass
