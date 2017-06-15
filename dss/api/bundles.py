import uuid

from flask import redirect

def get(uuid: str, bundle_version: str, replica: str):
    return []

def list_versions(uuid: str):
    return ["2014-10-23T00:35:14.800221Z"]

def list():
    return dict(bundles=[dict(uuid=str(uuid.uuid4()), versions=[])])

def post():
    pass
