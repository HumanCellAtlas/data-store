from flask import redirect

def get(uuid, bundle_version, replica):
    return []

def list_versions(uuid):
    return ["2014-10-23T00:35:14.800221Z"]

def list():
    return dict(bundles=[dict(uuid="", versions=[])])

def post():
    pass
