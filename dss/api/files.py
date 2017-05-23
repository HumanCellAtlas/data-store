from flask import redirect
from .. import logger

def get(uuid, replica):
    logger.info("This is a log message.")
    return redirect("http://example.com")

def list():
    return dict(files=[dict(uuid="", name="", versions=[])])

def post():
    pass
