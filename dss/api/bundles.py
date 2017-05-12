from flask import redirect

def get(uuid, replica):
    return redirect("http://example.com")

def list():
    return dict(bundles=[dict(uuid="", versions=[])])

def post():
    pass
