from flask import redirect

def get(uuid, replica):
    return redirect("http://example.com")

def list():
    return dict(files=[dict(uuid="", name="", versions=[])])

def post():
    pass
