import os


def get_env(varname):
    if varname not in os.environ:
        raise RuntimeError(
            "Please set the {} environment variable".format(varname))
    return os.environ[varname]
