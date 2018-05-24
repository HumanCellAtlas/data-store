import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss.stepfunctions.gscopyclient as gscopyclient
from dss.util import tracing

app = domovoi.Domovoi()
app.register_state_machine(gscopyclient.copy_write_metadata_sfn)
