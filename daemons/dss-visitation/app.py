import os
import sys
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss import BucketConfig, Config
from dss.stepfunctions.visitation.implementation import sfn


app = domovoi.Domovoi()
Config.set_config(BucketConfig.NORMAL)


app.register_state_machine(sfn)
