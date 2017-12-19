
import os
import sys
import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import BucketConfig, Config
import dss.stepfunctions.generator as generator
from dss.stepfunctions.visitation.implementation import sfn


app = domovoi.Domovoi()
Config.set_config(BucketConfig.NORMAL)


annotation_processor = generator.StateMachineAnnotationProcessor()
sfn = annotation_processor.process_annotations(sfn)
app.register_state_machine(sfn)
