import os
import sys

import domovoi

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), 'domovoilib'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss.stepfunctions.s3copyclient as s3copyclient
import dss.stepfunctions.generator as generator

app = domovoi.Domovoi()


annotation_processor = generator.StateMachineAnnotationProcessor()
sfn = annotation_processor.process_annotations(s3copyclient.sfn)
app.register_state_machine(sfn)
