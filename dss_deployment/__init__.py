import os
import sys
import json
import copy
import boto3
import click
import shutil
from botocore.exceptions import ProfileNotFound


pkg_root = os.path.abspath(os.path.dirname(__file__))  # noqa
dpl_root = os.path.abspath(os.path.join(pkg_root, '..', 'deployment'))


class DSSDeployment:
    def __init__(self, stage):
        self.root = os.path.abspath(os.path.join(dpl_root, stage))

        self.var_files = dict()
        self.variables = dict()
        for root, dirs, files in os.walk(os.path.join(pkg_root, 'stage_template')):
            for f in files:
                if f.startswith('.'):
                    continue
                if 'variable' in f:
                    self.var_files[f] = self._read_vars(f)['variable']
                    for k in self.var_files[f]:
                        self.variables[k] = self.var_files[f][k]
            break

        self.variables['DSS_DEPLOYMENT_STAGE']['default'] = stage

    def value(self, k):
        return self.variables[k]['default']

    def _read_vars(self, name):
        filepath = os.path.join(self.root, name)
        if os.path.isfile(filepath):
            with open(filepath, 'r') as fp:
                vars = json.loads(fp.read())
        else:
            vars = {'variable': {}}

        with open(os.path.join(pkg_root, 'stage_template', name), 'r') as fp:
            in_vars = json.loads(fp.read())

        for key, val in in_vars['variable'].items():
            if key not in vars['variable']:
                vars['variable'][key] = val

        return vars

    def _write_config(self, name, data):
        filepath = os.path.join(self.root, name)
        with open(filepath, 'w') as fp:
            fp.write(json.dumps(data, indent=2))

    @property
    def stage(self):
        return self.variables['DSS_DEPLOYMENT_STAGE']['default']

    def _backend(self, comp):
        bucket_url = self.variables['tf_backend']['default']

        if bucket_url is None:
            return None

        bucket = bucket_url[5:]
        region = _bucket_region(bucket_url, self.value('aws_profile'))

        if bucket_url.startswith('s3://'):
            template = s3_backend_template.strip().replace('\n', os.linesep)
        elif bucket_url.startswith('gs://'):
            template = gs_backend_template.strip().replace('\n', os.linesep)
        else:
            raise Exception(f'Unsupported backend {bucket_url}')

        return template.format(
            bucket=bucket,
            key=f'dss-{comp}-{self.stage}.tfstate',
            region=region
        )

    def write(self):
        stage = self.stage

        copytree_only_new(
            os.path.join(pkg_root, 'stage_template'),
            self.root
        )

        for f in self.var_files:
            self._write_config(f, {'variable': self.var_files[f]})

        def _link(name, comp):
            src_dir = os.path.join(self.root, name)
            dst_dir = os.path.join(self.root, comp)
            rel_link(src_dir, dst_dir, name)
    
        for comp in os.listdir(self.root):
            if not os.path.isdir(os.path.join(self.root, comp)):
                continue
    
            backend = self._backend(comp)
            if backend is not None:
                with open(os.path.join(self.root, comp, 'backend.tf'), 'w') as fp:
                    fp.write(backend)

            for f in self.var_files:
                _link(f, comp)
            _link('providers.tf', comp)


def copytree_only_new(src_root, dst_root):
    for root, dirs, files in os.walk(src_root):
        for file in files:
            if file.startswith('.'):
                continue
            src = os.path.join(root, file)
            dst = src.replace(src_root, dst_root)
            if not os.path.isdir(os.path.dirname(dst)):
                os.makedirs(os.path.dirname(dst))
            if not os.path.exists(dst):
                shutil.copy(src, dst)


def set_active_stage(stage):
    if stage not in current_stages():
        raise Exception(f'Stage {stage} does not exist')
    rel_link(os.path.join(dpl_root, stage), os.path.join(dpl_root), 'active')


def current_stages():
    for path, dirs, files in os.walk(dpl_root):
        for d in dirs:
            if not os.path.islink(os.path.join(dpl_root, d)):
                yield d
        break


def rel_link(srcdir, dstdir, target):
    try:
        os.remove(os.path.join(dstdir, target))
    except FileNotFoundError:
        pass

    os.symlink(
        os.path.relpath(srcdir, dstdir),
        os.path.join(dstdir, target)
    )

def active():
    stage = os.path.basename(
        os.path.realpath(
            os.path.join(dpl_root, 'active')
        )
    )
    return DSSDeployment(stage)


def _bucket_region(bucket_url, aws_profile):
    if not bucket_url.startswith('s3'):
        return None
    bucket = bucket_url[5:]
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.client('s3')
    loc = s3.get_bucket_location(Bucket=bucket)['LocationConstraint']
    if loc is None:
        loc = 'us-east-1'
    return loc


s3_backend_template = """
terraform {{
  backend "s3" {{
    bucket = "{bucket}"
    key = "{key}"
    region = "{region}"
  }}
}}
"""

gs_backend_template = """
terraform {{
  backend "gcs" {{
    bucket = "{bucket}"
    prefix = "{key}"
  }}
}}
"""
