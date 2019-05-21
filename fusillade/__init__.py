from .errors import FusilladeException, FusilladeBindingException
from .config import Config
from .clouddirectory import User, Group, Role, CloudDirectory
from fusillade import logging

'''
system config:
- directory schema

service config:
- provisioning policy
'''


def get_directory():
    directory_name = Config.get_directory_name()
    try:
        _directory = CloudDirectory.from_name(directory_name)
    except FusilladeException:
        from .clouddirectory import publish_schema, create_directory
        schema_name = Config.get_schema_name()
        schema_arn = publish_schema(schema_name, version="0.4")  # TODO make version an environment variable
        admins = Config.get_admin_emails()
        _directory = create_directory(directory_name, schema_arn, admins)
    return _directory


directory = get_directory()
# TODO: change the deployment to be in a separate file
