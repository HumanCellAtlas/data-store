from .errors import FusilladeException, FusilladeBindingException
from .config import Config
from .clouddirectory import User, Group, Role, CloudDirectory


'''
system config:
- directory schema

service config:
- provisioning policy
'''


def get_directory():
    directory_name = Config.get_directory_name()
    try:
        directory = CloudDirectory.from_name(directory_name)
    except FusilladeException:
        from .clouddirectory import publish_schema, create_directory
        schema_name = Config.get_schema_name()
        schema_arn = publish_schema(schema_name, version="0.2")  # TODO make version an environment variable
        directory = create_directory(directory_name, schema_arn)
    return directory


directory = get_directory()
