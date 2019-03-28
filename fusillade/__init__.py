from .clouddirectory import User, Group, Role, CloudDirectory
from .config import Config
from .errors import FusilladeException
'''
system config:
- directory schema

service config:
- provisioning policy

Q: how to model user-org relationships? e.g. member vs. admin
A: each org should be represented by the org node and sub-nodes (members, admins, ...)

Q: how do i enumerate all resources that a user has access to?

    org  project->policy[resource_id]
      |  |
members  members->policy[resource_id(parent?)]
      |  |
      user



'''

directory_name = Config.get_directory_name()
try:
    directory = CloudDirectory.from_name(directory_name)
except FusilladeException:
    from .clouddirectory import publish_schema, create_directory
    schema_name = Config.get_schema_name()
    schema_arn = publish_schema(schema_name, version="0.1")  # TODO make version an environment variable
    directory = create_directory(directory_name, schema_arn)
