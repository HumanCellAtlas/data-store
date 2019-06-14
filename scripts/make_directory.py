import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from fusillade.clouddirectory import publish_schema, create_directory
from fusillade import Config


def setup_clouddirectory():
    schema_name = Config.get_schema_name()
    schema_arn = publish_schema(schema_name, **Config.directory_schema_version)
    admins = Config.get_admin_emails()
    directory_name = Config.get_directory_name()
    return create_directory(directory_name, schema_arn, admins)


if __name__ == "__main__":
    setup_clouddirectory()