import json
import os

import boto3


class Config:
    _admin_emails: list = None
    _oauth2_config = None
    app = None
    audience = ["https://data.humancellatlas.org/" if os.environ["FUS_DEPLOYMENT_STAGE"] == 'prod'
                else "https://dev.data.humancellatlas.org/",
                "https://auth.data.humancellatlas.org/"]
    _openid_provider = None
    version = "unversioned"
    directory_schema_version = {"Version": '0', "MinorVersion": '0'}

    # TODO make configurable

    @classmethod
    def get_admin_emails(cls):
        if not cls._admin_emails:
            cls._admin_emails = [admin.strip() for admin in os.environ['FUS_ADMIN_EMAILS'].split(',') if admin.strip()]
            assert cls._admin_emails, "Initial administrator must be specified. Set FUS_ADMIN_EMAILS."
        return cls._admin_emails

    @classmethod
    def get_oauth2_config(cls):
        if not cls._oauth2_config:
            sm = boto3.client("secretsmanager")
            cls._oauth2_config = json.loads(
                sm.get_secret_value(
                    SecretId=f"{os.environ['FUS_SECRETS_STORE']}/"
                    f"{os.environ['FUS_DEPLOYMENT_STAGE']}/oauth2_config")["SecretString"])
        return cls._oauth2_config

    @classmethod
    def get_directory_name(cls):
        return os.getenv("FUSILLADE_DIR", f"hca_fusillade_{os.environ['FUS_DEPLOYMENT_STAGE']}")

    @classmethod
    def get_schema_name(cls):
        return f"hca_fusillade_base_{os.environ['FUS_DEPLOYMENT_STAGE']}"

    @classmethod
    def get_audience(cls):
        return cls.audience

    @classmethod
    def get_openid_provider(cls):
        if not cls._openid_provider:
            cls._openid_provider = os.environ['OPENID_PROVIDER']
        return cls._openid_provider

    @classmethod
    def log_level(cls):
        return int(os.environ.get("DEBUG", "1"))
