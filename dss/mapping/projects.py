import os

from dss import dynamodb  # type: ignore


project_to_bundles_db_table = f"dss-projects-db-{os.environ['DSS_DEPLOYMENT_STAGE']}"


def get_project_for_bundle(bundle_uuid: str):
    return dynamodb.get_item(table=project_to_bundles_db_table,
                             hash_key=bundle_uuid,
                             return_key='body')


def put_project_for_bundle(bundle_uuid: str, project_uuid: str):
    dynamodb.put_item(table=project_to_bundles_db_table,
                      hash_key=bundle_uuid,
                      value=project_uuid)


def delete_project_for_bundle(bundle_uuid: str):
    dynamodb.delete_item(table=project_to_bundles_db_table,
                         hash_key=bundle_uuid)
