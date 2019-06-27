import json
import os

from furl import furl

from tests.common import service_accounts, new_test_directory
from tests.infra.testmode import is_integration

if not is_integration():
    old_directory_name = os.getenv("FUSILLADE_DIR", None)
    directory, schema_arn = new_test_directory()

from fusillade import Config
from fusillade.clouddirectory import cleanup_directory, User, get_published_schema_from_directory, cleanup_schema


class BaseAPITest():

    @classmethod
    def setUpClass(cls):
        try:
            User.provision_user(service_accounts['admin']['client_email'], roles=['fusillade_admin'])
        except Exception:
            pass

        if is_integration():
            from tests.infra.integration_server import IntegrationTestHarness
            cls.app = IntegrationTestHarness()
        else:
            from tests.infra.server import ChaliceTestHarness
            # ChaliceTestHarness must be imported after FUSILLADE_DIR has be set
            cls.app = ChaliceTestHarness()

    @staticmethod
    def clear_directory(**kwargs):
        kwargs["users"] = kwargs.get('users', []) + [*Config.get_admin_emails()]
        Config.get_directory().clear(**kwargs)

    @classmethod
    def tearDownClass(cls):
        cls.clear_directory()
        if not is_integration():
            directory_arn = Config.get_directory()._dir_arn
            schema_arn = get_published_schema_from_directory(directory_arn)
            cleanup_directory(directory_arn)
            cleanup_schema(f"{schema_arn}/0")
            if old_directory_name:
                os.environ["FUSILLADE_DIR"] = old_directory_name

    def _test_paging(self, url, headers, per_page, key):
        url = furl(url)
        url.add(query_params={'per_page': per_page})
        resp = self.app.get(url.url, headers=headers)
        self.assertEqual(206, resp.status_code)
        self.assertEqual(per_page, len(json.loads(resp.body)[key]))
        self.assertTrue("Link" in resp.headers)
        result = json.loads(resp.body)[key]
        next_url = resp.headers['Link'].split(';')[0][1:-1]
        resp = self.app.get(next_url, headers=headers)
        self.assertEqual(200, resp.status_code)
        self.assertFalse("Link" in resp.headers)
        next_results = json.loads(resp.body)[key]
        self.assertLessEqual(len(next_results), per_page)
        result.extend(next_results)
        return result
