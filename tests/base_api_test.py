import json
import os

from furl import furl

from tests import random_hex_string

integration = os.getenv('INTEGRATION_TEST', 'False').lower() == 'true'
if not integration:
    old_directory_name = os.getenv("FUSILLADE_DIR", None)
    os.environ["FUSILLADE_DIR"] = "test_api_" + random_hex_string()

from tests.common import service_accounts
from fusillade import directory, Config
from fusillade.clouddirectory import cleanup_directory, User


class BaseAPITest():
    integration = False

    @classmethod
    def setUpClass(cls):
        try:
            User.provision_user(directory, service_accounts['admin']['client_email'], roles=['admin'])
        except Exception:
            pass

        if integration:
            from tests.infra.integration_server import IntegrationTestHarness
            cls.app = IntegrationTestHarness()
            cls.integration = integration
        else:
            from tests.infra.server import ChaliceTestHarness
            # ChaliceTestHarness must be imported after FUSILLADE_DIR has be set
            cls.app = ChaliceTestHarness()

    @staticmethod
    def clear_directory(**kwargs):
        kwargs["users"] = kwargs.get('users', []) + [*Config.get_admin_emails()]
        directory.clear(**kwargs)

    @classmethod
    def tearDownClass(cls):
        cls.clear_directory()

        if not integration:
            cleanup_directory(directory._dir_arn)
            if old_directory_name:
                os.environ["FUSILLADE_DIR"] = old_directory_name

    def _test_paging(self, url, headers, per_page, key):
        url = furl(url, query_params={'per_page': per_page})
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
        self.assertEqual(len(next_results), per_page)
        result.extend(next_results)
        return result