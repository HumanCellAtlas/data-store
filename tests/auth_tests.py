import requests
from tests import get_auth_header
from tests.infra import DSSAssertMixin


class TestAuthMixin(DSSAssertMixin):
    def _test_auth_errors(self, method: str, url: str, **kwargs):
        with self.subTest("Gibberish auth header"):  # type: ignore
            resp = self.assertResponse(method, url, requests.codes.unauthorized, headers=get_auth_header(False),
                                       **kwargs)
            self.assertEqual(resp.response.headers['Content-Type'], "application/problem+json")
            self.assertEqual(resp.json['title'], 'Failed to decode token.')

        with self.subTest("No auth header"):  # type: ignore
            resp = self.assertResponse(method, url, requests.codes.unauthorized, **kwargs)  # type: ignore
            self.assertEqual(resp.response.headers['Content-Type'], "application/problem+json")
            self.assertEqual(resp.json['title'], 'No authorization token provided')

        with self.subTest("unauthorized group"):  # type: ignore
            resp = self.assertResponse(method, url, requests.codes.forbidden, headers=get_auth_header(group='someone'),
                                       **kwargs)
            self.assertEqual(resp.response.headers['Content-Type'], "application/problem+json")
            self.assertEqual(resp.json['title'], 'User is not authorized to access this resource')

        # TODO test_bundle and test_file don't need email so remove this test so it can be run separatly
        if not url.split('/')[2] in ('files'):
            with self.subTest("no email claims"):  # type: ignore
                resp = self.assertResponse(method, url,
                                           requests.codes.unauthorized,
                                           headers=get_auth_header(email=False, email_claim=False),
                                           **kwargs)
                self.assertEqual(resp.response.headers['Content-Type'], "application/problem+json")
                self.assertEqual(resp.json['title'], 'Authorization token is missing email claims.')
