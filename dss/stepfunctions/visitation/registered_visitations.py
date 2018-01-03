
from .integration_test import IntegrationTest
from .reindex import Reindex


registered_visitations = {
    'IntegrationTest': IntegrationTest,
    'Reindex': Reindex
}
