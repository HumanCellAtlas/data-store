
from .integration_test import IntegrationTest
from .reindex import Reindex
from .storage import StorageVisitation

registered_visitations = {c.__name__: c for c in [
    IntegrationTest,
    Reindex,
    StorageVisitation,
]}
