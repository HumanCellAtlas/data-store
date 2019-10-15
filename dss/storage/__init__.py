from typing import NamedTuple, Optional


class Tombstone(NamedTuple):
    """
    Tombstone object compliant with RFC #4 (Deletion of data in the DCP).
    See HumanCellAtlas/dcp-community.
    """
    email: str
    reason: str
    details: Optional[dict] = {}
    admin_deleted: bool = True
