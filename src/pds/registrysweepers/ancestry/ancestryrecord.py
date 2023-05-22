from dataclasses import dataclass
from dataclasses import field
from typing import Set

from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


@dataclass
class AncestryRecord:
    lidvid: PdsLidVid
    parent_collection_lidvids: Set[PdsLidVid] = field(default_factory=set)
    parent_bundle_lidvids: Set[PdsLidVid] = field(default_factory=set)

    def __repr__(self):
        return f"AncestryRecord(lidvid={self.lidvid}, parent_collection_lidvids={sorted([str(x) for x in self.parent_collection_lidvids])}, parent_bundle_lidvids={sorted([str(x) for x in self.parent_bundle_lidvids])})"

    def __hash__(self):
        return hash(self.lidvid)
