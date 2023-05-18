from dataclasses import dataclass
from dataclasses import field
from typing import List

from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


@dataclass
class AncestryRecord:
    lidvid: PdsLidVid
    parent_collection_lidvids: List[PdsLidVid] = field(default_factory=list)
    parent_bundle_lidvids: List[PdsLidVid] = field(default_factory=list)

    def __repr__(self):
        return f"AncestryRecord(lidvid={self.lidvid}, parent_collection_lidvids={[str(x) for x in self.parent_collection_lidvids]}, parent_bundle_lidvids={[str(x) for x in self.parent_bundle_lidvids]})"

    def __hash__(self):
        return hash(self.lidvid)
