import json
import os.path
import unittest

from pds.registrysweepers import provenance
from pds.registrysweepers.utils.db import Update


class ProvenanceBasicFunctionalTestCase(unittest.TestCase):
    input_file_path = os.path.abspath(
        "./tests/pds/registrysweepers/ancestry/resources/test_provenance_mock_ProvenanceBasicFunctionalTestCase.json"
    )

    extant_lidvids = [
        "urn:nasa:pds:bundle::1.0",
        "urn:nasa:pds:bundle::1.1",
        "urn:nasa:pds:bundle::2.0",
        "urn:nasa:pds:bundle:collection::10.0",
        "urn:nasa:pds:bundle:collection::10.1",
        "urn:nasa:pds:bundle:collection::20.0",
        "urn:nasa:pds:bundle:collection:product::100.0",
        "urn:nasa:pds:bundle:collection:product::100.1",
        "urn:nasa:pds:bundle:collection:product::200.0",
    ]

    def test_correct_provenance_produced(self):
        successors_by_lidvid = provenance.get_successors_by_lidvid(self.extant_lidvids)
        expected_provenance = {
            "urn:nasa:pds:bundle::1.0": "urn:nasa:pds:bundle::1.1",
            "urn:nasa:pds:bundle::1.1": "urn:nasa:pds:bundle::2.0",
            "urn:nasa:pds:bundle:collection::10.0": "urn:nasa:pds:bundle:collection::10.1",
            "urn:nasa:pds:bundle:collection::10.1": "urn:nasa:pds:bundle:collection::20.0",
            "urn:nasa:pds:bundle:collection:product::100.0": "urn:nasa:pds:bundle:collection:product::100.1",
            "urn:nasa:pds:bundle:collection:product::100.1": "urn:nasa:pds:bundle:collection:product::200.0",
        }
        self.assertDictEqual(expected_provenance, successors_by_lidvid)

        def crude_update_hash(update: Update) -> str:
            d = {"id": update.id, "content": update.content}
            return json.dumps(d, sort_keys=True)

        updates = provenance.generate_updates(successors_by_lidvid)
        expected_updates = [
            Update(id=k, content={"ops:Provenance/ops:superseded_by": v}) for k, v in expected_provenance.items()
        ]

        self.assertSetEqual(
            set(crude_update_hash(u) for u in expected_updates), set(crude_update_hash(u) for u in updates)
        )


if __name__ == "__main__":
    unittest.main()
