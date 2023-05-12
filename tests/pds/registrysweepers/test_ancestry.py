import itertools
import os.path
import unittest
from typing import Dict
from typing import List

from pds.registrysweepers import ancestry
from pds.registrysweepers.ancestry import AncestryRecord

from tests.mocks.registryquerymock import RegistryQueryMock


class AncestryFunctionalTestCase(unittest.TestCase):
    input_file_path = os.path.abspath("./tests/pds/registrysweepers/test_ancestry_mock_registry.json")
    registry_query_mock = RegistryQueryMock(input_file_path)

    ancestry_records: List[AncestryRecord] = []
    bulk_updates: List[Dict[str, Dict]] = []

    expected_bundle_ancestry_by_collection = {
        "a:b:c:bundle:lidrefcollection::1.0": {"a:b:c:bundle::1.0"},
        "a:b:c:bundle:lidrefcollection::2.0": {"a:b:c:bundle::1.0"},
        "a:b:c:bundle:lidvidrefcollection::1.0": {"a:b:c:bundle::1.0"},
        "a:b:c:bundle:lidvidrefcollection::2.0": {
            # intentionally empty
        },
    }

    expected_collection_ancestry_by_nonaggregate = {
        "a:b:c:bundle:lidrefcollection:collectionsharedproduct::1.0": {
            "a:b:c:bundle:lidrefcollection::1.0",
            "a:b:c:bundle:lidrefcollection::2.0",
        },
        "a:b:c:bundle:lidrefcollection:collectionuniqueproduct::1.0": {
            "a:b:c:bundle:lidrefcollection::1.0",
        },
        "a:b:c:bundle:lidrefcollection:collectionuniqueproduct::2.0": {
            "a:b:c:bundle:lidrefcollection::2.0",
        },
        "a:b:c:bundle:lidvidrefcollection:collectionsharedproduct::1.0": {
            "a:b:c:bundle:lidvidrefcollection::1.0",
            "a:b:c:bundle:lidvidrefcollection::2.0",
        },
        "a:b:c:bundle:lidvidrefcollection:collectionuniqueproduct::1.0": {
            "a:b:c:bundle:lidvidrefcollection::1.0",
        },
        "a:b:c:bundle:lidvidrefcollection:collectionuniqueproduct::2.0": {
            "a:b:c:bundle:lidvidrefcollection::2.0",
        },
    }

    @classmethod
    def setUpClass(cls) -> None:
        ancestry.run(
            base_url="",
            username="",
            password="",
            registry_mock_query_f=cls.registry_query_mock.get_mocked_query,
            ancestry_records_accumulator=cls.ancestry_records,
            bulk_updates_sink=cls.bulk_updates,
        )

        cls.bundle_records = [r for r in cls.ancestry_records if r.lidvid.is_bundle()]
        cls.collection_records = [r for r in cls.ancestry_records if r.lidvid.is_collection()]
        cls.nonaggregate_records = [r for r in cls.ancestry_records if r.lidvid.is_basic_product()]

        cls.records_by_lidvid_str = {str(r.lidvid): r for r in cls.ancestry_records}
        cls.bundle_records_by_lidvid_str = {str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_bundle()}
        cls.collection_records_by_lidvid_str = {
            str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_collection()
        }
        cls.nonaggregate_records_by_lidvid_str = {
            str(r.lidvid): r for r in cls.ancestry_records if r.lidvid.is_basic_product()
        }

        cls.updates_by_lidvid_str = {id: content for id, content in cls.bulk_updates}

    def test_bundles_have_no_ancestry(self):
        for record in self.bundle_records:
            self.assertTrue(len(record.parent_bundle_lidvids) == 0)
            self.assertTrue(len(record.parent_collection_lidvids) == 0)

    def test_collections_have_no_collection_ancestry(self):
        for record in self.collection_records:
            self.assertTrue(len(record.parent_collection_lidvids) == 0)

    def test_collections_have_correct_bundle_ancestry(self):
        for record in self.collection_records:
            expected_bundle_ancestry = set(self.expected_bundle_ancestry_by_collection[str(record.lidvid)])
            self.assertEqual(expected_bundle_ancestry, set(str(id) for id in record.parent_bundle_lidvids))

    def test_nonaggregates_have_correct_collection_ancestry(self):
        for record in self.nonaggregate_records:
            expected_collection_ancestry = set(self.expected_collection_ancestry_by_nonaggregate[str(record.lidvid)])
            self.assertEqual(expected_collection_ancestry, set(str(id) for id in record.parent_collection_lidvids))

    def test_nonaggregates_have_correct_bundle_ancestry(self):
        print(
            "#### N.B. This test will always fail if test_nonaggregates_have_correct_collection_ancestry() fails! ####"
        )
        for record in self.nonaggregate_records:
            parent_collection_id_strs = set(str(id) for id in record.parent_collection_lidvids)
            parent_bundle_id_strs = set(str(id) for id in record.parent_bundle_lidvids)
            expected_bundle_id_strs = set(
                itertools.chain(*[self.expected_bundle_ancestry_by_collection[id] for id in parent_collection_id_strs])
            )
            self.assertEqual(expected_bundle_id_strs, parent_bundle_id_strs)

    def test_correct_bulk_update_kvs_are_produced(self):
        for record in self.ancestry_records:
            update = self.updates_by_lidvid_str[str(record.lidvid)]
            self.assertEqual(
                set(str(lidvid) for lidvid in record.parent_bundle_lidvids),
                set(update["ops:Provenance/ops:parent_bundle_identifiers"]),
            )
            self.assertEqual(
                set(str(lidvid) for lidvid in record.parent_collection_lidvids),
                set(update["ops:Provenance/ops:parent_collection_identifiers"]),
            )

        for doc_id, update in self.bulk_updates:
            record = self.records_by_lidvid_str[doc_id]
            self.assertEqual(
                set(update["ops:Provenance/ops:parent_bundle_identifiers"]),
                set(str(lidvid) for lidvid in record.parent_bundle_lidvids),
            )
            self.assertEqual(
                set(update["ops:Provenance/ops:parent_collection_identifiers"]),
                set(str(lidvid) for lidvid in record.parent_collection_lidvids),
            )


# TODO:  Add test-case for orphan detection logging (collections AND non-aggs)

if __name__ == "__main__":
    unittest.main()
