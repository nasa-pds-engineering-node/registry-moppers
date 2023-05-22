import unittest

from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid


class PdsLidTest(unittest.TestCase):
    def test_equality(self):
        base_lid = PdsLid("urn:nasa:pds:epoxi")
        equal_lid = PdsLid("urn:nasa:pds:epoxi")
        unequal_lid = PdsLid("urn:nasa:pds:notEpoxi")

        self.assertEqual(base_lid, equal_lid)
        self.assertEqual(base_lid.__hash__(), equal_lid.__hash__())

        self.assertNotEqual(base_lid, unequal_lid)
        self.assertNotEqual(base_lid.__hash__(), unequal_lid.__hash__())

    def test_bundle_status_correctly_identified(self):
        bundle_lid = PdsLid("urn:nasa:pds:bundle")
        self.assertTrue(bundle_lid.is_bundle())
        self.assertFalse(bundle_lid.is_collection())
        self.assertFalse(bundle_lid.is_basic_product())

    def test_collection_status_correctly_identified(self):
        collection_lid = PdsLid("urn:nasa:pds:bundle:collection")
        self.assertFalse(collection_lid.is_bundle())
        self.assertTrue(collection_lid.is_collection())
        self.assertFalse(collection_lid.is_basic_product())

    def test_basic_product_status_correctly_identified(self):
        nonaggregate_lid = PdsLid("urn:nasa:pds:bundle:collection:product")
        self.assertFalse(nonaggregate_lid.is_bundle())
        self.assertFalse(nonaggregate_lid.is_collection())
        self.assertTrue(nonaggregate_lid.is_basic_product())

    def test_bundle_fields_correctly_gotten(self):
        lid = PdsLid("urn:nasa:pds:bundlename")
        self.assertEqual("nasa", lid.national_agency_name)
        self.assertEqual("pds", lid.archiving_agency_name)
        self.assertEqual("bundlename", lid.bundle_name)
        self.assertIsNone(lid.collection_name)
        self.assertIsNone(lid.basic_product_name)

    def test_collection_fields_correctly_gotten(self):
        lid = PdsLid("urn:nasa:pds:bundlename:collectionname")
        self.assertEqual("nasa", lid.national_agency_name)
        self.assertEqual("pds", lid.archiving_agency_name)
        self.assertEqual("bundlename", lid.bundle_name)
        self.assertEqual("collectionname", lid.collection_name)
        self.assertIsNone(lid.basic_product_name)

    def test_nonaggregate_fields_correctly_gotten(self):
        lid = PdsLid("urn:nasa:pds:bundlename:collectionname:productname")
        self.assertEqual("nasa", lid.national_agency_name)
        self.assertEqual("pds", lid.archiving_agency_name)
        self.assertEqual("bundlename", lid.bundle_name)
        self.assertEqual("collectionname", lid.collection_name)
        self.assertEqual("productname", lid.basic_product_name)

    def test_get_parent_bundle_lid(self):
        bundle_lid = PdsLid("urn:nasa:pds:bundlename")
        collection_lid = PdsLid("urn:nasa:pds:bundlename:collectionname")
        nonaggregate_lid = PdsLid("urn:nasa:pds:bundlename:collectionname:productname")

        self.assertIsNone(bundle_lid.parent_bundle_lid)
        self.assertEqual(bundle_lid, collection_lid.parent_bundle_lid)
        self.assertEqual(bundle_lid, nonaggregate_lid.parent_bundle_lid)

    def test_get_parent_collection_lid(self):
        bundle_lid = PdsLid("urn:nasa:pds:bundlename")
        collection_lid = PdsLid("urn:nasa:pds:bundlename:collectionname")
        nonaggregate_lid = PdsLid("urn:nasa:pds:bundlename:collectionname:productname")

        self.assertIsNone(bundle_lid.parent_collection_lid)
        self.assertIsNone(collection_lid.parent_collection_lid)
        self.assertEqual(collection_lid, nonaggregate_lid.parent_collection_lid)


if __name__ == "__main__":
    unittest.main()
