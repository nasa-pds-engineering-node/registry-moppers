import unittest

from pds.registrysweepers.repairkit import allarrays


class AllArrays(unittest.TestCase):
    def test_valid_field(self):
        src = {"apple": ["orange"]}
        repair = allarrays.repair(src, "apple")
        self.assertEqual({}, repair)

    def test_invalid_field(self):
        src = {"apple": "orange"}
        repair = allarrays.repair(src, "apple")
        self.assertEqual({"apple": ["orange"]}, repair)

    def test_exclusion_logic(self):
        src = {
            "lid": "urn:nasa:pds:clementine_lwir_bt:data_flatfield:ff034ag.img",
            "vid": "1.0",
            "lidvid": "urn:nasa:pds:clementine_lwir_bt:data_flatfield:ff034ag.img::1.0",
            "title": "Clementine LWIR brightness temperature flat field product: ff034ag.img",
            "product_class": "Product_Observational",
            "_package_id": "c0491371-49f8-4e34-9d1c-f94ef1217b57",
            "ops:Provenance/ops:parent_collection_identifier": ["urn:nasa:pds:clementine_lwir_bt:data_flatfield::1.0"],
            "ops:Provenance/ops:parent_bundle_identifier": ["urn:nasa:pds:clementine_lwir_bt::1.0"],
            "ops:Provenance/ops:registry_sweepers_repairkit_version": 2,
            "ops:Provenance/someStringTypedProp": "someValue",
            "ops:Tracking_Meta/ops:archive_status": "archived",
        }

        repairs = {}

        for fieldname in src:
            if fieldname not in allarrays.EXCLUDED_PROPERTIES:
                repairs.update(allarrays.repair(src, fieldname))

        self.assertDictEqual({}, repairs, "test that excluded (string-typed) fields do not result in repair changes")

    def test_temporary_reversion_fix(self):
        # TODO: remove me once applied to prod -- edunn 20231206

        src = {
            "lid": ["urn:nasa:pds:clementine_lwir_bt:data_flatfield:ff034ag.img"],
            "vid": ["1.0"],
            "lidvid": ["urn:nasa:pds:clementine_lwir_bt:data_flatfield:ff034ag.img::1.0"],
            "title": ["Clementine LWIR brightness temperature flat field product: ff034ag.img"],
            "product_class": ["Product_Observational"],
            "_package_id": ["c0491371-49f8-4e34-9d1c-f94ef1217b57"],
            "someProperty": ["somePropertyValue"],
            "ops:Provenance/ops:parent_collection_identifier": ["urn:nasa:pds:clementine_lwir_bt:data_flatfield::1.0"],
            "ops:Provenance/ops:parent_bundle_identifier": ["urn:nasa:pds:clementine_lwir_bt::1.0"],
            "ops:Provenance/ops:registry_sweepers_repairkit_version": 2,
        }

        expected = {
            "lid": "urn:nasa:pds:clementine_lwir_bt:data_flatfield:ff034ag.img",
            "vid": "1.0",
            "lidvid": "urn:nasa:pds:clementine_lwir_bt:data_flatfield:ff034ag.img::1.0",
            "title": "Clementine LWIR brightness temperature flat field product: ff034ag.img",
            "product_class": "Product_Observational",
            "_package_id": "c0491371-49f8-4e34-9d1c-f94ef1217b57",
            "someProperty": ["somePropertyValue"],
            "ops:Provenance/ops:parent_collection_identifier": ["urn:nasa:pds:clementine_lwir_bt:data_flatfield::1.0"],
            "ops:Provenance/ops:parent_bundle_identifier": ["urn:nasa:pds:clementine_lwir_bt::1.0"],
            "ops:Provenance/ops:registry_sweepers_repairkit_version": 2,
        }

        repairs = {}

        for fieldname in src:
            if fieldname in allarrays.EXCLUDED_PROPERTIES:
                repairs.update(allarrays.apply_reversion_fix(src, fieldname))

        src.update(repairs)
        self.assertDictEqual(
            expected,
            src,
            "test that enumerated string-typed fields are converted back to strings when reversion fix is applied",
        )


if __name__ == "__main__":
    unittest.main()
