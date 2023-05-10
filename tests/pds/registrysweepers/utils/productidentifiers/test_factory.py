import unittest

from pds.registrysweepers.utils.productidentifiers.factory import PdsProductIdentifierFactory
from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


class PdsProductIdentifierFactoryTestCase(unittest.TestCase):
    def test_lidvid_instantiation(self):
        identifier = PdsProductIdentifierFactory.from_string("urn:nasa:pds:epoxi::1.0")
        self.assertEqual(PdsLidVid, identifier.__class__)

    def test_lid_instantiation(self):
        identifier = PdsProductIdentifierFactory.from_string("urn:nasa:pds:epoxi")
        self.assertEqual(PdsLid, identifier.__class__)

    def test_invalid_instantiation(self):
        bad_strings = [
            "urn:nasa:pds:epoxi::1.0.0",
            "urn:nasa:pds:epoxi::1.",
            "urn:nasa:pds:epoxi::1",
            "urn:nasa:pds:epoxi::",
        ]
        for identifier in bad_strings:
            try:
                self.assertRaises(ValueError, lambda: PdsProductIdentifierFactory.from_string(identifier))
            except AssertionError as err:
                print(f'ValueError not raised when instantiating from "{identifier}"')
                raise err


if __name__ == "__main__":
    unittest.main()
