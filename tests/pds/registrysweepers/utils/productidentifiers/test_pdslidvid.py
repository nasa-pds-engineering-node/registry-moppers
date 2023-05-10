import unittest

from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid


class PdsLidVidTestCase(unittest.TestCase):
    def test_invalid_instantiation(self):
        self.assertRaises(ValueError, lambda: PdsLidVid.from_string("some:lid:without:vid"))
        self.assertRaises(ValueError, lambda: PdsLidVid.from_string("some:lid:with:no:vid::"))
        self.assertRaises(ValueError, lambda: PdsLidVid.from_string("some:lid:with:bad:vid::1.2.3"))

    def test_equality(self):
        base = PdsLidVid.from_string("urn:nasa:pds:epoxi::1.0")
        equal = PdsLidVid.from_string("urn:nasa:pds:epoxi::1.0")
        different_lid = PdsLidVid.from_string("urn:nasa:pds:notEpoxi::1.0")
        different_vid = PdsLidVid.from_string("urn:nasa:pds:epoxi::1.1")

        self.assertEqual(base, equal)
        self.assertEqual(base.__hash__(), equal.__hash__())

        self.assertNotEqual(base, different_lid)
        self.assertNotEqual(base.__hash__(), different_lid.__hash__())

        self.assertNotEqual(base, different_vid)
        self.assertNotEqual(base.__hash__(), different_vid.__hash__())

    def test_comparison(self):
        first = PdsLidVid.from_string("something::1.0")
        second = PdsLidVid.from_string("something::2.0")
        third = PdsLidVid.from_string("something::3.0")
        mismatched = PdsLidVid.from_string("somethingElse::4.0")

        valid = [third, first, second]
        valid.sort()
        self.assertEqual(valid, [first, second, third])

        invalid = [third, first, second, mismatched]
        self.assertRaises(ValueError, invalid.sort)


if __name__ == "__main__":
    unittest.main()
