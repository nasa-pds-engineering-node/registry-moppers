import unittest

from pds.registrysweepers.utils.productidentifiers.pdsvid import PdsVid


class PdsVidTestCase(unittest.TestCase):
    def test_valid_instantiation(self):
        PdsVid.from_string("1.0")
        return self.defaultTestResult()

    def test_invalid_instantiation(self):
        bad_strings = {"1.-1", "1.0.0"}
        for string in bad_strings:
            self.assertRaises(ValueError, lambda: PdsVid.from_string(string))

    def test_equality(self):
        base = PdsVid(1, 0)
        equal = PdsVid(1, 0)
        different_major_version = PdsVid(2, 0)
        different_minor_version = PdsVid(1, 1)

        self.assertEqual(base, equal)
        self.assertEqual(base.__hash__(), equal.__hash__())

        self.assertNotEqual(base, different_major_version)
        self.assertNotEqual(base.__hash__(), different_major_version.__hash__())

        self.assertNotEqual(base, different_minor_version)
        self.assertNotEqual(base.__hash__(), different_minor_version.__hash__())

    def test_comparison(self):
        base = PdsVid(5, 5)
        equal = PdsVid(5, 5)
        higher_major_version = PdsVid(10, 5)
        lower_major_version = PdsVid(1, 5)
        higher_minor_version = PdsVid(5, 10)
        lower_minor_version = PdsVid(5, 1)
        higher_major_lower_minor = PdsVid(10, 1)
        lower_major_higher_minor = PdsVid(1, 10)

        self.assertEqual(base, equal)
        self.assertLess(base, higher_major_version)
        self.assertGreater(base, lower_major_version)
        self.assertLess(base, higher_minor_version)
        self.assertGreater(base, lower_minor_version)
        self.assertLess(base, higher_major_lower_minor)
        self.assertGreater(base, lower_major_higher_minor)


if __name__ == "__main__":
    unittest.main()
