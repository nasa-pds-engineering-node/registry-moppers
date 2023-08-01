import unittest

from pds.registrysweepers.repairkit import allarrays


class AllArrays(unittest.TestCase):
    def test_valid_field(self):
        src = {'apple': ['orange']} 
        repair = allarrays.repair(src, 'apple')
        self.assertEqual({}, repair)
    def test_invalid_field(self):
        src = {'apple': 'orange'} 
        repair = allarrays.repair(src, 'apple')
        self.assertEqual({'apple': ['orange']}, repair)


if __name__ == '__main__':
    unittest.main()
