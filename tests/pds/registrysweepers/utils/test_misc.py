import unittest

from pds.registrysweepers.utils.misc import iterate_pages_of_size


class IteratePagesOfTestCase(unittest.TestCase):
    def test_basic_functionality(self):
        page_size = 2
        input = [1, 2, 3, 4, 5, 6]
        output = list(iterate_pages_of_size(page_size, input))
        expected = [[1, 2], [3, 4], [5, 6]]
        self.assertListEqual(expected, output)

    def test_partial_final_page(self):
        page_size = 2
        input = [1, 2, 3]
        output = list(iterate_pages_of_size(page_size, input))
        expected = [[1, 2], [3]]
        self.assertListEqual(expected, output)

    def test_empty_input(self):
        self.assertEqual([], list(iterate_pages_of_size(1, [])))

    def test_invalid_page_size(self):
        self.assertRaises(ValueError, lambda: list(iterate_pages_of_size(0, [1, 2, 3])))


if __name__ == "__main__":
    unittest.main()
