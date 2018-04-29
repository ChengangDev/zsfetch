import unittest

from zsfetch.optionsites import fetch


class MyTestCase(unittest.TestCase):
    def test_something(self):
        fetch.FetchOption()
        self.assertEqual(True, True)

        
class YourTestCase(unittest.TestCase):
    def test_nothing(self):
        self.assertEqual(False, False)


if __name__ == '__main__':
    unittest.main()
