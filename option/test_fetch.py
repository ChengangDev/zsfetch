import unittest
from option import fetch


class MyTestCase(unittest.TestCase):
    def test_something(self):
        fetch.Fetch('', '')
        self.assertEqual(True, True)

        
class YourTestCase(unittest.TestCase):
    def test_nothing(self):
        self.assertEqual(True, False)


if __name__ == '__main__':
    unittest.main()
