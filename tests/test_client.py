import unittest

from tap_appstore.client import AppStoreStream


class TestAppStoreStream(unittest.TestCase):
    def test_convert_date_valid(self):
        # Test a valid date string
        date_str = "2024-05-01"
        expected_result = "2024-05-01T00:00:00"
        result = AppStoreStream.convert_date(date_str)
        self.assertEqual(result, expected_result)

    def test_convert_date_empty(self):
        # Test an empty date string
        date_str = ""
        result = AppStoreStream.convert_date(date_str)
        self.assertIsNone(result)

    def test_convert_date_custom_format(self):
        # Test a valid date string with a custom format
        date_str = "01-05-2024"
        expected_result = "2024-05-01T00:00:00"
        result = AppStoreStream.convert_date(date_str, date_format="%d-%m-%Y")
        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
