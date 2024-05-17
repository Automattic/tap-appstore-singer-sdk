import unittest
from tap_appstore.client import AppStoreStream

class TestAppStoreStream(unittest.TestCase):
    def test_convert_date_valid(self):
        # Test a valid date string
        date_str = '2024-05-01'
        expected_result = '2024-05-01T00:00:00'
        result = AppStoreStream.convert_date(date_str)
        self.assertEqual(result, expected_result)

    def test_convert_date_invalid(self):
        # Test an invalid date string
        date_str = 'invalid-date'
        result = AppStoreStream.convert_date(date_str)
        self.assertIsNone(result)

    def test_convert_date_empty(self):
        # Test an empty date string
        date_str = ''
        result = AppStoreStream.convert_date(date_str)
        self.assertIsNone(result)

    def test_convert_date_custom_format(self):
        # Test a valid date string with a custom format
        date_str = '01-05-2024'
        expected_result = '2024-05-01T00:00:00'
        result = AppStoreStream.convert_date(date_str, date_format='%d-%m-%Y')
        self.assertEqual(result, expected_result)

    def test_convert_fields_to_float(self):
        record = {'field1': '123.45', 'field2': 'invalid', 'field3': None}
        fields = {'field1', 'field2', 'field3'}
        AppStoreStream.convert_fields(None, record, fields, float)
        self.assertEqual(record['field1'], 123.45)
        self.assertIsNone(record['field2'])
        self.assertIsNone(record['field3'])

    def test_convert_fields_to_int(self):
        record = {'field1': '123', 'field2': 'invalid', 'field3': None}
        fields = {'field1', 'field2', 'field3'}
        AppStoreStream.convert_fields(None, record, fields, int)
        self.assertEqual(record['field1'], 123)
        self.assertIsNone(record['field2'])
        self.assertIsNone(record['field3'])


if __name__ == '__main__':
    unittest.main()
