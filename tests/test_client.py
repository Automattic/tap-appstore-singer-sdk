import unittest
from unittest.mock import MagicMock
from datetime import datetime

from appstoreconnect.api import APIError
from tap_appstore.client import AppStoreStream, RetriableAPIException


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


@unittest.mock.patch('tenacity.nap.time.sleep', return_value=None)
class TestGetReport(unittest.TestCase):
    """Tests for the _get_report method error handling."""

    def setUp(self):
        """Create a mock stream instance for testing."""
        self.mock_stream = MagicMock(spec=AppStoreStream)
        self.mock_stream.date_format = '%Y-%m-%d'
        self.mock_stream.api = MagicMock()
        self.test_date = datetime(2024, 1, 15)

    def _call_get_report(self, download_side_effect):
        """Helper to call _get_report with mocked download_data."""
        self.mock_stream.download_data.side_effect = download_side_effect
        # Call the undecorated method directly to avoid retry delays
        return AppStoreStream._get_report.__wrapped__(self.mock_stream, self.test_date)

    def test_get_report_success(self, mock_sleep):
        """Test successful report download."""
        expected_data = "header1\theader2\nvalue1\tvalue2"
        self.mock_stream.download_data.return_value = expected_data
        result = AppStoreStream._get_report.__wrapped__(self.mock_stream, self.test_date)
        self.assertEqual(result, expected_data)

    def test_get_report_no_data_for_date(self, mock_sleep):
        """Test APIError 'There were no... for the date specified.' returns None."""
        error = APIError("There were no sales for the date specified.")
        result = self._call_get_report(error)
        self.assertIsNone(result)

    def test_get_report_not_available_yet(self, mock_sleep):
        """Test APIError 'Report is not available yet' returns None."""
        error = APIError("Report is not available yet")
        result = self._call_get_report(error)
        self.assertIsNone(result)

    def test_get_report_bearer_token_error_raises_retriable(self, mock_sleep):
        """Test APIError with bearer token message raises RetriableAPIException."""
        error = APIError("Provide a properly configured and signed bearer token")
        with self.assertRaises(RetriableAPIException) as ctx:
            self._call_get_report(error)
        self.assertIn("bearer token", str(ctx.exception))

    def test_get_report_agreement_error_raises_retriable(self, mock_sleep):
        """Test APIError with agreement message raises RetriableAPIException."""
        error = APIError("This request requires an in-effect agreement that has not been signed")
        with self.assertRaises(RetriableAPIException) as ctx:
            self._call_get_report(error)
        self.assertIn("agreement", str(ctx.exception))

    def test_get_report_other_api_error_raises_retriable(self, mock_sleep):
        """Test other APIError messages are converted to RetriableAPIException."""
        error = APIError("Some other API error")
        with self.assertRaises(RetriableAPIException) as ctx:
            self._call_get_report(error)
        self.assertEqual(str(ctx.exception), "Some other API error")

    def test_retriable_exception_triggers_retry(self, mock_sleep):
        """Test that RetriableAPIException triggers retry behavior."""
        # Simulate: first 2 calls raise API error (converted to retriable), third call succeeds
        api_error = APIError("Some API error")
        expected_data = "header1\theader2\nvalue1\tvalue2"

        call_count = 0
        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise api_error
            return expected_data

        self.mock_stream.download_data.side_effect = side_effect

        # Call the decorated method (with retry) - not __wrapped__
        result = AppStoreStream._get_report(self.mock_stream, self.test_date)

        self.assertEqual(result, expected_data)
        self.assertEqual(call_count, 3)  # Retried twice before succeeding


if __name__ == "__main__":
    unittest.main()
