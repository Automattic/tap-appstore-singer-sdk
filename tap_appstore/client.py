"""REST client handling, including AppStoreStream base class."""

from __future__ import annotations

from typing import Callable
from datetime import datetime, timedelta
import logging
import requests
from singer_sdk.streams import Stream
import csv
from io import StringIO
from appstoreconnect import Api
from appstoreconnect.api import APIError
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log, retry_if_not_exception_type

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

logger = logging.getLogger(__name__)

class AppStoreStream(Stream):
    """AppStore stream class."""
    date_format = '%Y-%m-%d'
    date_increment = timedelta(days=1)
    skip_line_first_values = []
    replication_key = '_api_report_date'
    is_sorted = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = self.setup_api_connection()
        self.date_fields = {'_api_report_date': self.date_format}
        self.float_fields = {name for name, prop in self.schema["properties"].items() if 'number' in prop['type']}
        self.int_fields = {name for name, prop in self.schema["properties"].items() if 'integer' in prop['type']}

    def setup_api_connection(self):
        """Set up the API connection using provided configuration."""
        return Api(self.config['key_id'], self.config['key_file'], self.config['issuer_id'], submit_stats=False)

    def download_data(self, start_date, api):
        """Set up the endpoint for the API call. Override in subclass as needed."""
        raise NotImplementedError("Subclasses must implement this method.")

    def post_process(self, row, context=None):
        # Convert float fields
        if self.float_fields:
            self.convert_fields(row, self.float_fields, float)

        # Convert integer fields
        if self.int_fields:
            self.convert_fields(row, self.int_fields, int)

        # Convert date fields
        if self.date_fields:
            for field, format in self.date_fields.items():
                if field in row and row[field]:
                    row[field] = self.convert_date(row[field], format)

        return row

    def get_records(self, context: dict = None):
        """Return a generator of record-type dictionary objects."""
        line_id = 0
        starting_timestamp = self.get_starting_timestamp(context)
        start_date = starting_timestamp + self.date_increment if starting_timestamp else self.config['start_date']

        while report := self._get_report(start_date):
            start_date_fmt = start_date.strftime(self.date_format)
            logger.info(f'Extracting {self.tap_stream_id} starting from {start_date_fmt}')
            data_io = StringIO(report)
            first_line = data_io.readline().strip()
            fieldnames = [col.strip().replace(' ', '_').replace('-', '_').lower() for col in first_line.split('\t')]

            reader = csv.DictReader(data_io, delimiter='\t', fieldnames=fieldnames)

            for record in reader:
                first_value = next(iter(record.values()))
                if first_value and any(keyword in first_value for keyword in self.skip_line_first_values):
                    logger.info(f"Skipping line report {start_date_fmt}: {record.values()}")
                    continue

                line_id += 1
                record['_line_id'] = line_id
                record['_time_extracted'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                record['_api_report_date'] = start_date_fmt
                record['vendor_number'] = self.config['vendor']

                processed_record = self.post_process(record, context)
                if processed_record is not None:
                    yield processed_record

            start_date += self.date_increment

    @retry(
        retry=retry_if_not_exception_type(APIError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=3, min=300, max=1800),
        before_sleep=before_sleep_log(logger, logging.WARNING),

    )
    def _get_report(self, start_date):
        try:
            return self.download_data(start_date.strftime(self.date_format), self.api)
        except APIError as e:
            if str(e).startswith('There were no') and str(e).endswith('for the date specified.') or str(e).startswith('Report is not available yet'):
                logger.info(str(e))
                return None
            raise

    @staticmethod
    def convert_date(date_str, date_format='%Y-%m-%d'):
        """Converts date string to ISO format based on the given date format, defaulting to '%Y-%m-%d'.
           Returns None if the date string is invalid or empty.
        """
        if not date_str or date_str.strip() == '':
            logger.info(f'date_str: {date_str}')
            logger.warning(f"Empty or invalid date string provided; cannot convert using format {date_format}")
            return None

        try:
            return datetime.strptime(date_str, date_format).isoformat()
        except ValueError as e:
            logger.error(f"Date conversion error for date: '{date_str}' with format '{date_format}' | Error: {str(e)}")
            raise

    def convert_fields(self, record, fields, target_type):
        """Converts specified fields in a record to a target type."""
        for field in fields:
            if field in record and record[field] is not None:
                if isinstance(record[field], str) and record[field].strip() == "":
                    record[field] = None
                if record[field] is not None:
                    try:
                        record[field] = target_type(record[field])
                    except ValueError:
                        logger.warning(f"Invalid format for {field}: {record[field]}")
                        raise

