"""REST client handling, including AppStoreStream base class."""

from __future__ import annotations

from typing import Callable
from datetime import datetime, timedelta
from functools import cached_property
import logging
import requests
from singer_sdk.streams import Stream
import csv
from io import StringIO
import zipfile
import zlib
from appstoreconnect import Api
from appstoreconnect.api import APIError, BASE_API
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log, retry_if_exception_type

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

logger = logging.getLogger(__name__)

# Apple's API errors come and go, and so do network errors and damaged downloads.
# Anything else is a bug that retrying won't fix.
TRANSIENT_ERRORS = (APIError, OSError, EOFError, zlib.error, zipfile.BadZipFile)

# Every stream calls the same App Store Connect API, so they all retry the same way.
api_retry = retry(
    retry=retry_if_exception_type(TRANSIENT_ERRORS),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=3, min=300, max=1800),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)


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

    def get_start_date(self, context: dict = None):
        starting_timestamp = self.get_starting_timestamp(context)
        return starting_timestamp + self.date_increment if starting_timestamp else self.config['start_date']

    def get_records(self, context: dict = None):
        """Return a generator of record-type dictionary objects."""
        line_id = 0
        start_date = self.get_start_date(context)

        while report := self._get_report(start_date):
            start_date_fmt = start_date.strftime(self.date_format)
            logger.info(f'Extracting {self.tap_stream_id} starting from {start_date_fmt}')
            data_io = StringIO(report)
            first_line = data_io.readline().strip()
            fieldnames = [self.column_name(col) for col in first_line.split('\t')]

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

    @api_retry
    def _get_report(self, start_date):
        try:
            return self.download_data(start_date.strftime(self.date_format), self.api)
        except APIError as e:
            if str(e).startswith('There were no') and str(e).endswith('for the date specified.') or str(e).startswith('Report is not available yet'):
                logger.info(str(e))
                return None
            raise

    @staticmethod
    def column_name(header):
        """Report column header as a field name: 'App Apple Identifier' -> 'app_apple_identifier'."""
        return header.strip().replace(' ', '_').replace('-', '_').lower()

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


class AnalyticsReportStream(AppStoreStream):
    """One report from the App Store Connect Analytics Reports API.

    Every analytics report request of every app in `app_ids` is a partition with its own
    bookmark, so a snapshot that arrives late is still read. The README says how to
    deduplicate rows.
    """
    report_names = ()  # lowercase
    primary_keys = ['_instance_id', '_segment_id', '_line_id']
    is_sorted = False  # so the bookmark only moves once a request has been read in full

    @cached_property
    def partitions(self):
        return [
            {'app_id': str(app_id), 'request_id': request['id'], 'access_type': request['attributes']['accessType']}
            for app_id in self.config.get('app_ids') or []
            for request in self._report_requests(str(app_id))
        ] or None

    def _report_requests(self, app_id):
        report_requests = self._list(f'apps/{app_id}/analyticsReportRequests')
        if not any(r['attributes']['accessType'] == 'ONGOING' and not r['attributes'].get('stoppedDueToInactivity')
                   for r in report_requests):
            raise RuntimeError(f'App {app_id} has no active ONGOING analytics report request, so Apple is not '
                               f'generating its reports. An Admin has to create one with POST /v1/analyticsReportRequests.')
        return report_requests

    def get_records(self, context: dict = None):
        if not context:
            logger.info(f'{self.tap_stream_id}: no app_ids configured, nothing to extract')
            return

        instances = self._instances(context)
        if context['access_type'] == 'ONGOING':
            newest = max((instance['processing_date'] for instance in instances), default=None)
            stale_before = (datetime.utcnow() - timedelta(days=4)).strftime(self.date_format)
            if not newest or newest < stale_before:
                logger.warning(f"{self.tap_stream_id}: ONGOING request {context['request_id']} for app {context['app_id']} "
                               f"has no instance processed in the last 4 days (newest: {newest or 'none'}). "
                               f"Apple may have stopped generating it, or it is new.")
        bookmark = self.get_context_state(context).get('replication_key_value')
        if bookmark:
            bookmark = str(bookmark)[:10]
            new = [instance for instance in instances if instance['processing_date'] > bookmark]
            day_after = (datetime.strptime(bookmark, self.date_format) + self.date_increment).strftime(self.date_format)
            oldest = min((instance['processing_date'] for instance in instances), default=None)
            if context['access_type'] == 'ONGOING' and oldest and oldest > day_after:
                logger.error(f"{self.tap_stream_id}: app {context['app_id']} instances processed after {bookmark} and "
                             f"before {oldest} were deleted before they were read (Apple keeps them 35 days). "
                             f"A new ONE_TIME_SNAPSHOT request can recover them.")
        else:
            start_date = str(self.config['start_date'])[:10]
            new = [instance for instance in instances if instance['processing_date'] >= start_date]

        for instance in sorted(new, key=lambda instance: instance['processing_date']):
            logger.info(f"Extracting {self.tap_stream_id} for app {context['app_id']}: "
                        f"{context['access_type']} instance processed {instance['processing_date']}")
            for segment_id, text in self._segments(instance['id']):
                for line_id, record in enumerate(self._read_rows(text), start=1):
                    record['_line_id'] = line_id
                    record['_time_extracted'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                    record['_api_report_date'] = instance['processing_date']
                    record['_instance_id'] = instance['id']
                    record['_segment_id'] = segment_id
                    record['vendor_number'] = self.config['vendor']
                    yield self.post_process(record, context)

    def _instances(self, context):
        """This report's daily instances in one analytics report request."""
        reports = self._list(f"analyticsReportRequests/{context['request_id']}/reports?limit=200")
        matching = [r for r in reports if r['attributes']['name'].strip().lower() in self.report_names]
        if not matching:
            message = (f"{self.tap_stream_id}: {context['access_type']} request {context['request_id']} has no report "
                       f"named {' or '.join(self.report_names)}. It has: {sorted(r['attributes']['name'] for r in reports)}")
            # A request Apple hasn't generated yet may list no reports; one that lists others had this one renamed.
            if reports and context['access_type'] == 'ONGOING':
                raise RuntimeError(message)
            logger.warning(message)
        return [
            {'id': instance['id'], 'processing_date': instance['attributes']['processingDate']}
            for report in matching
            for instance in self._list(f"analyticsReports/{report['id']}/instances?filter[granularity]=DAILY&limit=200")
        ]

    def _segments(self, instance_id):
        """(segment id, text) for each segment of one instance, downloaded one at a time."""
        for segment in self._list(f'analyticsReportInstances/{instance_id}/segments'):
            yield segment['id'], self._download_segment(instance_id, segment['id'])

    @api_retry
    def _download_segment(self, instance_id, segment_id):
        # Segment URLs expire five minutes after they're listed, so every attempt lists them again.
        segments = {s['id']: s for s in self._fetch_all(f'analyticsReportInstances/{instance_id}/segments')}
        if segment_id not in segments:
            raise APIError(f'Segment {segment_id} of instance {instance_id} is no longer listed')
        # A presigned URL: Apple's bearer token must not be sent with it.
        response = requests.get(segments[segment_id]['attributes']['url'], timeout=300)
        response.raise_for_status()
        return Api._unwrap_report_archive(response.content).decode('utf-8')

    def _read_rows(self, text):
        """Rows of one segment file keyed by column name, since Apple may reorder columns."""
        text = text.lstrip('\ufeff')
        if '\t' in text.split('\n', 1)[0]:
            # A quote in a tab-separated file is just a character; csv's default quoting would
            # swallow every row after a stray one.
            reader = csv.reader(StringIO(text), delimiter='\t', quoting=csv.QUOTE_NONE)
        else:
            reader = csv.reader(StringIO(text))
        fieldnames = [self.column_name(col) for col in next(reader, [])]
        for row in reader:
            if row:
                yield dict(zip(fieldnames, row))

    @api_retry
    def _list(self, path):
        return self._fetch_all(path)

    def _fetch_all(self, path):
        """Every item of a paginated App Store Connect API collection."""
        url = f'{BASE_API}/v1/{path}'
        items = []
        while url:
            payload = self.api._api_call(url)
            if not isinstance(payload, dict):  # _api_call passes on any reply that isn't JSON
                raise APIError(f'Expected JSON from {url}, got {type(payload).__name__}')
            items.extend(payload.get('data', []))
            url = payload.get('links', {}).get('next')
        return items
