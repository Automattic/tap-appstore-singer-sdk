"""REST client handling, including AppStoreStream base class."""

from __future__ import annotations

from typing import Callable
from datetime import datetime, timedelta
import logging
import requests
from singer_sdk.streams import Stream
import csv
from io import StringIO
import zipfile
import zlib
from appstoreconnect import Api
from appstoreconnect.api import APIError, BASE_API, HttpMethod
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

    Apple only generates these reports for an app that has an analytics report request:
    ONGOING for daily data, ONE_TIME_SNAPSHOT for history. The tap creates them when they're
    missing, which takes a key with the Admin role. Each app has a partition, so a bookmark,
    per kind of request: Apple dates a history delivery the day it makes it, and it can arrive
    after newer daily data has been read. The README says how to deduplicate rows.
    """
    report_name = ''  # lowercase
    primary_keys = ['_instance_id', '_segment_id', '_line_id']
    is_sorted = False  # so a bookmark only moves once all its deliveries have been read

    @property
    def partitions(self):
        app_ids = self.config.get('app_ids')
        if not app_ids:
            raise ValueError(f'{self.tap_stream_id} needs the app_ids setting: the Apple IDs of the apps to extract.')
        return [{'app_id': str(app_id), 'access_type': access_type}
                for app_id in app_ids for access_type in ('ONGOING', 'ONE_TIME_SNAPSHOT')]

    def get_records(self, context: dict = None):
        app_id, access_type = context['app_id'], context['access_type']
        report_requests = self._report_requests(app_id, access_type)
        if access_type == 'ONGOING' and all(r['attributes'].get('stoppedDueToInactivity') for r in report_requests):
            report_requests.append(self._create_request(app_id, 'ONGOING'))
        instances = [instance for request in report_requests for instance in self._instances(request, access_type)]
        bookmark = self.get_context_state(context).get('replication_key_value')
        bookmark = str(bookmark)[:10] if bookmark else None
        if access_type == 'ONGOING':
            self._check_daily_deliveries(app_id, instances, bookmark)

        if bookmark:
            new = [instance for instance in instances if instance['processing_date'] > bookmark]
        else:
            start_date = str(self.config['start_date'])[:10]
            new = [instance for instance in instances if instance['processing_date'] >= start_date]

        for instance in sorted(new, key=lambda instance: instance['processing_date']):
            logger.info(f"Extracting {self.tap_stream_id} for app {app_id}: "
                        f"{access_type} instance processed {instance['processing_date']}")
            for segment_id, text in self._segments(instance['id']):
                for line_id, record in enumerate(self._read_rows(text), start=1):
                    record['_line_id'] = line_id
                    record['_time_extracted'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                    record['_api_report_date'] = instance['processing_date']
                    record['_instance_id'] = instance['id']
                    record['_segment_id'] = segment_id
                    record['request_id'] = instance['request_id']
                    record['vendor_number'] = self.config['vendor']
                    yield self.post_process(record, context)

    def _check_daily_deliveries(self, app_id, instances, bookmark):
        """Warn when daily data stops arriving, and ask for history on a first run or after a gap."""
        dates = [instance['processing_date'] for instance in instances]
        stale_before = (datetime.utcnow() - timedelta(days=4)).strftime(self.date_format)
        if not dates or max(dates) < stale_before:
            logger.warning(f"{self.tap_stream_id}: app {app_id} has no daily instance processed in the last 4 days "
                           f"(newest: {max(dates, default='none')}). Apple may have stopped generating it, or it is new.")
        if not bookmark:
            self._request_history(app_id)
            return
        day_after = (datetime.strptime(bookmark, self.date_format) + self.date_increment).strftime(self.date_format)
        if dates and min(dates) > day_after:
            logger.error(f"{self.tap_stream_id}: app {app_id} daily instances processed after {bookmark} and before "
                         f"{min(dates)} were deleted before they were read (Apple keeps them 35 days). "
                         f"Asking for history to recover them.")
            self._request_history(app_id, missing_until=min(dates))

    def _request_history(self, app_id, missing_until=None):
        """Create a history request, unless the app has one that will do.

        On an app's first run any history request will do. After a gap, it has to be still
        generating, or generated on or after `missing_until`, to hold the days that were missed.
        """
        history_requests = self._report_requests(app_id, 'ONE_TIME_SNAPSHOT')
        if history_requests and missing_until is None:
            return
        for request in history_requests:
            dates = [instance['processing_date'] for instance in self._instances(request, 'ONE_TIME_SNAPSHOT')]
            if not dates or max(dates) >= missing_until:
                return
        try:
            self._create_request(app_id, 'ONE_TIME_SNAPSHOT')
        except APIError as e:
            # Apple allows one history request a month.
            logger.error(f'{self.tap_stream_id}: could not ask Apple for the history of app {app_id}: {e}')

    def _report_requests(self, app_id, access_type):
        """The app's analytics report requests of one kind, stopped ones included."""
        return [r for r in self._list(f'apps/{app_id}/analyticsReportRequests')
                if r['attributes']['accessType'] == access_type]

    def _create_request(self, app_id, access_type):
        """Ask Apple to start generating an app's reports, which takes a key with the Admin role.

        Not retried: a retry after a lost reply could create a second request.
        """
        body = {'data': {'type': 'analyticsReportRequests', 'attributes': {'accessType': access_type},
                         'relationships': {'app': {'data': {'type': 'apps', 'id': app_id}}}}}
        try:
            payload = self.api._api_call(f'{BASE_API}/v1/analyticsReportRequests', HttpMethod.POST, body)
        except APIError as e:
            raise APIError(f'Could not create a {access_type} analytics report request for app {app_id}: {e}') from e
        if not isinstance(payload, dict):
            raise APIError(f'Expected JSON creating a {access_type} request for app {app_id}, got {type(payload).__name__}')
        logger.info(f'{self.tap_stream_id}: created a {access_type} analytics report request for app {app_id}. '
                    f'Apple takes a day or two to generate its reports.')
        return payload['data']

    def _instances(self, request, access_type):
        """This report's daily instances in one analytics report request."""
        reports = self._list(f"analyticsReportRequests/{request['id']}/reports?limit=200")
        report = next((r for r in reports if r['attributes']['name'].strip().lower() == self.report_name), None)
        if report is None:
            message = (f"{self.tap_stream_id}: {access_type} request {request['id']} has no report named "
                       f"{self.report_name!r}. It has: {sorted(r['attributes']['name'] for r in reports)}")
            # A request Apple hasn't generated yet may list no reports; one that lists others had this one renamed.
            if reports and access_type == 'ONGOING':
                raise RuntimeError(message)
            logger.warning(message)
            return []
        return [
            {'id': instance['id'], 'processing_date': instance['attributes']['processingDate'], 'request_id': request['id']}
            for instance in self._list(f"analyticsReports/{report['id']}/instances?filter[granularity]=DAILY&limit=200")
        ]

    def _segments(self, instance_id):
        """(segment id, text) for each segment of one instance, downloaded one at a time."""
        for segment in self._list(f'analyticsReportInstances/{instance_id}/segments'):
            yield segment['id'], self._download_segment(instance_id, segment['id'])

    @api_retry
    def _download_segment(self, instance_id, segment_id):
        # Segment URLs expire five minutes after they're listed, and a history instance can have dozens
        # of segments that take longer than that to read, so every download lists them again.
        segments = {s['id']: s for s in self._fetch_all(f'analyticsReportInstances/{instance_id}/segments')}
        if segment_id not in segments:
            raise APIError(f'Segment {segment_id} of instance {instance_id} is no longer listed')
        # A presigned URL: Apple's bearer token must not be sent with it.
        response = requests.get(segments[segment_id]['attributes']['url'], timeout=300)
        response.raise_for_status()
        return Api._unwrap_report_archive(response.content).decode('utf-8')

    def _read_rows(self, text):
        """Rows of one tab-separated segment file keyed by column name, since Apple may reorder columns."""
        # A quote in a tab-separated file is just a character; csv's default quoting would
        # swallow every row after a stray one.
        reader = csv.reader(StringIO(text), delimiter='\t', quoting=csv.QUOTE_NONE)
        fieldnames = [self.column_name(col) for col in next(reader, [])]
        if 'date' not in fieldnames:
            raise ValueError(f'{self.tap_stream_id}: expected a tab-separated file with a Date column, '
                             f'got the header {fieldnames}')
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
