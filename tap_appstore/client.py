"""REST client handling, including AppStoreStream base class."""

from __future__ import annotations

import os
import sys
from typing import Any, Callable, Iterable
from applaud.connection import Connection

from datetime import datetime, timedelta
import logging
import requests
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream
import csv
from io import StringIO
from appstoreconnect import Api
from appstoreconnect.api import APIError

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]

logger = logging.getLogger(__name__)

class AppStoreStream(RESTStream):
    """AppStore stream class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection = self.setup_connection()

    def setup_connection(self):
        """Set up the API connection using provided configuration."""
        KEY_ID = self.config['key_id']
        ISSUER_ID = self.config['issuer_id']
        PATH_TO_KEY = os.path.expanduser(self.config['key_file'])

        with open(PATH_TO_KEY, 'r') as f:
            PRIVATE_KEY = f.read()

        return Connection(ISSUER_ID, KEY_ID, PRIVATE_KEY)

    def get_start_date(self, date_format='%Y-%m-%d', default_date='2024-01-01'):
        """Retrieve the configured start date, formatted as specified."""
        start_date_str = self.config.get('start_date', default_date)
        try:
            return datetime.strptime(start_date_str, date_format)
        except ValueError as e:
            logger.error(f"Invalid start date format: {start_date_str}. Error: {e}")
            return None

    def get_data(self, start_date, api):
        """Set up the endpoint for the API call. Override in subclass as needed."""
        # Default to a generic endpoint configuration; specific streams will override this
        return None

    def parse_report_line(self, start_date):
        """Parse a line returned by get_records"""
        # Default to a generic configuration; specific streams will override this
        return None

    def increment_date(self, date):
        """Increment date by one day. Override in subclass if different increment is needed."""
        return date + timedelta(days=1)

    def update_stream_state(self, date):
        """Update the stream state with the new date."""
        self.stream_state['start_date'] = date.strftime('%Y-%m-%d')
        self.logger.info(f"Updating state, new start date is {self.stream_state['start_date']}")

    def get_records(self, context: dict = None):
        start_date = self.get_start_date(default_date='2024-01-01')
        if not start_date:
            logger.error("Start date could not be determined.")
            return

        start_date = start_date.replace(tzinfo=None)
        date_limit = datetime.now().replace(tzinfo=None) - timedelta(days=2)

        while start_date <= date_limit:

            try:
                logger.info(f"start_date_date_only: {start_date.strftime('%Y-%m-%d')}")

                api = Api(self.config['key_id'], self.config['key_file'], self.config['issuer_id'])
                all_data = self.get_data(start_date, api)

                data_io = StringIO(all_data)

                # Rename column names from 'App Name' -> 'app_name'
                first_line = data_io.readline().strip()
                fieldnames = [col.strip().replace(' ', '_').lower() for col in first_line.split('\t')]

                # Load TSV from memory
                reader = csv.DictReader(data_io, delimiter='\t', fieldnames=fieldnames)

                # TODO: move date formatting for sales_reports to SalesReportStream chile class
                for record in reader:
                    if record.get('begin_date'):
                        record['begin_date'] = self.convert_date(record['begin_date'], '%m/%d/%Y')
                    if record.get('end_date'):
                        record['end_date'] = self.convert_date(record['end_date'], '%m/%d/%Y')
                    if record.get('start_date'):
                        record['start_date'] = self.convert_date(record['start_date'], '%m/%d/%Y')

                    logger.info(f'row: {record}')
                    yield record

            except APIError as e:
                logger.error(f'Error during download report {self.name}.\n{e}')

            start_date = self.increment_date(start_date)
            self.update_stream_state(start_date)




    @staticmethod
    def convert_date(date_str, date_format='%Y-%m-%d'):
        """Converts date string to ISO format based on the given date format, defaulting to '%Y-%m-%d'.
           Returns None if the date string is invalid or empty.
        """

        if not date_str:
            logger.warning(f"Empty or invalid date string provided; cannot convert using format {date_format}")
            return None

        date_str = date_str.strip()
        if not date_str:
            logger.warning(f"Empty or invalid date string provided; cannot convert using format {date_format}")
            return None

        try:
            return datetime.strptime(date_str, date_format).isoformat()
        except ValueError as e:
            logger.error(f"Date conversion error for date: '{date_str}' with format '{date_format}' | Error: {str(e)}")
            return None

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        # TODO: hardcode a value here, or retrieve it from self.config
        return "https://api.mysample.com"

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    # Set this value or override `get_new_paginator`.
    next_page_token_jsonpath = "$.next_page"  # noqa: S105

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="x-api-key",
            value=self.config.get("auth_token", ""),
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        return headers

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return super().get_new_paginator()

    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token
        if self.replication_key:
            params["sort"] = "asc"
            params["order_by"] = self.replication_key
        return params

    def prepare_request_payload(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ARG002, ANN401
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary with the JSON body for a POST requests.
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        return row
