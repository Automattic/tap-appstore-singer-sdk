"""Stream type classes for tap-appstore."""

from __future__ import annotations
import tempfile

import os
from applaud.connection import Connection
from applaud.endpoints.sales_reports import SalesReportsEndpoint
from singer_sdk import Stream, typing as th
import datetime
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SalesReportStream(Stream):
    name = "sales_reports"
    #primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("provider", th.StringType),
        th.Property("provider_country", th.StringType),
        th.Property("sku", th.StringType),
        th.Property("developer", th.StringType),
        th.Property("title", th.StringType),
        th.Property("version", th.StringType),
        th.Property("product_type_identifier", th.StringType),
        th.Property("units", th.IntegerType),
        th.Property("developer_proceeds", th.NumberType),
        th.Property("begin_date", th.DateTimeType),
        th.Property("end_date", th.DateTimeType),
        th.Property("customer_currency", th.StringType),
        th.Property("country_code", th.StringType),
        th.Property("currency_of_proceeds", th.StringType),
        th.Property("apple_identifier", th.StringType),
        th.Property("customer_price", th.NumberType),
        th.Property("promo_code", th.StringType),
        th.Property("parent_identifier", th.StringType),
        th.Property("subscription", th.StringType),
        th.Property("period", th.StringType),
        th.Property("category", th.StringType),
        th.Property("cmb", th.StringType),
        th.Property("device", th.StringType),
        th.Property("supported_platforms", th.StringType),
        th.Property("proceeds_reason", th.StringType),
        th.Property("preserved_pricing", th.StringType),
        th.Property("client", th.StringType),
        th.Property("order_type", th.StringType)
    ).to_dict()
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_records(self, *args, **kwargs):
        """Fetch sales reports from the App Store Connect API and save raw data."""
        KEY_ID = self.config['key_id']
        ISSUER_ID = self.config['issuer_id']
        VENDOR_NUMBER = self.config['vendor']  # Updated to use the configuration
        PATH_TO_KEY = os.path.expanduser(self.config['key_file'])

        with open(PATH_TO_KEY, 'r') as f:
            PRIVATE_KEY = f.read()

        # Create the Connection
        connection = Connection(ISSUER_ID, KEY_ID, PRIVATE_KEY)
        # Prepare and make the API request using the dynamically set report date

        try:
            response = connection.sales_reports().filter(
                report_sub_type=SalesReportsEndpoint.ReportSubType.SUMMARY,
                report_type=SalesReportsEndpoint.ReportType.SALES,
                frequency=SalesReportsEndpoint.Frequency.DAILY,
                report_date=self.config.get('start_date', '2024-04-01'),
                vendor_number=VENDOR_NUMBER
            ).get()

            # Process response directly if it is a stream or a large string
            # Assume response can be iterated or read directly into memory
            for chunk in response.iter_decompress():
                # Assume each chunk can contain multiple lines, split them
                lines = chunk.decode('utf-8').splitlines()
                for line in lines:
                    if line.strip():  # Ensure the line is not just whitespace
                        record = self.parse_sales_report_line(line.strip())
                        if record:
                            yield record

        except Exception as e:
            logger.error(f"Failed to fetch sales reports: {str(e)}")

    def parse_sales_report_line(self, line):
        """Parses a single line of raw sales report data, handling optional fields."""
        fields = line.split('\t')
        # Ensure there are at least the number of required fields (assuming first 15 are required)
        if fields[0] == 'Provider' or len(fields) < 15:
            logger.warning(f"Skipping incomplete record: {line}")
            return None

        def convert_date(date_str):
            try:
                return datetime.strptime(date_str, '%m/%d/%Y').isoformat()
            except ValueError as e:
                logger.error(f"Date conversion error for date: {date_str} | Error: {str(e)}")
                return None

        try:
            # Use `.get()` with default values for optional fields
            return {
                "provider": fields[0],
                "provider_country": fields[1],
                "sku": fields[2],
                "developer": fields[3],
                "title": fields[4],
                "version": fields[5],
                "product_type_identifier": fields[6],
                "units": int(fields[7]),
                "developer_proceeds": float(fields[8]),
                "begin_date": convert_date(fields[9]),
                "end_date": convert_date(fields[10]),
                "customer_currency": fields[11],
                "country_code": fields[12],
                "currency_of_proceeds": fields[13],
                "apple_identifier": fields[14],
                "customer_price": float(fields[15]) if len(fields) > 15 else 0.0,
                "promo_code": fields[16] if len(fields) > 16 else '',
                "parent_identifier": fields[17] if len(fields) > 17 else '',
                "subscription": fields[18] if len(fields) > 18 else '',
                "period": fields[19] if len(fields) > 19 else '',
                "category": fields[20] if len(fields) > 20 else '',
                "cmb": fields[21] if len(fields) > 21 else '',
                "device": fields[22] if len(fields) > 22 else '',
                "supported_platforms": fields[23] if len(fields) > 23 else '',
                "proceeds_reason": fields[24] if len(fields) > 24 else '',
                "preserved_pricing": fields[25] if len(fields) > 25 else '',
                "client": fields[26] if len(fields) > 26 else '',
                "order_type": fields[27] if len(fields) > 27 else ''
            }
        except ValueError as e:  # Handle conversion errors for numbers and other types
            logger.error(f"Error parsing line due to type conversion: {line} | Error: {str(e)}")
            return None


