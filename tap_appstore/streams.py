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
        th.Property("Provider", th.StringType),
        th.Property("Provider Country", th.StringType),
        th.Property("SKU", th.StringType),
        th.Property("Developer", th.StringType),
        th.Property("Title", th.StringType),
        th.Property("Version", th.StringType),
        th.Property("Product Type Identifier", th.StringType),
        th.Property("Units", th.IntegerType),
        th.Property("Developer Proceeds", th.NumberType),
        th.Property("Begin Date", th.DateTimeType),
        th.Property("End Date", th.DateTimeType),
        th.Property("Customer Currency", th.StringType),
        th.Property("Country Code", th.StringType),
        th.Property("Currency of Proceeds", th.StringType),
        th.Property("Apple Identifier", th.StringType),
        th.Property("Customer Price", th.NumberType),
        th.Property("Promo Code", th.StringType),
        th.Property("Parent Identifier", th.StringType),
        th.Property("Subscription", th.StringType),
        th.Property("Period", th.StringType),
        th.Property("Category", th.StringType),
        th.Property("CMB", th.StringType),
        th.Property("Device", th.StringType),
        th.Property("Supported Platforms", th.StringType),
        th.Property("Proceeds Reason", th.StringType),
        th.Property("Preserved Pricing", th.StringType),
        th.Property("Client", th.StringType),
        th.Property("Order Type", th.StringType)
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
                frequency=SalesReportsEndpoint.Frequency.MONTHLY,
                report_date=self.config.get('start_date', '2023-12'),
                vendor_number=VENDOR_NUMBER
            ).get()

            # Assuming response is saved in a compressed text format
            with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                temp_file_name = temp_file.name
            response.save(temp_file_name, decompress=True)

            # Save raw data to a separate file for analysis
            with open(temp_file_name, 'r') as infile, open('raw_sales_reports.txt', 'w') as outfile:
                for line in infile:
                    record = self.parse_sales_report_line(line.strip())
                    if record:
                        yield record  # Yield the structured record
                    outfile.write(line)

            # Clean up the temporary file
            os.remove(temp_file_name)

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
                "Report Date": self.config['vendor'],
                "Vendor": self.config['vendor'],
                "Provider": fields[0],
                "Provider Country": fields[1],
                "SKU": fields[2],
                "Developer": fields[3],
                "Title": fields[4],
                "Version": fields[5],
                "Product Type Identifier": fields[6],
                "Units": int(fields[7]),
                "Developer Proceeds": float(fields[8]),
                "Begin Date": convert_date(fields[9]),
                "End Date": convert_date(fields[10]),
                "Customer Currency": fields[11],
                "Country Code": fields[12],
                "Currency of Proceeds": fields[13],
                "Apple Identifier": fields[14],
                "Customer Price": float(fields[15]) if len(fields) > 15 else 0.0,
                "Promo Code": fields[16] if len(fields) > 16 else '',
                "Parent Identifier": fields[17] if len(fields) > 17 else '',
                "Subscription": fields[18] if len(fields) > 18 else '',
                "Period": fields[19] if len(fields) > 19 else '',
                "Category": fields[20] if len(fields) > 20 else '',
                "CMB": fields[21] if len(fields) > 21 else '',
                "Device": fields[22] if len(fields) > 22 else '',
                "Supported Platforms": fields[23] if len(fields) > 23 else '',
                "Proceeds Reason": fields[24] if len(fields) > 24 else '',
                "Preserved Pricing": fields[25] if len(fields) > 25 else '',
                "Client": fields[26] if len(fields) > 26 else '',
                "Order Type": fields[27] if len(fields) > 27 else ''
            }
        except ValueError as e:  # Handle conversion errors for numbers and other types
            logger.error(f"Error parsing line due to type conversion: {line} | Error: {str(e)}")
            return None


