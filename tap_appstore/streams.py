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
from tap_appstore import client

logger = logging.getLogger(__name__)


class SalesReportStream(client.AppStoreStream):
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
        """Overrides the generic get_records to specify the endpoint."""

        logger.info(f"Vendor Number: {self.config['vendor_number']}")

        endpoint = self.connection.sales_reports().filter(
            frequency=SalesReportsEndpoint.Frequency.DAILY,
            report_sub_type=SalesReportsEndpoint.ReportSubType.SUMMARY,
            report_type=SalesReportsEndpoint.ReportType.SALES,  # Change as needed for this stream
            report_date=self.config.get('start_date', '2024-04-01'),
            vendor_number=self.config['vendor_number'],
        )
        # Now call the generic get_records from the parent class
        return super().get_records(endpoint)


    def parse_report_line(self, line):
        """Parses a single line of raw sales report data, handling optional fields."""
        fields = line.split('\t')
        # Ensure there are at least the number of required fields (assuming first 15 are required)
        if fields[0] == 'Provider' or len(fields) < 15:
            logger.warning(f"Skipping incomplete record: {line}")
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
                "begin_date": self.convert_date(fields[9], '%m/%d/%Y'),
                "end_date": self.convert_date(fields[10], '%m/%d/%Y'),
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


class SubscriberReportStream(client.AppStoreStream):
    name = "subscriber_reports"
    #primary_keys = ["_line_id"]
    schema = th.PropertiesList(
        th.Property("event_date", th.DateTimeType),
        th.Property("app_name", th.StringType),
        th.Property("app_apple_id", th.IntegerType),
        th.Property("subscription_name", th.StringType),
        th.Property("subscription_apple_id", th.IntegerType),
        th.Property("subscription_group_id", th.IntegerType),
        th.Property("standard_subscription_duration", th.StringType),
        th.Property("promotional_offer_name", th.StringType),
        th.Property("promotional_offer_id", th.StringType),
        th.Property("subscription_offer_type", th.StringType),
        th.Property("subscription_offer_duration", th.StringType),
        th.Property("marketing_opt_in_duration", th.StringType),
        th.Property("customer_price", th.NumberType),
        th.Property("customer_currency", th.StringType),
        th.Property("developer_proceeds", th.NumberType),
        th.Property("proceeds_currency", th.StringType),
        th.Property("preserved_pricing", th.StringType),
        th.Property("proceeds_reason", th.StringType),
        th.Property("client", th.StringType),
        th.Property("device", th.StringType),
        th.Property("country", th.StringType),
        th.Property("subscriber_id", th.StringType),
        th.Property("subscriber_id_reset", th.StringType),
        th.Property("refund", th.StringType),
        th.Property("purchase_date", th.StringType),
        th.Property("units", th.IntegerType)
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_records(self, *args, **kwargs):
        """Overrides the generic get_records to specify the endpoint for subscriber reports."""

        logger.info(f"Vendor Number: {self.config['vendor_number']}")
        endpoint = self.connection.sales_reports().filter(
            frequency=SalesReportsEndpoint.Frequency.DAILY,
            report_sub_type=SalesReportsEndpoint.ReportSubType.DETAILED,
            report_type=SalesReportsEndpoint.ReportType.SUBSCRIBER,
            report_date=self.config.get('start_date', '2024-02-01'),
            vendor_number=self.config['vendor_number'],
            version="1_3",
        )
        # Now call the generic get_records from the parent class
        return super().get_records(endpoint)

    def parse_report_line(self, line):
        """Parses a single line of raw subscriber report data, handling optional fields."""
        fields = line.split('\t')
        if fields[0] == 'Event Date' or len(fields) < 15:
            logger.warning(f"Skipping incomplete record: {line}")
            return None

        try:
            return {
                "event_date": self.convert_date(fields[0], '%Y-%m-%d'),
                "app_name": fields[1],
                "app_apple_id": int(fields[2]),
                "subscription_name": fields[3],
                "subscription_apple_id": int(fields[4]),
                "subscription_group_id": int(fields[5]),
                "standard_subscription_duration": fields[6],
                "promotional_offer_name": fields[7],
                "promotional_offer_id": fields[8],
                "subscription_offer_type": fields[9],
                "subscription_offer_duration": fields[10],
                "marketing_opt_in_duration": fields[11],
                "customer_price": float(fields[12]) if fields[12] else None,
                "customer_currency": fields[13],
                "developer_proceeds": float(fields[14]) if fields[14] else None,
                "proceeds_currency": fields[15],
                "preserved_pricing": fields[16],
                "proceeds_reason": fields[17],
                "client": fields[18],
                "device": fields[19],
                "country": fields[20],
                "subscriber_id": fields[21],
                "subscriber_id_reset": fields[22],
                "refund": fields[23],
                "purchase_date": fields[24],
                "units": int(fields[25]) if fields[25] else 0
            }
        except ValueError as e:  # Handle conversion errors for numbers and other types
            logger.error(f"Error parsing line due to type conversion: {line} | Error: {str(e)}")
            return None
        