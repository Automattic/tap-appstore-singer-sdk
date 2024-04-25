"""Stream type classes for tap-appstore."""

from __future__ import annotations
import tempfile

import os
from applaud.connection import Connection
from applaud.endpoints.sales_reports import SalesReportsEndpoint
from applaud.endpoints.finance_reports import FinanceReportsEndpoint
from singer_sdk import Stream, typing as th
import datetime
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import logging
from tap_appstore import client

logger = logging.getLogger(__name__)


class SalesReportStream(client.AppStoreStream):
    name = "sales_reports"
    #primary_keys = ["id"]
    schema = th.PropertiesList(
        th.Property("report_date", th.StringType),
        th.Property("provider", th.StringType),
        th.Property("provider_country", th.StringType),
        th.Property("sku", th.StringType),
        th.Property("developer", th.StringType),
        th.Property("title", th.StringType),
        th.Property("version", th.StringType),
        th.Property("product_type_identifier", th.StringType),
        th.Property("units", th.StringType),
        th.Property("developer_proceeds", th.StringType),
        th.Property("begin_date", th.DateTimeType),
        th.Property("end_date", th.DateTimeType),
        th.Property("customer_currency", th.StringType),
        th.Property("country_code", th.StringType),
        th.Property("currency_of_proceeds", th.StringType),
        th.Property("apple_identifier", th.StringType),
        th.Property("customer_price", th.StringType),
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

    def setup_endpoint(self, start_date):
        return self.connection.sales_reports().filter(
            frequency=SalesReportsEndpoint.Frequency.DAILY,
            report_sub_type=SalesReportsEndpoint.ReportSubType.SUMMARY,
            report_type=SalesReportsEndpoint.ReportType.SALES,
            report_date=start_date.strftime('%Y-%m-%d'),
            vendor_number=self.config['vendor_number'],
        )


class SubscriberReportStream(client.AppStoreStream):
    name = "subscriber_reports"
    #primary_keys = ["_line_id"]
    schema = th.PropertiesList(
        th.Property("event_date", th.DateTimeType),
        th.Property("app_name", th.StringType),
        th.Property("app_apple_id", th.StringType),
        th.Property("subscription_name", th.StringType),
        th.Property("subscription_apple_id", th.StringType),
        th.Property("subscription_group_id", th.StringType),
        th.Property("standard_subscription_duration", th.StringType),
        th.Property("promotional_offer_name", th.StringType),
        th.Property("promotional_offer_id", th.StringType),
        th.Property("subscription_offer_type", th.StringType),
        th.Property("subscription_offer_duration", th.StringType),
        th.Property("marketing_opt_in_duration", th.StringType),
        th.Property("customer_price", th.StringType),
        th.Property("customer_currency", th.StringType),
        th.Property("developer_proceeds", th.StringType),
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
        th.Property("units", th.StringType)
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setup_endpoint(self, start_date):
        """Specify the API endpoint for subscriber reports."""
        return self.connection.sales_reports().filter(
            frequency=SalesReportsEndpoint.Frequency.DAILY,
            report_sub_type=SalesReportsEndpoint.ReportSubType.DETAILED,
            report_type=SalesReportsEndpoint.ReportType.SUBSCRIBER,
            report_date=start_date.strftime('%Y-%m-%d'),
            vendor_number=self.config['vendor_number'],
            version="1_3"
        )


class SubscriptionReportStream(client.AppStoreStream):
    name = "subscription_reports"
    schema = th.PropertiesList(
        th.Property("app_name", th.StringType),
        th.Property("app_apple_id", th.StringType),
        th.Property("subscription_name", th.StringType),
        th.Property("subscription_apple_id", th.StringType),
        th.Property("subscription_group_id", th.StringType),
        th.Property("subscription_group_name", th.StringType),
        th.Property("subscription_duration", th.StringType),
        th.Property("subscription_offer_type", th.StringType),
        th.Property("marketing_opt_in_duration", th.StringType),
        th.Property("customer_price", th.StringType),
        th.Property("customer_currency", th.StringType),
        th.Property("developer_proceeds", th.StringType),
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
        th.Property("units", th.StringType)  # Assuming you want this as string too
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setup_endpoint(self, start_date):
        return self.connection.sales_reports().filter(
            frequency=SalesReportsEndpoint.Frequency.DAILY,
            report_sub_type=SalesReportsEndpoint.ReportSubType.SUMMARY,
            report_type=SalesReportsEndpoint.ReportType.SUBSCRIPTION,
            report_date=self.config.get('start_date', '2024-04-01'),
            vendor_number=self.config['vendor_number'],
            version="1_3",
        )



class SubscriptionEventReportStream(client.AppStoreStream):
    name = "subscription_event_reports"
    schema = th.PropertiesList(
        th.Property("event_date", th.DateTimeType),
        th.Property("event", th.StringType),
        th.Property("app_name", th.StringType),
        th.Property("app_apple_id", th.StringType),
        th.Property("subscription_name", th.StringType),
        th.Property("subscription_apple_id", th.StringType),
        th.Property("subscription_group_id", th.StringType),
        th.Property("standard_subscription_duration", th.StringType),
        th.Property("promotional_offer_name", th.StringType),
        th.Property("promotional_offer_id", th.StringType),
        th.Property("subscription_offer_type", th.StringType),
        th.Property("subscription_offer_duration", th.StringType),
        th.Property("marketing_opt_in", th.StringType),
        th.Property("marketing_opt_in_duration", th.StringType),
        th.Property("preserved_pricing", th.StringType),
        th.Property("proceeds_reason", th.StringType),
        th.Property("consecutive_paid_periods", th.StringType),
        th.Property("paid_service_days_recovered", th.StringType),
        th.Property("original_start_date", th.DateTimeType),
        th.Property("client", th.StringType),
        th.Property("device", th.StringType),
        th.Property("state", th.StringType),
        th.Property("country", th.StringType),
        th.Property("previous_subscription_name", th.StringType),
        th.Property("previous_subscription_apple_id", th.StringType),
        th.Property("days_before_canceling", th.StringType),
        th.Property("cancellation_reason", th.StringType),
        th.Property("days_canceled", th.StringType),
        th.Property("quantity", th.StringType)
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setup_endpoint(self, start_date):
        return self.connection.sales_reports().filter(
            frequency=SalesReportsEndpoint.Frequency.DAILY,
            report_sub_type=SalesReportsEndpoint.ReportSubType.SUMMARY,
            report_type=SalesReportsEndpoint.ReportType.SUBSCRIPTION_EVENT,
            report_date=self.config.get('start_date', '2024-04-01'),
            vendor_number=self.config['vendor_number'],
            version="1_3",
        )


class FinancialReportStream(client.AppStoreStream):
    name = "financial_reports"
    schema = th.PropertiesList(
        th.Property("start_date", th.DateTimeType),
        th.Property("end_date", th.DateTimeType),
        th.Property("vendor_identifier", th.StringType),
        th.Property("quantity", th.StringType),
        th.Property("partner_share", th.StringType),
        th.Property("extended_partner_share", th.StringType),
        th.Property("partner_share_currency", th.StringType),
        th.Property("sales_or_return", th.StringType),
        th.Property("apple_identifier", th.StringType),
        th.Property("title", th.StringType),
        th.Property("product_type_identifier", th.StringType),
        th.Property("country_of_sale", th.StringType),
        th.Property("pre_order_flag", th.StringType),
        th.Property("promo_code", th.StringType),
        th.Property("customer_price", th.StringType),
        th.Property("customer_currency", th.StringType)
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setup_endpoint(self, start_date):
        month_str = start_date.strftime('%Y-%m')
        return self.connection.finance_reports().filter(
            region_code=self.config.get('region_code', 'US'),
            report_date=month_str,
            report_type=FinanceReportsEndpoint.ReportType.FINANCIAL,
            vendor_number=self.config['vendor_number']
        )

    def increment_date(self, date):
        """Increment date by one month."""
        return date + relativedelta(months=1)
