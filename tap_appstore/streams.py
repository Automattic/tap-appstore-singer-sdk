"""Stream type classes for tap-appstore."""

from __future__ import annotations
import tempfile

import os
from applaud.connection import Connection
from applaud.endpoints.sales_reports import SalesReportsEndpoint
from applaud.endpoints.finance_reports import FinanceReportsEndpoint
from singer_sdk import Stream, typing as th

from dateutil.relativedelta import relativedelta
import logging
from tap_appstore import client

logger = logging.getLogger(__name__)


class SalesReportStream(client.AppStoreStream):
    name = "sales_reports"
    schema = th.PropertiesList(
        th.Property("report_date", th.StringType),
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
        th.Property("apple_identifier", th.IntegerType),
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

        self.float_fields = ['developer_proceeds', 'customer_price']
        self.int_fields = ['units', 'apple_identifier']

        # These date fields are Month/Date/Year
        # so we have to convert them
        self.date_fields = {
            'begin_date': '%m/%d/%Y',
            'end_date': '%m/%d/%Y',
        }

    def download_data(self, start_date, api):
        filters = {
            'frequency': 'DAILY',
            'reportType': 'SALES',
            'reportSubType': 'SUMMARY',
            'reportDate': start_date.strftime('%Y-%m-%d'),
            'version': '1_0',
            'vendorNumber': self.config['vendor_number']
        }
        return api.download_sales_and_trends_reports(filters=filters)


class SubscriberReportStream(client.AppStoreStream):
    name = "subscriber_reports"
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
        self.float_fields = ['developer_proceeds', 'customer_price']
        self.int_fields = ['units', 'app_apple_id', 'subscription_apple_id', 'subscription_group_id']

    def download_data(self, start_date, api):
        filters = {
            'frequency': 'DAILY',
            'reportType': 'SUBSCRIBER',
            'reportSubType': 'DETAILED',
            'reportDate': start_date.strftime('%Y-%m-%d'),
            'version': '1_3',
            'vendorNumber': self.config['vendor_number']
        }
        return api.download_sales_and_trends_reports(filters=filters)


class SubscriptionReportStream(client.AppStoreStream):
    name = "subscription_reports"
    schema = th.PropertiesList(
        th.Property("app_name", th.StringType),
        th.Property("app_apple_id", th.IntegerType),
        th.Property("subscription_name", th.StringType),
        th.Property("subscription_apple_id", th.IntegerType),
        th.Property("subscription_group_id", th.IntegerType),
        th.Property("subscription_group_name", th.StringType),
        th.Property("subscription_duration", th.StringType),
        th.Property("subscription_offer_type", th.StringType),
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
        th.Property("subscriber_id", th.IntegerType),
        th.Property("subscriber_id_reset", th.StringType),
        th.Property("refund", th.StringType),
        th.Property("purchase_date", th.StringType),
        th.Property("units", th.IntegerType)  # Assuming you want this as string too
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.float_fields = ['developer_proceeds', 'customer_price']
        self.int_fields = ['units', 'app_apple_id', 'subscription_apple_id', 'subscription_group_id']

    def download_data(self, start_date, api):
        filters = {
            'frequency': 'DAILY',
            'reportType': 'SUBSCRIPTION',
            'reportSubType': 'SUMMARY',
            'reportDate': start_date.strftime('%Y-%m-%d'),
            'version': '1_3',
            'vendorNumber': self.config['vendor_number']
        }
        return api.download_sales_and_trends_reports(filters=filters)


class SubscriptionEventReportStream(client.AppStoreStream):
    name = "subscription_event_reports"
    schema = th.PropertiesList(
        th.Property("event_date", th.DateTimeType),
        th.Property("event", th.StringType),
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
        th.Property("marketing_opt_in", th.StringType),
        th.Property("marketing_opt_in_duration", th.StringType),
        th.Property("preserved_pricing", th.StringType),
        th.Property("proceeds_reason", th.StringType),
        th.Property("consecutive_paid_periods", th.IntegerType),
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
        th.Property("days_canceled", th.IntegerType),
        th.Property("quantity", th.IntegerType)
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.int_fields = ['consecutive_paid_periods', 'app_apple_id', 'subscription_apple_id', 'subscription_group_id', 'days_canceled', 'quantity']

    def download_data(self, start_date, api):
        filters = {
            'frequency': 'DAILY',
            'reportType': 'SUBSCRIPTION',
            'reportSubType': 'SUMMARY',
            'reportDate': start_date.strftime('%Y-%m-%d'),
            'version': '1_3',
            'vendorNumber': self.config['vendor_number']
        }
        return api.download_sales_and_trends_reports(filters=filters)


class FinancialReportStream(client.AppStoreStream):
    name = "financial_reports"
    schema = th.PropertiesList(
        th.Property("start_date", th.DateTimeType),
        th.Property("end_date", th.DateTimeType),
        th.Property("vendor_identifier", th.StringType),
        th.Property("quantity", th.IntegerType),
        th.Property("partner_share", th.NumberType),
        th.Property("extended_partner_share", th.NumberType),
        th.Property("partner_share_currency", th.StringType),
        th.Property("sales_or_return", th.StringType),
        th.Property("apple_identifier", th.StringType),
        th.Property("title", th.StringType),
        th.Property("product_type_identifier", th.StringType),
        th.Property("country_of_sale", th.StringType),
        th.Property("pre_order_flag", th.StringType),
        th.Property("promo_code", th.StringType),
        th.Property("customer_price", th.NumberType),
        th.Property("customer_currency", th.StringType)
    ).to_dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.float_fields = ['customer_price', 'partner_share', 'extended_partner_share']
        self.int_fields = ['quantity']

        # These date fields are Month/Date/Year
        # so we have to convert them
        self.date_fields = {
            'start_date': '%m/%d/%Y',
            'end_date': '%m/%d/%Y',
        }

    def download_data(self, start_date, api):
        filters = {'vendorNumber': self.config['vendor_number'],
                   'regionCode': 'US',
                   'reportType': 'FINANCIAL',
                   'reportDate': start_date.strftime('%Y-%m'),
                   }
        return api.download_finance_reports(filters=filters)

    def increment_date(self, date):
        """Increment date by one month."""
        return date + relativedelta(months=1)
