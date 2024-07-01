"""Stream type classes for tap-appstore."""

from __future__ import annotations

from singer_sdk import typing as th

from dateutil.relativedelta import relativedelta
import logging
from tap_appstore import client

logger = logging.getLogger(__name__)


class SalesReportStream(client.AppStoreStream):
    name = "sales_report"
    schema = th.PropertiesList(
        th.Property("_line_id", th.IntegerType),
        th.Property("_time_extracted", th.StringType),
        th.Property("_api_report_date", th.DateTimeType),
        th.Property("vendor_number", th.StringType),
        th.Property("provider", th.StringType),
        th.Property("provider_country", th.StringType),
        th.Property("sku", th.StringType),
        th.Property("developer", th.StringType),
        th.Property("title", th.StringType),
        th.Property("version", th.StringType),
        th.Property("product_type_identifier", th.StringType),
        th.Property("units", th.IntegerType),
        th.Property("developer_proceeds", th.NumberType),
        th.Property("begin_date", th.StringType),
        th.Property("end_date", th.StringType),
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

    def download_data(self, start_date, api):
        filters = {
            'frequency': 'DAILY',
            'reportType': 'SALES',
            'reportSubType': 'SUMMARY',
            'reportDate': start_date,
            'version': '1_0',
            'vendorNumber': self.config['vendor']
        }
        return api.download_sales_and_trends_reports(filters=filters)


class SubscriberReportStream(client.AppStoreStream):
    name = "subscriber_report"
    schema = th.PropertiesList(
        th.Property("_line_id", th.IntegerType),
        th.Property("_time_extracted", th.StringType),
        th.Property("_api_report_date", th.DateTimeType),
        th.Property("vendor_number", th.StringType),
        th.Property("event_date", th.StringType),
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

    def download_data(self, start_date, api):
        filters = {
            'frequency': 'DAILY',
            'reportType': 'SUBSCRIBER',
            'reportSubType': 'DETAILED',
            'reportDate': start_date,
            'version': '1_3',
            'vendorNumber': self.config['vendor']
        }
        return api.download_sales_and_trends_reports(filters=filters)


class SubscriptionReportStream(client.AppStoreStream):
    name = "subscription_report"
    schema = th.PropertiesList(
        th.Property("_line_id", th.IntegerType),
        th.Property("_time_extracted", th.StringType),
        th.Property("_api_report_date", th.DateTimeType),
        th.Property("vendor_number", th.StringType),
        th.Property("app_name", th.StringType),
        th.Property("app_apple_id", th.IntegerType),
        th.Property("subscription_name", th.StringType),
        th.Property("subscription_offer_name", th.StringType),
        th.Property("subscription_apple_id", th.StringType),
        th.Property("subscription_group_id", th.StringType),
        th.Property("standard_subscription_duration", th.StringType),
        th.Property("promotional_offer_name", th.StringType),
        th.Property("promotional_offer_id", th.StringType),
        th.Property("customer_price", th.NumberType),
        th.Property("customer_currency", th.StringType),
        th.Property("developer_proceeds", th.NumberType),
        th.Property("proceeds_currency", th.StringType),
        th.Property("preserved_pricing", th.StringType),
        th.Property("proceeds_reason", th.StringType),
        th.Property("client", th.StringType),
        th.Property("device", th.StringType),
        th.Property("state", th.StringType),
        th.Property("country", th.StringType),
        th.Property("subscribers", th.StringType),
        th.Property("active_standard_price_subscriptions", th.IntegerType),
        th.Property("active_free_trial_introductory_offer_subscriptions", th.IntegerType),
        th.Property("active_pay_up_front_introductory_offer_subscriptions", th.IntegerType),
        th.Property("active_pay_as_you_go_introductory_offer_subscriptions", th.IntegerType),
        th.Property("free_trial_offer_code_subscriptions", th.IntegerType),
        th.Property("free_trial_promotional_offer_subscriptions", th.IntegerType),
        th.Property("pay_as_you_go_offer_code_subscriptions", th.IntegerType),
        th.Property("pay_up_front_promotional_offer_subscriptions", th.IntegerType),
        th.Property("pay_up_front_offer_code_subscriptions", th.IntegerType),
        th.Property("pay_as_you_go_promotional_offer_subscriptions", th.IntegerType),
        th.Property("marketing_opt_ins", th.IntegerType),
        th.Property("billing_retry", th.IntegerType),
        th.Property("grace_period", th.IntegerType)
    ).to_dict()

    def download_data(self, start_date, api):
        filters = {
            'frequency': 'DAILY',
            'reportType': 'SUBSCRIPTION',
            'reportSubType': 'SUMMARY',
            'reportDate': start_date,
            'version': '1_3',
            'vendorNumber': self.config['vendor']
        }
        return api.download_sales_and_trends_reports(filters=filters)


class SubscriptionEventReportStream(client.AppStoreStream):
    name = "subscription_event_report"
    schema = th.PropertiesList(
        th.Property("_line_id", th.IntegerType),
        th.Property("_time_extracted", th.StringType),
        th.Property("_api_report_date", th.DateTimeType),
        th.Property("vendor_number", th.StringType),
        th.Property("event_date", th.StringType),
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

    def download_data(self, start_date, api):
        filters = {
            'frequency': 'DAILY',
            'reportType': 'SUBSCRIPTION_EVENT',
            'reportSubType': 'SUMMARY',
            'reportDate': start_date,
            'version': '1_3',
            'vendorNumber': self.config['vendor']
        }
        return api.download_sales_and_trends_reports(filters=filters)


class FinancialReportStream(client.AppStoreStream):
    name = "financial_report"
    date_format = '%Y-%m'
    date_increment = relativedelta(months=1)
    skip_line_first_values = ["Total_Rows", "Total_Amount", "Total_Units"]
    schema = th.PropertiesList(
        th.Property("_line_id", th.IntegerType),
        th.Property("_time_extracted", th.StringType),
        th.Property("_api_report_date", th.DateTimeType),
        th.Property("vendor_identifier", th.StringType),
        th.Property("vendor_number", th.StringType),
        th.Property("start_date", th.StringType),
        th.Property("end_date", th.StringType),
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

    def download_data(self, start_date, api):
        filters = {'vendorNumber': self.config['vendor'],
                   'regionCode': 'ZZ',
                   'reportType': 'FINANCIAL',
                   'reportDate': start_date,
                   }
        return api.download_finance_reports(filters=filters)

