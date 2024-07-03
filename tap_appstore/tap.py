"""AppStore tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_appstore import streams


class TapAppStore(Tap):
    """AppStore tap class."""

    name = "tap-appstore"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "key_id",
            th.StringType,
            required=True,
            description="The AppStore key ID",
        ),
        th.Property(
            "key_file",
            th.StringType,
            required=True,
            description="Path to the AppStore key file",
        ),
        th.Property(
            "issuer_id",
            th.StringType,
            required=True,
            description="The ID of the issuer",
        ),
        th.Property(
            "vendor",
            th.StringType,
            required=True,
            description="The ID of the vendor",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            required=True,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.AppStoreStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.SalesReportStream(self),
            streams.SubscriberReportStream(self),
            streams.SubscriptionReportStream(self),
            streams.SubscriptionEventReportStream(self),
            streams.FinancialReportStream(self)
        ]


if __name__ == "__main__":
    TapAppStore.cli()
