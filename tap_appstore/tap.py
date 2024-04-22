"""AppStore tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_appstore import streams


class TapAppStore(Tap):
    """AppStore tap class."""

    name = "tap-appstore"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "key_id",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The AppStore key ID",
        ),
        th.Property(
            "key_file",
            th.StringType,
            required=True,
            secret=True,
            description="Path to the AppStore key file",
        ),
        th.Property(
            "issuer_id",
            th.StringType,
            description="The ID of the issuer",
        ),
        th.Property(
            "vendor",
            th.StringType,
            default="https://api.mysample.com",
            description="The ID of the vendor",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
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
            streams.SubscriptionReportStream(self)
        ]


if __name__ == "__main__":
    TapAppStore.cli()
