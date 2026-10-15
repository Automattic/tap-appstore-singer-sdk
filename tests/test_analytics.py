"""Analytics Reports streams against a fake App Store Connect API."""

import gzip
import io
import json
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from unittest.mock import MagicMock, patch
from urllib.parse import urlsplit

from appstoreconnect.api import APIError, HttpMethod
from requests import HTTPError
from tap_appstore.client import AppStoreStream
from tap_appstore.tap import TapAppStore

APP_ID = "1044867788"
REQUESTS = f"/v1/apps/{APP_ID}/analyticsReportRequests"
REPORTS = "/v1/analyticsReportRequests/{}/reports?limit=200"
INSTANCES = "/v1/analyticsReports/{}/instances?filter[granularity]=DAILY&limit=200"

DISCOVERY_TSV = (
    "Date\tApp Name\tApp Apple Identifier\tEvent\tPage Type\tSource Type\tEngagement Type\t"
    "Device\tPlatform Version\tTerritory\tCounts\tUnique Counts\n"
    "{date}\tDay One\t1044867788\tImpression\tNo page\tApp Store search\t\tiPhone\tiOS 26.0\tUS\t120\t100\n"
    "{date}\tDay One\t1044867788\tPage view\tProduct page\tApp Store browse\t\tiPad\tiOS 26.0\tGB\t7\t6\n"
)


def page(items, next_url=None):
    return {"data": items, "links": {"next": next_url} if next_url else {}}


def report_request(request_id, access_type, stopped=False):
    return {"id": request_id, "attributes": {"accessType": access_type, "stoppedDueToInactivity": stopped}}


def report(report_id, name):
    return {"id": report_id, "attributes": {"name": name}}


def instance(instance_id, processing_date):
    return {"id": instance_id, "attributes": {"processingDate": processing_date}}


def segment(instance_id):
    return {"id": f"seg-{instance_id}", "attributes": {"url": f"https://s3.example/{instance_id}.csv.gz"}}


def bookmark_state(bookmarks):
    """Saved state with a bookmark per kind of request."""
    return {"bookmarks": {"discovery_engagement_report": {"partitions": [
        {"context": {"app_id": APP_ID, "access_type": access_type},
         "replication_key": "_api_report_date", "replication_key_value": value}
        for access_type, value in bookmarks.items()
    ]}}}


class FakeAppStoreConnect:
    """Answers App Store Connect API paths, ignoring the host, and records the requests it's asked to create."""

    def __init__(self, responses, files):
        self.responses = responses
        self.files = files
        self.calls = []
        self.created = []
        self.create_error = None

    def _api_call(self, url, method=HttpMethod.GET, post_data=None):
        parts = urlsplit(url)
        key = parts.path + (f"?{parts.query}" if parts.query else "")
        self.calls.append(key)
        if method != HttpMethod.POST:
            response = self.responses[key]
            if isinstance(response, list):  # successive answers, the last one repeating
                return response.pop(0) if len(response) > 1 else response[0]
            return response
        data = post_data["data"]
        self.created.append((data["relationships"]["app"]["data"]["id"], data["attributes"]["accessType"]))
        if self.create_error:
            raise self.create_error
        request_id = f"req-new-{len(self.created)}"
        self.responses[REPORTS.format(request_id)] = page([])  # Apple hasn't generated anything yet
        return {"data": report_request(request_id, data["attributes"]["accessType"])}

    def download(self, url, timeout=None):
        if url not in self.files:  # how S3 answers an expired link
            raise HTTPError("403 Client Error: Forbidden")
        response = MagicMock()
        response.content = gzip.compress(self.files[url].encode("utf-8"))
        return response


def standard_responses():
    return {
        REQUESTS: page([
            report_request("req-ongoing", "ONGOING"),
            report_request("req-snapshot", "ONE_TIME_SNAPSHOT"),
        ]),
        # The ONGOING request's report list spans two pages.
        REPORTS.format("req-ongoing"): page(
            [report("rep-detailed", "App Store Discovery and Engagement Detailed")],
            next_url="https://api.appstoreconnect.apple.com/v1/analyticsReportRequests/req-ongoing/reports?cursor=2",
        ),
        "/v1/analyticsReportRequests/req-ongoing/reports?cursor=2": page(
            [report("rep-ongoing", "App Store Discovery and Engagement Standard")]
        ),
        REPORTS.format("req-snapshot"): page([report("rep-snapshot", "App Store Discovery and Engagement Standard")]),
        INSTANCES.format("rep-ongoing"): page([instance("i-03", "2026-10-03"), instance("i-01", "2026-10-01")]),
        INSTANCES.format("rep-snapshot"): page([instance("i-02", "2026-10-02")]),
        "/v1/analyticsReportInstances/i-01/segments": page([segment("i-01")]),
        "/v1/analyticsReportInstances/i-02/segments": page([segment("i-02")]),
        "/v1/analyticsReportInstances/i-03/segments": page([segment("i-03")]),
    }


def standard_files():
    return {
        f"https://s3.example/{i}.csv.gz": DISCOVERY_TSV.format(date=date)
        for i, date in (("i-01", "2026-09-30"), ("i-02", "2026-09-29"), ("i-03", "2026-10-02"))
    }


class AnalyticsStreamTestCase(unittest.TestCase):
    def setUp(self):
        self.fake = FakeAppStoreConnect(standard_responses(), standard_files())
        for patcher in (patch("tap_appstore.client.requests.get", side_effect=self.fake.download),
                        patch("tenacity.nap.time.sleep")):  # no real waits between retries
            patcher.start()
            self.addCleanup(patcher.stop)

    def make_stream(self, app_ids=(APP_ID,), name="discovery_engagement_report",
                    start_date="2026-09-01T00:00:00Z", state=None):
        config = {"key_id": "KEY", "key_file": "key.p8", "issuer_id": "issuer", "vendor": "85219326",
                  "start_date": start_date}
        if app_ids is not None:
            config["app_ids"] = list(app_ids)
        with patch.object(AppStoreStream, "setup_api_connection", return_value=self.fake):
            tap = TapAppStore(config=config, state=state, parse_env_config=False)
        return tap.streams[name]

    def sync(self, stream):
        """Records and STATE messages of a sync, as Meltano would see them."""
        output = io.StringIO()
        try:
            with redirect_stdout(output):
                stream.sync()
        finally:
            messages = [json.loads(line) for line in output.getvalue().splitlines()]
            self.states = [m["value"] for m in messages if m["type"] == "STATE"]
        return [m["record"] for m in messages if m["type"] == "RECORD"]

    def bookmarks(self):
        partitions = self.states[-1]["bookmarks"]["discovery_engagement_report"]["partitions"]
        return {p["context"]["access_type"]: p.get("replication_key_value") for p in partitions}


class TestDiscoveryEngagementReport(AnalyticsStreamTestCase):
    def test_reads_daily_data_and_history_with_a_bookmark_each(self):
        records = self.sync(self.make_stream())

        self.assertEqual(
            [(r["request_id"], r["_instance_id"], r["date"], r["_line_id"]) for r in records],
            [
                ("req-ongoing", "i-01", "2026-09-30", 1),
                ("req-ongoing", "i-01", "2026-09-30", 2),
                ("req-ongoing", "i-03", "2026-10-02", 1),
                ("req-ongoing", "i-03", "2026-10-02", 2),
                ("req-snapshot", "i-02", "2026-09-29", 1),
                ("req-snapshot", "i-02", "2026-09-29", 2),
            ],
        )
        self.assertEqual(self.bookmarks(), {"ONGOING": "2026-10-03T00:00:00", "ONE_TIME_SNAPSHOT": "2026-10-02T00:00:00"})
        self.assertFalse(any("rep-detailed" in call for call in self.fake.calls))  # Standard only
        listings = sorted(call for call in self.fake.calls if call.endswith("/segments"))
        self.assertEqual(listings, [f"/v1/analyticsReportInstances/i-0{n}/segments" for n in (1, 2, 3)])  # once each
        self.assertEqual(self.fake.created, [])  # the app already has both requests

        first = records[0]
        self.assertEqual(first["app_id"], APP_ID)
        self.assertEqual(first["access_type"], "ONGOING")
        self.assertEqual(first["_api_report_date"], "2026-10-01T00:00:00")
        self.assertEqual(first["_segment_id"], "seg-i-01")
        self.assertEqual(first["source_type"], "App Store search")
        self.assertEqual(first["territory"], "US")
        self.assertEqual(first["counts"], 120)
        self.assertEqual(first["unique_counts"], 100)
        self.assertEqual(first["app_apple_identifier"], 1044867788)
        self.assertEqual(first["vendor_number"], "85219326")

    def test_history_is_read_after_newer_daily_data(self):
        stream = self.make_stream(state=bookmark_state({"ONGOING": "2026-10-03T00:00:00"}))
        records = self.sync(stream)
        # i-03 was processed on the daily bookmark's date, so it was already read.
        self.assertEqual({r["_instance_id"] for r in records}, {"i-02"})

    def test_first_run_starts_on_start_date(self):
        records = self.sync(self.make_stream(start_date="2026-10-02T00:00:00Z"))
        self.assertEqual({r["_instance_id"] for r in records}, {"i-02", "i-03"})

    def test_a_failed_run_reads_the_deliveries_again(self):
        i03 = self.fake.files.pop("https://s3.example/i-03.csv.gz")
        stream = self.make_stream()
        stream.STATE_MSG_FREQUENCY = 1
        with self.assertRaises(HTTPError):
            self.sync(stream)

        self.fake.files["https://s3.example/i-03.csv.gz"] = i03
        records = self.sync(self.make_stream(state=self.states[-1]))
        self.assertEqual({r["_instance_id"] for r in records if r["access_type"] == "ONGOING"}, {"i-01", "i-03"})

    def test_fails_when_a_file_has_no_date_column(self):
        self.fake.files["https://s3.example/i-01.csv.gz"] = "Date,App Name,Counts\n2026-09-30,Day One,5\n"
        with self.assertRaisesRegex(ValueError, "tab-separated file with a Date column"):
            self.sync(self.make_stream())

    def test_a_quote_in_a_tab_separated_file_is_a_plain_character(self):
        rows = list(self.make_stream()._read_rows('Date\tApp Name\tCounts\n2026-09-30\t"Day One\t5\n2026-09-30\tDay One\t7\n'))
        self.assertEqual([row["app_name"] for row in rows], ['"Day One', "Day One"])

    def test_fails_without_app_ids(self):
        with self.assertRaisesRegex(ValueError, "needs the app_ids setting"):
            self.sync(self.make_stream(app_ids=None))
        self.assertEqual(self.fake.calls, [])

    def test_accepts_numeric_app_ids(self):
        stream = self.make_stream(app_ids=(1044867788,))
        self.assertEqual(stream.partitions[0], {"app_id": APP_ID, "access_type": "ONGOING"})

    def test_creates_a_daily_request_when_none_is_active(self):
        self.fake.responses[REQUESTS] = page([
            report_request("req-ongoing", "ONGOING", stopped=True),
            report_request("req-snapshot", "ONE_TIME_SNAPSHOT"),
        ])
        records = self.sync(self.make_stream())
        self.assertEqual(self.fake.created, [(APP_ID, "ONGOING")])
        # What the stopped request delivered before it stopped is still read.
        self.assertEqual({r["_instance_id"] for r in records}, {"i-01", "i-02", "i-03"})

    def test_fails_when_it_cannot_create_a_daily_request(self):
        self.fake.responses[REQUESTS] = page([])
        self.fake.create_error = APIError("The API key in use does not allow this request", 403)
        with self.assertRaisesRegex(APIError, "Could not create a ONGOING analytics report request"):
            self.sync(self.make_stream())

    def test_asks_for_history_when_an_app_has_none(self):
        self.fake.responses[REQUESTS] = page([report_request("req-ongoing", "ONGOING")])
        self.sync(self.make_stream())
        self.assertEqual(self.fake.created, [(APP_ID, "ONE_TIME_SNAPSHOT")])

    def test_asks_for_history_again_when_it_expired_before_it_was_read(self):
        self.fake.responses[INSTANCES.format("rep-snapshot")] = page([])
        self.sync(self.make_stream())
        self.assertEqual(self.fake.created, [(APP_ID, "ONE_TIME_SNAPSHOT")])

    def test_does_not_ask_for_history_once_it_has_been_read(self):
        self.fake.responses[REQUESTS] = page([report_request("req-ongoing", "ONGOING")])
        self.sync(self.make_stream(state=bookmark_state({"ONE_TIME_SNAPSHOT": "2026-10-02T00:00:00"})))
        self.assertEqual(self.fake.created, [])

    def test_warns_when_apple_refuses_a_history_request(self):
        self.fake.responses[REQUESTS] = page([report_request("req-ongoing", "ONGOING")])
        self.fake.create_error = APIError("Only one ONE_TIME_SNAPSHOT request is allowed per month", 409)
        with self.assertLogs("tap_appstore.client", level="WARNING") as logs:
            records = self.sync(self.make_stream())
        self.assertTrue(any("Apple refused a history request" in line for line in logs.output))
        self.assertEqual({r["_instance_id"] for r in records}, {"i-01", "i-03"})

    def test_asks_for_history_when_daily_instances_expired_unread(self):
        self.fake.responses[INSTANCES.format("rep-snapshot")] = page([instance("i-02", "2026-09-15")])
        stream = self.make_stream(state=bookmark_state({"ONGOING": "2026-09-01T00:00:00"}))
        with self.assertLogs("tap_appstore.client", level="ERROR") as logs:
            self.sync(stream)
        self.assertIn("processed after 2026-09-01 and before 2026-10-01 were deleted", logs.output[0])
        self.assertEqual(self.fake.created, [(APP_ID, "ONE_TIME_SNAPSHOT")])

    def test_a_newer_history_covers_expired_instances(self):
        # The history was generated on 2026-10-02, after the oldest daily instance left, so it holds the missed days.
        stream = self.make_stream(state=bookmark_state({"ONGOING": "2026-09-01T00:00:00"}))
        with self.assertLogs("tap_appstore.client", level="ERROR"):
            self.sync(stream)
        self.assertEqual(self.fake.created, [])

    def test_fails_when_apple_renames_the_report(self):
        self.fake.responses[REPORTS.format("req-ongoing")] = page(
            [report("rep-ongoing", "App Store Discovery and Engagement Standard v2")]
        )
        with self.assertRaisesRegex(RuntimeError, "has no report named"):
            self.sync(self.make_stream())

    def test_waits_for_a_new_request_to_list_its_reports(self):
        self.fake.responses[REPORTS.format("req-ongoing")] = page([])
        with self.assertLogs("tap_appstore.client", level="WARNING"):
            records = self.sync(self.make_stream())
        self.assertEqual({r["request_id"] for r in records}, {"req-snapshot"})

    def test_warns_when_daily_instances_stop_arriving(self):
        with self.assertLogs("tap_appstore.client", level="WARNING") as logs:
            self.sync(self.make_stream())
        self.assertTrue(any("no daily instance processed in the last 4 days (newest: 2026-10-03)" in line
                            for line in logs.output))

    def test_stays_quiet_while_daily_instances_arrive(self):
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.fake.responses[INSTANCES.format("rep-ongoing")] = page([instance("i-01", today)])
        with patch("tap_appstore.client.logger.warning") as warning:
            self.sync(self.make_stream())
        warning.assert_not_called()

    def test_gets_a_fresh_link_right_away_when_one_has_expired(self):
        # The link from the first listing has expired by the time the tap downloads the file.
        expired = {"id": "seg-i-01", "attributes": {"url": "https://s3.example/expired.csv.gz"}}
        self.fake.responses["/v1/analyticsReportInstances/i-01/segments"] = [page([expired]), page([segment("i-01")])]
        with patch("tenacity.nap.time.sleep") as sleep:
            records = self.sync(self.make_stream())
        self.assertEqual(len([r for r in records if r["_instance_id"] == "i-01"]), 2)
        self.assertEqual(self.fake.calls.count("/v1/analyticsReportInstances/i-01/segments"), 2)
        sleep.assert_not_called()  # no five-minute retry wait

    def test_retries_apple_errors_but_not_bugs(self):
        stream = self.make_stream()
        stream.api = MagicMock()
        stream.api._api_call.side_effect = [APIError("Service unavailable", 503), page([])]
        self.assertEqual(stream._list("apps"), [])

        stream.api._api_call.side_effect = KeyError("data")
        with self.assertRaises(KeyError):
            stream._list("apps")
        self.assertEqual(stream.api._api_call.call_count, 3)


class TestDownloadsReport(AnalyticsStreamTestCase):
    def test_reads_app_downloads_standard(self):
        self.fake.responses = {
            REQUESTS: page([report_request("req", "ONGOING"), report_request("req-history", "ONE_TIME_SNAPSHOT")]),
            REPORTS.format("req"): page([report("rep", "App Downloads Standard")]),
            REPORTS.format("req-history"): page([]),
            INSTANCES.format("rep"): page([instance("i-01", "2026-10-01")]),
            "/v1/analyticsReportInstances/i-01/segments": page([segment("i-01")]),
        }
        self.fake.files["https://s3.example/i-01.csv.gz"] = (
            "Date\tApp Name\tApp Apple Identifier\tDownload Type\tApp Version\tDevice\tPlatform Version\t"
            "Source Type\tPage Type\tPre-Order\tTerritory\tCounts\n"
            "2026-09-30\tDay One\t1044867788\tFirst-time download\t2026.20\tiPhone\tiOS 26.0\t"
            "App Store search\tProduct page\t\tUS\t42\n"
        )
        record = self.sync(self.make_stream(name="downloads_report"))[0]
        self.assertEqual(record["download_type"], "First-time download")
        self.assertEqual(record["pre_order"], "")
        self.assertEqual(record["counts"], 42)

    def test_accepts_both_names_apple_uses_for_the_report(self):
        stream = self.make_stream(name="downloads_report")
        self.fake.responses[INSTANCES.format("rep")] = page([instance("i-01", "2026-10-01")])
        for name in ("App Downloads Standard", "App Store Downloads Standard"):
            with self.subTest(name):
                self.fake.responses[REPORTS.format("req")] = page([report("rep", name)])
                instances = stream._instances({"id": "req"}, "ONGOING")
                self.assertEqual([i["id"] for i in instances], ["i-01"])


if __name__ == "__main__":
    unittest.main()
