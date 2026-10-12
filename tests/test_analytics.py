"""Analytics Reports streams against a fake App Store Connect API."""

import gzip
import io
import json
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from unittest.mock import MagicMock, patch
from urllib.parse import urlsplit

from appstoreconnect.api import APIError
from tap_appstore.client import AppStoreStream
from tap_appstore.tap import TapAppStore

APP_ID = "1044867788"
ONGOING = {"app_id": APP_ID, "request_id": "req-ongoing", "access_type": "ONGOING"}

DISCOVERY_TSV = (
    "Date\tApp Name\tApp Apple Identifier\tEvent\tPage Type\tSource Type\tEngagement Type\t"
    "Device\tPlatform Version\tTerritory\tCounts\tUnique Counts\n"
    "{date}\tDay One\t1044867788\tImpression\tNo page\tApp Store search\t\tiPhone\tiOS 26.0\tUS\t120\t100\n"
    "{date}\tDay One\t1044867788\tPage view\tProduct page\tApp Store browse\t\tiPad\tiOS 26.0\tGB\t7\t6\n"
)
REPORTS = "/v1/analyticsReportRequests/{}/reports?limit=200"
INSTANCES = "/v1/analyticsReports/{}/instances?filter[granularity]=DAILY&limit=200"


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
    """Saved state with a bookmark per request id."""
    return {"bookmarks": {"discovery_engagement_report": {"partitions": [
        {"context": {"app_id": APP_ID, "request_id": request_id, "access_type": access_type},
         "replication_key": "_api_report_date", "replication_key_value": value}
        for (request_id, access_type), value in bookmarks.items()
    ]}}}


class FakeAppStoreConnect:
    """Answers App Store Connect API paths, ignoring the host."""

    def __init__(self, responses, files):
        self.responses = responses
        self.files = files
        self.calls = []

    def _api_call(self, url):
        parts = urlsplit(url)
        key = parts.path + (f"?{parts.query}" if parts.query else "")
        self.calls.append(key)
        return self.responses[key]

    def download(self, url, timeout=None):
        response = MagicMock()
        response.content = gzip.compress(self.files[url].encode("utf-8"))
        return response


def standard_responses():
    return {
        f"/v1/apps/{APP_ID}/analyticsReportRequests": page([
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
        return {p["context"]["request_id"]: p.get("replication_key_value") for p in partitions}


class TestDiscoveryEngagementReport(AnalyticsStreamTestCase):
    def test_reads_each_request_with_its_own_bookmark(self):
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
        self.assertEqual(self.bookmarks(), {"req-ongoing": "2026-10-03T00:00:00", "req-snapshot": "2026-10-02T00:00:00"})
        self.assertFalse(any("rep-detailed" in call for call in self.fake.calls))  # Standard only

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

    def test_a_snapshot_is_read_after_the_ongoing_bookmark_passed_it(self):
        stream = self.make_stream(state=bookmark_state({("req-ongoing", "ONGOING"): "2026-10-03T00:00:00"}))
        records = self.sync(stream)
        # i-03 was processed on the bookmark's date, so it was already read.
        self.assertEqual({r["_instance_id"] for r in records}, {"i-02"})

    def test_first_run_starts_on_start_date(self):
        records = self.sync(self.make_stream(start_date="2026-10-02T00:00:00Z"))
        self.assertEqual({r["_instance_id"] for r in records}, {"i-02", "i-03"})

    def test_a_failed_run_reads_the_request_again(self):
        i03 = self.fake.files.pop("https://s3.example/i-03.csv.gz")
        stream = self.make_stream()
        stream.STATE_MSG_FREQUENCY = 1
        with self.assertRaises(KeyError):
            self.sync(stream)

        self.fake.files["https://s3.example/i-03.csv.gz"] = i03
        records = self.sync(self.make_stream(state=self.states[-1]))
        self.assertEqual({r["_instance_id"] for r in records if r["request_id"] == "req-ongoing"}, {"i-01", "i-03"})

    def test_reads_comma_separated_files_with_a_byte_order_mark(self):
        self.fake.files["https://s3.example/i-01.csv.gz"] = (
            "\ufeffDate,App Name,App Apple Identifier,Event,Territory,Counts\n"
            '2026-09-30,"Day One: Journal, Diary",1044867788,Impression,US,5\n'
        )
        record = self.sync(self.make_stream())[0]
        self.assertEqual(record["date"], "2026-09-30")
        self.assertEqual(record["app_name"], "Day One: Journal, Diary")
        self.assertEqual(record["counts"], 5)

    def test_a_quote_in_a_tab_separated_file_is_a_plain_character(self):
        rows = list(self.make_stream()._read_rows('Date\tApp Name\tCounts\n2026-09-30\t"Day One\t5\n2026-09-30\tDay One\t7\n'))
        self.assertEqual([row["app_name"] for row in rows], ['"Day One', "Day One"])

    def test_extracts_nothing_without_app_ids(self):
        stream = self.make_stream(app_ids=None)
        self.assertIsNone(stream.partitions)
        self.assertEqual(self.sync(stream), [])
        self.assertEqual(self.fake.calls, [])

    def test_accepts_numeric_app_ids(self):
        stream = self.make_stream(app_ids=(1044867788,))
        self.assertEqual(stream.partitions[0], ONGOING)

    def test_fails_without_an_active_ongoing_request(self):
        self.fake.responses[f"/v1/apps/{APP_ID}/analyticsReportRequests"] = page([
            report_request("req-ongoing", "ONGOING", stopped=True),
            report_request("req-snapshot", "ONE_TIME_SNAPSHOT"),
        ])
        with self.assertRaisesRegex(RuntimeError, "no active ONGOING analytics report request"):
            self.sync(self.make_stream())

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
        self.assertTrue(any("no instance processed in the last 4 days (newest: 2026-10-03)" in line for line in logs.output))

    def test_stays_quiet_while_daily_instances_arrive(self):
        today = datetime.utcnow().strftime("%Y-%m-%d")
        self.fake.responses[INSTANCES.format("rep-ongoing")] = page([instance("i-01", today)])
        with patch("tap_appstore.client.logger.warning") as warning:
            self.sync(self.make_stream())
        warning.assert_not_called()

    def test_logs_an_error_when_instances_expired_before_they_were_read(self):
        stream = self.make_stream(state=bookmark_state({("req-ongoing", "ONGOING"): "2026-09-01T00:00:00"}))
        with self.assertLogs("tap_appstore.client", level="ERROR") as logs:
            self.sync(stream)
        self.assertIn("processed after 2026-09-01 and before 2026-10-01 were deleted", logs.output[0])

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
    def test_matches_either_name_apple_uses(self):
        self.fake.responses = {
            f"/v1/apps/{APP_ID}/analyticsReportRequests": page([report_request("req", "ONGOING")]),
            REPORTS.format("req"): page([report("rep", "App Downloads Standard")]),
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


if __name__ == "__main__":
    unittest.main()
