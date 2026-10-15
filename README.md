# tap-appstore

`tap-appstore` is a Singer tap for AppStore.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

<!--

Developer TODO: Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

## Installation

Install from PyPi:

```bash
pipx install tap-appstore
```

Install from GitHub:

```bash
pipx install git+https://github.com/ORG_NAME/tap-appstore.git@main
```

-->

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-appstore --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-appstore --about
```

### Analytics reports

`discovery_engagement_report` and `downloads_report` read the [Analytics Reports API](https://developer.apple.com/documentation/appstoreconnectapi/downloading-analytics-reports):
App Store impressions, product page views and downloads by source, territory and device.
They need the `app_ids` setting (Apple IDs, such as `["1044867788"]`) and fail without it.

- **The tap asks Apple for the reports**, which takes a key with the Admin role. Apple only
  generates them for an app with an analytics report request. When an app has no active
  `ONGOING` request (daily data), the tap creates one; Apple also stops one that nobody reads for
  a long time. Until it has read an app's history, the tap creates a `ONE_TIME_SNAPSHOT` request
  (history from January 1, 2024) whenever there is none to read: the app has no history request,
  or Apple deleted its instances first. Apple's first reports take a day or two.
- **Gaps fill themselves.** Apple deletes report instances after 35 days. If some were deleted
  before the tap read them, it logs an error and creates a new `ONE_TIME_SNAPSHOT` request, unless
  one already holds those days.
- **Apple allows one history request a month**, so it refuses another while one is being
  generated. The tap logs that as a warning and carries on.
- **Each app has two bookmarks**, one for `ONGOING` and one for `ONE_TIME_SNAPSHOT` requests:
  `_api_report_date`, the processing date of the newest instance read. Apple dates a history
  instance the day it makes it, and it can arrive after newer daily data has been read. A bookmark
  moves only once all its instances have been read, so a failed run reads them again.
- **History is big.** A history instance can run to millions of rows in dozens of files, so the
  run that reads it takes a while.
- **Deduplicate before counting.** `_instance_id`, `_segment_id` and `_line_id` identify a row,
  which a rerun can load twice. A later instance can restate earlier dates, so for each
  `app_apple_identifier` and `date`, keep only the rows of the instance with the latest
  `_api_report_date`, preferring `access_type` `ONGOING` when a snapshot instance shares it.
- Both read Apple's Standard reports. The Detailed versions add fields but drop rows with fewer
  than five users or devices and add noise.

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-appstore` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-appstore --version
tap-appstore --help
tap-appstore --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-appstore` CLI interface directly using `poetry run`:

```bash
poetry run tap-appstore --help
```

### `constraints.txt` generation:

```bash
$ poetry self add poetry-plugin-export
$ poetry export --output constraints.txt --without-hashes
```

Change to tag instead of commit here:
`appstoreconnect @ git+https://github.com/Automattic/appstoreconnectapi.git@10.0.2 ; python_version >= "3.9"`

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-appstore
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-appstore --version
# OR run a test `elt` pipeline:
meltano elt tap-appstore target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
