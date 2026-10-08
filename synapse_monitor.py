import os
import csv
import requests

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from azure.identity import ClientSecretCredential


# ============================================================
# CONFIGURATION
# ============================================================

TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

SYNAPSE_ENDPOINT = os.environ["SYNAPSE_ENDPOINT"].rstrip("/")

API_VERSION = "2020-12-01"

SYNAPSE_SCOPE = "https://dev.azuresynapse.net/.default"

LOCAL_TIMEZONE = ZoneInfo("Asia/Kolkata")

CSV_FILE = "synapse_pipeline_results.csv"


# ============================================================
# MONITORED PIPELINES
# ============================================================

MONITORED_PIPELINES = [
    "PL_Captura_Snapshots",
    "BronzeToBI_daily_530_am",
    "BronzeToBI_daily_530_am_1",
    "BronzeToBI_daily_5am",
]


# ============================================================
# REQUIRED DISPLAY ORDER
# ============================================================

PIPELINE_ORDER = {
    "PL_Captura_Snapshots": 0,
    "BronzeToBI_daily_530_am": 1,
    "BronzeToBI_daily_530_am_1": 2,
    "BronzeToBI_daily_5am": 3,
}


# ============================================================
# GET AZURE ACCESS TOKEN
# ============================================================

def get_access_token():

    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    token = credential.get_token(
        SYNAPSE_SCOPE
    )

    return token.token


# ============================================================
# PARSE DATETIME
# ============================================================

def parse_datetime(value):

    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except Exception:
        return None


# ============================================================
# CONVERT UTC TO IST
# ============================================================

def convert_to_ist(value):

    dt = parse_datetime(value)

    if dt is None:
        return None

    return dt.astimezone(
        LOCAL_TIMEZONE
    )


# ============================================================
# FORMAT DATE/TIME
# ============================================================

def format_datetime(value):

    dt = convert_to_ist(value)

    if dt is None:
        return "-"

    return dt.strftime(
        "%d/%m/%Y %I:%M:%S %p"
    )


# ============================================================
# GET DURATION IN MILLISECONDS
# ============================================================

def calculate_duration_ms(
    run_start,
    run_end
):

    start_dt = parse_datetime(
        run_start
    )

    end_dt = parse_datetime(
        run_end
    )

    if start_dt is None or end_dt is None:
        return None

    return (
        end_dt - start_dt
    ).total_seconds() * 1000


# ============================================================
# FORMAT DURATION
# ============================================================

def format_duration(
    duration_ms
):

    if duration_ms is None:
        return "-"

    total_seconds = round(
        duration_ms / 1000
    )

    hours = total_seconds // 3600

    minutes = (
        total_seconds % 3600
    ) // 60

    seconds = (
        total_seconds % 60
    )

    if hours > 0:

        return (
            f"{hours}h "
            f"{minutes}m "
            f"{seconds}s"
        )

    return (
        f"{minutes}m "
        f"{seconds}s"
    )


# ============================================================
# IDENTIFY PL_CAPTURA JOB
# ============================================================

def get_captura_job_type(
    run_start
):

    dt = convert_to_ist(
        run_start
    )

    if dt is None:
        return None

    # 1:30 PM job
    if (
        dt.hour == 13
        and dt.minute == 30
    ):
        return "PL_Captura_Snapshots_1_30_PM"

    # 11:30 PM job
    if (
        dt.hour == 23
        and dt.minute == 30
    ):
        return "PL_Captura_Snapshots_11_30_PM"

    return None


# ============================================================
# GET AVERAGE GROUP
# ============================================================

def get_average_group(run):

    pipeline = run.get(
        "pipelineName"
    )

    if pipeline == "PL_Captura_Snapshots":

        job_type = get_captura_job_type(
            run.get("runStart")
        )

        if job_type is None:
            return None

        return (
            pipeline,
            job_type
        )

    return (
        pipeline,
        "single_job"
    )


# ============================================================
# GET LAST 5 BUSINESS DAYS
# ============================================================

def get_reporting_dates():

    today = datetime.now(
        LOCAL_TIMEZONE
    ).date()

    dates = []

    current_date = today

    while len(dates) < 5:

        # Monday = 0
        # Sunday = 6

        if current_date.weekday() < 5:

            dates.append(
                current_date
            )

        current_date -= timedelta(
            days=1
        )

    return set(dates)


# ============================================================
# GET PIPELINE RUNS FROM SYNAPSE
# ============================================================

def get_pipeline_runs():

    access_token = get_access_token()

    url = (
        f"{SYNAPSE_ENDPOINT}"
        f"/queryPipelineRuns"
        f"?api-version={API_VERSION}"
    )

    now_utc = datetime.now(
        timezone.utc
    )

    # Fetch enough data to cover
    # the last 5 business days.
    query_start_utc = (
        now_utc -
        timedelta(days=10)
    )

    payload = {

        "lastUpdatedAfter":
            query_start_utc.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),

        "lastUpdatedBefore":
            now_utc.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),

        "orderBy": [
            {
                "orderBy": "RunStart",
                "order": "DESC"
            }
        ]
    }

    headers = {

        "Authorization":
            f"Bearer {access_token}",

        "Content-Type":
            "application/json"
    }

    all_runs = []

    continuation_token = None

    while True:

        if continuation_token:

            payload[
                "continuationToken"
            ] = continuation_token

        response = requests.post(

            url,

            headers=headers,

            json=payload,

            timeout=60
        )

        response.raise_for_status()

        data = response.json()

        runs = data.get(
            "value",
            []
        )

        all_runs.extend(
            runs
        )

        continuation_token = (
            data.get(
                "continuationToken"
            )
        )

        if not continuation_token:
            break

    return all_runs


# ============================================================
# FILTER LAST 5 BUSINESS DAYS
# ============================================================

def get_monitored_runs(
    all_runs
):

    reporting_dates = (
        get_reporting_dates()
    )

    monitored_names = {
        name.lower()
        for name in MONITORED_PIPELINES
    }

    filtered = []

    for run in all_runs:

        pipeline_name = run.get(
            "pipelineName",
            ""
        )

        if (
            pipeline_name.lower()
            not in monitored_names
        ):
            continue

        run_start = run.get(
            "runStart"
        )

        run_start_ist = convert_to_ist(
            run_start
        )

        if run_start_ist is None:
            continue

        if (
            run_start_ist.date()
            not in reporting_dates
        ):
            continue

        filtered.append(
            run
        )

    return filtered


# ============================================================
# CALCULATE AVERAGES
# ============================================================

def calculate_average_durations(
    runs
):

    durations = {}

    for run in runs:

        duration_ms = (
            calculate_duration_ms(
                run.get("runStart"),
                run.get("runEnd")
            )
        )

        # Ignore jobs that are still running.
        if duration_ms is None:
            continue

        average_group = (
            get_average_group(run)
        )

        if average_group is None:
            continue

        durations.setdefault(
            average_group,
            []
        ).append(
            duration_ms
        )

    averages = {}

    for group, values in durations.items():

        averages[group] = (
            sum(values)
            / len(values)
        )

    return averages


# ============================================================
# SORT RESULTS
# ============================================================

def sort_runs(
    runs
):

    def sort_key(run):

        pipeline = run.get(
            "pipelineName",
            ""
        )

        pipeline_order = (
            PIPELINE_ORDER.get(
                pipeline,
                999
            )
        )

        job_order = 0

        if pipeline == "PL_Captura_Snapshots":

            job_type = (
                get_captura_job_type(
                    run.get("runStart")
                )
            )

            # 1:30 PM first
            if (
                job_type ==
                "PL_Captura_Snapshots_1_30_PM"
            ):

                job_order = 0

            # 11:30 PM second
            elif (
                job_type ==
                "PL_Captura_Snapshots_11_30_PM"
            ):

                job_order = 1

            else:

                job_order = 2

        run_start = parse_datetime(
            run.get("runStart")
        )

        if run_start:

            timestamp = (
                -run_start.timestamp()
            )

        else:

            timestamp = float("inf")

        return (
            pipeline_order,
            job_order,
            timestamp
        )

    return sorted(
        runs,
        key=sort_key
    )


# ============================================================
# GET TRIGGERED BY
# ============================================================

def get_triggered_by(
    run
):

    invoked_by = (
        run.get(
            "invokedBy"
        )
        or {}
    )

    return invoked_by.get(
        "name",
        "-"
    )


# ============================================================
# BUILD REPORT
# ============================================================

def build_report(
    runs,
    averages
):

    rows = []

    # Track first row of every
    # average group.
    average_already_displayed = set()

    for run in runs:

        pipeline = run.get(
            "pipelineName",
            "-"
        )

        run_start = run.get(
            "runStart"
        )

        run_end = run.get(
            "runEnd"
        )

        duration_ms = (
            calculate_duration_ms(
                run_start,
                run_end
            )
        )

        average_group = (
            get_average_group(run)
        )

        average_duration = ""

        # Show average only once
        # for each job group.
        if (
            average_group is not None
            and average_group
            not in average_already_displayed
        ):

            average_duration = (
                format_duration(
                    averages.get(
                        average_group
                    )
                )
            )

            average_already_displayed.add(
                average_group
            )

        rows.append({

            "Pipeline name":
                pipeline,

            "Run start":
                format_datetime(
                    run_start
                ),

            "Run end":
                format_datetime(
                    run_end
                ),

            "Duration":
                format_duration(
                    duration_ms
                ),

            "Triggered by":
                get_triggered_by(
                    run
                ),

            "Status":
                run.get(
                    "status",
                    "-"
                ),

            "Run ID":
                run.get(
                    "runId",
                    "-"
                ),

            "Avg of duration":
                average_duration
        })

    return rows


# ============================================================
# PRINT TABLE TO GITHUB LOG
# ============================================================

def print_table(
    rows
):

    print()
    print("=" * 160)
    print("SYNAPSE JOB MONITORING RESULTS")
    print("=" * 160)

    if not rows:

        print(
            "No monitored pipeline runs found."
        )

        return

    headers = [
        "Pipeline name",
        "Run start",
        "Run end",
        "Duration",
        "Triggered by",
        "Status",
        "Run ID",
        "Avg of duration"
    ]

    # Determine column widths.
    widths = {}

    for header in headers:

        widths[header] = len(
            header
        )

    for row in rows:

        for header in headers:

            widths[header] = max(
                widths[header],
                len(
                    str(
                        row.get(
                            header,
                            ""
                        )
                    )
                )
            )

    # Header
    header_line = " | ".join(
        header.ljust(
            widths[header]
        )
        for header in headers
    )

    separator = "-+-".join(
        "-" * widths[header]
        for header in headers
    )

    print(header_line)
    print(separator)

    for row in rows:

        print(
            " | ".join(
                str(
                    row.get(
                        header,
                        ""
                    )
                ).ljust(
                    widths[header]
                )
                for header in headers
            )
        )

    print()
    print(
        f"Total rows: {len(rows)}"
    )


# ============================================================
# CREATE CSV
# ============================================================

def create_csv(
    rows
):

    headers = [

        "Pipeline name",

        "Run start",

        "Run end",

        "Duration",

        "Triggered by",

        "Status",

        "Run ID",

        "Avg of duration"
    ]

    with open(
        CSV_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=headers
        )

        writer.writeheader()

        writer.writerows(
            rows
        )

    print()
    print(
        f"CSV generated: {CSV_FILE}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 80)
    print("Starting JobMonitoring")
    print("=" * 80)

    print(
        "Connecting to Azure Synapse..."
    )

    all_runs = get_pipeline_runs()

    print(
        f"Total runs received: "
        f"{len(all_runs)}"
    )

    monitored_runs = (
        get_monitored_runs(
            all_runs
        )
    )

    print(
        f"Runs in last 5 business days: "
        f"{len(monitored_runs)}"
    )

    monitored_runs = sort_runs(
        monitored_runs
    )

    averages = (
        calculate_average_durations(
            monitored_runs
        )
    )

    rows = build_report(
        monitored_runs,
        averages
    )

    print_table(
        rows
    )

    create_csv(
        rows
    )

    print()
    print(
        "JobMonitoring completed successfully."
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
