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

SYNAPSE_SCOPE = (
    "https://dev.azuresynapse.net/.default"
)

LOCAL_TIMEZONE = ZoneInfo("Asia/Kolkata")

LOOKBACK_DAYS = 7

CSV_FILE = "synapse_pipeline_results.csv"


# ============================================================
# PIPELINES TO MONITOR
# ============================================================

MONITORED_PIPELINES = [
    "PL_Captura_Snapshots",
    "BronzeToBI_daily_530_am",
    "BronzeToBI_daily_530_am_1",
    "BronzeToBI_daily_5am",
]


# ============================================================
# PIPELINE ORDER
# ============================================================

PIPELINE_ORDER = {
    "PL_Captura_Snapshots": 0,
    "BronzeToBI_daily_530_am": 1,
    "BronzeToBI_daily_530_am_1": 2,
    "BronzeToBI_daily_5am": 3,
}


# ============================================================
# AZURE ACCESS TOKEN
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
# PARSE UTC DATETIME
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

    return dt.astimezone(LOCAL_TIMEZONE)


# ============================================================
# FORMAT DATETIME
# ============================================================

def format_datetime(value):

    dt = convert_to_ist(value)

    if dt is None:
        return "-"

    return dt.strftime(
        "%d/%m/%Y %I:%M:%S %p"
    )


# ============================================================
# GET PL_CAPTURA JOB TYPE
# ============================================================

def get_captura_job_type(run_start):

    dt = convert_to_ist(run_start)

    if dt is None:
        return None

    # 1:30 PM job
    if dt.hour == 13 and dt.minute == 30:
        return "PL_Captura_Snapshots_1_30_PM"

    # 11:30 PM job
    if dt.hour == 23 and dt.minute == 30:
        return "PL_Captura_Snapshots_11_30_PM"

    return None


# ============================================================
# CALCULATE DURATION IN MILLISECONDS
# ============================================================

def calculate_duration_ms(run_start, run_end):

    start_dt = parse_datetime(run_start)
    end_dt = parse_datetime(run_end)

    if start_dt is None or end_dt is None:
        return None

    duration = (
        end_dt - start_dt
    )

    return duration.total_seconds() * 1000


# ============================================================
# FORMAT DURATION
# ============================================================

def format_duration(duration_ms):

    if duration_ms is None:
        return "-"

    try:

        total_seconds = round(
            duration_ms / 1000
        )

    except Exception:

        return "-"

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
# GET SYNAPSE PIPELINE RUNS
# ============================================================

def get_pipeline_runs():

    access_token = get_access_token()

    url = (
        f"{SYNAPSE_ENDPOINT}"
        f"/queryPipelineRuns"
        f"?api-version={API_VERSION}"
    )

    now_utc = datetime.now(timezone.utc)

    query_start_utc = (
        now_utc -
        timedelta(days=LOOKBACK_DAYS)
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
# FILTER MONITORED RUNS
# ============================================================

def get_monitored_runs(runs):

    monitored_names = {
        name.lower()
        for name in MONITORED_PIPELINES
    }

    now_ist = datetime.now(
        LOCAL_TIMEZONE
    )

    start_ist = (
        now_ist -
        timedelta(days=LOOKBACK_DAYS)
    )

    filtered_runs = []

    for run in runs:

        pipeline_name = (
            run.get(
                "pipelineName",
                ""
            )
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

        # Keep only runs from the
        # selected 7-day period.
        if (
            run_start_ist < start_ist
            or run_start_ist > now_ist
        ):
            continue

        filtered_runs.append(
            run
        )

    return filtered_runs


# ============================================================
# GET AVERAGE KEY
# ============================================================

def get_average_key(run):

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
# CALCULATE 1-WEEK AVERAGES
# ============================================================

def calculate_average_durations(runs):

    durations = {}

    for run in runs:

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

        # Ignore in-progress runs
        # because they don't have an end time.
        if duration_ms is None:
            continue

        average_key = get_average_key(
            run
        )

        if average_key is None:
            continue

        durations.setdefault(
            average_key,
            []
        ).append(
            duration_ms
        )

    averages = {}

    for key, values in durations.items():

        if values:

            averages[key] = (
                sum(values)
                / len(values)
            )

    return averages


# ============================================================
# FORMAT AVERAGE
# ============================================================

def format_average_duration(
    duration_ms
):

    if duration_ms is None:
        return "-"

    return format_duration(
        duration_ms
    )


# ============================================================
# SORT RUNS
# ============================================================

def sort_runs(runs):

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
# BUILD REPORT ROWS
# ============================================================

def build_report_rows(
    runs,
    averages
):

    table_rows = []

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

        average_key = (
            get_average_key(run)
        )

        average_ms = None

        if average_key:

            average_ms = averages.get(
                average_key
            )

        triggered_by = (
            run.get(
                "invokedBy",
                {}
            ) or {}
        )

        triggered_by_name = (
            triggered_by.get(
                "name",
                "-"
            )
        )

        row = {

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
                triggered_by_name,

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
                format_average_duration(
                    average_ms
                )
        }

        table_rows.append(
            row
        )

    return table_rows


# ============================================================
# PRINT REPORT
# ============================================================

def print_report(
    rows
):

    print()
    print("=" * 120)
    print("SYNAPSE JOB MONITORING")
    print("=" * 120)

    print(
        f"Reporting period : "
        f"Last {LOOKBACK_DAYS} days"
    )

    print()

    if not rows:

        print(
            "No monitored pipeline runs found."
        )

        return

    for row in rows:

        print("-" * 120)

        print(
            f"Pipeline       : "
            f"{row['Pipeline name']}"
        )

        print(
            f"Run Start      : "
            f"{row['Run start']}"
        )

        print(
            f"Run End        : "
            f"{row['Run end']}"
        )

        print(
            f"Duration       : "
            f"{row['Duration']}"
        )

        print(
            f"Triggered By   : "
            f"{row['Triggered by']}"
        )

        print(
            f"Status         : "
            f"{row['Status']}"
        )

        print(
            f"Run ID         : "
            f"{row['Run ID']}"
        )

        print(
            f"1-Week Average : "
            f"{row['Avg of duration']}"
        )

    print("-" * 120)


# ============================================================
# CREATE CSV
# ============================================================

def create_csv(rows):

    columns = [

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
            fieldnames=columns
        )

        writer.writeheader()

        writer.writerows(
            rows
        )

    print()
    print(
        f"CSV generated successfully: "
        f"{CSV_FILE}"
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("=" * 70)
    print("Starting JobMonitoring")
    print("=" * 70)

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
        f"Monitored runs found: "
        f"{len(monitored_runs)}"
    )

    monitored_runs = sort_runs(
        monitored_runs
    )

    # Calculate average using
    # completed runs from the
    # 7-day period.
    averages = (
        calculate_average_durations(
            monitored_runs
        )
    )

    rows = build_report_rows(
        monitored_runs,
        averages
    )

    print_report(
        rows
    )

    create_csv(
        rows
    )

    print()
    print(
        "Monitoring completed successfully."
    )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
