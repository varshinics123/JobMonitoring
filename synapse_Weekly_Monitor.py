import os
import csv
import requests
import base64
import html

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from azure.identity import ClientSecretCredential


# ============================================================
# CONFIGURATION
# ============================================================

TENANT_ID = os.environ["AZURE_TENANT_ID"]

CLIENT_ID = os.environ["AZURE_CLIENT_ID"]

CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

SYNAPSE_ENDPOINT = (
    os.environ["SYNAPSE_ENDPOINT"]
    .rstrip("/")
)

API_VERSION = "2020-12-01"

SYNAPSE_SCOPE = (
    "https://dev.azuresynapse.net/.default"
)

GRAPH_SCOPE = (
    "https://graph.microsoft.com/.default"
)

LOCAL_TIMEZONE = ZoneInfo(
    "Asia/Kolkata"
)

CSV_FILE = (
    "synapse_pipeline_results.csv"
)


# ============================================================
# EMAIL CONFIGURATION
# ============================================================

# Actual Microsoft 365 UPN used by Microsoft Graph
EMAIL_SENDER = (
    "varshini.cs@usclarroit.onmicrosoft.com"
)

# Email recipients
EMAIL_RECIPIENTS = [

    "Varshini.cs@marlabs.com",

    "Varshini.cs@usclaro.com"
]


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
# PIPELINE DISPLAY ORDER
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
            value.replace(
                "Z",
                "+00:00"
            )
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
# CALCULATE DURATION
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

    if (
        start_dt is None
        or end_dt is None
    ):
        return None

    duration = (
        end_dt - start_dt
    )

    return (
        duration.total_seconds()
        * 1000
    )


# ============================================================
# FORMAT DURATION
# ============================================================

def format_duration(
    duration_ms
):

    if duration_ms is None:
        return "-"

    try:

        total_seconds = round(
            duration_ms / 1000
        )

    except Exception:

        return "-"

    hours = (
        total_seconds
        // 3600
    )

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
# IDENTIFY PL_CAPTURA JOB TYPE
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

        return (
            "PL_Captura_Snapshots_1_30_PM"
        )

    # 11:30 PM job

    if (
        dt.hour == 23
        and dt.minute == 30
    ):

        return (
            "PL_Captura_Snapshots_11_30_PM"
        )

    return None


# ============================================================
# GET AVERAGE GROUP
# ============================================================

def get_average_group(run):

    pipeline = run.get(
        "pipelineName"
    )

    # PL_Captura has two separate jobs

    if pipeline == (
        "PL_Captura_Snapshots"
    ):

        job_type = (
            get_captura_job_type(
                run.get("runStart")
            )
        )

        if job_type is None:
            return None

        return (
            pipeline,
            job_type
        )

    # Other pipelines have one group each

    return (
        pipeline,
        "single_job"
    )


# ============================================================
# GET PREVIOUS WEEK MONDAY-FRIDAY
# ============================================================

def get_previous_week_range():

    today = datetime.now(
        LOCAL_TIMEZONE
    ).date()

    # Current week's Monday

    current_week_monday = (
        today
        - timedelta(
            days=today.weekday()
        )
    )

    # Previous week's Monday

    previous_week_monday = (
        current_week_monday
        - timedelta(days=7)
    )

    # Previous week's Friday

    previous_week_friday = (
        previous_week_monday
        + timedelta(days=4)
    )

    return (
        previous_week_monday,
        previous_week_friday
    )


# ============================================================
# GET SYNAPSE PIPELINE RUNS
# ============================================================

def get_pipeline_runs(
    start_date,
    end_date
):

    access_token = (
        get_access_token()
    )

    url = (
        f"{SYNAPSE_ENDPOINT}"
        f"/queryPipelineRuns"
        f"?api-version={API_VERSION}"
    )

    # Previous Monday 00:00:00 IST

    start_ist = datetime.combine(
        start_date,
        datetime.min.time()
    ).replace(
        tzinfo=LOCAL_TIMEZONE
    )

    # Previous Friday 23:59:59.999999 IST

    end_ist = datetime.combine(
        end_date,
        datetime.max.time()
    ).replace(
        tzinfo=LOCAL_TIMEZONE
    )

    # Convert boundaries to UTC

    start_utc = (
        start_ist.astimezone(
            timezone.utc
        )
    )

    end_utc = (
        end_ist.astimezone(
            timezone.utc
        )
    )

    payload = {

        "lastUpdatedAfter":
            start_utc.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),

        "lastUpdatedBefore":
            end_utc.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),

        "orderBy": [

            {
                "orderBy":
                    "RunStart",

                "order":
                    "DESC"
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

    # Synapse pagination

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

def get_monitored_runs(
    all_runs,
    start_date,
    end_date
):

    monitored_names = {

        name.lower()

        for name
        in MONITORED_PIPELINES
    }

    filtered_runs = []

    for run in all_runs:

        pipeline_name = run.get(
            "pipelineName",
            ""
        )

        # Check pipeline name

        if (
            pipeline_name.lower()
            not in monitored_names
        ):

            continue

        run_start = run.get(
            "runStart"
        )

        run_start_ist = (
            convert_to_ist(
                run_start
            )
        )

        if run_start_ist is None:

            continue

        run_date = (
            run_start_ist.date()
        )

        # Keep only previous Monday-Friday

        if (
            run_date < start_date
            or
            run_date > end_date
        ):

            continue

        filtered_runs.append(
            run
        )

    return filtered_runs


# ============================================================
# CALCULATE WEEKLY AVERAGES
# ============================================================

def calculate_average_durations(
    runs
):

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

        # Ignore InProgress / incomplete jobs

        if duration_ms is None:

            continue

        average_group = (
            get_average_group(
                run
            )
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

    for group, values in (
        durations.items()
    ):

        if values:

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

        # PL_Captura ordering

        if pipeline == (
            "PL_Captura_Snapshots"
        ):

            job_type = (
                get_captura_job_type(
                    run.get(
                        "runStart"
                    )
                )
            )

            # 1:30 PM first

            if job_type == (
                "PL_Captura_Snapshots_1_30_PM"
            ):

                job_order = 0

            # 11:30 PM second

            elif job_type == (
                "PL_Captura_Snapshots_11_30_PM"
            ):

                job_order = 1

            else:

                job_order = 2

        run_start = parse_datetime(
            run.get(
                "runStart"
            )
        )

        if run_start:

            timestamp = (
                -run_start.timestamp()
            )

        else:

            timestamp = float(
                "inf"
            )

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
# BUILD REPORT ROWS
# ============================================================

def build_report(
    runs,
    averages
):

    rows = []

    # Average is displayed only on the
    # first row of each job group.

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
            get_average_group(
                run
            )
        )

        average_duration = ""

        # Display average only once per group

        if (
            average_group is not None
            and
            average_group
            not in average_already_displayed
        ):

            average_ms = averages.get(
                average_group
            )

            if average_ms is not None:

                average_duration = (
                    format_duration(
                        average_ms
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
# PRINT TABLE
# ============================================================

def print_table(
    rows
):

    print()
    print("=" * 180)

    print(
        "SYNAPSE JOB MONITORING RESULTS"
    )

    print("=" * 180)

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

    # Calculate column widths

    widths = {}

    for header in headers:

        widths[header] = len(
            header
        )

    for row in rows:

        for header in headers:

            value = str(
                row.get(
                    header,
                    ""
                )
            )

            widths[header] = max(

                widths[header],

                len(value)
            )

    # Header

    header_line = (
        " | ".join(

            header.ljust(
                widths[header]
            )

            for header in headers
        )
    )

    separator = (
        "-+-".join(

            "-" * widths[header]

            for header in headers
        )
    )

    print(
        header_line
    )

    print(
        separator
    )

    # Rows

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
        f"CSV generated: "
        f"{CSV_FILE}"
    )


# ============================================================
# SEND EMAIL REPORT
# ============================================================

def send_email_report(
    rows,
    start_date,
    end_date
):

    print()
    print(
        "Sending email report..."
    )

    # --------------------------------------------------------
    # Get Microsoft Graph access token
    # --------------------------------------------------------

    credential = ClientSecretCredential(

        tenant_id=TENANT_ID,

        client_id=CLIENT_ID,

        client_secret=CLIENT_SECRET
    )

    graph_token = credential.get_token(
        GRAPH_SCOPE
    ).token

    # --------------------------------------------------------
    # Microsoft Graph headers
    # --------------------------------------------------------

    headers = {

        "Authorization":
            f"Bearer {graph_token}",

        "Content-Type":
            "application/json"
    }

    # --------------------------------------------------------
    # Email subject
    # --------------------------------------------------------

    subject = (
        "Weekly Synapse Job Monitoring Report - "
        f"{start_date.strftime('%d/%m/%Y')} to "
        f"{end_date.strftime('%d/%m/%Y')}"
    )

    # --------------------------------------------------------
    # Table headers
    # --------------------------------------------------------

    table_headers = [

        "Pipeline name",

        "Run start",

        "Run end",

        "Duration",

        "Triggered by",

        "Status",

        "Run ID",

        "Avg of duration"
    ]

    # --------------------------------------------------------
    # Build HTML table
    # --------------------------------------------------------

    table_html = """
    <table border="1"
           cellpadding="6"
           cellspacing="0"
           style="
               border-collapse: collapse;
               font-family: Arial, sans-serif;
               font-size: 12px;
           ">

        <thead>

            <tr>
    """

    for header in table_headers:

        table_html += (
            '<th style="font-weight:bold;padding:6px;">'
            + html.escape(header)
            + "</th>"
        )

    table_html += """
            </tr>

        </thead>

        <tbody>
    """

    for row in rows:

        table_html += "<tr>"

        for header in table_headers:

            value = str(
                row.get(
                    header,
                    ""
                )
            )

            table_html += (
                '<td style="padding:6px;white-space:nowrap;">'
                + html.escape(value)
                + "</td>"
            )

        table_html += "</tr>"

    table_html += """
        </tbody>

    </table>
    """

    # --------------------------------------------------------
    # Email body
    # --------------------------------------------------------

    email_body = f"""
    <html>

    <body style="
        font-family: Arial, sans-serif;
    ">

        <p>Hello,</p>

        <p>
            Please find below the weekly Synapse Job Monitoring
            report.
        </p>

        <p>
            <b>Reporting Period:</b>
            {start_date.strftime('%d/%m/%Y')}
            to
            {end_date.strftime('%d/%m/%Y')}
        </p>

        {table_html}

        <br>

        <p>
            The CSV report is attached to this email.
        </p>

        <p>
            Regards,<br>
            Job Monitoring
        </p>

    </body>

    </html>
    """

    # --------------------------------------------------------
    # Read CSV attachment
    # --------------------------------------------------------

    with open(
        CSV_FILE,
        "rb"
    ) as file:

        attachment_content = (
            base64.b64encode(
                file.read()
            ).decode("utf-8")
        )

    # --------------------------------------------------------
    # Build recipients
    # --------------------------------------------------------

    recipients = []

    for email_address in EMAIL_RECIPIENTS:

        recipients.append({

            "emailAddress": {

                "address":
                    email_address
            }
        })

    # --------------------------------------------------------
    # Microsoft Graph email payload
    # --------------------------------------------------------

    payload = {

        "message": {

            "subject":
                subject,

            "body": {

                "contentType":
                    "HTML",

                "content":
                    email_body
            },

            "toRecipients":
                recipients,

            "attachments": [

                {

                    "@odata.type":
                        "#microsoft.graph.fileAttachment",

                    "name":
                        CSV_FILE,

                    "contentType":
                        "text/csv",

                    "contentBytes":
                        attachment_content
                }
            ]
        },

        "saveToSentItems":
            True
    }

    # --------------------------------------------------------
    # Send email using Microsoft Graph
    # --------------------------------------------------------

    url = (
        "https://graph.microsoft.com/v1.0/"
        f"users/{EMAIL_SENDER}/sendMail"
    )

    response = requests.post(

        url,

        headers=headers,

        json=payload,

        timeout=60
    )

    # Print response details if email fails

    if not response.ok:

        print(
            "Email sending failed."
        )

        print(
            f"HTTP status: "
            f"{response.status_code}"
        )

        print(
            f"Response: "
            f"{response.text}"
        )

        response.raise_for_status()

    print()

    print(
        "Email sent successfully."
    )

    print(
        "Recipients:"
    )

    for email_address in EMAIL_RECIPIENTS:

        print(
            f"  - {email_address}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    print()

    print("=" * 80)

    print(
        "Starting JobMonitoring"
    )

    print("=" * 80)

    # --------------------------------------------------------
    # Determine previous week's Monday-Friday
    # --------------------------------------------------------

    start_date, end_date = (
        get_previous_week_range()
    )

    print()

    print(
        "Reporting period:"
    )

    print(

        f"{start_date.strftime('%d/%m/%Y')}"
        f" to "
        f"{end_date.strftime('%d/%m/%Y')}"
    )

    print()

    print(
        "Connecting to Azure Synapse..."
    )

    # --------------------------------------------------------
    # Fetch Synapse runs
    # --------------------------------------------------------

    all_runs = get_pipeline_runs(

        start_date,

        end_date
    )

    print()

    print(
        f"Total runs received: "
        f"{len(all_runs)}"
    )

    # --------------------------------------------------------
    # Filter monitored pipelines
    # --------------------------------------------------------

    monitored_runs = (
        get_monitored_runs(

            all_runs,

            start_date,

            end_date
        )
    )

    print(
        f"Monitored runs found: "
        f"{len(monitored_runs)}"
    )

    # --------------------------------------------------------
    # Sort results
    # --------------------------------------------------------

    monitored_runs = sort_runs(
        monitored_runs
    )

    # --------------------------------------------------------
    # Calculate previous week's averages
    # --------------------------------------------------------

    averages = (
        calculate_average_durations(

            monitored_runs
        )
    )

    # --------------------------------------------------------
    # Build final report
    # --------------------------------------------------------

    rows = build_report(

        monitored_runs,

        averages
    )

    # --------------------------------------------------------
    # Print table to GitHub log
    # --------------------------------------------------------

    print_table(
        rows
    )

    # --------------------------------------------------------
    # Create CSV
    # --------------------------------------------------------

    create_csv(
        rows
    )

    # --------------------------------------------------------
    # Send email
    # --------------------------------------------------------

    send_email_report(

        rows,

        start_date,

        end_date
    )

    print()

    print(
        "JobMonitoring completed successfully."
    )


# ============================================================
# PROGRAM START
# ============================================================

if __name__ == "__main__":

    main()
