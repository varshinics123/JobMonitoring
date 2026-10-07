import os
import requests
from datetime import datetime, timedelta, timezone
from azure.identity import ClientSecretCredential


# ============================================================
# CONFIGURATION
# ============================================================

TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

SYNAPSE_ENDPOINT = os.environ["SYNAPSE_ENDPOINT"].rstrip("/")

API_VERSION = "2020-12-01"

MONITORED_PIPELINES = [
    "PL_Captura_Snapshots",
    "BronzeToBI_daily_530_am",
    "BronzeToBI_daily_530_am_1",
    "BronzeToBI_daily_5am",
]


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
        "https://dev.azuresynapse.net/.default"
    )

    return token.token


# ============================================================
# GET PIPELINE RUNS
# ============================================================

def get_pipeline_runs():

    access_token = get_access_token()

    url = (
        f"{SYNAPSE_ENDPOINT}"
        f"/queryPipelineRuns?api-version={API_VERSION}"
    )

    now = datetime.now(timezone.utc)

    # Temporary lookback.
    # We will make the date range configurable in a later step.
    start_time = now - timedelta(days=7)

    payload = {
        "lastUpdatedAfter": start_time.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "lastUpdatedBefore": now.strftime(
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
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    all_runs = []
    continuation_token = None

    while True:

        if continuation_token:
            payload["continuationToken"] = continuation_token

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=60
        )

        response.raise_for_status()

        data = response.json()

        runs = data.get("value", [])

        all_runs.extend(runs)

        continuation_token = data.get(
            "continuationToken"
        )

        if not continuation_token:
            break

    return all_runs


# ============================================================
# FILTER MONITORED PIPELINES
# ============================================================

def get_monitored_runs(runs):

    monitored_runs = []

    for run in runs:

        pipeline_name = run.get("pipelineName", "")

        if pipeline_name.lower() in [
            name.lower()
            for name in MONITORED_PIPELINES
        ]:
            monitored_runs.append(run)

    return monitored_runs


# ============================================================
# DISPLAY RESULTS
# ============================================================

def display_results(runs):

    print()
    print("=" * 70)
    print("SYNAPSE JOB MONITORING")
    print("=" * 70)

    print(f"Total runs received : {len(runs)}")

    print()

    if not runs:
        print("No monitored pipeline runs found.")
        return

    for run in runs:

        print("-" * 70)

        print(
            f"Pipeline : "
            f"{run.get('pipelineName')}"
        )

        print(
            f"Run ID   : "
            f"{run.get('runId')}"
        )

        print(
            f"Status   : "
            f"{run.get('status')}"
        )

        print(
            f"Start    : "
            f"{run.get('runStart')}"
        )

        print(
            f"End      : "
            f"{run.get('runEnd')}"
        )

        if run.get("message"):
            print(
                f"Message  : "
                f"{run.get('message')}"
            )

    print("-" * 70)


# ============================================================
# MAIN
# ============================================================

def main():

    print()
    print("Starting JobMonitoring...")
    print("Connecting to Azure Synapse...")

    runs = get_pipeline_runs()

    monitored_runs = get_monitored_runs(runs)

    display_results(monitored_runs)

    print()
    print("Monitoring completed.")


# ============================================================
# PROGRAM START
# ============================================================

if __name__ == "__main__":
    main()
