"""Daily quality gate for bounded StreamPulse maintenance work.

The always-on Spark streams run independently. Airflow is used here for a
scheduled control-plane task: verify the latest materialized run and publish
an auditable quality snapshot.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

SUMMARY_PATH = os.getenv("STREAMPULSE_SUMMARY_PATH", "/opt/streampulse/output/run_summary.json")
MIN_PASS_RATE = float(os.getenv("STREAMPULSE_MIN_PASS_RATE", "0.98"))


@dag(
    dag_id="streampulse_daily_quality_gate",
    description="Validate the latest playback analytics run summary.",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["streaming", "quality", "analytics"],
)
def streaming_analytics_quality_gate():
    @task
    def load_summary(path: str) -> dict:
        summary_path = Path(path)
        if not summary_path.exists():
            raise AirflowException(f"Missing run summary: {summary_path}")
        return json.loads(summary_path.read_text(encoding="utf-8"))

    @task
    def enforce_contract(summary: dict, minimum_pass_rate: float) -> dict:
        required = {
            "run_id",
            "input_records",
            "valid_records",
            "quarantined_records",
            "quality_pass_rate",
        }
        missing = required.difference(summary)
        if missing:
            raise AirflowException(f"Summary contract missing keys: {sorted(missing)}")
        if summary["quality_pass_rate"] < minimum_pass_rate:
            raise AirflowException(
                f"Quality pass rate {summary['quality_pass_rate']:.2%} is below "
                f"the {minimum_pass_rate:.2%} threshold"
            )
        return summary

    @task
    def publish_audit_record(summary: dict) -> None:
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "run_id": summary["run_id"],
                    "quality_pass_rate": summary["quality_pass_rate"],
                    "checked_at": datetime.utcnow().isoformat() + "Z",
                },
                sort_keys=True,
            )
        )

    publish_audit_record(enforce_contract(load_summary(SUMMARY_PATH), MIN_PASS_RATE))


streaming_analytics_quality_gate()

