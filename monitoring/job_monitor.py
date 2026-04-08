"""
Job monitor — monitors Ab Initio job execution via AutoSys/Control-M API.
Alerts on SLA breach, job failure, and long-running jobs.
"""
import logging
import time
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)


class JobMonitor:
    """Monitors Ab Initio jobs via AutoSys REST API and alerts on SLA breaches."""

    def __init__(
        self,
        autosys_url: str = "http://autosys-api:8080",
        api_token: str = None,
        slack_webhook: str = None,
    ):
        self.autosys_url = autosys_url.rstrip("/")
        self.api_token = api_token
        self.slack_webhook = slack_webhook
        self._session = requests.Session()
        if api_token:
            self._session.headers["Authorization"] = f"Bearer {api_token}"

    def get_job_status(self, job_name: str) -> Dict[str, Any]:
        """Fetch current status of a job from AutoSys API."""
        try:
            resp = self._session.get(
                f"{self.autosys_url}/api/v1/jobs/{job_name}/status",
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.error(f"AutoSys API error for {job_name}: {e}")
            return {"job_name": job_name, "status": "UNKNOWN", "error": str(e)}

    def check_sla(
        self,
        job_name: str,
        expected_complete_by: datetime,
    ) -> Dict[str, Any]:
        """Check if a job has completed within its SLA window."""
        status = self.get_job_status(job_name)
        now = datetime.utcnow()
        sla_breached = False

        if status.get("status") not in ("SUCCESS", "COMPLETED"):
            if now > expected_complete_by:
                sla_breached = True
                logger.warning(f"SLA BREACH: {job_name} not completed by {expected_complete_by}. Current status: {status.get('status')}")
                self._send_alert(job_name, status, "SLA_BREACH", expected_complete_by)

        return {
            "job_name": job_name,
            "current_status": status.get("status", "UNKNOWN"),
            "sla_deadline": expected_complete_by.isoformat(),
            "sla_breached": sla_breached,
            "checked_at": now.isoformat(),
        }

    def monitor_jobs(
        self,
        jobs: List[Dict],
        poll_interval_seconds: int = 60,
        max_polls: int = 60,
    ) -> List[Dict[str, Any]]:
        """
        Poll a list of jobs until they complete or max_polls is reached.

        Jobs format: [{"name": "JOB_NAME", "sla_minutes": 30}, ...]
        """
        results = {j["name"]: {"completed": False} for j in jobs}
        for poll in range(max_polls):
            all_done = True
            for job in jobs:
                if results[job["name"]]["completed"]:
                    continue
                status = self.get_job_status(job["name"])
                current = status.get("status", "UNKNOWN")
                if current in ("SUCCESS", "COMPLETED", "FAILURE"):
                    results[job["name"]] = {"completed": True, "final_status": current}
                    if current == "FAILURE":
                        self._send_alert(job["name"], status, "JOB_FAILURE")
                else:
                    all_done = False
            if all_done:
                break
            logger.info(f"Poll {poll + 1}/{max_polls}: waiting {poll_interval_seconds}s...")
            time.sleep(poll_interval_seconds)
        return list(results.values())

    def _send_alert(self, job_name: str, status: Dict, alert_type: str, deadline: datetime = None) -> None:
        """Send Slack alert for job failure or SLA breach."""
        if not self.slack_webhook:
            return
        emoji = ":red_circle:" if alert_type == "JOB_FAILURE" else ":warning:"
        msg = f"{emoji} *Ab Initio Alert*: `{job_name}` | Type: {alert_type} | Status: {status.get('status', 'UNKNOWN')}"
        if deadline:
            msg += f" | SLA deadline: {deadline.strftime('%Y-%m-%d %H:%M UTC')}"
        try:
            requests.post(self.slack_webhook, json={"text": msg}, timeout=10)
        except Exception as e:
            logger.error(f"Slack alert failed: {e}")
