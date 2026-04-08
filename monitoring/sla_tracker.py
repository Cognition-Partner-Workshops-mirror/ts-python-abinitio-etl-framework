"""SLA tracker — tracks job SLAs and generates daily reports."""
import logging
from datetime import datetime, time, timedelta
from typing import Dict, List, Any
import json
from pathlib import Path

logger = logging.getLogger(__name__)


class SLATracker:
    """Tracks SLA compliance across all monitored Ab Initio jobs."""

    def __init__(self, state_file: str = "/tmp/abinitio_sla_state.json"):
        self.state_file = Path(state_file)
        self._state: Dict[str, Any] = self._load_state()

    def _load_state(self) -> Dict:
        if self.state_file.exists():
            try:
                return json.loads(self.state_file.read_text())
            except Exception:
                return {"jobs": {}}
        return {"jobs": {}}

    def _save_state(self):
        self.state_file.write_text(json.dumps(self._state, indent=2, default=str))

    def register_job(
        self,
        job_name: str,
        sla_window_end: str,  # e.g. "06:00" = must complete by 6am UTC
        criticality: str = "high",
    ) -> None:
        """Register a job with its SLA definition."""
        self._state["jobs"][job_name] = {
            "sla_window_end": sla_window_end,
            "criticality": criticality,
            "run_history": [],
        }
        self._save_state()

    def record_completion(
        self,
        job_name: str,
        completed_at: datetime,
        status: str,
        duration_minutes: float,
    ) -> Dict[str, Any]:
        """Record a job run completion and evaluate SLA compliance."""
        job_config = self._state["jobs"].get(job_name)
        if not job_config:
            logger.warning(f"Job '{job_name}' not registered in SLA tracker.")
            return {}

        sla_time = datetime.strptime(job_config["sla_window_end"], "%H:%M").time()
        today_sla = datetime.combine(completed_at.date(), sla_time)
        sla_met = completed_at <= today_sla and status in ("SUCCESS", "COMPLETED")

        run_record = {
            "date": completed_at.date().isoformat(),
            "completed_at": completed_at.isoformat(),
            "status": status,
            "duration_minutes": round(duration_minutes, 1),
            "sla_met": sla_met,
        }

        if "run_history" not in job_config:
            job_config["run_history"] = []
        job_config["run_history"].append(run_record)
        job_config["run_history"] = job_config["run_history"][-90:]  # Keep 90 days
        self._save_state()

        if not sla_met:
            logger.warning(f"SLA MISS: {job_name} completed at {completed_at} vs SLA {today_sla}")

        return run_record

    def generate_report(self, days: int = 7) -> Dict[str, Any]:
        """Generate a daily SLA compliance report for all registered jobs."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
        report = {"generated_at": datetime.utcnow().isoformat(), "period_days": days, "jobs": {}}

        for job_name, config in self._state["jobs"].items():
            recent = [r for r in config.get("run_history", []) if r.get("date", "") >= cutoff]
            if not recent:
                continue
            sla_met_count = sum(1 for r in recent if r.get("sla_met"))
            report["jobs"][job_name] = {
                "total_runs": len(recent),
                "sla_met": sla_met_count,
                "sla_compliance_pct": round(sla_met_count / len(recent) * 100, 1) if recent else 0,
                "avg_duration_min": round(sum(r.get("duration_minutes", 0) for r in recent) / len(recent), 1),
                "criticality": config.get("criticality", "unknown"),
            }

        return report
