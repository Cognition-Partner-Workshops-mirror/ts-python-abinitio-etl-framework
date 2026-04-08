"""Pre-deployment validation checks for Ab Initio graph deployments."""
import logging
import os
from typing import List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)


class ChangeValidator:
    """Runs pre-deployment checks before pushing Ab Initio graphs to production."""

    def validate(self, graphs: List[str], target_env: str, pset_manager=None) -> Dict[str, Any]:
        """
        Run all pre-deployment validations.

        Returns:
            Dict with 'approved', 'checks', 'blocking_issues'.
        """
        checks = []

        for graph_path in graphs:
            checks.extend(self._check_graph(graph_path, target_env))

        blocking = [c for c in checks if not c["passed"] and c.get("blocking", True)]
        warnings = [c for c in checks if not c["passed"] and not c.get("blocking", True)]

        approved = len(blocking) == 0
        return {
            "approved": approved,
            "target_env": target_env,
            "graphs_checked": len(graphs),
            "checks_run": len(checks),
            "blocking_issues": len(blocking),
            "warnings": len(warnings),
            "details": checks,
        }

    def _check_graph(self, graph_path: str, target_env: str) -> List[Dict]:
        checks = []

        # 1. File existence
        exists = Path(graph_path).exists() if os.path.isabs(graph_path) else True
        checks.append({
            "check": "graph_file_exists",
            "graph": graph_path,
            "passed": exists,
            "blocking": True,
            "message": "Graph file found." if exists else f"Graph file not found: {graph_path}",
        })

        # 2. No dev/test paths in prod deployments
        if target_env == "prod":
            dev_patterns = ["/dev/", "/test/", "/uat/", "DEV.", "TEST.", "UAT."]
            has_dev_paths = any(p in graph_path for p in dev_patterns)
            checks.append({
                "check": "no_dev_paths_in_prod",
                "graph": graph_path,
                "passed": not has_dev_paths,
                "blocking": True,
                "message": "No dev paths detected." if not has_dev_paths else f"Dev path detected in prod graph: {graph_path}",
            })

        # 3. Graph naming convention
        graph_name = Path(graph_path).name
        follows_convention = graph_name.endswith(".mp") or graph_name.endswith(".mf")
        checks.append({
            "check": "naming_convention",
            "graph": graph_path,
            "passed": follows_convention,
            "blocking": False,
            "message": "Follows naming convention." if follows_convention else f"Graph '{graph_name}' should end with .mp or .mf",
        })

        return checks
