"""
PSET Manager — manages Ab Initio PSETs for environment-aware job parameterisation.
Allows changing job behaviour (source paths, targets, credentials) without graph code changes.
"""
import os
import re
import logging
from typing import Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class PSETManager:
    """
    Manages Ab Initio PSET (Parameter SET) files.
    PSETs define runtime parameters: paths, credentials, partition counts, etc.
    """

    def __init__(self, pset_dir: str = "psets/pset_templates"):
        self.pset_dir = Path(pset_dir)

    def load_pset(self, pset_name: str, environment: str = "dev") -> Dict[str, str]:
        """
        Load a PSET file for a given environment and return as a dict.

        Searches for: {pset_dir}/{environment}/{pset_name}.pset
        Falls back to:  {pset_dir}/{pset_name}.pset
        """
        env_path = self.pset_dir / environment / f"{pset_name}.pset"
        base_path = self.pset_dir / f"{pset_name}.pset"

        if env_path.exists():
            pset_file = env_path
        elif base_path.exists():
            pset_file = base_path
        else:
            raise FileNotFoundError(
                f"PSET '{pset_name}' not found for environment '{environment}'. "
                f"Searched: {env_path}, {base_path}"
            )

        return self._parse_pset(pset_file, environment)

    def _parse_pset(self, pset_file: Path, environment: str) -> Dict[str, str]:
        """Parse Ab Initio PSET file format into a Python dict."""
        params = {}
        with open(pset_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("//"):
                    continue
                # Handle: PARAM_NAME=value or define PARAM_NAME value
                if line.startswith("define "):
                    parts = line[7:].split(None, 1)
                    if len(parts) == 2:
                        params[parts[0]] = parts[1]
                elif "=" in line:
                    key, _, value = line.partition("=")
                    params[key.strip()] = value.strip()

        # Resolve environment variables
        for key, value in params.items():
            params[key] = os.path.expandvars(value)

        # Inject environment marker
        params["_environment"] = environment
        params["_pset_file"] = str(pset_file)

        logger.info(f"Loaded PSET '{pset_file.stem}' for env='{environment}': {len(params)} params")
        return params

    def render_pset(self, template_params: Dict[str, str], overrides: Dict[str, str] = None) -> str:
        """Render a PSET file string from params dict (for dynamic generation)."""
        all_params = {**template_params, **(overrides or {})}
        lines = ["# Auto-generated PSET", ""]
        for key, value in all_params.items():
            if not key.startswith("_"):
                lines.append(f"define {key} {value}")
        return "\n".join(lines)

    def write_pset(self, pset_name: str, params: Dict[str, str], environment: str = "dev") -> Path:
        """Write a rendered PSET file for a given environment."""
        env_dir = self.pset_dir / environment
        env_dir.mkdir(parents=True, exist_ok=True)
        output_path = env_dir / f"{pset_name}.pset"
        content = self.render_pset(params)
        output_path.write_text(content)
        logger.info(f"Written PSET: {output_path}")
        return output_path

    def diff_psets(self, pset_a: Dict[str, str], pset_b: Dict[str, str]) -> Dict[str, Any]:
        """Compare two PSET dicts and return differences."""
        keys_a = set(k for k in pset_a if not k.startswith("_"))
        keys_b = set(k for k in pset_b if not k.startswith("_"))
        only_in_a = keys_a - keys_b
        only_in_b = keys_b - keys_a
        changed = {k: {"a": pset_a[k], "b": pset_b[k]} for k in (keys_a & keys_b) if pset_a[k] != pset_b[k]}
        return {"only_in_a": list(only_in_a), "only_in_b": list(only_in_b), "changed": changed}
