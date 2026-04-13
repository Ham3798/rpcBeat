from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class DuneCliError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        returncode: int | None = None,
        stdout: str = "",
        stderr: str = "",
    ) -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


@dataclass(frozen=True)
class DuneCliDoctor:
    dune_installed: bool
    dune_path: str | None
    env_api_key_present: bool
    config_file_present: bool
    codex_skill_present: bool
    codex_skill_candidates: list[str]
    runnable: bool
    message: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "dune_installed": self.dune_installed,
            "dune_path": self.dune_path,
            "env_api_key_present": self.env_api_key_present,
            "config_file_present": self.config_file_present,
            "codex_skill_present": self.codex_skill_present,
            "codex_skill_candidates": self.codex_skill_candidates,
            "runnable": self.runnable,
            "message": self.message,
        }


class DuneCli:
    def __init__(
        self,
        *,
        binary: str = "dune",
        home: Path | None = None,
        env: dict[str, str] | None = None,
        runner=subprocess.run,
        which=shutil.which,
    ) -> None:
        self.binary = binary
        self.home = home or Path.home()
        self.env = env or os.environ.copy()
        self._runner = runner
        self._which = which

    def path(self) -> str | None:
        return self._which(self.binary)

    def is_installed(self) -> bool:
        return self.path() is not None

    def config_file(self) -> Path:
        return self.home / ".config" / "dune" / "config.yaml"

    def codex_skill_candidates(self) -> list[Path]:
        skills_dir = self.home / ".codex" / "skills"
        return [
            skills_dir / "dune",
            skills_dir / "duneanalytics",
            skills_dir / "duneanalytics" / "skills" / "skills" / "dune",
        ]

    def doctor(self) -> DuneCliDoctor:
        dune_path = self.path()
        installed = dune_path is not None
        env_api_key_present = bool(self.env.get("DUNE_API_KEY"))
        config_file_present = self.config_file().exists()
        skill_candidates = self.codex_skill_candidates()
        codex_skill_present = any(path.exists() for path in skill_candidates)
        runnable = installed and (env_api_key_present or config_file_present)
        if not installed:
            message = "Dune CLI is not installed. Run the Dune install script, then `dune auth`."
        elif not runnable:
            message = "Dune CLI is installed, but no API key config was found. Run `dune auth`."
        else:
            message = "Dune CLI appears ready for JSON smoke checks."
        return DuneCliDoctor(
            dune_installed=installed,
            dune_path=dune_path,
            env_api_key_present=env_api_key_present,
            config_file_present=config_file_present,
            codex_skill_present=codex_skill_present,
            codex_skill_candidates=[str(path) for path in skill_candidates],
            runnable=runnable,
            message=message,
        )

    def run_sql(self, sql: str) -> dict[str, Any]:
        if not self.is_installed():
            raise DuneCliError("Dune CLI is not installed.")
        command = [self.binary, "query", "run-sql", "--sql", sql, "-o", "json"]
        completed = self._runner(command, capture_output=True, text=True, env=self.env, check=False)
        if completed.returncode != 0:
            raise DuneCliError(
                "Dune CLI run-sql failed.",
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
        try:
            parsed = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise DuneCliError(
                "Dune CLI did not return valid JSON.",
                returncode=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            ) from exc
        if not isinstance(parsed, dict):
            raise DuneCliError("Dune CLI JSON output was not an object.", stdout=completed.stdout)
        return parsed

