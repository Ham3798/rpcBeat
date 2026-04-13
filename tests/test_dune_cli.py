from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from app.dune.cli import DuneCli, DuneCliError


def test_is_installed_uses_which(tmp_path: Path) -> None:
    missing = DuneCli(home=tmp_path, which=lambda _: None)
    installed = DuneCli(home=tmp_path, which=lambda _: "/usr/local/bin/dune")

    assert missing.is_installed() is False
    assert installed.is_installed() is True


def test_doctor_reports_missing_install(tmp_path: Path) -> None:
    cli = DuneCli(home=tmp_path, env={}, which=lambda _: None)

    report = cli.doctor()

    assert report.dune_installed is False
    assert report.runnable is False
    assert "not installed" in report.message


def test_doctor_reports_ready_with_env_key(tmp_path: Path) -> None:
    cli = DuneCli(
        home=tmp_path,
        env={"DUNE_API_KEY": "redacted"},
        which=lambda _: "/usr/local/bin/dune",
    )

    report = cli.doctor()

    assert report.dune_installed is True
    assert report.env_api_key_present is True
    assert report.runnable is True


def test_run_sql_parses_json(tmp_path: Path) -> None:
    def runner(command, capture_output, text, env, check):
        assert command == ["dune", "query", "run-sql", "--sql", "SELECT 1", "-o", "json"]
        assert capture_output is True
        assert text is True
        assert check is False
        return subprocess.CompletedProcess(command, 0, stdout=json.dumps({"rows": [{"ok": 1}]}))

    cli = DuneCli(home=tmp_path, runner=runner, which=lambda _: "/usr/local/bin/dune")

    assert cli.run_sql("SELECT 1") == {"rows": [{"ok": 1}]}


def test_run_sql_raises_on_nonzero(tmp_path: Path) -> None:
    def runner(command, capture_output, text, env, check):
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="auth failed")

    cli = DuneCli(home=tmp_path, runner=runner, which=lambda _: "/usr/local/bin/dune")

    with pytest.raises(DuneCliError) as exc:
        cli.run_sql("SELECT 1")

    assert exc.value.returncode == 1
    assert exc.value.stderr == "auth failed"

