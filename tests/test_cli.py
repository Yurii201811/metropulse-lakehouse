from click import unstyle
from typer.testing import CliRunner

from metropulse.cli import app


def test_run_command_exposes_replayable_snapshot_options() -> None:
    result = CliRunner().invoke(app, ["run", "--help"])

    assert result.exit_code == 0
    assert "--as-of" in unstyle(result.stdout)


def test_replay_command_requires_a_run_id() -> None:
    result = CliRunner().invoke(app, ["replay", "--help"])

    assert result.exit_code == 0
    assert "--run-id" in unstyle(result.stdout)
