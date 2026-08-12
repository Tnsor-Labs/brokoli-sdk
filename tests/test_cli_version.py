"""brokoli-sdk#50: `brokoli --version` / `-V`."""

import pytest

import brokoli
import brokoli.cli as cli


@pytest.mark.parametrize("flag", ["--version", "-V"])
def test_version_flag_prints_and_exits_zero(flag, capsys, monkeypatch):
    monkeypatch.setattr("sys.argv", ["brokoli", flag])
    with pytest.raises(SystemExit) as exc_info:
        cli.main()
    assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip() == f"brokoli {brokoli.__version__}"
