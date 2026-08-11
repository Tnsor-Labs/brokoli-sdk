"""brokoli-sdk#15 M2: pipeline discovery is separated from run-time side
effects.

Discovering pipelines means importing the file, so the pipeline-defining
code at module scope necessarily runs. Two guarantees keep that from
running *arbitrary run-time* side effects:

  * the file is imported under its own module name, never ``__main__``, so
    the ``if __name__ == "__main__":`` deploy/run idiom does not fire;
  * ``BROKOLI_DISCOVERY`` is set during the import, so module-level code
    can guard expensive run-time-only setup and skip it.

The env var is restored afterward, so discovery leaves the process
environment untouched.
"""

import os

from brokoli.cli import DISCOVERY_ENV_VAR, load_pipeline_from_file


def _write(tmp_path, body):
    f = tmp_path / "pipe.py"
    f.write_text(body)
    return str(f)


def test_main_guard_does_not_run_during_discovery(tmp_path):
    marker = tmp_path / "main_ran.txt"
    path = _write(
        tmp_path,
        "from brokoli import Pipeline, source_api\n"
        "with Pipeline('p', pipeline_id='p') as p:\n"
        "    source_api('S', url='https://x')\n"
        "if __name__ == '__main__':\n"
        f"    open({str(marker)!r}, 'w').write('ran')\n",
    )
    pipelines = load_pipeline_from_file(path)
    assert [p.name for p in pipelines] == ["p"]
    # The deploy/run idiom must not have executed.
    assert not marker.exists()


def test_discovery_env_var_is_set_during_import(tmp_path):
    seen = tmp_path / "seen.txt"
    path = _write(
        tmp_path,
        "import os\n"
        "from brokoli import Pipeline, source_api\n"
        f"open({str(seen)!r}, 'w').write(os.getenv('BROKOLI_DISCOVERY', 'unset'))\n"
        "with Pipeline('p', pipeline_id='p') as p:\n"
        "    source_api('S', url='https://x')\n",
    )
    load_pipeline_from_file(path)
    assert seen.read_text() == "1"


def test_discovery_env_var_is_cleaned_up_afterward(tmp_path):
    assert DISCOVERY_ENV_VAR not in os.environ
    path = _write(
        tmp_path,
        "from brokoli import Pipeline, source_api\n"
        "with Pipeline('p', pipeline_id='p') as p:\n"
        "    source_api('S', url='https://x')\n",
    )
    load_pipeline_from_file(path)
    # Discovery leaves the environment exactly as it found it.
    assert DISCOVERY_ENV_VAR not in os.environ


def test_discovery_restores_a_preexisting_value(tmp_path, monkeypatch):
    monkeypatch.setenv(DISCOVERY_ENV_VAR, "outer")
    path = _write(
        tmp_path,
        "from brokoli import Pipeline, source_api\n"
        "with Pipeline('p', pipeline_id='p') as p:\n"
        "    source_api('S', url='https://x')\n",
    )
    load_pipeline_from_file(path)
    assert os.environ[DISCOVERY_ENV_VAR] == "outer"


def test_env_restored_even_when_import_raises(tmp_path):
    assert DISCOVERY_ENV_VAR not in os.environ
    path = _write(tmp_path, "raise RuntimeError('boom')\n")
    try:
        load_pipeline_from_file(path)
    except RuntimeError:
        pass
    assert DISCOVERY_ENV_VAR not in os.environ
