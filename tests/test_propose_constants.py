"""brokoli propose: a value built from a constant is changed where the
constant is defined."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("libcst")

import brokoli.cli as cli  # noqa: E402
from brokoli.cli import load_pipeline_from_file  # noqa: E402
from brokoli.propose import Change, propose  # noqa: E402

FOLDER = "workspaces/shop/pipelines/products"
OTHER = "workspaces/shop/pipelines/carts"

CONFIG = """version: 1
workspaces:
  shop: { path: workspaces/shop, id: ws-shop }
"""

ENDPOINTS = '''"""Where the shop's data comes from."""

# The shop's public API.
SHOP_API = "https://dummyjson.com/v1"
TIMEOUT = 20
'''

PRODUCTS = """from brokoli import Pipeline, source_api

from shared.endpoints import SHOP_API

with Pipeline("Products", pipeline_id="products", schedule="0 * * * *") as p:
    products = source_api(
        "Fetch products",
        url=f"{SHOP_API}/products",
        records="products",
    )
"""

CARTS = """from brokoli import Pipeline, source_api

from shared.endpoints import SHOP_API

with Pipeline("Carts", pipeline_id="carts") as p:
    source_api("Fetch carts", url=SHOP_API + "/carts", records="carts")
"""

FETCH = "fetch_products_1"


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "brokoli.yaml").write_text(CONFIG)
    (tmp_path / "shared").mkdir()
    (tmp_path / "shared" / "__init__.py").write_text("")
    (tmp_path / "shared" / "endpoints.py").write_text(ENDPOINTS)
    for folder, text in ((FOLDER, PRODUCTS), (OTHER, CARTS)):
        (tmp_path / folder).mkdir(parents=True)
        (tmp_path / folder / "pipeline.py").write_text(text)
    return tmp_path


def url_of(root: Path, folder: str, files: dict[str, str]) -> str:
    """The URL the pipeline in *folder* compiles to with *files* written."""
    before = {name: (root / name).read_text() for name in files}
    for name, text in files.items():
        (root / name).write_text(text)
    sys.path.insert(0, str(root))
    try:
        for name in [m for m in sys.modules if m == "shared" or m.startswith("shared.")]:
            del sys.modules[name]
        ir = load_pipeline_from_file(str(root / folder / "pipeline.py"))[0].to_json()
    finally:
        sys.path.remove(str(root))
        for name, text in before.items():
            (root / name).write_text(text)
    return ir["nodes"][0]["config"]["url"]


def test_the_constant_is_set_where_it_is_defined(root: Path) -> None:
    change = Change("node", "config.url", "https://dummyjson.com/products", node_id=FETCH)
    p = propose(root, FOLDER, [change])
    assert p.skipped == [] and p.applied == [change]
    # pipeline.py is as it was; the constant is what changed.
    assert p.source == PRODUCTS
    assert p.others == {
        "shared/endpoints.py": (
            ENDPOINTS,
            ENDPOINTS.replace('"https://dummyjson.com/v1"', '"https://dummyjson.com"'),
        )
    }
    assert change.constant == {
        "path": "shared/endpoints.py",
        "constant": "SHOP_API",
        "value": "https://dummyjson.com",
        "also_used_in": [f"{OTHER}/pipeline.py"],
    }
    path = f"{FOLDER}/pipeline.py"
    assert [name for name, _, _ in p.files(path)] == ["shared/endpoints.py"]
    out = p.to_dict(path)
    assert out["changed"] and [f["path"] for f in out["files"]] == ["shared/endpoints.py"]
    assert out["applied"][0]["constant"]["constant"] == "SHOP_API"
    assert out["diff"].startswith("--- a/shared/endpoints.py")
    # Nothing on disk changed, and the result compiles to what was asked.
    assert (root / "shared" / "endpoints.py").read_text() == ENDPOINTS
    assert (root / FOLDER / "pipeline.py").read_text() == PRODUCTS
    new = dict((name, text) for name, _, text in p.files(path))
    assert url_of(root, FOLDER, new) == "https://dummyjson.com/products"
    # The reader it reaches follows too: that is why it is reported.
    assert url_of(root, OTHER, new) == "https://dummyjson.com/carts"


def test_a_constant_in_pipeline_py_keeps_its_quotes(root: Path) -> None:
    (root / FOLDER / "pipeline.py").write_text(
        PRODUCTS.replace(
            "from shared.endpoints import SHOP_API\n", "BASE = 'https://shop.test/v1'\n"
        ).replace('f"{SHOP_API}/products"', 'BASE + "/products"')
    )
    change = Change("node", "config.url", "https://shop.test/v2/products", node_id=FETCH)
    p = propose(root, FOLDER, [change])
    assert p.applied == [change] and p.others == {}
    assert "BASE = 'https://shop.test/v2'\n" in p.source
    assert 'url=BASE + "/products",' in p.source
    assert change.constant == {
        "path": f"{FOLDER}/pipeline.py",
        "constant": "BASE",
        "value": "https://shop.test/v2",
    }


def test_a_pipeline_field_set_from_a_constant(root: Path) -> None:
    (root / FOLDER / "pipeline.py").write_text(
        PRODUCTS.replace('schedule="0 * * * *"', "schedule=HOURLY").replace(
            "with Pipeline", 'HOURLY = "0 * * * *"\n\nwith Pipeline'
        )
    )
    change = Change("pipeline", "schedule", "*/30 * * * *")
    p = propose(root, FOLDER, [change])
    assert p.applied == [change] and change.constant["constant"] == "HOURLY"
    assert 'HOURLY = "*/30 * * * *"' in p.source and "schedule=HOURLY" in p.source


def test_a_quote_in_the_new_value_is_escaped_as_the_constant_quotes(root: Path) -> None:
    (root / FOLDER / "pipeline.py").write_text(
        PRODUCTS.replace('schedule="0 * * * *"', "description=ABOUT").replace(
            "with Pipeline", "ABOUT = 'The catalog'\n\nwith Pipeline"
        )
    )
    change = Change("pipeline", "description", "The shop's catalog")
    p = propose(root, FOLDER, [change])
    assert p.applied == [change] and change.constant is not None
    assert "ABOUT = 'The shop\\'s catalog'\n" in p.source


def test_the_last_assignment_is_the_one_set(root: Path) -> None:
    # The module ends with the last value; setting the first would change
    # nothing the pipeline reads.
    (root / "shared" / "endpoints.py").write_text(
        'SHOP_API = "https://old.example.com"\n' + ENDPOINTS
    )
    change = Change("node", "config.url", "https://dummyjson.com/products", node_id=FETCH)
    p = propose(root, FOLDER, [change])
    assert change.constant is not None and p.source == PRODUCTS
    new = p.others["shared/endpoints.py"][1]
    assert 'SHOP_API = "https://old.example.com"\n' in new
    assert 'SHOP_API = "https://dummyjson.com"\n' in new


@pytest.mark.parametrize(
    "case,edit",
    [
        # The new value drops the expression's fixed text.
        ("other path", None),
        # The constant feeds another value of this pipeline too.
        (
            "shared in the pipeline",
            lambda t: t.replace(
                '        records="products",\n    )\n',
                '        records="products",\n    )\n'
                '    source_api("Fetch users", url=f"{SHOP_API}/users", records="users")\n',
            ),
        ),
        # Bound twice: not a constant.
        (
            "rebound",
            lambda t: t.replace(
                "from shared.endpoints import SHOP_API\n",
                'from shared.endpoints import SHOP_API\nSHOP_API = "https://dummyjson.com/v1"\n',
            ),
        ),
        # Read through a call: not text plus one constant.
        (
            "a call",
            lambda t: t.replace('f"{SHOP_API}/products"', 'SHOP_API.rstrip("/") + "/products"'),
        ),
        # Two constants: which one to set is a guess.
        (
            "two names",
            lambda t: t.replace("import SHOP_API", "import SHOP_API, TIMEOUT").replace(
                'f"{SHOP_API}/products"', 'f"{SHOP_API}/products/{TIMEOUT}"'
            ),
        ),
    ],
)
def test_otherwise_the_value_is_written_in_pipeline_py(root: Path, case: str, edit) -> None:
    if edit is not None:
        (root / FOLDER / "pipeline.py").write_text(edit(PRODUCTS))
    value = (
        "https://shop.example.com/items"
        if case == "other path"
        else "https://dummyjson.com/products"
    )
    change = Change("node", "config.url", value, node_id=FETCH)
    p = propose(root, FOLDER, [change])
    assert p.applied == [change] and change.constant is None
    assert p.others == {}
    assert f'url="{value}",' in p.source
    assert (root / "shared" / "endpoints.py").read_text() == ENDPOINTS


def test_a_constant_set_twice_reads_the_file_as_set(root: Path) -> None:
    # The second change compiles against the module the first one set; a
    # stale import would refuse it.
    first = Change("node", "config.url", "https://dummyjson.com/products", node_id=FETCH)
    second = Change("node", "config.url", "https://dummyjson.com/v2/products", node_id=FETCH)
    p = propose(root, FOLDER, [first, second])
    assert p.applied == [first, second] and p.source == PRODUCTS
    assert 'SHOP_API = "https://dummyjson.com/v2"' in p.others["shared/endpoints.py"][1]
    assert p.others["shared/endpoints.py"][0] == ENDPOINTS


def test_the_cli_writes_every_file(
    root: Path,
    tmp_path_factory: pytest.TempPathFactory,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    changes = tmp_path_factory.mktemp("in") / "changes.json"
    changes.write_text(
        json.dumps(
            [
                {
                    "target": "node",
                    "node_id": FETCH,
                    "field": "config.url",
                    "value": "https://dummyjson.com/products",
                }
            ]
        )
    )
    monkeypatch.chdir(root)
    monkeypatch.setattr(
        sys, "argv", ["brokoli", "propose", FOLDER, "--changes", str(changes), "--write"]
    )
    try:
        cli.main()
    except SystemExit as exc:
        assert not exc.code
    assert capsys.readouterr().out.startswith("--- a/shared/endpoints.py")
    assert 'SHOP_API = "https://dummyjson.com"' in (root / "shared" / "endpoints.py").read_text()
    assert (root / FOLDER / "pipeline.py").read_text() == PRODUCTS
