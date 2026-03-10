from __future__ import annotations

import pytest

from pipeline_codegen.api import map_to_target_ir
from helpers import load_opos_fixture


def test_mapping_error_for_unknown_profile() -> None:
    opos = load_opos_fixture("ok_sequential.opos.yaml")
    with pytest.raises(ValueError) as exc:
        map_to_target_ir(opos, target="airflow", target_version="9.9", config={"strict": True})
    assert "MAP001" in str(exc.value)


def test_mapping_error_for_unsupported_exec_type_in_strict() -> None:
    opos = load_opos_fixture("ok_sequential.opos.yaml")
    opos["components"][0]["executor"]["type"] = "r"  # unsupported in v1
    with pytest.raises(ValueError) as exc:
        map_to_target_ir(opos, target="airflow", target_version="2.8", config={"strict": True})
    assert "MAP005" in str(exc.value)
