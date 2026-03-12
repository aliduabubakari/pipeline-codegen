from __future__ import annotations

from pathlib import Path

from pipeline_codegen.kb_service.settings import ServiceSettings


def test_service_settings_default_paths_are_persistent(monkeypatch, tmp_path: Path) -> None:
    home = tmp_path / "home"
    home.mkdir()
    tmp_runtime = tmp_path / "tmp-runtime"
    tmp_runtime.mkdir()

    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("TMPDIR", str(tmp_runtime))
    monkeypatch.delenv("USERPROFILE", raising=False)
    monkeypatch.delenv("PIPELINE_CODEGEN_DATA_DIR", raising=False)
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.delenv("KB_OBJECT_STORE_DIR", raising=False)
    monkeypatch.delenv("KB_SQLITE_PATH", raising=False)

    settings = ServiceSettings.from_env()

    assert settings.object_store_dir.parts[-2:] == ("kb_service", "object_store")
    assert settings.sqlite_path.parent.name == "kb_service"
    assert settings.sqlite_path.name == "metadata.db"
    legacy_tmp_path = (tmp_runtime / "pipeline_codegen_kb" / "object_store").resolve()
    assert settings.object_store_dir != legacy_tmp_path


def test_service_settings_allows_persistent_root_override(monkeypatch, tmp_path: Path) -> None:
    root = tmp_path / "persistent-root"
    monkeypatch.setenv("PIPELINE_CODEGEN_DATA_DIR", str(root))
    monkeypatch.delenv("KB_OBJECT_STORE_DIR", raising=False)
    monkeypatch.delenv("KB_SQLITE_PATH", raising=False)

    settings = ServiceSettings.from_env()

    assert settings.object_store_dir == (root / "kb_service" / "object_store").resolve()
    assert settings.sqlite_path == (root / "kb_service" / "metadata.db").resolve()

