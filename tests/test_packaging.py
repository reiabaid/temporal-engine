from pathlib import Path

from temporal_engine.mcp_server import default_db_path, main
from temporal_engine.runtime import Runtime


def test_the_database_location_is_explicit_or_a_fixed_per_user_path_never_the_working_directory():
    assert default_db_path({"TEMPORAL_ENGINE_DB": "/data/x.db"}) == "/data/x.db"
    default = Path(default_db_path({}))
    assert default.is_absolute()                          # independent of whichever folder a client starts us in
    assert default.parent.name == ".temporal-engine" and default.parent.parent == Path.home()


def test_an_empty_setting_falls_back_to_the_default_rather_than_meaning_the_current_directory():
    assert Path(default_db_path({"TEMPORAL_ENGINE_DB": ""})).is_absolute()


def test_the_database_folder_is_created_when_missing(tmp_path):
    runtime = Runtime(tmp_path / "does" / "not" / "exist" / "t.db")
    try:
        assert (tmp_path / "does" / "not" / "exist" / "t.db").exists()
    finally:
        runtime.close()


def test_there_is_a_console_script_entry_point():
    assert callable(main)
