from quant_ex.models.trainer import _sanitize_artifact_tag


def test_sanitize_artifact_tag_removes_path_and_space_characters():
    assert _sanitize_artifact_tag(" sector/full run ") == "sector_full_run"


def test_sanitize_artifact_tag_returns_none_for_empty_result():
    assert _sanitize_artifact_tag("///") is None