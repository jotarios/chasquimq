import chasquimq


def test_version_is_non_empty_string() -> None:
    value = chasquimq.version()
    assert isinstance(value, str)
    assert value
