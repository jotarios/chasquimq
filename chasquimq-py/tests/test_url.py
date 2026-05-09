import importlib.util
import pathlib

_SPEC = importlib.util.spec_from_file_location(
    "_chasquimq_url_under_test",
    pathlib.Path(__file__).resolve().parent.parent / "src" / "chasquimq" / "_url.py",
)
assert _SPEC is not None and _SPEC.loader is not None
_MOD = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MOD)
apply_tls = _MOD.apply_tls


def test_apply_tls_false_passes_through():
    assert apply_tls("redis://h:6379", False) == "redis://h:6379"
    assert apply_tls("rediss://h:6379", False) == "rediss://h:6379"
    assert apply_tls("h:6379", False) == "h:6379"


def test_apply_tls_true_upgrades_redis_scheme():
    assert apply_tls("redis://h:6379", True) == "rediss://h:6379"


def test_apply_tls_true_leaves_rediss_scheme():
    assert apply_tls("rediss://h:6379", True) == "rediss://h:6379"


def test_apply_tls_true_prepends_for_schemeless():
    assert apply_tls("my-cluster.cache.amazonaws.com:6379", True) == (
        "rediss://my-cluster.cache.amazonaws.com:6379"
    )


def test_apply_tls_case_insensitive():
    assert apply_tls("REDIS://h:6379", True) == "rediss://h:6379"
    assert apply_tls("REDISS://h:6379", True) == "REDISS://h:6379"
