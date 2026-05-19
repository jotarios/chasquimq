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


def test_apply_tls_true_preserves_cluster_scheme():
    # Regression: the old prefix-strip produced the malformed
    # "rediss://redis-cluster://..." here, silently breaking TLS Redis
    # Cluster. fred routes by scheme, so the -cluster suffix must survive.
    assert apply_tls("redis-cluster://h:6379", True) == "rediss-cluster://h:6379"
    assert apply_tls("redis-cluster://h:6379?node=h2:6380", True) == (
        "rediss-cluster://h:6379?node=h2:6380"
    )


def test_apply_tls_true_leaves_rediss_cluster_scheme():
    assert apply_tls("rediss-cluster://h:6379", True) == "rediss-cluster://h:6379"


def test_apply_tls_true_upgrades_valkey_schemes():
    assert apply_tls("valkey://h:6379", True) == "valkeys://h:6379"
    assert apply_tls("valkey-cluster://h:6379", True) == "valkeys-cluster://h:6379"
    assert apply_tls("valkeys://h:6379", True) == "valkeys://h:6379"
    assert apply_tls("valkeys-cluster://h:6379", True) == "valkeys-cluster://h:6379"


def test_apply_tls_false_passes_cluster_through():
    assert apply_tls("redis-cluster://h:6379", False) == "redis-cluster://h:6379"
    assert apply_tls("rediss-cluster://h:6379", False) == "rediss-cluster://h:6379"


def test_apply_tls_cluster_case_insensitive():
    assert apply_tls("REDIS-CLUSTER://h:6379", True) == "rediss-cluster://h:6379"
