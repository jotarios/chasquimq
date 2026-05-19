# Fallback prefix for a TLS-requested URL whose scheme is not in the
# plain->TLS map below (most commonly a schemeless `host:port`).
_TLS = "rediss://"

# Plain -> TLS scheme map. fred routes a clustered connection by the URL
# scheme, so the cluster schemes must keep their `-cluster` suffix when TLS
# is layered on: `redis-cluster://` becomes `rediss-cluster://`, never
# `rediss://redis-cluster://`. valkey schemes are fred aliases for the
# redis ones and get the same treatment.
_TLS_SCHEME = {
    "redis": "rediss",
    "redis-cluster": "rediss-cluster",
    "valkey": "valkeys",
    "valkey-cluster": "valkeys-cluster",
}
_ALREADY_TLS = ("rediss://", "rediss-cluster://", "valkeys://", "valkeys-cluster://")


def apply_tls(redis_url: str, tls: bool) -> str:
    if not tls:
        return redis_url
    lower = redis_url.lower()
    if lower.startswith(_ALREADY_TLS):
        return redis_url
    sep = redis_url.find("://")
    if sep != -1:
        scheme = lower[:sep]
        tls_scheme = _TLS_SCHEME.get(scheme)
        if tls_scheme is not None:
            return tls_scheme + redis_url[sep:]
    return _TLS + redis_url
