_PLAIN = "redis://"
_TLS = "rediss://"


def apply_tls(redis_url: str, tls: bool) -> str:
    if not tls:
        return redis_url
    lower = redis_url.lower()
    if lower.startswith(_TLS):
        return redis_url
    if lower.startswith(_PLAIN):
        return _TLS + redis_url[len(_PLAIN):]
    return _TLS + redis_url
