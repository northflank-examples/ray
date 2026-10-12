import hashlib
import os
import ssl
from urllib.parse import unquote, urlsplit

from northflank_ray.registry import redis_url


GCS_MANAGED_ENV = {
    "RAY_REDIS_ADDRESS", "RAY_REDIS_PASSWORD", "RAY_REDIS_USERNAME",
    "RAY_external_storage_namespace", "RAY_gcs_storage", "RAY_gcs_storage_path",
}


def gcs_flags(provider):
    if any(os.environ.get(name) for name in GCS_MANAGED_ENV):
        raise ValueError("Use provider.gcs_fault_tolerance instead of native GCS storage variables")
    if not provider.get("gcs_fault_tolerance", False):
        return []

    parsed = urlsplit(redis_url())
    if parsed.path not in ("", "/", "/0"):
        raise ValueError("GCS Redis requires database 0 in RAY_NF_REDIS_URL")
    if ":" in parsed.hostname:
        raise ValueError("GCS Redis requires an IPv4 address or DNS hostname")

    origin = provider["api_url"].rstrip("/")
    identity = f"{origin}/{provider['project_id']}/{provider['cluster_id']}"
    namespace = "northflank-ray-gcs-v1-" + hashlib.sha256(identity.encode()).hexdigest()
    address = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 6379}"
    if parsed.scheme == "rediss":
        configure_redis_tls(parsed.hostname)
    os.environ.update({
        "RAY_REDIS_ADDRESS": address,
        "RAY_external_storage_namespace": namespace,
        "RAY_gcs_storage": "redis",
    })

    flags = []
    if parsed.username is not None:
        flags.append("--redis-username=" + unquote(parsed.username))
    if parsed.password is not None:
        flags.append("--redis-password=" + unquote(parsed.password))
    return flags


def configure_redis_tls(hostname):
    # Ray's C++ Redis client needs its own CA and server-name settings.
    paths = ssl.get_default_verify_paths()
    if not os.environ.get("RAY_REDIS_CA_CERT") and not os.environ.get("RAY_REDIS_CA_PATH"):
        if paths.cafile:
            os.environ["RAY_REDIS_CA_CERT"] = paths.cafile
        elif paths.capath:
            os.environ["RAY_REDIS_CA_PATH"] = paths.capath
        else:
            raise ValueError("GCS Redis TLS requires a trusted CA file or directory")
    os.environ["RAY_REDIS_SERVER_NAME"] = hostname
