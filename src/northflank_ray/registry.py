import hashlib
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass, replace
from urllib.parse import urlsplit

from redis import Redis, RedisError, WatchError

from northflank_ray.metadata import NodeMetadata


def redis_url(*, local=False):
    name = "RAY_NF_REDIS_URL"
    if local and os.environ.get("RAY_NF_REDIS_CONNECT_URL"):
        name = "RAY_NF_REDIS_CONNECT_URL"
    value = os.environ.get(name, "")
    try:
        parsed = urlsplit(value)
        valid = parsed.scheme in ("redis", "rediss") and bool(parsed.hostname)
        valid = valid and not parsed.query and not parsed.fragment
        if parsed.path not in ("", "/") and not parsed.path[1:].isdigit():
            valid = False
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            valid = False
    except ValueError:
        valid = False
    if not valid:
        raise ValueError(f"Set {name} to a Redis URL without query parameters or a fragment")

    return value


@dataclass(frozen=True)
class NodeRecord:
    metadata: NodeMetadata
    uid: str | None = None
    phase: str = "pending"

    def __post_init__(self):
        if self.phase not in ("pending", "active", "deleting"):
            raise ValueError("Invalid registry node phase")
        if self.phase == "pending":
            if self.uid is not None:
                raise ValueError("Pending registry nodes cannot have a UID")
        elif not isinstance(self.uid, str) or not self.uid:
            raise ValueError("Registered nodes require a Northflank UID")
        if self.metadata.kind == "head" and self.phase == "deleting":
            raise ValueError("The registry cannot delete the head")

    def to_dict(self):
        return {"metadata": self.metadata.to_dict(), "uid": self.uid, "phase": self.phase}

    @classmethod
    def from_dict(cls, value):
        return cls(NodeMetadata(**value["metadata"]), value["uid"], value["phase"])

    def bound(self, service):
        if not service.get("uid"):
            raise ValueError("Northflank omitted the service UID")

        return replace(self, uid=service["uid"], phase="active")

    def matches(self, service):
        return bool(self.uid) and service.get("uid") == self.uid


class RedisRegistry:
    def __init__(self, provider, *, local=False):
        self.identity = {
            "version": 1,
            "api_url": provider["api_url"].rstrip("/"),
            "project_id": provider["project_id"],
            "cluster_id": provider["cluster_id"],
            "head_service_id": provider["head_service_id"],
        }
        origin = hashlib.sha256(self.identity["api_url"].encode()).hexdigest()[:16]
        self.key = f"northflank-ray:v1:{origin}:{provider['project_id']}:{provider['cluster_id']}"
        url = redis_url(local=local)
        options = {"ssl_check_hostname": True} if url.startswith("rediss://") else {}
        self.client = Redis.from_url(
            url, decode_responses=True, socket_connect_timeout=5, socket_timeout=5,
            retry_on_timeout=False, **options,
        )

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.client.close()

    @contextmanager
    def _connection(self):
        try:
            yield
        except WatchError:
            raise RuntimeError("Redis registry changed concurrently; retry the operation") from None
        except RedisError:
            # Connection exceptions can contain credentials. Never include their text.
            raise RuntimeError("Redis registry unavailable; scaling is stopped") from None

    def _decode(self, raw):
        if raw is None:
            raise RuntimeError("Redis registry is missing; restore it before starting the head")
        try:
            data = json.loads(raw)
            if data["identity"] != self.identity:
                raise ValueError("Registry identity differs")
            records = {name: NodeRecord.from_dict(value) for name, value in data["nodes"].items()}
            self._validate(records)
        except (KeyError, TypeError, ValueError, AttributeError):
            raise RuntimeError("Redis registry is invalid; scaling is stopped") from None

        return records

    def _validate(self, records):
        head_id = self.identity["head_service_id"]
        cluster_id = self.identity["cluster_id"]
        if head_id not in records or records[head_id].metadata.kind != "head":
            raise ValueError("Registry requires its head record")
        for name, record in records.items():
            if record.metadata.cluster_id != cluster_id:
                raise ValueError("Registry node belongs to another cluster")
            expected_head = record.metadata.kind == "head"
            if expected_head != (name == head_id):
                raise ValueError("Registry node has the wrong role")
            if not expected_head and not name.startswith(f"ray-{cluster_id[:12]}-w-"):
                raise ValueError("Registry worker has the wrong service name")

    def _encode(self, records):
        self._validate(records)
        return json.dumps({
            "identity": self.identity,
            "nodes": {name: record.to_dict() for name, record in records.items()},
        }, sort_keys=True)

    def read(self, *, allow_missing=False):
        with self._connection():
            raw = self.client.get(self.key)
            if raw is None and allow_missing:
                return None
            return self._decode(raw)

    def initialize(self, records):
        value = self._encode(records)
        with self._connection():
            if not self.client.set(self.key, value, nx=True):
                raise ValueError("Redis registry already exists; it was not overwritten")

    def change(self, node_id, *, expected, replacement):
        with self._connection(), self.client.pipeline() as pipe:
            pipe.watch(self.key)
            records = self._decode(pipe.get(self.key))
            if records.get(node_id) != expected:
                raise ValueError(f"Registry identity or state changed: {node_id}")
            if expected is None:
                self.require_active(records)
            if replacement is None:
                records.pop(node_id)
            else:
                records[node_id] = replacement
            # A confirmed absent, pending head is the only case that can remove the registry.
            empty = not records and expected.phase == "pending" and expected.metadata.kind == "head"
            value = None if empty else self._encode(records)
            pipe.multi()
            if empty:
                pipe.delete(self.key)
            else:
                pipe.set(self.key, value)
            pipe.execute()

    @staticmethod
    def require_active(records):
        pending = [name for name, record in records.items() if record.phase == "pending"]
        if pending:
            raise RuntimeError(
                "Unconfirmed service creation; use ray-northflank reconcile for " + ", ".join(pending)
            )
