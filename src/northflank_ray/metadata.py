import re
import time
from dataclasses import asdict, dataclass, replace

from ray.autoscaler.tags import (
    NODE_KIND_HEAD, NODE_KIND_WORKER, STATUS_UNINITIALIZED, STATUS_UP_TO_DATE,
    TAG_RAY_CLUSTER_NAME, TAG_RAY_LAUNCH_CONFIG, TAG_RAY_NODE_KIND,
    TAG_RAY_NODE_NAME, TAG_RAY_NODE_STATUS, TAG_RAY_USER_NODE_TYPE,
)


@dataclass(frozen=True)
class NodeMetadata:
    cluster_id: str
    kind: str
    node_type: str
    launch_hash: str
    created_at: int
    status: str = STATUS_UNINITIALIZED

    def __post_init__(self):
        fields = (self.cluster_id, self.kind, self.node_type, self.launch_hash, self.status)
        if any(not isinstance(field, str) or not re.fullmatch(r"[a-zA-Z0-9._-]+", field)
               for field in fields):
            raise ValueError("Invalid Ray node metadata field")
        if self.kind not in (NODE_KIND_HEAD, NODE_KIND_WORKER):
            raise ValueError("Invalid Ray node role")
        if type(self.created_at) is not int or self.created_at < 0:
            raise ValueError("Invalid Ray node creation time")

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_legacy_description(cls, description):
        if not isinstance(description, str):
            return None
        fields = description.split(";")
        if len(fields) != 7 or fields[0] != "nf-ray-v1":
            return None
        try:
            node = cls(fields[1], fields[2], fields[3], fields[4], int(fields[5]), fields[6])
        except (ValueError, TypeError):
            return None
        return node

    def tags(self, cluster_name, ready=False):
        return {
            TAG_RAY_CLUSTER_NAME: cluster_name,
            TAG_RAY_NODE_KIND: self.kind,
            TAG_RAY_USER_NODE_TYPE: self.node_type,
            TAG_RAY_LAUNCH_CONFIG: self.launch_hash,
            TAG_RAY_NODE_NAME: f"ray-{cluster_name}-{self.kind}",
            TAG_RAY_NODE_STATUS: STATUS_UP_TO_DATE if ready else self.status,
        }

    def with_tags(self, tags, cluster_name):
        current = self.tags(cluster_name)
        immutable = (TAG_RAY_CLUSTER_NAME, TAG_RAY_NODE_KIND, TAG_RAY_USER_NODE_TYPE,
                     TAG_RAY_NODE_NAME)
        if any(key not in current for key in tags):
            raise ValueError("This V1 provider supports only the standard node launch tags")
        if any(key in tags and tags[key] != current[key] for key in immutable):
            raise ValueError("Cannot change a node's cluster, role, name or node type")
        return replace(
            self,
            launch_hash=tags.get(TAG_RAY_LAUNCH_CONFIG, self.launch_hash),
            status=tags.get(TAG_RAY_NODE_STATUS, self.status),
        )

    @classmethod
    def from_tags(cls, cluster_id, tags):
        return cls(
            cluster_id, tags[TAG_RAY_NODE_KIND], tags[TAG_RAY_USER_NODE_TYPE],
            tags[TAG_RAY_LAUNCH_CONFIG], int(time.time()),
        )
