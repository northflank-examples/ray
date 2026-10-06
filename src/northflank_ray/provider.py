import logging
import os
import socket
import threading
import time
from uuid import uuid4

from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import NODE_KIND_HEAD, NODE_KIND_WORKER, TAG_RAY_NODE_KIND

from northflank_ray.api import NorthflankClient
from northflank_ray.config import require_pinned_ray
from northflank_ray.metadata import NodeMetadata
from northflank_ray.service import service_body

logger = logging.getLogger(__name__)


class NorthflankNodeProvider(NodeProvider):
    def __init__(self, provider_config, cluster_name):
        require_pinned_ray()
        super().__init__(provider_config, cluster_name)
        if not provider_config.get("disable_node_updaters"):
            raise ValueError("Northflank workers self-start; disable_node_updaters must be true")
        self.client = NorthflankClient(
            provider_config["project_id"],
            base_url=provider_config.get("api_url", "https://api.northflank.com"),
        )
        self.cluster_id = provider_config["cluster_id"]
        self.head_id = provider_config["head_service_id"]
        self.worker_prefix = f"ray-{self.cluster_id[:12]}-w-"
        self.lock = threading.RLock()
        self.nodes = {}
        self.ips = {}
        self.deleted = set()
        self.refreshed_at = float("-inf")

    def _metadata(self, service):
        metadata = NodeMetadata.decode(service.get("description"))
        if not metadata or metadata.cluster_id != self.cluster_id:
            return None
        service_id = service["id"]
        if metadata.kind == NODE_KIND_HEAD:
            return metadata if service_id == self.head_id else None
        return metadata if service_id.startswith(self.worker_prefix) else None

    def _refresh(self):
        if time.monotonic() - self.refreshed_at < self.provider_config.get("refresh_seconds", 30):
            return
        services = self.client.list_services()
        nodes = {
            service["id"]: service for service in services
            if self._metadata(service) and service["id"] not in self.deleted
        }
        ips = {node_id: self._resolve_ip(node_id) for node_id in nodes}
        self.nodes = nodes
        self.ips = ips
        self.deleted.intersection_update(service["id"] for service in services)
        self.refreshed_at = time.monotonic()

    def _resolve_ip(self, node_id):
        if node_id == self.head_id and os.environ.get("RAY_NF_ROLE") == NODE_KIND_HEAD:
            return os.environ["NF_POD_IP"]
        try:
            addresses = socket.getaddrinfo(node_id + "-headless", None, socket.AF_INET)
        except socket.gaierror:
            return None
        ips = {address[4][0] for address in addresses}
        if len(ips) != 1:
            raise RuntimeError(f"{node_id} must resolve to exactly one pod IP; found {len(ips)}")
        return ips.pop()

    def non_terminated_nodes(self, tag_filters):
        with self.lock:
            self._refresh()
            return [
                node_id for node_id in self.nodes
                if all(self.node_tags(node_id).get(key) == value
                       for key, value in tag_filters.items())
            ]

    def is_running(self, node_id):
        with self.lock:
            return node_id in self.nodes and bool(self.ips.get(node_id))

    def is_terminated(self, node_id):
        with self.lock:
            return node_id not in self.nodes

    def node_tags(self, node_id):
        with self.lock:
            metadata = self._metadata(self.nodes[node_id])
            startup_expired = time.time() - metadata.created_at > self.provider_config.get(
                "startup_timeout_seconds", 900
            )
            # Expired pending nodes enter Ray's normal heartbeat-based replacement path.
            ready = bool(self.ips.get(node_id)) or startup_expired
            return metadata.tags(self.cluster_name, ready=ready)

    def internal_ip(self, node_id):
        with self.lock:
            if node_id not in self.nodes:
                raise ValueError(f"Unknown Northflank Ray node {node_id}")
            # V1 also asks for pending IPs. Keep these unique and non-routable.
            return self.ips.get(node_id) or f"pending:{node_id}"

    def external_ip(self, node_id):
        return self.internal_ip(node_id)

    def get_node_id(self, ip_address, use_internal_ip=False):
        with self.lock:
            self._refresh()
            matches = [node_id for node_id, ip in self.ips.items() if ip == ip_address]
            if len(matches) != 1:
                raise ValueError(f"No unique service for Ray IP {ip_address}")
            return matches[0]

    def create_node(self, node_config, tags, count):
        raise NotImplementedError("Use Ray 2.59.0's create_node_with_resources_and_labels path")

    def create_node_with_resources_and_labels(self, node_config, tags, count, resources, labels):
        if tags.get(TAG_RAY_NODE_KIND) != NODE_KIND_WORKER:
            raise ValueError("Create the head with ray-northflank bootstrap, not ray up")
        with self.lock:
            created = [
                self._create_worker(node_config=node_config, tags=tags, resources=resources, labels=labels)
                for _ in range(count)
            ]

        return {service["id"]: service for service in created}

    def _create_worker(self, *, node_config, tags, resources, labels):
        name = self.worker_prefix + uuid4().hex[:12]
        metadata = NodeMetadata.from_tags(self.cluster_id, tags)
        body = service_body(
            provider=self.provider_config, name=name, metadata=metadata,
            node_config=node_config, resources=resources, labels=labels,
        )
        try:
            service = self.client.create_service(body)
        except Exception:
            self.refreshed_at = float("-inf")
            raise
        self.nodes[service["id"]] = service
        self.ips[service["id"]] = None
        logger.info("Created Northflank Ray worker service=%s", service["id"])

        return service

    def _owned_worker(self, node_id):
        if node_id == self.head_id or not node_id.startswith(self.worker_prefix):
            raise ValueError(f"Refusing to mutate non-worker service {node_id}")
        service = self.client.get_service(node_id)
        if service is None:
            return None
        metadata = self._metadata(service)
        if not metadata or metadata.kind != NODE_KIND_WORKER:
            raise ValueError(f"Refusing to mutate service without cluster ownership: {node_id}")
        previous = self.nodes.get(node_id)
        if previous and previous.get("uid") != service.get("uid"):
            raise ValueError(f"Service identity changed: {node_id}")
        return service

    def set_node_tags(self, node_id, tags):
        with self.lock:
            service = self._owned_worker(node_id)
            if service is None:
                raise ValueError(f"Worker service no longer exists: {node_id}")
            metadata = self._metadata(service).with_tags(tags, self.cluster_name)
            description = metadata.encode()
            self.client.set_description(node_id, description)
            service["description"] = description
            self.nodes[node_id] = service

    def terminate_node(self, node_id):
        with self.lock:
            service = self._owned_worker(node_id)
            if service:
                self.client.delete_service(node_id)
                logger.info("Deleted Northflank Ray worker service=%s", node_id)
            self.deleted.add(node_id)
            self.nodes.pop(node_id, None)
            self.ips.pop(node_id, None)

    def safe_to_scale(self):
        with self.lock:
            return self.head_id in self.nodes

    def get_command_runner(self, *args, **kwargs):
        raise NotImplementedError("SSH/rsync and ray up/down are unsupported; use the bootstrap CLI")
