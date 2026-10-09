import ipaddress
import logging
import os
import socket
import threading
import time
from dataclasses import replace
from uuid import uuid4

from ray._raylet import GcsClient
from ray.core.generated.gcs_pb2 import GcsNodeInfo
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import NODE_KIND_HEAD, NODE_KIND_WORKER, TAG_RAY_NODE_KIND

from northflank_ray.api import NorthflankClient
from northflank_ray.config import require_pinned_ray
from northflank_ray.metadata import NodeMetadata
from northflank_ray.registry import NodeRecord, RedisRegistry
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
        self.registry = RedisRegistry(provider_config)
        self.head_id = provider_config["head_service_id"]
        self.worker_prefix = f"ray-{self.cluster_id[:12]}-w-"
        self.lock = threading.RLock()
        self.nodes = {}
        self.records = {}
        self.ips = {}
        self.gcs_client = None
        self.refreshed_at = float("-inf")

    def _refresh(self, *, force=False):
        records = self.registry.read()
        self.registry.require_active(records)
        recent = time.monotonic() - self.refreshed_at < self.provider_config.get("refresh_seconds", 30)
        if not force and recent and records == self.records:
            return
        services = self.client.list_services()
        by_id = {service["id"]: service for service in services}
        unregistered = [
            name for name in by_id
            if (name == self.head_id or name.startswith(self.worker_prefix)) and name not in records
        ]
        if unregistered:
            raise RuntimeError("Services missing from Redis registry: " + ", ".join(unregistered))
        nodes = self._reconcile(records, by_id)
        ips = {node_id: self._resolve_ip(node_id) for node_id in nodes}
        missing = {node_id for node_id, ip in ips.items() if ip is None}
        if missing:
            ips.update(self._registered_worker_ips(missing))
        self.nodes = nodes
        self.records = records
        self.ips = ips
        self.refreshed_at = time.monotonic()

    def _reconcile(self, records, services):
        head = services.get(self.head_id) or self.client.get_service(self.head_id)
        if head is None or not records[self.head_id].matches(head):
            raise RuntimeError("Registered head service is missing or its UID changed")
        nodes = {}
        for node_id, record in records.items():
            service = services.get(node_id)
            if service is None or record.phase == "deleting":
                service = self.client.get_service(node_id)
            if service is None:
                if node_id == self.head_id:
                    raise RuntimeError("Registered head service no longer exists")
                self.registry.change(node_id, expected=record, replacement=None)
                continue
            if not record.matches(service):
                raise ValueError(f"Service UID differs from the Redis registry: {node_id}")
            if record.phase == "deleting":
                self.client.delete_service(node_id)
                continue
            nodes[node_id] = service

        return nodes

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

    def _registered_worker_ips(self, missing):
        if os.environ.get("RAY_NF_ROLE") != NODE_KIND_HEAD:
            return {}
        if self.gcs_client is None:
            self.gcs_client = GcsClient(address=os.environ["RAY_NF_HEAD_ADDRESS"])
        # Older images use the pod hostname; new images advertise the service ID explicitly.
        nodes = self.gcs_client.get_all_node_info(timeout=5)
        ips = {}
        for node in nodes.values():
            name = node.node_name if node.node_name in missing else node.node_manager_hostname
            if node.state != GcsNodeInfo.ALIVE or name not in missing:
                continue
            if name in ips:
                raise RuntimeError(f"Multiple live Ray nodes advertise service {name}")
            ips[name] = str(ipaddress.IPv4Address(node.node_manager_address))

        return ips

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
            metadata = self.records[node_id].metadata
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
            self._refresh(force=True)
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
        pending = NodeRecord(metadata)
        self.registry.change(name, expected=None, replacement=pending)
        try:
            service = self.client.create_service(body)
            record = pending.bound(service)
            self.registry.change(name, expected=pending, replacement=record)
        except Exception:
            # Keep the intent even after an API error: the service may already exist.
            self.refreshed_at = float("-inf")
            raise
        self.nodes[service["id"]] = service
        self.records[service["id"]] = record
        self.ips[service["id"]] = None
        logger.info("Created Northflank Ray worker service=%s", service["id"])

        return service

    def _owned_worker(self, node_id):
        if node_id == self.head_id or not node_id.startswith(self.worker_prefix):
            raise ValueError(f"Refusing to mutate non-worker service {node_id}")
        records = self.registry.read()
        self.registry.require_active(records)
        record = records.get(node_id)
        if record is None or record.metadata.kind != NODE_KIND_WORKER:
            raise ValueError(f"Service has no registered worker ownership: {node_id}")
        service = self.client.get_service(node_id)
        if service is not None and not record.matches(service):
            raise ValueError(f"Service identity changed: {node_id}")

        return record, service

    def set_node_tags(self, node_id, tags):
        with self.lock:
            record, service = self._owned_worker(node_id)
            if service is None:
                raise ValueError(f"Worker service no longer exists: {node_id}")
            if record.phase != "active":
                raise ValueError(f"Worker is being deleted: {node_id}")
            updated = replace(record, metadata=record.metadata.with_tags(tags, self.cluster_name))
            self.registry.change(node_id, expected=record, replacement=updated)
            self.records[node_id] = updated
            self.nodes[node_id] = service

    def terminate_node(self, node_id):
        with self.lock:
            record, service = self._owned_worker(node_id)
            if service:
                deleting = replace(record, phase="deleting")
                self.registry.change(node_id, expected=record, replacement=deleting)
                self.client.delete_service(node_id)
                logger.info("Deleted Northflank Ray worker service=%s", node_id)
            else:
                self.registry.change(node_id, expected=record, replacement=None)
            self.nodes.pop(node_id, None)
            self.records.pop(node_id, None)
            self.ips.pop(node_id, None)
            self.refreshed_at = float("-inf")

    def safe_to_scale(self):
        with self.lock:
            self._refresh()
            return self.head_id in self.nodes

    def get_command_runner(self, *args, **kwargs):
        raise NotImplementedError("SSH/rsync and ray up/down are unsupported; use the bootstrap CLI")
