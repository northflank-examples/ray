import argparse
import json
import time

import yaml

from northflank_ray.api import NorthflankClient
from northflank_ray.config import load_config
from northflank_ray.metadata import NodeMetadata
from northflank_ray.registry import NodeRecord, RedisRegistry, redis_url
from northflank_ray.service import service_body


def head_metadata(config):
    provider = config["provider"]
    return NodeMetadata(
        provider["cluster_id"], "head", config["head_node_type"], "bootstrap", int(time.time())
    )


def head_body(config):
    provider = config["provider"]
    node_type = config["head_node_type"]
    node = config["available_node_types"][node_type]
    metadata = head_metadata(config)
    body = service_body(
        provider=provider, name=provider["head_service_id"], metadata=metadata,
        node_config=node["node_config"], resources=node["resources"], labels=node.get("labels", {}),
    )
    body["runtimeEnvironment"]["RAY_NF_CLUSTER_CONFIG"] = yaml.safe_dump(config)
    return body


def apply_head(config, body):
    provider = config["provider"]
    private_url = redis_url()
    with NorthflankClient(provider["project_id"], base_url=provider["api_url"]) as client, \
            RedisRegistry(provider, local=True) as registry:
        existing = client.get_service(body["name"])
        if existing:
            records = registry.read()
            record = records[body["name"]]
            if record.phase != "active" or not record.matches(existing):
                raise ValueError("The existing head does not match its Redis ownership record")
            print(f"Head {existing['id']} already exists; configuration was not changed.")
            return

        prefix = f"ray-{provider['cluster_id'][:12]}-w-"
        if any(service["id"].startswith(prefix) for service in client.list_services()):
            raise ValueError("Workers already exist; restore or migrate their registry first")
        pending = NodeRecord(head_metadata(config))
        registry.initialize({body["name"]: pending})
        body["runtimeEnvironment"]["NF_API_TOKEN"] = client.token
        body["runtimeEnvironment"]["RAY_NF_REDIS_URL"] = private_url
        service = client.create_service(body)
        registry.change(body["name"], expected=pending, replacement=pending.bound(service))

    print(f"Created head service {service['id']} in project {provider['project_id']}.")
    print("The head will start Ray and provision workers as resource demand grows.")


def migration_records(client, provider, existing):
    head_id = provider["head_service_id"]
    prefix = f"ray-{provider['cluster_id'][:12]}-w-"
    records = {}
    for service in client.list_services():
        name = service["id"]
        if name != head_id and not name.startswith(prefix):
            continue
        metadata = NodeMetadata.from_legacy_description(service.get("description"))
        record = (existing or {}).get(name)
        if record:
            if not record.matches(service) or record.phase != "active":
                raise ValueError(f"Service does not match its Redis record: {name}")
            if metadata is not None and metadata != record.metadata:
                raise ValueError(f"Legacy metadata differs from Redis: {name}")
        else:
            if not metadata or metadata.cluster_id != provider["cluster_id"]:
                raise ValueError(f"Service has no matching legacy ownership metadata: {name}")
            record = NodeRecord(metadata).bound(service)
        records[name] = record
    if head_id not in records:
        raise ValueError("The legacy head must exist before migration")
    if existing is not None and existing != records:
        raise ValueError("Redis and Northflank inventories differ; resolve them before migration")

    return records


def migrate_metadata(client, registry, provider, arguments):
    existing = registry.read(allow_missing=True)
    records = migration_records(client, provider, existing)
    print(json.dumps({name: record.to_dict() for name, record in records.items()}, indent=2))
    if not arguments.apply:
        print("Dry run. Stop the head before applying this migration.")
        return

    if existing is None:
        registry.initialize(records)
    for name, record in records.items():
        service = client.get_service(name)
        if service is None or not record.matches(service):
            raise ValueError(f"Service identity changed during migration: {name}")
        metadata = NodeMetadata.from_legacy_description(service.get("description"))
        if metadata is not None:
            if metadata != record.metadata:
                raise ValueError(f"Legacy metadata changed during migration: {name}")
            client.set_description(name, f"Ray {record.metadata.kind} service.")
    print("Metadata imported into Redis. Legacy descriptions replaced.")
    print("Update the stopped head image and Redis secret before restarting it.")


def reconcile(client, registry, arguments):
    name = arguments.service_id
    record = registry.read().get(name)
    if record is None or record.phase != "pending":
        raise ValueError("Only an unconfirmed, pending creation can be reconciled")
    service = client.get_service(name)
    replacement = None
    if arguments.absent:
        if service is not None:
            raise ValueError("The service still exists; its pending record was not removed")
    else:
        if service is None or service.get("uid") != arguments.uid:
            raise ValueError("The service does not have the operator-supplied UID")
        replacement = record.bound(service)
    if not arguments.apply:
        print(f"Dry run: {'register' if replacement else 'forget absent'} service {name}.")
        return

    registry.change(name, expected=record, replacement=replacement)
    print(f"Reconciled service {name}; no Northflank service was changed.")


def manage_registry(config, arguments):
    provider = config["provider"]
    if arguments.apply and not arguments.head_stopped:
        raise ValueError("Stop the head and wait for its pod to exit, then pass --head-stopped")
    with RedisRegistry(provider, local=True) as registry:
        if arguments.command == "registry":
            print(json.dumps({
                "key": registry.key,
                "nodes": {name: record.to_dict() for name, record in registry.read().items()},
            }, indent=2))
            return
        with NorthflankClient(provider["project_id"], base_url=provider["api_url"]) as client:
            if arguments.command == "migrate-metadata":
                migrate_metadata(client, registry, provider, arguments)
            else:
                reconcile(client, registry, arguments)


def main():
    parser = argparse.ArgumentParser(
        prog="python -m northflank_ray", description="Bootstrap a Ray head on Northflank",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    bootstrap = commands.add_parser("bootstrap", help="Render a head service; --apply creates it")
    bootstrap.add_argument("config")
    bootstrap.add_argument("--apply", action="store_true", help="Create live infrastructure")
    for name, help_text in (
        ("registry", "Inspect the Redis ownership registry"),
        ("migrate-metadata", "Import legacy descriptions into Redis"),
        ("reconcile", "Resolve an unconfirmed service creation"),
    ):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("config")
        command.add_argument("--apply", action="store_true", help="Apply the displayed changes")
        command.add_argument("--head-stopped", action="store_true", help="Confirm the head pod exited")
        if name == "reconcile":
            command.add_argument("--service-id", required=True)
            outcome = command.add_mutually_exclusive_group(required=True)
            outcome.add_argument("--uid", help="UID of a service you confirmed belongs to this cluster")
            outcome.add_argument("--absent", action="store_true", help="Confirm no service was created")
    arguments = parser.parse_args()
    config = load_config(arguments.config)
    if arguments.command != "bootstrap":
        manage_registry(config, arguments)
        return
    body = head_body(config)
    if arguments.apply:
        apply_head(config, body)
        return
    print(json.dumps(body, indent=2))
