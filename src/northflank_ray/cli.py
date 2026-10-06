import argparse
import json
import time

import yaml

from northflank_ray.api import NorthflankClient
from northflank_ray.config import load_config
from northflank_ray.metadata import NodeMetadata
from northflank_ray.service import service_body


def head_body(config):
    provider = config["provider"]
    node_type = config["head_node_type"]
    node = config["available_node_types"][node_type]
    metadata = NodeMetadata(provider["cluster_id"], "head", node_type, "bootstrap", int(time.time()))
    body = service_body(
        provider=provider, name=provider["head_service_id"], metadata=metadata,
        node_config=node["node_config"], resources=node["resources"], labels=node.get("labels", {}),
    )
    body["runtimeEnvironment"]["RAY_NF_CLUSTER_CONFIG"] = yaml.safe_dump(config)
    return body


def apply_head(config, body):
    provider = config["provider"]
    with NorthflankClient(provider["project_id"], base_url=provider["api_url"]) as client:
        existing = client.get_service(body["name"])
        if existing:
            metadata = NodeMetadata.decode(existing.get("description"))
            if not metadata or metadata.cluster_id != provider["cluster_id"] or metadata.kind != "head":
                raise ValueError("The head service name is already owned by another resource")
            print(f"Head {existing['id']} already exists; configuration was not changed.")
            return

        body["runtimeEnvironment"]["NF_API_TOKEN"] = client.token
        service = client.create_service(body)

    print(f"Created head service {service['id']} in project {provider['project_id']}.")
    print("The head will start Ray and provision workers as resource demand grows.")


def main():
    parser = argparse.ArgumentParser(description="Bootstrap a Ray head on Northflank")
    commands = parser.add_subparsers(dest="command", required=True)
    bootstrap = commands.add_parser("bootstrap", help="Render a head service; --apply creates it")
    bootstrap.add_argument("config")
    bootstrap.add_argument("--apply", action="store_true", help="Create live infrastructure")
    arguments = parser.parse_args()
    config = load_config(arguments.config)
    body = head_body(config)
    if arguments.apply:
        apply_head(config, body)
        return
    print(json.dumps(body, indent=2))
