import copy
import json

from northflank_ray.api import NorthflankClient
from northflank_ray.config import parse_config
from northflank_ray.registry import RedisRegistry


def validate_update(config, previous):
    for key in ("cluster_name", "head_node_type"):
        if config[key] != previous[key]:
            raise ValueError(f"An update cannot change {key}")
    for key in ("cluster_id", "project_id", "head_service_id", "api_url"):
        if config["provider"][key] != previous["provider"][key]:
            raise ValueError(f"An update cannot change provider.{key}")

    head_type = config["head_node_type"]
    service = config["available_node_types"][head_type]["node_config"]["service"]
    old_service = previous["available_node_types"][head_type]["node_config"]["service"]
    if service.get("infrastructure") != old_service.get("infrastructure"):
        raise ValueError("An update cannot move the head to different infrastructure")
    internal = service["deployment"].get("internal")
    if internal and internal.get("buildSHA") in (None, "", "latest", "disabled"):
        raise ValueError("Updating an internal head image requires a pinned deployment.internal.buildSHA")
    if ("internal" in service["deployment"]) != ("internal" in old_service["deployment"]):
        raise ValueError("An update cannot switch between internal and external head images")


def checked_head(client, registry, head_id):
    records = registry.read()
    registry.require_active(records)
    record = records[head_id]
    service = client.get_service(head_id)
    if service is None or record.phase != "active" or not record.matches(service):
        raise ValueError("The existing head does not match its Redis ownership record")

    return record


def update_head(config, body, *, apply=False):
    provider = config["provider"]
    head_id = provider["head_service_id"]
    with NorthflankClient(provider["project_id"], base_url=provider["api_url"]) as client, \
            RedisRegistry(provider, local=True) as registry:
        record = checked_head(client, registry, head_id)
        environment = client.get_runtime_environment(head_id)
        previous = parse_config(environment["RAY_NF_CLUSTER_CONFIG"])
        validate_update(config, previous)
        patch = copy.deepcopy(body)
        for key in ("name", "description", "infrastructure"):
            patch.pop(key, None)
        patch["runtimeEnvironment"] = {**environment, **body["runtimeEnvironment"]}

        print(f"Update head {head_id} in project {provider['project_id']}.")
        print("Apply head image/settings and JSON cluster config; preserve other environment variables.")
        print("The head may restart, which interrupts running Ray jobs. Existing workers are not redeployed.")
        if not apply:
            print("Dry run. Pass --apply to update the service.")
            return

        if checked_head(client, registry, head_id) != record:
            raise ValueError("Head ownership changed during the update")
        if client.get_runtime_environment(head_id) != environment:
            raise ValueError("Head environment changed during the update; retry")
        client.patch_service(head_id, patch)
        stored = client.get_runtime_environment(head_id)
        if stored != patch["runtimeEnvironment"]:
            raise RuntimeError("Head environment verification failed; inspect the service before retrying")
        json.loads(stored["RAY_NF_CLUSTER_CONFIG"])

    print("Head service updated. Verify its running image and Ray status after the rollout.")
