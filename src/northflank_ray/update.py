import copy
import json

from northflank_ray.api import NorthflankClient
from northflank_ray.config import parse_config


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


def checked_head(client, head_id):
    service = client.get_service(head_id)
    if service is None or not service.get("uid"):
        raise ValueError("The head service is missing or Northflank omitted its UID")

    return service["uid"]


def update_head(config, body, *, apply=False, skip_unchanged=False):
    provider = config["provider"]
    head_id = provider["head_service_id"]
    with NorthflankClient(provider["project_id"], base_url=provider["api_url"]) as client:
        uid = checked_head(client, head_id)
        environment = client.get_runtime_environment(head_id)
        if environment.get("RAY_NF_ROLE") != "head" or not environment.get("RAY_NF_CLUSTER_CONFIG"):
            raise ValueError("The existing service is not a configured Ray head")

        previous = parse_config(environment["RAY_NF_CLUSTER_CONFIG"])
        patch = copy.deepcopy(body)
        for key in ("name", "description", "infrastructure"):
            patch.pop(key, None)
        patch["runtimeEnvironment"] = {**environment, **body["runtimeEnvironment"]}

        if skip_unchanged and config == previous and patch["runtimeEnvironment"] == environment:
            print(f"Head {head_id} already has this configuration; no changes made.")
            return
        validate_update(config, previous)

        print(f"Update head {head_id} in project {provider['project_id']}.")
        print("Apply head image/settings and JSON cluster config; preserve other environment variables.")
        print("The head may restart, which interrupts running Ray jobs. Existing workers are not redeployed.")
        if not apply:
            print("Dry run. No service changes were made; Redis is checked by the head at startup.")
            return

        if checked_head(client, head_id) != uid:
            raise ValueError("Head service was replaced during the update; retry")
        if client.get_runtime_environment(head_id) != environment:
            raise ValueError("Head environment changed during the update; retry")
        client.patch_service(head_id, patch)
        stored = client.get_runtime_environment(head_id)
        if stored != patch["runtimeEnvironment"]:
            raise RuntimeError("Head environment verification failed; inspect the service before retrying")
        json.loads(stored["RAY_NF_CLUSTER_CONFIG"])

    print("Head service updated. Verify its running image and Ray status after the rollout.")
