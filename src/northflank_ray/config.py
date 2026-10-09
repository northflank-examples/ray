import copy
import math
import re
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

import ray
import yaml
from ray.autoscaler._private.util import validate_config

from northflank_ray import RAY_VERSION


def require_pinned_ray():
    if ray.__version__ != RAY_VERSION:
        raise ValueError(f"This provider requires Ray {RAY_VERSION}; found {ray.__version__}")


def load_config(path):
    return prepare_config(yaml.safe_load(Path(path).read_text()))


def prepare_config(config):
    require_pinned_ray()
    config = copy.deepcopy(config)
    provider = config["provider"]
    if provider.get("type") != "external":
        raise ValueError("provider.type must be external")
    if provider.get("module") != "northflank_ray.provider.NorthflankNodeProvider":
        raise ValueError("Use the NorthflankNodeProvider module in provider.module")
    provider["cluster_id"] = UUID(provider["cluster_id"]).hex
    provider.setdefault("api_url", "https://api.northflank.com")
    api_url = urlsplit(provider["api_url"])
    if api_url.scheme != "https" or not api_url.hostname:
        raise ValueError("provider.api_url must be an HTTPS API origin")
    if api_url.username or api_url.password or api_url.path not in ("", "/"):
        raise ValueError("provider.api_url must not include credentials or an API path")
    if api_url.query or api_url.fragment:
        raise ValueError("provider.api_url must not include a query or fragment")
    for key in ("project_id", "head_service_id"):
        if not re.fullmatch(r"[a-z][a-z0-9-]{2,53}", provider[key]):
            raise ValueError(f"provider.{key} must be a lowercase Northflank slug")
    required_flags = {
        "disable_node_updaters": True,
        "use_internal_ips": True,
        "worker_liveness_check": True,
        "disable_launch_config_check": False,
        "use_node_id_as_ip": False,
    }
    for key, value in required_flags.items():
        if provider.get(key, value) is not value:
            raise ValueError(f"provider.{key} must be {value}")
        provider[key] = value

    provider.setdefault("refresh_seconds", 30)
    provider.setdefault("startup_timeout_seconds", 900)
    refresh_seconds = provider["refresh_seconds"]
    if isinstance(refresh_seconds, bool) or not isinstance(refresh_seconds, (int, float)):
        raise ValueError("refresh_seconds must be a whole number between 5 and 300")
    if not 5 <= refresh_seconds <= 300 or int(refresh_seconds) != refresh_seconds:
        raise ValueError("refresh_seconds must be a whole number between 5 and 300")
    provider["refresh_seconds"] = int(refresh_seconds)
    if not 60 <= provider["startup_timeout_seconds"] <= 3600:
        raise ValueError("startup_timeout_seconds must be between 60 and 3600")
    reject_vm_setup(config)
    validate_node_types(config)
    config.setdefault("idle_timeout_minutes", 5)
    config.setdefault("upscaling_speed", 1.0)
    validate_config(config)
    return config


def reject_vm_setup(config):
    empty_fields = {
        "auth": {}, "docker": {}, "file_mounts": {}, "cluster_synced_files": [],
        "initialization_commands": [], "setup_commands": [], "head_setup_commands": [],
        "worker_setup_commands": [], "head_start_ray_commands": [],
        "worker_start_ray_commands": [], "rsync_exclude": [], "rsync_filter": [],
    }
    for field, value in empty_fields.items():
        if config.get(field):
            raise ValueError(f"{field} is unsupported: bake dependencies into the image")
        config[field] = value
    config["file_mounts_sync_continuously"] = False


def validate_node_types(config):
    head_type = config["head_node_type"]
    node_types = config["available_node_types"]
    if head_type not in node_types:
        raise ValueError("head_node_type must identify an available node type")
    for name, node in node_types.items():
        if not re.fullmatch(r"[a-zA-Z0-9._-]{1,48}", name):
            raise ValueError("Node type names must be at most 48 letters, digits, dots or dashes")
        node.setdefault("min_workers", 0)
        node.setdefault("max_workers", 0 if name == head_type else config["max_workers"])
        if name == head_type and (node["min_workers"] or node["max_workers"]):
            raise ValueError("The head node type must have min_workers and max_workers of zero")
        if not 0 <= node["min_workers"] <= node["max_workers"] <= config["max_workers"]:
            raise ValueError(f"Invalid worker bounds for {name}")
        for field in ("initialization_commands", "worker_setup_commands", "docker"):
            if node.get(field):
                raise ValueError(f"{name}.{field} is unsupported: bake setup into the image")
        validate_resources(node["resources"])
        validate_service(node["node_config"])
        labels = node.get("labels", {})
        if not all(isinstance(k, str) and isinstance(v, str) for k, v in labels.items()):
            raise ValueError("Ray labels must be string pairs")


def validate_resources(resources):
    if "CPU" not in resources:
        raise ValueError("Declare CPU explicitly, including CPU: 0 on a dedicated head")
    for name, amount in resources.items():
        if not isinstance(amount, (int, float)) or isinstance(amount, bool):
            raise ValueError(f"Resource {name} must be numeric")
        if not math.isfinite(amount) or amount < 0:
            raise ValueError(f"Resource {name} must be finite and nonnegative")
        if name != "memory" and int(amount) != amount:
            raise ValueError(f"Resource {name} must be a whole number for ray start")
    if resources.get("GPU", 0):
        raise ValueError("This provider does not configure Northflank GPU allocation")
    if "object_store_memory" in resources:
        raise ValueError("Set object_store_memory in node_config, not resources")


def validate_service(node_config):
    service = node_config["service"]
    allowed = {"billing", "deployment", "runtimeEnvironment", "tags", "infrastructure", "stageId"}
    if set(service) - allowed:
        raise ValueError(f"Unsupported service fields: {sorted(set(service) - allowed)}")
    deployment = service["deployment"]
    allowed_deployment = {"external", "internal", "storage", "gracePeriodSeconds"}
    if set(deployment) - allowed_deployment:
        raise ValueError("The adapter owns instance count, deployment type, command and strategy")
    if not service["billing"].get("deploymentPlan"):
        raise ValueError("Choose a Northflank billing.deploymentPlan")
    validate_image_source(deployment)
    memory = node_config["object_store_memory"]
    if not isinstance(memory, int) or memory < 80 * 1024**2:
        raise ValueError("object_store_memory must be an integer of at least 80 MiB, in bytes")
    if deployment.get("storage", {}).get("shmSize", 0) * 1024**2 < memory:
        raise ValueError("deployment.storage.shmSize (MiB) must cover object_store_memory")
    environment = service.get("runtimeEnvironment", {})
    if any(key.startswith(("RAY_NF_", "NF_")) for key in environment):
        raise ValueError("NF_* and RAY_NF_* environment keys are managed by the adapter/platform")
    if not all(isinstance(value, str) for value in environment.values()):
        raise ValueError("Quote all runtimeEnvironment values as strings")


def validate_image_source(deployment):
    sources = {"external", "internal"}.intersection(deployment)
    if len(sources) != 1:
        raise ValueError("Choose exactly one deployment.external or deployment.internal image")
    if "external" in sources:
        if not deployment["external"].get("imagePath"):
            raise ValueError("Set deployment.external.imagePath to an image containing this package")
        return

    internal = deployment["internal"]
    if not internal.get("id") or not internal.get("branch"):
        raise ValueError("An internal image requires the build service id and branch")
    if not internal.get("buildId"):
        raise ValueError("Pin deployment.internal.buildId so every worker uses the same image")
