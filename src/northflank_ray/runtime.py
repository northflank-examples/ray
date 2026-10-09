import ipaddress
import json
import os
import resource
import shutil
import tempfile

import yaml

from northflank_ray.config import prepare_config, require_pinned_ray, validate_resources
from northflank_ray.registry import RedisRegistry


def resource_flags(resources):
    validate_resources(resources)
    flags = [f"--num-cpus={int(resources['CPU'])}", "--num-gpus=0"]
    if "memory" in resources:
        flags.append(f"--memory={int(resources['memory'])}")
    custom = {key: value for key, value in resources.items() if key not in ("CPU", "GPU", "memory")}
    if custom:
        flags.append("--resources=" + json.dumps(custom))
    return flags


def head_flags():
    if not os.environ.get("NF_API_TOKEN"):
        raise ValueError("The head requires NF_API_TOKEN for worker provisioning")
    config = prepare_config(yaml.safe_load(os.environ["RAY_NF_CLUSTER_CONFIG"]))
    with RedisRegistry(config["provider"]) as registry:
        registry.read()
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as file:
        yaml.safe_dump(config, file)
        config_path = file.name
    return [
        "--head", "--port=6379", "--dashboard-host=0.0.0.0", "--dashboard-port=8265",
        "--include-dashboard=true", "--autoscaling-config=" + config_path,
    ]


def main():
    require_pinned_ray()
    role = os.environ.get("RAY_NF_ROLE")
    if role not in ("head", "worker"):
        raise ValueError("RAY_NF_ROLE must be head or worker")
    pod_ip = str(ipaddress.IPv4Address(os.environ["NF_POD_IP"]))
    resources = json.loads(os.environ["RAY_NF_RESOURCES"])
    object_store_memory = int(os.environ["RAY_NF_OBJECT_STORE_MEMORY"])
    if object_store_memory > shutil.disk_usage("/dev/shm").total:
        raise ValueError("Object store exceeds /dev/shm; increase deployment.storage.shmSize")
    flags = [
        "start", "--block", "--node-ip-address=" + pod_ip,
        "--node-manager-port=8077", "--object-manager-port=8076",
        f"--object-store-memory={object_store_memory}", "--disable-usage-stats",
    ]
    flags.extend(resource_flags(resources))
    labels = json.loads(os.environ.get("RAY_NF_LABELS", "{}"))
    if labels:
        flags.append("--labels=" + json.dumps(labels))
    if role == "head":
        flags.extend(head_flags())
    else:
        # Fail closed if an unrestricted project secret group leaked the controller token.
        if os.environ.get("NF_API_TOKEN"):
            raise ValueError("Restrict NF_API_TOKEN to the head service; workers must not inherit it")
        if any(os.environ.get(name) for name in ("RAY_NF_REDIS_URL", "RAY_NF_REDIS_CONNECT_URL")):
            raise ValueError("Restrict Redis registry credentials to the head service")
        flags.append("--address=" + os.environ["RAY_NF_HEAD_ADDRESS"])

    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    target = 65536 if hard == resource.RLIM_INFINITY else min(65536, hard)
    if soft != resource.RLIM_INFINITY and soft < target:
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    os.environ["RAY_enable_autoscaler_v2"] = "0"
    executable = shutil.which("ray")
    if executable is None:
        raise RuntimeError("Ray CLI is missing from the container PATH")
    os.execv(executable, [executable, *flags])
