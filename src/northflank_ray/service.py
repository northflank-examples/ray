import copy
import json

from ray.autoscaler.tags import NODE_KIND_HEAD


def service_body(*, provider, name, metadata, node_config, resources, labels):
    body = copy.deepcopy(node_config["service"])
    body.update(name=name, description=f"Ray {metadata.kind} service.")
    body["autoscaling"] = {"horizontal": {"enabled": False, "minReplicas": 1, "maxReplicas": 1}}
    body["deployment"].update({
        "instances": 1,
        "type": "deployment" if metadata.kind == NODE_KIND_HEAD else "statefulSet",
        "docker": {"configType": "customEntrypoint", "customEntrypoint": "ray-northflank-start"},
    })
    if metadata.kind == NODE_KIND_HEAD:
        body["deployment"]["strategy"] = {"type": "recreate"}
    environment = body.setdefault("runtimeEnvironment", {})
    environment.update({
        "RAY_NF_ROLE": metadata.kind,
        "RAY_NF_HEAD_ADDRESS": provider["head_service_id"] + ":6379",
        "RAY_NF_RESOURCES": json.dumps(resources),
        "RAY_NF_LABELS": json.dumps(labels),
        "RAY_NF_OBJECT_STORE_MEMORY": str(node_config["object_store_memory"]),
        "RAY_enable_autoscaler_v2": "0",
        "RAY_USAGE_STATS_ENABLED": "0",
        "AUTOSCALER_UPDATE_INTERVAL_S": str(provider["refresh_seconds"]),
        "AUTOSCALER_HEARTBEAT_TIMEOUT_S": "120",
    })
    ports = [("ray-node", 8077, "TCP")]
    if metadata.kind == NODE_KIND_HEAD:
        ports.extend([("ray-gcs", 6379, "TCP"), ("ray-jobs", 8265, "HTTP")])
    body["ports"] = [
        {"name": port_name, "internalPort": port, "protocol": protocol, "public": False}
        for port_name, port, protocol in ports
    ]
    return body
