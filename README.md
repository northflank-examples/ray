# Ray on Northflank

Run Ray on Northflank with one service for the head and one service per worker.
The Ray autoscaler creates and deletes worker services as jobs request and release resources.
[`examples/cluster.yaml`](examples/cluster.yaml) starts with zero to three workers.

The adapter uses [Ray 2.59.0](https://github.com/ray-project/ray/releases/tag/ray-2.59.0),
Python 3.11, autoscaler V1 and the official `northflank==2.0.0` Python SDK.
It supports CPU workloads submitted through the Ray Jobs CLI or API.

## Setup

Create a Northflank project with private traffic between pods. Choose compute plans for
the head and workers. Both services use one instance and the `recreate` deployment
strategy. If your account requires a feature flag for `recreate`, enable it first.

Create a private [Redis addon](https://northflank.com/docs/v1/application/databases-and-persistence/deploy-databases-on-northflank/deploy-redis-on-northflank)
in the same project. Keep AOF persistence and the `noeviction` policy enabled. The adapter
stores service ownership in Redis, so use persistent storage and configure backups.
Use a single primary endpoint. This adapter does not discover primaries through Sentinel
or connect to Redis Cluster.

Add your application dependencies to the Dockerfile. Build for the architecture of your
Northflank nodes. This example builds and pushes an x86-64 image:

```bash
docker build --platform linux/amd64 -t ghcr.io/YOUR_ORG/ray-northflank:0.1.0 .
docker push ghcr.io/YOUR_ORG/ray-northflank:0.1.0
```

Install the package locally with Python 3.11 and copy the example configuration:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
cp examples/cluster.yaml cluster.local.yaml
```

In `cluster.local.yaml`, set `project_id`, generate a new `cluster_id` with `uuidgen`,
and set the image and compute plan for each node type. Match the declared Ray resources
to each plan. Keep `max_workers: 3` for the first run.

Use an image digest or a tag that you will not overwrite. For private images, set
`deployment.external.credentials` to a Northflank registry credential ID on both node types.

If Northflank builds the image, replace `deployment.external` in both node types with:

```yaml
internal:
  id: ray-image
  branch: main
  buildId: YOUR_SUCCESSFUL_BUILD_ID
```

Pin `buildId` so that every worker uses the same build. When patching an existing
deployment, also set `buildSHA` to that build's commit SHA. Otherwise, Northflank can
retain the previous SHA and reject the new build ID. The API defaults to
`https://api.northflank.com`. For another Northflank environment, set `provider.api_url`
to its HTTPS origin without `/v1`.

Run the setup commands with the Python environment where you installed this package.
Render the head service definition:

```bash
python -m northflank_ray bootstrap cluster.local.yaml > head-service.json
```

This command writes JSON without calling Northflank or Redis, or reading credentials.
Create a project-scoped token with permission to list, read, create and delete services.
The legacy metadata migration also needs permission to patch service descriptions.
Supply these variables through your shell's secret manager:

- `NF_API_TOKEN`: the Northflank API token.
- `RAY_NF_REDIS_URL`: the private Redis connection URL that the head will use.
- `RAY_NF_REDIS_CONNECT_URL`: an optional URL for local access through a Northflank port forward.

The bootstrap command uses `RAY_NF_REDIS_CONNECT_URL` when set. It only passes
`RAY_NF_REDIS_URL` to the head. Use the addon's `REDIS_MASTER_URL` connection secret,
with a `redis://` or `rediss://` scheme and an optional database path such as `/0`.
URL query parameters are unsupported. TLS connections verify the server certificate and hostname.
If forwarding a TLS connection, retain a hostname that matches the certificate and resolves
to the forwarded address on your machine.

Create the head:

```bash
python -m northflank_ray bootstrap cluster.local.yaml --apply
```

`--apply` creates a billable head service. The head starts Ray with the YAML configuration
and creates workers within its limits. Running bootstrap again leaves an existing owned
head unchanged. It registers the head in Redis before the autoscaler starts provisioning workers.
Use bootstrap to create the head so that the service and registry agree.

The bootstrap command stores the API token and private Redis URL in the head's runtime secrets.
Workers refuse to start if they inherit either credential. For credential rotation, you can
link the Redis connection secret through a secret group restricted to the head service.
Remove the direct runtime value when switching to an inherited secret.

Keep application passwords in Northflank secret groups. Ray logs the cluster YAML.
Use existing resource tags to attach secret groups to workers created by the autoscaler.

## Submit jobs

Use the same Ray and Python versions on the machine that submits jobs. Forward the private
head port `8265` through Northflank. Use the local port reported by the forwarding tool
in place of `8265` below:

```bash
ray job submit --address=http://127.0.0.1:8265 --working-dir=./your-app -- python main.py
```

Inside a Ray job, connect with `ray.init(address="auto")`. Ray schedules tasks by their CPU,
memory and custom resource requirements. The example head declares `CPU: 0`, so workers
run CPU tasks. Size the head for any work that the job driver performs locally.

Idle workers scale down after `idle_timeout_minutes`, subject to `min_workers`, active
actors and retained objects. Keep the Northflank replica autoscaler disabled.

Run `ray status` inside the head to see resource demand. Scaling decisions appear in
`/tmp/ray/session_latest/logs/monitor.log`. Startup errors appear in the service container
logs. A Northflank rollout marked `COMPLETED` means that deployment finished.
Use Ray Jobs to inspect job status.

## Smoke test

The sample workload needs two workers with 2 CPUs each. Forward the private Jobs port
as described above, then run:

```bash
ray job submit --address=http://127.0.0.1:8265 --working-dir=./examples -- python smoke.py start
ray job submit --address=http://127.0.0.1:8265 --working-dir=./examples -- python smoke.py status
ray job submit --address=http://127.0.0.1:8265 --working-dir=./examples -- python smoke.py stop
```

`start` creates two detached actors, which stay alive after the job exits. It makes sure
that they run on distinct workers and transfers a 34.5 MB object between them.
The actors keep both workers allocated until `stop`.

To test recovery, delete one worker service and run `status`. Wait for Northflank to
finish deleting the service and for Ray to start a replacement. The other worker must keep
its identity. Run `stop` even if a test fails. Make sure that the workers disappear after
the idle timeout, then pause the head to stop its compute usage and further worker creation.

## Worker identity and networking

Each service runs one pod. The `recreate` strategy prevents two pods from temporarily
sharing a service identity during an update. This setup does not require StatefulSets.

Workers connect to the private head service on port `6379`, which runs the Ray Global
Control Service (GCS). Each Ray process advertises its `NF_POD_IP`. The provider resolves
`<worker-service-id>-headless` to find the worker pod IP. It supports IPv4 and stops a
refresh if a worker resolves to more than one IP.

Services must share a Northflank project and network. Ports `6379` for GCS, `8265` for
Ray Jobs and `8077` for the node manager are private service ports. Workers also need
direct pod access to object-manager port `8076` and the dynamic Ray worker and agent ports.
Keep the cluster private and limit access to trusted workloads and users. The Jobs API can run
code on the head, which holds the API token.

The Northflank service ID is the Ray provider node ID. Redis stores each service ID and UID,
cluster UUID, node type, role, launch hash and creation time. Service descriptions are plain text
and can be edited. Before deleting a worker, the provider reads its Redis record and the live
service. It rejects the head, unregistered services and services whose UID changed.

Each cluster uses a key under `northflank-ray:v1`, scoped by API origin, project and cluster UUID.
The key has no expiry. The adapter never reads or modifies Ray's own Redis keys.

## Operation

Workers start from a container image. Use `python -m northflank_ray bootstrap` to create the head
and Ray Jobs to submit work. `ray up`, `ray down`, `ray exec`, SSH and rsync are unsupported.

By default, workers have 15 minutes to acquire an IP. After that, or once an IP is available,
Ray uses a 120-second heartbeat timeout to replace unresponsive workers. Bad images or unavailable
capacity can cause repeated replacements. Stop the head to stop those attempts.

The adapter retries reads a limited number of times. It records a pending creation in Redis
before calling Northflank. A successful response identifies the created service, and a follow-up
read supplies its UID. An HTTPX wrapper prevents the SDK from retrying uncertain creates.
If a response is lost, the adapter retains the pending record and stops scaling until an operator
resolves it. A matching name alone does not prove ownership.

Before deletion, the adapter records its intent in Redis. If the head restarts during deletion,
it reads the service UID again before retrying. It removes the record only after Northflank
confirms that the service is absent.

If Redis is unavailable, its registry is missing, or a service has no matching record, the adapter
stops scaling. It does not rebuild ownership from service names. Restore the registry from a
current backup and inspect it before restarting the head. Run only one head per cluster.

API calls run serially. By default, the provider refreshes its service list and DNS every 30 seconds.
Service lists use pages of 100 entries, so API usage grows with the number of services.
Account for API rate limits and available compute capacity when choosing worker limits.

The head has no persistent GCS state or high availability. Losing it loses active jobs.
Long runs need application checkpoints, task or actor retries, and a plan to recover the head.
Ray drains idle workers before deletion, but that does not guarantee that arbitrary work
finishes. During teardown, stop the head before deleting workers so that Ray cannot replace them.

## Migration and interrupted creates

To migrate a cluster that stores metadata in descriptions, install this package locally and
supply the API token and Redis URLs described above. Preview the import:

```bash
python -m northflank_ray migrate-metadata cluster.local.yaml
```

Stop application work, pause the head and wait for its pod to exit. Pausing the head loses
active jobs because this deployment does not persist Ray GCS state. Do not delete the services.
The `--head-stopped` flag confirms that you completed this step. The command does not stop the head.

```bash
python -m northflank_ray migrate-metadata cluster.local.yaml --apply --head-stopped
```

The migration imports the existing UIDs and Ray metadata, then replaces the encoded descriptions.
It can resume after a partial failure. While the head remains stopped, update its image and cluster
configuration to this version and set its private `RAY_NF_REDIS_URL` secret. Resume the head after
these changes. Do not restart the old adapter after removing its description metadata.
Changing worker image settings can cause Ray to replace workers, so use a maintenance window.

Inspect the registry without printing credentials:

```bash
python -m northflank_ray registry cluster.local.yaml
```

For a pending creation, stop the head and make sure that the original API request has finished.
Inspect the exact service in Northflank. If it belongs to this cluster, supply its immutable UID:

```bash
python -m northflank_ray reconcile cluster.local.yaml --service-id SERVICE_ID --uid SERVICE_UID
python -m northflank_ray reconcile cluster.local.yaml --service-id SERVICE_ID --uid SERVICE_UID --apply --head-stopped
```

If Northflank confirms that no service was created, use `--absent` in place of `--uid SERVICE_UID`.
This only removes the pending record. It never deletes a service. An absent pending head also
removes its otherwise empty registry, so bootstrap can be retried. Keep the head stopped until
all pending records are resolved. Never register a service that you cannot identify as your own.

## Application configuration

Match Ray CPU and memory declarations to the Northflank compute plan. Leave memory for
the object store, Ray processes and application overhead. Ray resource declarations
control scheduling. They do not change the Northflank compute plan.

Install application dependencies and include required files in the container image.
The adapter does not run VM setup commands or copy files through `file_mounts`.
Use `--working-dir` when submitting a job to upload its application code.

Ray uses GCS on port `6379`. The Redis addon stores adapter metadata. It does not enable
Ray GCS persistence or head failover. Those features require separate configuration and testing.
Provision application databases, object storage and cloud permissions separately.
Use Northflank secret groups to supply their connection details.

## References

- [Ray NodeProvider interface](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/autoscaler/node_provider.py)
- [Ray V1 autoscaler](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/autoscaler/_private/autoscaler.py)
- [Ray startup CLI](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/scripts/scripts.py)
- [Northflank API](https://northflank.com/docs/v1/api)
- [Northflank Python SDK 2.0.0](https://pypi.org/project/northflank/2.0.0/)
