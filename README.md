# Ray on Northflank

Run Ray on Northflank with one service for the head and one service per worker.
The Ray autoscaler creates and deletes worker services as jobs request and release resources.
[`examples/cluster.yaml`](examples/cluster.yaml) starts with zero to three workers.

The adapter uses [Ray 2.59.0](https://github.com/ray-project/ray/releases/tag/ray-2.59.0),
Python 3.11, autoscaler V1 and the official `northflank==2.0.0` Python SDK.
It supports CPU workloads submitted through the Ray Jobs CLI or API.

## Setup

Create a Northflank project with private traffic between pods. Choose compute plans for
the head and workers. Each service uses the `deployment` type with one replica and
the `recreate` strategy. If your account requires a feature flag for `recreate`, enable it first.

Create a private [Redis addon](https://northflank.com/docs/v1/application/databases-and-persistence/deploy-databases-on-northflank/deploy-redis-on-northflank)
in the same project. Keep AOF persistence and the `noeviction` policy enabled. The adapter
stores service ownership in Redis, so use persistent storage and configure backups.
Use a single primary endpoint. This adapter does not discover primaries through Sentinel
or connect to Redis Cluster.

Create a Northflank build service named `ray-image` in the same project:

1. Connect this repository, or your fork, and select the `main` branch.
2. Select the repository's `Dockerfile` with the repository root as the build context.
3. Build for the architecture of your Northflank nodes.
4. Wait for a successful build and copy its build ID and commit SHA.

If your application needs additional dependencies, add them to the Dockerfile in your fork before building.
Northflank builds and stores the image for the head and workers.

Install the package locally with Python 3.11 and copy the example configuration:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
cp examples/cluster.yaml cluster.local.yaml
```

In `cluster.local.yaml`, set `project_id` and generate a new `cluster_id` with `uuidgen`.
Set the compute plan for each node type. Match the declared Ray resources to each plan.
Keep `max_workers: 3` for the first run.

Under `node_config.service.deployment.internal` in both node types, set the successful build's identifiers:

```yaml
internal:
  id: ray-image
  branch: main
  buildId: YOUR_SUCCESSFUL_BUILD_ID
  buildSHA: YOUR_BUILD_COMMIT_SHA
```

Pin both `buildId` and `buildSHA` so the head and workers use the selected build.
For updates, replace both values with the new build's identifiers before running `apply`.
Northflank can reject a new build ID if the configuration retains the previous commit SHA.

The API defaults to `https://api.northflank.com`. For another Northflank environment,
set `provider.api_url` to its HTTPS origin without `/v1`.

Run the commands with the Python environment where you installed this package.
Create a project-scoped token with permission to list, read, create and delete services.
Updating an existing head also requires Services General Update and Secrets Services
Read/Update permissions. The legacy metadata migration needs permission to patch service descriptions.
Set `NF_API_TOKEN` in a local `.env` file or through your shell's secret manager.
This is the only local credential required by `apply`.

Local commands automatically load `.env` from the directory where you run them.
Exported shell variables take precedence. A missing `.env` is fine; parent directories
are not searched. Values are literal: `${NAME}` is not expanded. For example:

```dotenv
NF_API_TOKEN=your-project-scoped-token
```

`.env` is already ignored by Git. Head and worker startup does not load `.env` files.

Set `RAY_NF_REDIS_URL` on the head through a Northflank secret group linked only to the
head service. For a new head, use a tag on the head node type's `node_config.service.tags`
to attach that secret group at creation. Do not use that tag on worker node types.
Alternatively, set the variable directly on the head after creation. The head cannot
start until valid Redis credentials are supplied. The head needs a reachable private
Redis endpoint, but your laptop does not need its URL or a Redis port forward.

For compatibility, a local `RAY_NF_REDIS_URL` is still copied to the head on creation
when provided. Updates preserve the head's existing Redis credentials.
Use the addon's `REDIS_MASTER_URL` connection secret with a `redis://` or `rediss://`
scheme and an optional database path such as `/0`. URL query parameters are unsupported.
The adapter registry verifies the TLS server certificate and hostname.

Preview the changes, then apply the configuration:

```bash
python -m northflank_ray apply cluster.local.yaml --dry-run
python -m northflank_ray apply cluster.local.yaml
```

`apply` creates a billable head service if it is missing, or updates an existing head
whose stored Ray cluster identity matches the config. The head starts Ray and creates workers within
the configured limits. The head registers itself in Redis on startup before workers can be provisioned.
If the stored config and managed environment already match, the command makes no changes.
It does not detect manual changes to other service settings when the config is unchanged.

`apply` and `--dry-run` only contact the Northflank API. The dry run writes nothing;
Redis connectivity and registry checks happen inside the head at startup.

On creation, the command stores the local API token in the head's runtime secrets.
Use a token limited to this project's worker provisioning permissions for initial creation.
For later updates, a separate operator token can have the additional update permissions;
apply preserves the token already stored on the head.
Workers refuse to start if they inherit either credential. For credential rotation, you can
link the Redis connection secret through a secret group restricted to the head service.
Remove the direct runtime value when switching to an inherited secret.

Keep application passwords in Northflank secret groups. Ray logs the cluster configuration.
Use existing resource tags to attach secret groups to workers created by the autoscaler.

## Update a cluster

Edit your local YAML or JSON config, then preview and apply the update:

```bash
python -m northflank_ray apply cluster.local.yaml --dry-run
python -m northflank_ray apply cluster.local.yaml
```

Creation and updates store `RAY_NF_CLUSTER_CONFIG` as JSON. Existing YAML values
remain readable and are converted to JSON on the next update. The head writes a temporary
YAML file for Ray's `--autoscaling-config` to match Ray's parser, including numeric values.

Applying a config checks the head's role and stored cluster config through the Northflank API.
It does not read or modify Redis from your laptop.
The operator token needs Services General Read/Update and Secrets Services Read/Update
permissions. The head's worker-provisioning token does not need those extra permissions.
The command preserves the head's existing token, Redis URL and other environment variables.
It never prints their values. Avoid concurrent environment edits while applying an update.

The update applies the head image, resources and managed settings as well as worker
configuration. Pin both `buildId` and `buildSHA` for an internal head image. It can restart
the head and interrupt running jobs. Existing workers are not redeployed; Ray applies
its scaling and replacement rules to them. Check the running head image and `ray status`
after the rollout. Cluster identity, head node type and head infrastructure cannot change.

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

Each service runs one pod. Each worker has its own Northflank service, so Ray can
delete any idle worker independently. All services use the `recreate` strategy to avoid
overlapping instances. Workers do not require StatefulSets or persistent volumes.

Workers connect to the private head service on port `6379`, which runs the Ray Global
Control Service (GCS). Each Ray process advertises its `NF_POD_IP`. The provider resolves
`<worker-service-id>-headless` to find the worker pod IP. For deployments without
headless DNS, it uses live GCS nodes whose Ray node name matches a registered service
ID. The startup command sets that name from `NF_SERVICE_INTERNAL_ID`; older images can
also match by pod hostname. Service UIDs are checked before discovery. It supports IPv4 and stops a refresh if
DNS returns multiple IPs or multiple live GCS nodes claim the same service.

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

Workers start from a container image. Use `python -m northflank_ray apply cluster.local.yaml`
to create or update the head and Ray Jobs to submit work. `ray up`, `ray down`, `ray exec`, SSH and rsync are unsupported.

By default, workers have 15 minutes to acquire an IP. After that, or once an IP is available,
Ray uses a 120-second heartbeat timeout to replace unresponsive workers. Bad images or unavailable
capacity can cause repeated replacements. Stop the head to stop those attempts.

The adapter retries reads a limited number of times. For workers, it records a pending creation in Redis
before calling Northflank. A successful response identifies the created service, and a follow-up
read supplies its UID. An HTTPX wrapper prevents the SDK from retrying uncertain creates.
If a response is lost, the adapter retains the pending record and stops scaling until an operator
resolves it. A matching name alone does not prove ownership.

Before deletion, the adapter records its intent in Redis. If the head restarts during deletion,
it reads the service UID again before retrying. It removes the record only after Northflank
confirms that the service is absent.

If Redis is unavailable or worker ownership is missing, the adapter stops scaling. On startup,
the head creates an absent registry only when no worker services exist for this cluster.
If workers already exist, restore the registry from a current backup; the adapter does not
rebuild their ownership from service names. Existing head records must match the service UID.
Run only one head per cluster.

API calls run serially. By default, the provider refreshes its service list and DNS every 30 seconds.
Service lists use pages of 100 entries, so API usage grows with the number of services.
Account for API rate limits and available compute capacity when choosing worker limits.

By default, GCS state is held in memory. Enable Redis persistence as described below to retain that metadata.
Long runs still need application checkpoints, task or actor retries, and a plan to recover the head.
Ray drains idle workers before deletion, but that does not guarantee that arbitrary work
finishes. During teardown, stop the head before deleting workers so that Ray cannot replace them.

## GCS Redis persistence

To persist Ray's cluster metadata, enable this option in your cluster configuration:

```yaml
provider:
  # Keep the other provider fields from your configuration.
  gcs_fault_tolerance: true
```

Build an image containing this feature, update both node types' build identifiers, and run:

```bash
python -m northflank_ray apply cluster.local.yaml
```

The head uses its existing `RAY_NF_REDIS_URL` for both the adapter registry and GCS.
Use Redis database 0: omit the database path or use `/0`.
Keep this credential in the head's Northflank secrets. No additional local credential or Redis port forward is needed.
Native GCS storage variables are managed by the adapter and must not be set separately.

The adapter derives a stable GCS storage namespace from the API origin, project ID and cluster UUID.
GCS keys are separate from the adapter registry keys. Keep the cluster UUID unchanged across head restarts.
Use a new UUID for a new cluster. Run only one head for each cluster.
Deleting Northflank services does not remove persisted GCS keys.
Do not reuse old GCS state after running the cluster without persistence. Start a new cluster with a new UUID instead.

Both `redis://` and `rediss://` URLs support a username and password.
For TLS, GCS uses system CA certificates to validate the certificate chain and sends the Redis hostname as SNI.
Ray's Redis client does not provide hostname verification through this setting. Keep Redis on the private network.
For a private CA, set `RAY_REDIS_CA_CERT` or `RAY_REDIS_CA_PATH` on the head.
The adapter registry must also trust that CA. Redis Cluster and Sentinel endpoints are unsupported.

New services receive a 600-second GCS reconnect timeout. Workers connect through the head's DNS name.
You can override `RAY_gcs_rpc_server_reconnect_timeout_s` in a node type's runtime environment.
Existing workers retain their old timeout until replaced.
Before first enabling persistence, stop all jobs and wait for all workers to be removed.
The first persisted head starts a new Ray cluster session. Workers from the previous session cannot reconnect to it.
Keep Redis persistent, backed up and configured with `noeviction`. Redis availability limits GCS recovery.

This persists GCS metadata, not the head's processes, object store or local files.
A job driver running on a lost head cannot resume merely because its metadata survives.
Uploaded application files and local logs are not durable backups. Use application checkpoints and resubmit interrupted jobs.
Northflank must restart the failed head before the workers' reconnect timeout expires.
Before relying on recovery, test a head replacement with your workload and confirm that workers reconnect.

Ray officially supports external Redis fault tolerance with KubeRay for Ray Serve.
This adapter uses Ray's documented `ray start` integration and needs its own recovery validation.
See [Ray GCS fault tolerance](https://docs.ray.io/en/latest/ray-core/fault_tolerance/gcs.html).

## Compatibility and recovery commands

`bootstrap` and `update` remain available for existing scripts. Both default to a preview
and require `--apply` to write changes. `bootstrap` without `--apply` still renders the
head service JSON offline. `update --apply` reapplies settings even when the stored config
is unchanged. Use `apply` for normal setup and updates.

`registry`, `reconcile` and `migrate-metadata` are inspection and recovery tools. They are
not part of the normal deployment flow. Apply does not repair worker ownership records.

### Migration and interrupted creates

To migrate a cluster that stores metadata in descriptions, install this package locally and
supply the API token, `RAY_NF_REDIS_URL` and, if needed, `RAY_NF_REDIS_CONNECT_URL` for a
local port forward. Only these registry inspection/recovery commands need local Redis access.
For TLS forwarding, retain a hostname matching the certificate and resolving to the forwarded
address. Preview the import:

```bash
python -m northflank_ray migrate-metadata cluster.local.yaml
```

Stop application work, pause the head and wait for its pod to exit. Pausing the head interrupts
job drivers running on it, including when GCS metadata is persisted. Do not delete the services.
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
removes its otherwise empty registry, so apply can be retried. Keep the head stopped until
all pending records are resolved. Never register a service that you cannot identify as your own.

New heads register themselves and do not create pending head records. Pending head records
from older releases still require the recovery procedure above. Build an image containing
this startup change before creating a head with the API-only apply command; older images
expect the registry to have been initialized by the CLI.

## Application configuration

Match Ray CPU and memory declarations to the Northflank compute plan. Leave memory for
the object store, Ray processes and application overhead. Ray resource declarations
control scheduling. They do not change the Northflank compute plan.

Install application dependencies and include required files in the container image.
The adapter does not run VM setup commands or copy files through `file_mounts`.
Use `--working-dir` when submitting a job to upload its application code.

Ray uses GCS on port `6379`. The Redis addon stores adapter metadata and, when enabled, GCS metadata.
GCS persistence requires the configuration above. Head recovery still needs workload testing.
Provision application databases, object storage and cloud permissions separately.
Use Northflank secret groups to supply their connection details.

## References

- [Ray NodeProvider interface](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/autoscaler/node_provider.py)
- [Ray V1 autoscaler](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/autoscaler/_private/autoscaler.py)
- [Ray startup CLI](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/scripts/scripts.py)
- [Northflank API](https://northflank.com/docs/v1/api)
- [Northflank Python SDK 2.0.0](https://pypi.org/project/northflank/2.0.0/)
