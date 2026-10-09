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

Pin `buildId` so that every worker uses the same build. The API defaults to
`https://api.northflank.com`. For another Northflank environment, set `provider.api_url`
to its HTTPS origin without `/v1`.

Render the head service definition:

```bash
ray-northflank bootstrap cluster.local.yaml > head-service.json
```

This command writes JSON without calling the API or reading `NF_API_TOKEN`.
Create a project-scoped token with permission to list, read, create, patch and delete services.
Set `NF_API_TOKEN` in your shell, then create the head:

```bash
# Supply NF_API_TOKEN through your shell's secret manager.
ray-northflank bootstrap cluster.local.yaml --apply
```

`--apply` creates a billable head service. The head starts Ray with the YAML configuration
and creates workers within its limits. Running bootstrap again leaves an existing owned
head unchanged. To change its configuration, stop the jobs and head, remove the workers,
then recreate the head.

The bootstrap command stores the token in the runtime secrets of the head. You can also
create the rendered service manually and attach a secret group restricted to that service.
Workers refuse to start if they inherit `NF_API_TOKEN`.

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

The Northflank service ID is the Ray provider node ID. The service description stores
the cluster UUID, node type, role, launch hash and creation time. Do not edit it.
Before deleting a worker, the provider reads the service again to make sure that it owns it.
It rejects the head, foreign services and services whose UID changed.

## Operation

Workers start from a container image. Use `ray-northflank bootstrap` to create the head
and Ray Jobs to submit work. `ray up`, `ray down`, `ray exec`, SSH and rsync are unsupported.

By default, workers have 15 minutes to acquire an IP. After that, or once an IP is available,
Ray uses a 120-second heartbeat timeout to replace unresponsive workers. Bad images or unavailable
capacity can cause repeated replacements. Stop the head to stop those attempts.

The adapter retries reads, patches and deletes a limited number of times. After an uncertain
create response, it looks up the service by name. It does not repeat the create request.
A later refresh discovers services left by an incomplete launch. An HTTPX wrapper prevents
the SDK from retrying uncertain creates.

API calls run serially. By default, the provider refreshes its service list and DNS every 30 seconds.
Service lists use pages of 100 entries, so API usage grows with the number of services.
Account for API rate limits and available compute capacity when choosing worker limits.

The head has no persistent GCS state or high availability. Losing it loses active jobs.
Long runs need application checkpoints, task or actor retries, and a plan to recover the head.
Ray drains idle workers before deletion, but that does not guarantee that arbitrary work
finishes. During teardown, stop the head before deleting workers so that Ray cannot replace them.

## Application configuration

Match Ray CPU and memory declarations to the Northflank compute plan. Leave memory for
the object store, Ray processes and application overhead. Ray resource declarations
control scheduling. They do not change the Northflank compute plan.

Install application dependencies and include required files in the container image.
The adapter does not run VM setup commands or copy files through `file_mounts`.
Use `--working-dir` when submitting a job to upload its application code.

Ray uses GCS on port `6379` and does not require a Redis service for this setup.
Provision application databases, object storage and cloud permissions separately.
Use Northflank secret groups to supply their connection details.

## References

- [Ray NodeProvider interface](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/autoscaler/node_provider.py)
- [Ray V1 autoscaler](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/autoscaler/_private/autoscaler.py)
- [Ray startup CLI](https://github.com/ray-project/ray/blob/ray-2.59.0/python/ray/scripts/scripts.py)
- [Northflank API](https://northflank.com/docs/v1/api)
- [Northflank Python SDK 2.0.0](https://pypi.org/project/northflank/2.0.0/)
