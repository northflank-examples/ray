import hashlib
import json
import os
import sys

import ray

ray.init(address='auto', namespace='nf-ray-live-test')

@ray.remote(num_cpus=2, max_restarts=1)
class Worker:
    def identity(self):
        return {'node_id': ray.get_runtime_context().get_node_id(), 'pod_ip': os.environ['NF_POD_IP']}

    def produce(self):
        return [ray.put(b'northflank-ray-transfer' * 1500000)]

    def consume(self, refs):
        value = ray.get(refs[0])
        return {**self.identity(), 'bytes': len(value), 'sha256': hashlib.sha256(value).hexdigest()}

mode = sys.argv[1]
if mode == 'start':
    actors = [Worker.options(name=f'worker-{i}', lifetime='detached').remote() for i in range(2)]
    identities = ray.get([actor.identity.remote() for actor in actors], timeout=900)
    assert len({identity['node_id'] for identity in identities}) == 2, identities
    assert len({identity['pod_ip'] for identity in identities}) == 2, identities
    refs = ray.get(actors[0].produce.remote())
    transfer = ray.get(actors[1].consume.remote(refs))
    assert transfer['bytes'] == 34500000, transfer
    assert transfer['sha256'] == hashlib.sha256(b'northflank-ray-transfer' * 1500000).hexdigest()
    print(json.dumps({'workers': identities, 'transfer': transfer}))
elif mode == 'status':
    actors = [ray.get_actor(f'worker-{i}') for i in range(2)]
    print(json.dumps(ray.get([actor.identity.remote() for actor in actors], timeout=900)))
elif mode == 'stop':
    for i in range(2):
        try:
            ray.kill(ray.get_actor(f'worker-{i}'), no_restart=True)
        except ValueError:
            pass
    print('Released test actors; waiting for idle scale-down is a separate check.')
else:
    raise ValueError('Choose start, status, or stop')
