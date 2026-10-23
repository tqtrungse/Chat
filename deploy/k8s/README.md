# Hub service identity on Kubernetes

These manifests are deployment examples for the Noise NK v2 runtime. Applying
them and provisioning cluster Secrets remain operator actions.

Use `hub-statefulset.example.yaml`, not an ordinary Deployment: the current
`MachineIDFromHostname` derives numeric Hub IDs from StatefulSet ordinals.
With SERVICE_ID=1 and HUB_ID=0, hub-0/hub-1 get distinct IDs. They share one
Noise **service** keyring but keep separate connection state. Never assign
the same explicit HUB_ID to all replicas.

1. Generate a service key once with `scripts/hub/gen-noise-key.go` as documented
   in `docs/protocol/noise-v2.md`. Set address to the real public TCP endpoint.
2. Provision the private `hub-noise` Secret outside Git, for example:

   ```sh
   kubectl create secret generic hub-noise \
     --from-file=keyring.json=cmd/hub/.local/service.noise-keyring.json
   ```

3. Provision `hub-runtime` with `config.env` containing the current Hub config,
   including IAM ticket verification public keys and Redis/NATS settings. No AES
   ticket encryption keys are needed.
4. Supply the **public** descriptor to IAM as a ConfigMap/file, mounted at
   `/etc/iam/hub/descriptor.json`, then configure HUB_NOISE_DESCRIPTOR_FILE.
   IAM never receives the hub-noise Secret. Public descriptor files need not
   have private-file permissions.
5. Replace the placeholder Hub image/entrypoint, verify resource settings and
   select a namespace before applying the two manifests with your usual workflow.
   Secret volume mode 0440 plus fsGroup=10001 gives the non-root process read access.

The headless Service supplies stable internal pod names. The public Service
forwards TCP port 9000 to pod port 3008; it does not terminate Noise. Configure
DNS for the descriptor address using your real LoadBalancer address. Kubernetes
Service routing does not inherently guarantee least-connections scheduling;
that requires a suitable L4 proxy/provider configuration. IAM returns a common
service endpoint in this design, not the address of a selected pod.

The TCP readiness probe confirms only that a listener accepts TCP. Before
switching IAM to a new key, verify that all replicas have loaded that key in
the eventual Noise-enabled runtime. Changes to a projected Secret do not
automatically reload the in-memory keyring; perform the controlled rollout.

Rotation order: all Hub pods accept A+B -> IAM advertises B -> wait for old
ticket TTL/skew/pre-auth/deployment overlap -> remove A from new-handshake
support. Existing transport sessions do not require static key A to continue.

Private keys and runtime Secret manifests must not be committed. Use your
cluster's Secret encryption and access controls; only the Hub workload needs
the private keyring. No Kubernetes resources are created by the Go generator.
