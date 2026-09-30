<!-- llm-readme-management spec=1 commit=dc78473b2c4ffc540c01eccd241c750e87e8033e template=terraform model=qwen3.8-27b-q4 digest=f68682b55584 generated=2026-09-30T13:43:26Z -->
<a href="https://hauke.cloud" target="_blank"><img src="https://img.shields.io/badge/home-hauke.cloud-brightgreen" alt="hauke.cloud" style="display: block;" /></a>
<a href="https://github.com/hauke-cloud" target="_blank"><img src="https://img.shields.io/badge/github-hauke.cloud-blue" alt="hauke.cloud Github Organisation" style="display: block;" /></a>
<a href="https://github.com/hauke-cloud/llm-readme-management" target="_blank"><img src="https://img.shields.io/badge/template-terraform-orange" alt="Repository type - terraform" style="display: block;" /></a>


# Template Repository


<img src="https://raw.githubusercontent.com/hauke-cloud/.github/main/resources/img/organisation-logo-small.png" alt="hauke.cloud logo" width="109" height="123" align="right">


<llm header hint="Say whether this is a reusable module or a root module that owns real state.">

This Go Kubernetes operator manages Zigbee IoT devices behind Tasmota bridges via MQTT. It reconciles `MQTTBridge` and `Device` custom resources, auto-discovers devices on the Zigbee radio, and syncs friendly names to the hardware. It is a root module that owns real state in your cluster, deployed via a bundled Helm chart for operators running their own infrastructure.

</llm>


## :book: Description

<llm description>

This repository is a Kubernetes operator that manages Zigbee IoT devices behind Tasmota bridges. You describe your bridges and devices with `MQTTBridge` and `Device` custom resources (group `iot.hauke.cloud`); the operator maintains the MQTT connections, discovers devices on the radio, and keeps metadata in sync so you do not have to track each device manually.

For each `MQTTBridge` CR the operator opens a persistent MQTT connection (optional TLS, credentials from a Kubernetes Secret) and, for Tasmota Zigbee controllers, publishes discovery commands every 30 seconds. Discovered devices become `Device` CRs keyed by IEEE address. When you change a device's `spec.friendlyName`, the operator publishes the matching Tasmota `ZbName` command so the physical device reflects the new name.

- Maintains and reconnects MQTT connections per `MQTTBridge` CR, including TLS and Secret-based credentials.
- Discovers Zigbee devices on Tasmota bridges and creates or updates `Device` CRs with model, manufacturer, and reachability.
- Syncs `spec.friendlyName` changes back to Tasmota via `ZbName` commands.
- Tracks bridge connectivity by parsing Tasmota `STATE` messages and updating `MQTTBridge` status.
- Auto-installs and updates its own CRDs at startup, so no separate CRD deployment step is required.

The CR types are defined in the separate `github.com/hauke-cloud/kubernetes-iot-api` module; this repository is the controller that acts on them within the broader hauke.cloud platform.

</llm>


## :clipboard: Requirements

<llm requirements hint="Give the Terraform version from .terraform-version and the provider constraints from versions.tf, plus the credentials the providers need.">

- Go 1.25.3 (pinned in `go.mod`) for building, testing, and regenerating manifests.
- controller-gen v0.20.1 (pinned in the Makefile) for `make manifests` and `make generate`.
- A reachable Kubernetes cluster with a valid kubeconfig.
- RBAC covering full CRUD on `iot.hauke.cloud` devices and mqttbridges (including status and finalizers), get/list/watch on secrets, and create/get/list/update/patch on customresourcedefinitions.
- An MQTT broker reachable from the cluster pods.
- (Optional) A Kubernetes Secret with `username` and `password` keys for MQTT authentication.
- A Tasmota device acting as a Zigbee (Z2M) controller.
- Docker (or another container tool) for `make docker-build` / `docker-push`.
- kubectl for `make install` / `make deploy`.
- pre-commit for contributors.
- Terraform 1.9 and OpenTofu 1.8.0 are pinned in version files, but no `.tf` manifests exist in the repository; they are not required to build or run anything here.

</llm>


## 🚀 Getting started

<llm getting_started hint="terraform init, plan and apply, with the backend configuration the repository actually uses. Say plainly if apply touches real infrastructure.">

You need Go 1.25, a reachable Kubernetes cluster with a kubeconfig at `~/.kube/config`, and an MQTT broker the cluster can reach. The operator auto-installs its CRDs at startup, so no separate install step is required.

1. Clone the repository.

```bash
git clone https://github.com/hauke-cloud/mqtt-device-manager.git
cd mqtt-device-manager
```

2. Build the operator binary.

```bash
make build
```

3. Run the operator from your host; it connects to the cluster via `~/.kube/config`, installs the `iot.hauke.cloud` CRDs, and begins reconciling `MQTTBridge` and `Device` resources.

```bash
make run
```

Once the process is running, create an `MQTTBridge` custom resource pointing at your Tasmota bridge and the operator will discover Zigbee devices and create corresponding `Device` resources automatically.

</llm>


## :airplane: Usage

<llm usage hint="For a reusable module, the central example is a module block with source, version and the required variables filled in from variables.tf. For a root module, show the workflow instead.">

Once the operator is running in your cluster you interact with it through two custom resources in the `iot.hauke.cloud` group.

**Deploy the operator**

Install the Helm chart that ships in the repository:

```bash
helm install mqtt-device-manager ./deployments/helm/mqtt-device-manager \
  --namespace mqtt-device-manager \
  --create-namespace
```

The chart deploys one replica of `ghcr.io/hauke-cloud/mqtt-device-manager` with leader election enabled. The operator installs or updates the `MQTTBridge` and `Device` CRDs automatically at startup, so no separate `kubectl apply` step is required. For local development you can run the operator directly against your kubeconfig:

```bash
make run
```

**Define a Tasmota bridge**

Create an `MQTTBridge` resource pointing at your Tasmota device. The operator opens an MQTT connection, subscribes to state and result topics, and begins periodic Zigbee discovery every 30 seconds.

```yaml
apiVersion: iot.hauke.cloud/v1alpha1
kind: MQTTBridge
metadata:
  name: living-room
  namespace: iot
spec:
  host: 192.168.1.42
  port: 1883
  deviceType: tasmota
  bridgeName: TasmotaLivingRoom
  credentialsSecretRef:
    name: mqtt-creds
    namespace: iot
```

The referenced Secret must contain `username` and `password` keys.

**Rename a discovered device**

Discovery creates `Device` CRs named `device-<bridge>-<ieee-address>`. To change a device's name on the Tasmota bridge, update `spec.friendlyName`:

```yaml
apiVersion: iot.hauke.cloud/v1alpha1
kind: Device
metadata:
  name: device-living-room-0x00124b0023456789
  namespace: iot
spec:
  bridgeRef:
    name: living-room
  ieeeAddr: "0x00124b0023456789"
  friendlyName: Kitchen Valve
```

The operator publishes a `ZbName` command to the bridge on the next reconcile.

</llm>


## :wrench: Configuration

<llm configuration hint="A table of the variables in variables.tf: name, type, default, required. Point at variables.tf for the full set and mention outputs.tf if it exists.">

No Terraform variables exist in this repository; no `.tf` files are present. Configuration is set through CLI flags, Helm values, and CRD fields.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--metrics-bind-address` | string | `:8080` | Prometheus metrics endpoint |
| `--health-probe-bind-address` | string | `:8081` | Healthz/readyz endpoint |
| `--leader-elect` | bool | `false` | Enable leader election |
| `--log-level` | string | `info` | Log level (`debug`/`info`/`warn`/`error`) |

Helm values (full set in `deployments/helm/mqtt-device-manager/values.yaml`):

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `replicaCount` | int | `1` | Operator replicas |
| `image.repository` | string | `ghcr.io/hauke-cloud/mqtt-device-manager` | Container image |
| `image.tag` | string | *(empty → chart `appVersion`)* | Image tag |
| `operator.leaderElection` | bool | `true` | Passes `--leader-elect` |
| `logging.level` | string | `info` | Passed as `--log-level` |
| `resources.limits` | object | `500m` / `128Mi` | CPU / memory limits |
| `resources.requests` | object | `10m` / `64Mi` | CPU / memory requests |

CRD fields (full schemas in `config/crd/`):

`MQTTBridge.spec`: `host` (string, required), `port` (int32, default `1883`), `deviceType` (enum `tasmota`/`zigbee2mqtt`/`generic`, default `tasmota`), `bridgeName` (string), `credentialsSecretRef` (object), `tls.enabled` (bool, default `false`), `topics[]` (list of `{topic, type, qos}`).

`Device.spec`: `bridgeRef.name` (string, required), `ieeeAddr` (string, required), `friendlyName` (string), `disabled` (bool, default `false`).

</llm>


## :hammer: Development

<llm development hint="Cover terraform fmt, validate, tflint and terraform-docs where the repository configures them.">

Before pushing, install the pre-commit hooks and run them against every file:

```bash
pre-commit install
pre-commit run --all-files
```

The configured hooks (pre-commit-hooks v4.4.0, gitleaks v8.18.0) check formatting and scan for secrets. A PR that introduces a secret will be rejected.

CI also enforces a conventional-commit title on every pull request: the subject must start with an uppercase letter and the type must be one of `fix`, `feat`, `docs`, `ci`, or `chore`.

Run the same checks CI runs before opening a PR:

```bash
gofmt -s -l . && go vet ./...
go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
```

The Makefile wraps these as `make fmt`, `make vet`, and `make test`.

If you change Go types or add new CRD fields, regenerate the manifests and deep-copy functions and commit the output:

```bash
make manifests
make generate
```

Both targets invoke controller-gen v0.20.1. `make manifests` writes the CRD YAML into `config/crd/`; `make generate` updates the deep-copy methods. Because CI compiles the module with `go vet` and `go test`, stale generated code will fail the build.

</llm>


## 📄 License

This Project is licensed under the GNU General Public License v3.0

- see the [LICENSE](LICENSE) file for details.


## :coffee: Contributing

To become a contributor, please check out the [CONTRIBUTING](CONTRIBUTING.md) file.


## :email: Contact

For any inquiries or support requests, please open an issue in this
repository or contact us at [contact@hauke.cloud](mailto:contact@hauke.cloud).
