<!-- llm-readme-management spec=1 commit=dc78473b2c4ffc540c01eccd241c750e87e8033e template=terraform model=qwen3.8-27b-q4 digest=f68682b55584 generated=2026-10-12T09:49:11Z -->
<a href="https://hauke.cloud" target="_blank"><img src="https://img.shields.io/badge/home-hauke.cloud-brightgreen" alt="hauke.cloud" style="display: block;" /></a>
<a href="https://github.com/hauke-cloud" target="_blank"><img src="https://img.shields.io/badge/github-hauke.cloud-blue" alt="hauke.cloud Github Organisation" style="display: block;" /></a>
<a href="https://github.com/hauke-cloud/llm-readme-management" target="_blank"><img src="https://img.shields.io/badge/template-terraform-orange" alt="Repository type - terraform" style="display: block;" /></a>


# Template Repository


<img src="https://raw.githubusercontent.com/hauke-cloud/.github/main/resources/img/organisation-logo-small.png" alt="hauke.cloud logo" width="109" height="123" align="right">


<llm header hint="Say whether this is a reusable module or a root module that owns real state.">

This Go Kubernetes operator manages `MQTTBridge` and `Device` custom resources in the `iot.hauke.cloud` API group, maintaining MQTT connections to Tasmota Zigbee bridges and auto-discovering their endpoints as cluster objects. It is a standalone deployment, shipped as a Helm chart, for operators of a Kubernetes cluster running the hauke.cloud IoT stack.

</llm>


## :book: Description

<llm description>

mqtt-device-manager is a Kubernetes operator for clusters running the hauke.cloud IoT stack. You define an `MQTTBridge` custom resource pointing at a Tasmota Zigbee bridge, and the operator discovers the endpoints on that bridge and represents each one as a `Device` custom resource in the `iot.hauke.cloud` API group.

At startup it installs its two CRDs, then runs two reconcilers. For each `MQTTBridge` it opens a persistent MQTT connection (Paho client, auto-reconnect, optional TLS) and, for `deviceType: tasmota`, subscribes to state and result topics and runs a 30-second discovery loop. For each `Device` it syncs the `friendlyName` back to the bridge.

- Maintains a persistent MQTT connection per bridge, with credentials from a Kubernetes Secret.
- Discovers Zigbee endpoints via Tasmota `ZbStatus1`/`ZbStatus3` and creates or updates `Device` CRs with model, manufacturer, reachability, and link-quality status.
- Syncs `Device.spec.friendlyName` to the bridge via Tasmota `ZbName`.
- Updates `MQTTBridge.status` (connection state, last-connected time, RSSI/uptime) from Tasmota `STATE` messages.

Only `deviceType: tasmota` bridges receive discovery and state handling; `zigbee2mqtt` and `generic` bridges connect but no further processing is implemented. The operator sits alongside the MQTT broker and Tasmota bridges in the hauke.cloud stack.

</llm>


## :clipboard: Requirements

<llm requirements hint="Give the Terraform version from .terraform-version and the provider constraints from versions.tf, plus the credentials the providers need.">

- Go 1.25.3 or later (pinned in `go.mod`).
- A reachable Kubernetes cluster with a kubeconfig at `~/.kube/config` or the current context.
- RBAC for the operator's service account: manage `customresourcedefinitions`, `iot.hauke.cloud` `devices` and `mqttbridges` (including status and finalizers), and `secrets` (get, list, watch).
- An MQTT broker reachable from the cluster. For device discovery and friendly-name sync, a Tasmota Zigbee bridge that supports `ZbStatus1`, `ZbStatus3`, and `ZbName` commands.
- Docker (or another container tool) for `make docker-build` and `make docker-push`.
- `pre-commit` for contributors, as required by `CONTRIBUTING.md`.

</llm>


## 🚀 Getting started

<llm getting_started hint="terraform init, plan and apply, with the backend configuration the repository actually uses. Say plainly if apply touches real infrastructure.">

1. Clone the repository and enter the project directory.

```bash
git clone https://github.com/hauke-cloud/mqtt-device-manager.git
cd mqtt-device-manager
```

2. Build the operator binary; this also runs CRD codegen, formatting, and vetting.

```bash
make build
```

3. Install the `iot.hauke.cloud` CRDs into your current Kubernetes context.

```bash
make install
```

4. Run the operator against the cluster pointed to by `~/.kube/config`.

```bash
make run
```

The operator starts, ensures the `devices.iot.hauke.cloud` and `mqttbridges.iot.hauke.cloud` CRDs are present, and begins reconciling any `MQTTBridge` and `Device` custom resources it finds. For it to connect to a broker and discover Zigbee endpoints, you need a reachable MQTT broker and at least one `MQTTBridge` CR with `deviceType: tasmota` and a valid `credentialsSecretRef`.

</llm>


## :airplane: Usage

<llm usage hint="For a reusable module, the central example is a module block with source, version and the required variables filled in from variables.tf. For a root module, show the workflow instead.">

Once the operator is running in your cluster, you interact with it through two custom resources in the `iot.hauke.cloud/v1alpha1` API group.

**Deploy the operator**

The Helm chart lives in `deployments/helm/mqtt-device-manager`. Install it with:

```bash
helm install mqtt-device-manager ./deployments/helm/mqtt-device-manager \
  --set image.repository=ghcr.io/hauke-cloud/mqtt-device-manager \
  --set image.tag=1.0.0
```

The chart creates a Deployment, ServiceAccount, ClusterRole, and ClusterRoleBinding. Leader election is enabled by default; the log level defaults to `info`.

**Register a Tasmota bridge**

Create an `MQTTBridge` resource pointing at your broker. The operator opens a persistent MQTT connection, subscribes to the bridge's state topics, and runs a 30-second Zigbee discovery loop:

```yaml
apiVersion: iot.hauke.cloud/v1alpha1
kind: MQTTBridge
metadata:
  name: living-room
spec:
  host: mqtt.example.com
  port: 1883
  deviceType: tasmota
  bridgeName: tasmota-living-room
  credentialsSecretRef:
    name: mqtt-bridge-creds
```

The referenced Secret must contain `username` and `password` keys. When no `topics` are listed, the operator subscribes to `stat/tasmota-living-room/STATE` and `stat/tasmota-living-room/RESULT`.

**Name a discovered device**

Discovery creates `Device` CRs named `device-<bridge>-<ieeeAddr>`. Set `spec.friendlyName` to rename the endpoint on the Tasmota bridge:

```yaml
apiVersion: iot.hauke.cloud/v1alpha1
kind: Device
metadata:
  name: device-living-room-0x00158d0001a2b3c4
spec:
  friendlyName: Kitchen Motion Sensor
```

The operator publishes `ZbName` with the IEEE address and new name to `cmnd/tasmota-living-room/ZbName`.

</llm>


## :wrench: Configuration

<llm configuration hint="A table of the variables in variables.tf: name, type, default, required. Point at variables.tf for the full set and mention outputs.tf if it exists.">

This repository contains no Terraform or OpenTofu code. The `.opentofu-version` (1.8.0) and `.terraform-version` (1.9) files are template leftovers; no `*.tf` or `*.tfvars` files exist.

Configuration is provided through CLI flags, Helm values, and CRD specs.

**CLI flags** (`cmd/main.go`):

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `--metrics-bind-address` | string | `:8080` | Prometheus metrics bind address |
| `--health-probe-bind-address` | string | `:8081` | Health/ready probe bind address |
| `--leader-elect` | bool | `false` | Enable leader election |
| `--log-level` | string | `info` | Log level: `debug`, `info`, `warn`, `error` |

**Helm values** (full set in `deployments/helm/mqtt-device-manager/values.yaml`):

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `replicaCount` | int | `1` | Operator pod replicas |
| `image.repository` | string | `ghcr.io/hauke-cloud/mqtt-device-manager` | Container image |
| `image.tag` | string | `""` (falls back to `appVersion` 1.0.0) | Image tag |
| `operator.leaderElection` | bool | `true` | Maps to `--leader-elect` |
| `operator.metrics.port` | int | `8080` | Metrics port |
| `operator.health.port` | int | `8081` | Health probe port |
| `logging.level` | string | `info` | Pod env `LOG_LEVEL` (not read by the binary) |
| `rbac.rules` | list | `[]` | Extra ClusterRole rules |
| `resources` | object | limits 500m/128Mi, requests 10m/64Mi | Pod resources |

Runtime device and bridge configuration is expressed in the `MQTTBridge` and `Device` CRD specs (group `iot.hauke.cloud/v1alpha1`). See `config/crd/iot.hauke.cloud_mqttbridges.yaml` and `config/crd/iot.hauke.cloud_devices.yaml` for the full schema.

</llm>


## :hammer: Development

<llm development hint="Cover terraform fmt, validate, tflint and terraform-docs where the repository configures them.">

Before pushing, install the pre-commit hooks and run them locally:

```bash
pre-commit install
pre-commit run --all-files
```

The configured hooks (pre-commit-hooks v4.4.0, gitleaks v8.18.0) check formatting and scan for secrets. A pull request whose title does not match the conventional-commit pattern enforced by `.github/workflows/pr-title.yml` (allowed types: `fix`, `feat`, `docs`, `ci`, `chore`) will be rejected.

Run the test suite and linters:

```bash
make test
make fmt
make vet
```

CI additionally runs `go test -v -race -coverprofile=coverage.out -covermode=atomic ./...` and `gofmt -s -l . && go vet ./...`. Note the `-s` (simplify) flag on `gofmt`; `make fmt` does not apply it, so code that passes locally can still fail CI.

If you change Go types or CRD schemas, regenerate the checked-in artefacts and commit them:

```bash
make generate
make manifests
```

`make generate` rewrites deepcopy methods via controller-gen v0.20.1; `make manifests` regenerates the CRD YAMLs in `config/crd/`. CI runs both before the build step, so stale generated files will fail the pipeline.

The CRD YAML is also embedded under `cmd/crds/` for runtime self-install. The Makefile does not copy regenerated files there, so keep both copies in sync manually.

</llm>


## 📄 License

This Project is licensed under the GNU General Public License v3.0

- see the [LICENSE](LICENSE) file for details.


## :coffee: Contributing

To become a contributor, please check out the [CONTRIBUTING](CONTRIBUTING.md) file.


## :email: Contact

For any inquiries or support requests, please open an issue in this
repository or contact us at [contact@hauke.cloud](mailto:contact@hauke.cloud).
