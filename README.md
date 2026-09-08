<!-- llm-readme-management spec=1 commit=dc78473b2c4ffc540c01eccd241c750e87e8033e template=terraform model=qwen3.6-35b-a3b digest=f68682b55584 generated=2026-09-08T22:18:19Z -->
<a href="https://hauke.cloud" target="_blank"><img src="https://img.shields.io/badge/home-hauke.cloud-brightgreen" alt="hauke.cloud" style="display: block;" /></a>
<a href="https://github.com/hauke-cloud" target="_blank"><img src="https://img.shields.io/badge/github-hauke.cloud-blue" alt="hauke.cloud Github Organisation" style="display: block;" /></a>
<a href="https://github.com/hauke-cloud/llm-readme-management" target="_blank"><img src="https://img.shields.io/badge/template-terraform-orange" alt="Repository type - terraform" style="display: block;" /></a>


# Template Repository


<img src="https://raw.githubusercontent.com/hauke-cloud/.github/main/resources/img/organisation-logo-small.png" alt="hauke.cloud logo" width="109" height="123" align="right">


<llm header hint="Say whether this is a reusable module or a root module that owns real state.">

This Go Kubernetes operator manages MQTT IoT device lifecycles and automatically discovers Zigbee endpoints on Tasmota bridges. It reconciles `MQTTBridge` and `Device` custom resources to maintain broker connections, poll for new sensors, and sync configuration back to your cluster. Keep reading if you operate heterogeneous IoT gateways on Kubernetes and need automated device registration.

</llm>


## :book: Description

<llm description>

This repository provides a Kubernetes operator that automates the lifecycle management of MQTT-connected IoT devices. If you operate heterogeneous IoT gateways and need to track Zigbee sensors and actuators without manual configuration, this operator continuously discovers devices attached to Tasmota-based MQTT bridges and represents them as native Kubernetes custom resources. It connects to your MQTT brokers using optional TLS and secret-managed credentials, then polls the bridges for new endpoints every thirty seconds. The operator runs as a single Deployment replica with leader election and automatically installs its Custom Resource Definitions on startup.

- Watches and reconciles `MQTTBridge` and `Device` custom resources in the `iot.hauke.cloud/v1alpha1` API group.
- Establishes persistent MQTT connections with automatic reconnection and TLS support.
- Discovers Zigbee devices on Tasmota bridges via periodic `ZbStatus1` and `ZbStatus3` command polling.
- Syncs `Device.spec.friendlyName` changes back to connected Tasmota bridges over MQTT.

</llm>


## :clipboard: Requirements

<llm requirements hint="Give the Terraform version from .terraform-version and the provider constraints from versions.tf, plus the credentials the providers need.">

Before you can build, deploy, or run this repository, ensure your environment meets the following requirements:
- Go 1.25.3 installed for local compilation and testing
- Docker or an OCI-compatible container tool for building and pushing images
- `kubectl` configured with access to a Kubernetes cluster supporting `apiextensions.k8s.io/v1` CustomResourceDefinitions
- `.terraform-version` pinned to 1.9 and `.opentofu-version` pinned to 1.8.0 (present in the repository root)
- A Kubernetes ServiceAccount with permissions to manage `Device` and `MQTTBridge` custom resources, read Secrets, and perform leader election

</llm>


## 🚀 Getting started

<llm getting_started hint="terraform init, plan and apply, with the backend configuration the repository actually uses. Say plainly if apply touches real infrastructure.">

1. Clone the repository and change into the directory.
```bash
git clone https://github.com/hauke-cloud/mqtt-device-manager.git
cd mqtt-device-manager
```

2. Run the operator locally against your configured Kubernetes cluster to build dependencies, install CRDs, and start controllers.
```bash
make run
```

</llm>


## :airplane: Usage

<llm usage hint="For a reusable module, the central example is a module block with source, version and the required variables filled in from variables.tf. For a root module, show the workflow instead.">

You deploy the operator using the provided Helm chart. Configure the image repository, enable leader election for high availability, and set the logging level before installing into your cluster.

```yaml
# values.yaml
image:
  repository: ghcr.io/hauke-cloud/mqtt-device-manager
  tag: "1.0.0"
operator:
  leaderElection: true
  metrics:
    enabled: true
    port: 8080
logging:
  level: info
  format: json
crds:
  install: true
```

After deployment, create an `MQTTBridge` custom resource to define the broker connection. The operator requires a Kubernetes Secret containing `username` and `password` keys for authentication. Only bridges with `deviceType: tasmota` trigger automatic Zigbee discovery.

```yaml
apiVersion: iot.hauke.cloud/v1alpha1
kind: MQTTBridge
metadata:
  name: living-room-bridge
spec:
  host: 192.168.1.50
  port: 8883
  tls: true
  credentialsSecretRef:
    name: bridge-credentials
    namespace: default
  deviceType: tasmota
  bridgeName: LivingRoom
```

The operator automatically creates `Device` custom resources when it detects Zigbee endpoints via `ZbStatus1` and `ZbStatus3` polling. You can update the `friendlyName` field on a `Device` CR to sync the name back to the Tasmota bridge over MQTT.

```yaml
apiVersion: iot.hauke.cloud/v1alpha1
kind: Device
metadata:
  name: temperature-sensor-01
spec:
  bridgeRef:
    name: living-room-bridge
    namespace: default
  ieeeAddr: "0x00124b001a2b3c4d"
  friendlyName: LivingRoomTemp
  sensorType: temperature
```

</llm>


## :wrench: Configuration

<llm configuration hint="A table of the variables in variables.tf: name, type, default, required. Point at variables.tf for the full set and mention outputs.tf if it exists.">

You configure this repository via CLI flags and a Helm chart, as it contains no Terraform files despite the `.terraform-version` and `.opentofu-version` markers. The primary configuration surface is documented below.

| Name | Type | Default | Description |
|---|---|---|---|
| `--metrics-bind-address` | string | `:8080` | Bind address for the metrics HTTP endpoint |
| `--health-probe-bind-address` | string | `:8081` | Bind address for liveness and readiness probes |
| `--leader-elect` | bool | `false` | Enable leader election for high availability |
| `--log-level` | string | `info` | Logging verbosity (`debug`, `info`, `warn`, `error`) |
| `replicaCount` (Helm) | int | `1` | Number of operator Deployment replicas |
| `image.repository` (Helm) | string | `ghcr.io/hauke-cloud/mqtt-device-manager` | Container image registry path |
| `rbac.create` (Helm) | bool | `true` | Automatically create ClusterRole and bindings |
| `crds.install` (Helm) | bool | `true` | Install CRDs on Helm install/upgrade |
| `monitoring.serviceMonitor.enabled` (Helm) | bool | `false` | Expose a Prometheus ServiceMonitor resource |

Bridge credentials are provided through Kubernetes Secrets referenced by the `MQTTBridge` custom resource. You can override any Helm value in your release configuration, and all CLI flags map directly to their corresponding Helm parameters under the `operator.` prefix. The complete flag definitions reside in `cmd/main.go`, while the full Helm schema is available in `values.yaml`. No Terraform variables or outputs are exposed.

</llm>


## :hammer: Development

<llm development hint="Cover terraform fmt, validate, tflint and terraform-docs where the repository configures them.">

To modify this repository, ensure you have Go 1.25+ and Docker installed. Run the full test suite with:
```bash
make test
```
This target compiles manifests, generates DeepCopy code, and executes all unit tests. Format your Go source files using:
```bash
make fmt
```
Check for static analysis issues with:
```bash
make vet
```
The repository enforces code quality through pre-commit hooks defined in `.pre-commit-config.yaml`. Install them locally with:
```bash
pre-commit install
```
You can verify all hooks against your working tree before committing by running:
```bash
pre-commit run --all-files
```
Several files are generated and must be committed alongside your changes. Whenever you modify the Go types or API definitions, regenerate the CRD manifests and DeepCopy methods:
```bash
make manifests
make generate
```
These commands update `config/crd/` and the internal controller stubs. Always run `make build` to verify that formatting, vetting, and code generation complete successfully before opening a pull request.

</llm>


## 📄 License

This Project is licensed under the GNU General Public License v3.0

- see the [LICENSE](LICENSE) file for details.


## :coffee: Contributing

To become a contributor, please check out the [CONTRIBUTING](CONTRIBUTING.md) file.


## :email: Contact

For any inquiries or support requests, please open an issue in this
repository or contact us at [contact@hauke.cloud](mailto:contact@hauke.cloud).
