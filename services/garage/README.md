# Garage S3 Object Storage Service

Garage is a lightweight, distributed, S3-compatible object storage server written in Rust by Deuxfleurs, designed for high reliability, low resource consumption, and self-hosting.

In Daisy Streams, Garage serves as the **state persistence engine** storing synchronization watermarks and batch metadata at `s3://daisy-state/{table}/state.json`.

---

## 1. Service Configuration

- **Docker Image**: `dxflrs/garage:v2.4.1`
- **S3 API Port**: `3900`
- **RPC Port**: `3901`
- **Admin/Metrics API Port**: `3903`
- **Configuration File**: [`services/garage/garage.toml`](./garage.toml) (mounted to `/etc/garage.toml`)
- **Default Bucket**: `daisy-state`
- **Default Access Key**: `GKdaisystreamskey01234`
- **Default Secret Key**: `daisystreamssecretkey0123456789abcdef0123456789abcdef`

Garage is started with `--single-node --default-bucket`, which automatically provisions the single-node layout, creates the default API access keys, and initializes the `daisy-state` bucket on startup.

---

## 2. Docker Operations & Debugging Guide

Official Reference: [Garage Documentation — Debugging the Docker container](https://garagehq.deuxfleurs.fr/documentation/operations/debugging/)

### 2.1 The Distroless Architecture

The official `dxflrs/garage` image is built `FROM scratch`:
- It contains **only the statically-linked `/garage` binary**.
- There is **no shell** (`/bin/sh`, `/bin/bash`) and no standard Unix utilities (`ls`, `curl`, `cat`).
- `docker exec -it daisy-garage /bin/sh` **will not work**.

### 2.2 Inspecting Logs

```bash
# Follow live container logs
docker logs -f daisy-garage

# In docker compose:
docker compose logs -f garage
```

To increase log verbosity for troubleshooting, set `RUST_LOG=garage=debug` or `RUST_LOG=garage=trace` in the container environment.

### 2.3 Running `garage` CLI Commands Directly

You do not need a shell to run Garage administrative commands. Invoke the entrypoint binary directly using its absolute path `/garage`:

```bash
# Check node and cluster status
docker exec daisy-garage /garage status

# List buckets
docker exec daisy-garage /garage bucket list

# List access keys
docker exec daisy-garage /garage key list

# Check storage stats
docker exec daisy-garage /garage stats

# Display all available CLI commands
docker exec daisy-garage /garage --help
```

### 2.4 Advanced Debugging via Sidecar Debug Container

If you need to inspect the container's filesystem (such as `/etc/garage.toml`) or run network debugging tools (`curl`, `ping`, `netstat`), launch a throwaway debug container sharing the Garage container's namespaces:

```bash
docker run --rm -it \
  --pid=container:daisy-garage \
  --net=container:daisy-garage \
  alpine sh
```

From inside this Alpine debug container:
1. **Target Filesystem**: The Garage container filesystem is visible under `/proc/1/root/`:
   ```sh
   cat /proc/1/root/etc/garage.toml
   ls -la /proc/1/root/var/lib/garage/
   ```
2. **Network Diagnostics**: You share Garage's network namespace:
   ```sh
   apk add --no-cache curl
   curl -v http://localhost:3900
   ```

---

## 3. Testing S3 Connectivity via Python (`boto3`)

```python
import boto3
import botocore.client

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:3900",
    aws_access_key_id="GKdaisystreamskey01234",
    aws_secret_access_key="daisystreamssecretkey0123456789abcdef0123456789abcdef",
    region_name="garage",
    config=botocore.client.Config(
        s3={"addressing_style": "path"},
        signature_version="s3v4",
    ),
)

# List objects in daisy-state bucket
response = s3.list_objects_v2(Bucket="daisy-state")
print([obj["Key"] for obj in response.get("Contents", [])])
```
