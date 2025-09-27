# Airflow

This repository contains a complete stack for running Apache Airflow. It includes the setup for all Airflow components: **Webserver**, **Scheduler**, **Worker**, **MetaDB (PostgreSQL)**, **Redis**, **StatsD Exporter** for metrics and **MinIO** to emulate s3 storage.

---

## Prerequisites

Ensure you have the following installed on your machine:
- Docker  
- Docker Compose  
- `dos2unix` (to convert `.sh` scripts if you use **Windows**)

---

## Directory Setup

Before running `docker-compose`, create the necessary directories:

```bash
mkdir -p dags/ logs/ plugins/
```

If you'd like to make additional data persistent, you can also:

```bash
mkdir -p data
```


> If you add new host folders, adjust the `volumes` section in `docker-compose.yaml` accordingly.

---

## Preparing Shell Scripts

If you edited scripts on Windows, ensure all shell scripts in the `configs` directory are converted to Unix format:

```bash
find ./configs -type f -name "*.sh" -exec dos2unix {} \;
```

---

## Running the Stack

To start the entire stack:

```bash
docker-compose up -d
```

To stop the stack:

```bash
docker-compose down
```

---

## Services and Access

| Service         | URL / Port                               | Login / Password (default) |
|-----------------|-------------------------------------------|----------------------------|
| **Airflow UI**  | <http://localhost:8080>                   | `admin` / `admin` (if not overridden) |
| **StatsD**      | Prometheus metrics: <http://localhost:9102/metrics> | — |
| **MinIO**        | <http://localhost:9001> (console) <br> S3 API: <http://localhost:9000> | `minio-root` / `minio-root` (default) |

> **Note:** The webserver port is `8080` as defined in `docker-compose.yaml` (`8080:8080`). If you change the mapping, update the URL above.

---

## Directory Tree

```
.
|-- README.md
|-- configs
|   |-- airflow.cfg         -- used to override default Airflow .cfg parameters
|   |-- statsd.yaml         -- used to map Airflow metrics for future alerting & monitoring
|   `-- init-db.sh          -- used to initializes the Airflow DB (used by the airflow-init service and postgres)
|-- dags
|   `-- test.py             -- used to store Airflow DAGs
|-- docker-compose.yaml
|-- logs
|   |-- ...
|   `-- scheduler
|       |-- ...
|-- plugins
`-- requirements.txt
```

---

## Configuration Files

- **`configs/airflow.cfg`** — override selected Airflow parameters (e.g., executor, logging, concurrency). Mounted into containers so changes apply on container restart.  
- **`configs/statsd.yaml`** — StatsD mapping used by the exporter to expose Airflow metrics in Prometheus format.  
- **`configs/init-db.sh`** — initializes PostgreSQL roles/DB required by Airflow; needed for the `airflow-init` service to run correctly.  
- **`requirements.txt`** — additional Python deps; installed in the containers via the mounted file and `_PIP_ADDITIONAL_REQUIREMENTS`.

---

## Health Check

Built-in health checks are configured in `docker-compose.yaml`:
- **PostgreSQL:** `pg_isready`
- **Redis:** `redis-cli ping`
- **Webserver:** `GET /health`
- **Scheduler:** `airflow jobs check --job-type SchedulerJob`
- **Worker:** `celery ... inspect ping`

Quick manual checks:

```bash
curl -f http://localhost:8080/health
curl -f http://localhost:9102/metrics
```

---

## Tips

- If port `8080` (webserver) or `9102` (StatsD exporter) is taken, change the **left side** of the port mapping in `docker-compose.yaml` (e.g., `8088:8080`).  
- After editing `requirements.txt`, recreate Airflow containers to ensure packages are installed.  
- Keep DAGs in `dags/` and plugins in `plugins/`; both are mounted into the containers.  
- On Linux hosts, ensure file permissions allow the container user to read/write mounted volumes.

---

## Troubleshooting

- **Airflow UI doesn’t load:**  
  - Ensure `airflow-init` completed successfully (`docker-compose logs airflow-init`).  
  - Check webserver logs: `docker-compose logs airflow-webserver`.  
  - Confirm the port mapping and that nothing else uses the host port.

- **Line-ending issues in scripts:**  
  - Run `dos2unix` on scripts inside `configs/` if they were edited on Windows.

- **Package installation not reflecting:**  
  - Recreate containers after changing `requirements.txt`:  
    ```bash
    docker-compose down
    docker-compose up -d --build
    ```

- **Scheduler/Worker not picking up DAGs:**  
  - Verify DAGs exist in `dags/` and there are no syntax errors.  
  - Check `logs/scheduler` and `logs/dag_processor_manager` for parse errors.  

---
