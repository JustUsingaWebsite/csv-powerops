# CSV PowerOps — Roadmap & Setup Guide

## Vision

CSV PowerOps will be an all-in-one CSV toolkit with:

* **Web UI** for login, file upload, and advanced CSV operations.
* **SSH TUI** for text-based interaction.
* **CLI tool** for automation.
* **Backend in Go** for concurrency, stability, and API services.
* **Worker system** to handle heavy CSV processing (Go core, with optional Python workers for analytics).
* **Zero-downtime updates** with containerized deployment.

Initially, we will focus on **core backend CSV functions** with no UI or API.

---

## Roadmap

### Stage 1 — Core Backend Functions (No UI, No API)

Implement reusable Go functions for:

1. **List Cross-Referencing**
   Check if items in one CSV exist in another CSV by matching a column key.

2. **Find Unique Data (Set Difference)**
   Identify rows in file #2 that are not in file #1.

3. **Find Common Data (Set Intersection)**
   Identify rows that exist in both files.

* Input: Local CSV files.
* Output: Processed CSV files saved locally.

---

### Stage 2 — API Layer & Job Queue

* Create Go-based REST API for CSV operations.
* Integrate job queue (NATS, RabbitMQ, or Redis) for async processing.
* Store CSV files in S3-compatible object storage (e.g., MinIO).

---

### Stage 3 — TUI (SSH Access)

* Build TUI using **Bubble Tea** or **tview** in Go.
* Allow SSH logins directly into the TUI via `gliderlabs/ssh`.

---

### Stage 4 — Web UI

* Build SPA in Vanilla JS or React.
* Features: File upload, function selection, result download.

---

### Stage 5 — Deployment & Scaling

* Containerize with Docker.
* Use Kubernetes for rolling updates & zero downtime.
* Add metrics, monitoring, and logging.

---

## Directory Structure

```
csv-powerops/
│
├── backend/
│   ├── cmd/                # CLI entrypoints for dev/testing
│   ├── internal/
│   │   ├── csvops/         # Core CSV processing functions
│   │   ├── utils/          # Helper functions
│   │   └── auth/           # Auth logic (later stages)
│   ├── go.mod              # Go module definition
│   └── go.sum
│
├── frontend/               # Web UI code (Stage 4)
│
├── tui/                    # TUI code (Stage 3)
│
├── deployments/            # Docker/K8s configs (Stage 5)
│
├── scripts/                # Dev/build scripts
│
├── README.md               # This roadmap
└── .gitignore
```

---

## How to Get Started (Development)

### Step 1 — Prerequisites

Ensure you have installed:

* **Git** ([https://git-scm.com/downloads](https://git-scm.com/downloads))
* **Go (latest)** ([https://go.dev/dl/](https://go.dev/dl/))
* **Docker** ([https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop))
* **VS Code** ([https://code.visualstudio.com/](https://code.visualstudio.com/))
* **Make** (optional, for easier builds)

---

### Step 2 — Clone & Setup Project

```bash
# Clone repository
git clone https://github.com/<your-username>/csv-powerops.git
cd csv-powerops

# Initialize Go module (inside backend)
cd backend
go mod init github.com/<your-username>/csv-powerops
go mod tidy
```

---

### Step 3 — Running Core Functions

For Stage 1, we will:

1. Place CSV files in a `data/` folder.
2. Call functions from `backend/internal/csvops/` via CLI.
3. Output will be saved to `output/`.

Example run (after implementing functions):

```bash
go run cmd/main.go crossref data/list.csv data/master.csv --key Email
```

---

### Step 4 — Future Deployment (Server Install)

For installing on a server:

```bash
# Install Go
download and install from https://go.dev/dl/

# Clone repo
git clone https://github.com/<your-username>/csv-powerops.git

# Build binary
cd csv-powerops/backend
go build -o csv-powerops

# Run binary
./csv-powerops
```

Docker/Kubernetes setup will be documented in Stage 5.

---