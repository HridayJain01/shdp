# Self-Healing Data Quality Platform (SHDP)

An AI-powered platform that ingests datasets, profiles them, detects anomalies, generates
LLM-driven healing plans, applies fixes automatically, and scores data quality — all with a
React + Tailwind dashboard.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend API | Python 3.11, FastAPI |
| Data Processing | Pandas, NumPy |
| ML / Anomaly Detection | Scikit-learn, IsolationForest, LOF |
| AI Reasoning | OpenRouter API (Claude / GPT-4o) |
| Task Queue | Celery + Redis |
| Storage | PostgreSQL (metadata) + S3-compatible (datasets) |
| Frontend | React 18, Tailwind CSS, Recharts |
| Containerisation | Docker + Docker Compose |

---

## Folder Structure

```
shdp/
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   └── v1/
│   │   │       └── routes/
│   │   │           ├── upload.py        # Dataset ingestion endpoints
│   │   │           ├── profile.py       # Profiling trigger & results
│   │   │           ├── heal.py          # Healing plan + execution
│   │   │           ├── quality.py       # Quality score endpoints
│   │   │           └── reports.py       # Before/after comparison, exports
│   │   ├── core/
│   │   │   ├── config.py                # Settings (pydantic-settings)
│   │   │   ├── logging.py               # Structured JSON logging
│   │   │   └── security.py              # API key / JWT auth
│   │   ├── modules/
│   │   │   ├── ingestion/
│   │   │   │   ├── parser.py            # CSV / JSON / Excel → DataFrame
│   │   │   │   └── validator.py         # Schema & size validation
│   │   │   ├── profiling/
│   │   │   │   ├── profiler.py          # Statistical + categorical profiling
│   │   │   │   └── schema_detector.py   # Auto-detect types & constraints
│   │   │   ├── anomaly/
│   │   │   │   ├── detector.py          # Orchestrator
│   │   │   │   ├── statistical.py       # Z-score, IQR, missing-rate checks
│   │   │   │   └── ml_detector.py       # IsolationForest, LOF
│   │   │   ├── healing/
│   │   │   │   ├── planner.py           # LLM-driven plan generation
│   │   │   │   ├── executor.py          # Apply plan to DataFrame
│   │   │   │   └── strategies/
│   │   │   │       ├── imputation.py    # Null filling strategies
│   │   │   │       ├── normalization.py # Range / z-score normalisation
│   │   │   │       ├── deduplication.py # Exact + fuzzy dedup
│   │   │   │       └── type_coercion.py # Type casting & format fixes
│   │   │   ├── scoring/
│   │   │   │   ├── scorer.py            # Composite quality score (0-100)
│   │   │   │   └── metrics.py           # Completeness, validity, uniqueness…
│   │   │   ├── ai/
│   │   │   │   ├── openrouter_client.py # Async HTTP client for OpenRouter
│   │   │   │   ├── reasoning.py         # Build context & call LLM
│   │   │   │   └── prompts.py           # Prompt templates
│   │   │   └── reporting/
│   │   │       ├── comparison.py        # Before vs after diff
│   │   │       └── visualizer.py        # Chart-ready JSON payloads
│   │   ├── models/                      # Pydantic schemas
│   │   │   ├── dataset.py
│   │   │   ├── profile.py
│   │   │   ├── anomaly.py
│   │   │   ├── healing.py
│   │   │   └── quality.py
│   │   ├── db/
│   │   │   ├── base.py                  # SQLAlchemy declarative base
│   │   │   ├── session.py               # Async session factory
│   │   │   └── repositories/
│   │   │       ├── dataset_repo.py
│   │   │       └── job_repo.py
│   │   ├── tasks/
│   │   │   ├── worker.py                # Celery app instance
│   │   │   └── pipeline.py              # Full pipeline as Celery chain
│   │   └── main.py                      # FastAPI app factory
│   ├── tests/
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── components/
│   │   │   ├── Upload/
│   │   │   ├── Dashboard/
│   │   │   ├── ProfileReport/
│   │   │   ├── HealingPanel/
│   │   │   └── QualityScore/
│   │   ├── pages/
│   │   ├── hooks/
│   │   ├── services/       # Axios API client
│   │   └── store/          # Zustand global state
│   ├── Dockerfile
│   └── package.json
├── docker-compose.yml
└── .env.example
```

---

## Module Responsibilities

### `ingestion/`
Accepts multipart file upload (CSV, JSON, XLSX). `parser.py` converts any format into a
normalised Pandas DataFrame. `validator.py` enforces row-count, column-count, and file-size
limits before the pipeline proceeds.

### `profiling/`
`profiler.py` computes per-column statistics: null rate, cardinality, min/max/mean/std,
top-N values, data type distribution. `schema_detector.py` infers semantic types (email,
phone, date, numeric ID) beyond pandas dtype to power smarter healing.

### `anomaly/`
`statistical.py` flags nulls above threshold, outliers via Z-score and IQR, constant columns,
and duplicate rows. `ml_detector.py` runs IsolationForest and LocalOutlierFactor for
multivariate anomaly detection. `detector.py` merges results into a unified `AnomalyReport`.

### `healing/`
`planner.py` serialises the profiling + anomaly report into a structured prompt and calls the
LLM (via `ai/`) to produce a ranked, JSON-structured healing plan. `executor.py` interprets
the plan and dispatches each action to the correct strategy in `strategies/`. Strategies are
pluggable — new ones can be added without touching the executor.

### `ai/`
`openrouter_client.py` is a thin async `httpx` wrapper around the OpenRouter chat completion
endpoint. `reasoning.py` builds the full prompt context (profile summary + anomaly list) and
parses the LLM JSON response. `prompts.py` holds versioned, parameterised prompt templates.

### `scoring/`
`metrics.py` defines five pillars: **Completeness**, **Validity**, **Uniqueness**,
**Consistency**, **Timeliness**. `scorer.py` computes a weighted composite score (0–100)
both pre- and post-healing, enabling a clear delta measurement.

### `reporting/`
`comparison.py` diffs the original and healed DataFrames, producing a cell-level change log.
`visualizer.py` converts profiling and scoring data into chart-ready JSON consumed directly
by the React frontend (no extra transformation needed).

### `tasks/`
Long-running pipeline runs are executed as a Celery chain:
`parse → profile → detect → plan → heal → score → report`. Each step updates job status in
PostgreSQL so the frontend can poll for live progress.

---

## High-Level Data Flow

```
User uploads file (CSV / JSON / Excel)
        │
        ▼
[Ingestion] parser.py + validator.py
        │  DataFrame + metadata
        ▼
[Profiling] profiler.py + schema_detector.py
        │  ProfileReport (per-column stats, types)
        ▼
[Anomaly Detection] statistical.py + ml_detector.py
        │  AnomalyReport (issues list with severity)
        ▼
[AI Reasoning] openrouter_client.py → LLM
        │  HealingPlan (ordered list of actions + rationale)
        ▼
[Healing Executor] executor.py → strategies/*
        │  Healed DataFrame
        ▼
[Quality Scoring] scorer.py  ×2 (before & after)
        │  QualityScore {before: N, after: M, delta: +D}
        ▼
[Reporting] comparison.py + visualizer.py
        │  ComparisonReport + chart JSON
        ▼
[FastAPI Response / WebSocket push]
        │
        ▼
[React Dashboard] Upload → Profile view → Anomaly list
                  → Healing plan review → Score card
                  → Before/After diff table → Charts
```
