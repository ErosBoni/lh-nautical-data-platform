# LH Nautical — End-to-End Data Platform

Ingestion, transformation, governance, demand forecasting and a RAG assistant for a fictional
nautical-equipment retailer, built on Databricks + dbt.

> ### ⚠️ Everything here is mock data
> **LH Nautical / TeslaVinci is a fictional company and every record is synthetic.** This repository
> is the delivery of the "Lighthouse" training project (Group 10), built over six months by a team of
> four. There is no real customer, supplier, employee or financial data in it, and the source database
> does not exist outside the training environment — see
> [What is *not* reproducible](#what-is-not-reproducible) before trying to run it.
>
> The project was developed in a company Bitbucket repository; this is a sanitized export, which is
> why there is no commit history. Workspace hosts, e-mail addresses and job ids have been replaced
> with `<placeholders>`.

---

## What this project demonstrates

| Area | What was built |
| :--- | :--- |
| **Idempotent incremental ingestion** | Strategy chosen per table from the shape of its primary key: watermark pagination for single keys, full reload + `MERGE` for composite keys. Re-running never duplicates. |
| **Operational instrumentation** | Control, audit and alert-history tables; consecutive-failure streak detection so a single transient API blip does not page anyone. |
| **LGPD governance** | Dynamic column masking through a Unity Catalog UDF driven by a config file, plus RBAC grants per group. |
| **Dimensional modelling** | 11-model star schema with an SCD2 dimension and an incremental fact that repairs late-arriving surrogate keys through a post-hook MERGE. |
| **BI-ready semantics** | 6 pre-aggregated marts that net out cancellations, returns and ICMS/IPI taxes, deliberately split to avoid fan-out double-counting. |
| **Honest forecasting evaluation** | 11 analysis stages and 5 model families scored under one shared protocol against a held-static baseline on 4 rolling origins — and a conclusion that *no* trained model beat the naive champion, with the reason traced to the data. |
| **Grounded RAG** | Document-level RAG where the LLM is structurally unable to produce a number: every figure is rendered by a deterministic Spark template before indexing. |

---

## Repository structure

```
databricks/
  ingestion/                            # Bundle "grupo10": SQL Server + BCB -> Bronze
    databricks.yml                      # Bundle definition (dev/prod targets)
    resources/ingestion.job.yml         # Ingestion job (4 tasks)
    src/
      tables_config.json                # 24 source tables and their primary keys
      governance_config.json            # Sensitive columns per table (LGPD)
      sqlserver_ingest_setup.ipynb      # Setup task: catalog/schema, audit/control tables, table list
      sqlserver_ingest.ipynb            # Ingests one table (one for_each iteration)
      bcb_exchange_rates_ingest.ipynb   # BCB USD/BRL exchange-rate ingestion
      check_bcb_consecutive_failures.ipynb  # Alerts only after consecutive BCB failures
      governance_setup.ipynb            # Creates the dynamic masking UDF
      apply_rbca.ipynb                  # Applies RBAC grants
      grupo10/                          # Python package: ingest.py + utils/{audit,alerting,logger}.py
    tests/                              # pytest config (Databricks Connect)
  transformation/                       # Bundle "transformation": runs dbt as a Databricks job task
  orchestration/                        # Bundle "orchestration": chains ingestion -> transformation
  rag/                                  # Bundle "rag": corpus, vector index and Streamlit chat app
dbt/                                    # dbt project "desafio_lh": staging -> intermediate -> marts
forecasting/                            # Demand-forecasting analysis (not deployed)
  01_..09_*/                            # Phase 1: exploration, baselines, classification, reconciliation
  10_feature_engineering/               # Stage 2: feature tests and the frozen feature set
  experiments/                          # Stage 3: shared protocol + five model experiments
  11_model_comparison/                  # Stage 3 close-out: final side-by-side comparison
docs/dashboard_guide.md                 # Power BI executive dashboard guide (KPI dictionary)
pyproject.toml                          # Python dependencies (uv)
databricks.local.yml.example            # Template for the per-developer bundle override (required)
profiles.yml.example                    # Template for ~/.dbt/profiles.yml
```

### Architecture

```
SQL Server (mock source)  ─┐
                           ├─> [ingestion]  ──> Bronze (Delta, Unity Catalog)
BCB exchange-rate API     ─┘                         │
                                                     ▼
                                        [transformation] dbt
                                     staging -> intermediate -> marts
                                                     │
                            ┌────────────────────────┼────────────────────────┐
                            ▼                        ▼                        ▼
                      Power BI dashboard      forecasting/ (analysis)   [rag] vector index
                                                                        + Streamlit app
                            └──────── [orchestration] daily 01:00 America/Sao_Paulo ────────┘
```

**Four independent Databricks Asset Bundles** live under `databricks/`, each with its own
`databricks.yml`, deployed and run separately. There is no single root bundle. `orchestration` does
not include the others — it wires them at the Databricks job level through `run_job_task` with
explicit job ids, so ingestion and transformation must already be deployed before it can run.

Every bundle has a `dev` target (the default, writing to per-developer schemas) and a `prod` target
(the `lh_nautical` catalog).

---

## 1. Ingestion — `databricks/ingestion`

The `SQL Server Ingestion` job runs four notebook tasks:

```
setup ──┬──> ingest_tables              (for_each: one run per table, concurrency 5)
        └──> ingest_bcb_exchange_rates  ──(only if it fails)──> check_bcb_consecutive_failures
```

* `setup` creates the catalog, schema, audit and control tables, and publishes the table list from
  `tables_config.json` for the `for_each_task` to fan out over.
* `ingest_tables` and `ingest_bcb_exchange_rates` run in parallel after `setup`.
* `check_bcb_consecutive_failures` runs only when the BCB task fails, and fails on purpose on the
  3rd consecutive failed day so the native `on_failure` e-mail fires on a real outage rather than on
  a single blip.
* `governance_setup.ipynb` and `apply_rbca.ipynb` are standalone — not part of the job.

### Ingestion strategy

`run_incremental_ingestion()` in `src/grupo10/ingest.py` picks the strategy from the primary key of
each of the **24 mapped tables**:

* **Single key** (e.g. `"id"`) → incremental watermark pagination, appended in batches. The
  `_ingestion_control` table tracks `last_processed_id` per table.
* **Composite key** (`stock_levels`, `product_suppliers`, `variant_attribute_values`) → full reload
  through `_merge_into_delta()`, keyed on every PK column.

All writes are idempotent, so a rerun cannot duplicate rows. A batch failure re-raises, which marks
the Databricks task failed and allows a selective rerun of just the affected table.

| Internal table | Role |
| :--- | :--- |
| `_ingestion_control` | Watermark per table; read at the start and updated at the end of each run. |
| `_ingestion_audit` | Run history: status, duration, row count, error. |
| `_alert_run_history` | Daily outcome of the BCB task, keyed on `(task_name, run_date)`, used to count consecutive failures. |

### Governance and security (LGPD)

Dynamic masking through Unity Catalog: the `regra_mascaramento` function returns the original value
for the `admins` and `engenharia_dados` groups and `████████ (RESTRITO LGPD)` for everyone else.
Sensitive columns are declared per table in `governance_config.json` (tax ids, CPF, legal names,
e-mails, NF-e access keys). RBAC grants for `stakeholders`, `engenharia_dados` and `time_negocios`
are applied by `apply_rbca.ipynb`.

Source credentials are always read with `dbutils.secrets.get()` from the `sqlserver` scope — never
hardcoded, never in this repository.

---

## 2. Transformation — `dbt/`

22 staging views → 5 intermediate views → 11 marts tables → 6 aggregates, with 11 singular tests
plus schema tests. Full detail in [`dbt/README.md`](dbt/README.md).

| Layer | Materialization | Content |
| :--- | :--- | :--- |
| `staging/` | view | 1:1 cleanup, renaming and typing of the Bronze sources. |
| `intermediate/` | view | Cross-entity logic: product categorization, enriched sales/purchases/returns, supplier delivery delay. |
| `marts/` | table | 7 dimensions (`dim_calendary`, `dim_customers`, `dim_customer_addresses`, `dim_employees`, `dim_locations`, `dim_products`, `dim_suppliers`) and 4 facts (`fct_sales`, `fct_purchases`, `fct_returns`, `fct_daily_stock_levels`). |
| `marts/aggregates/` | table | `agg_sales_daily`, `agg_sales_regional_category_daily`, `agg_sales_regional_orders_daily`, `agg_product_performance`, `agg_customer_metrics`, `agg_customer_monthly`. |

Two design points worth highlighting:

* **`dim_customer_addresses` is SCD Type 2** — `is_current` / `valid_from` / `valid_to` flag the
  active version.
* **`fct_daily_stock_levels` has deferred surrogate-key backfill.** Dimension rows can arrive after
  the fact rows, so `product_sk` / `location_sk` are populated best-effort on insert and then
  repaired by a `post_hook` MERGE once the dimension row shows up. The `merge` incremental strategy
  and that post-hook have to stay in sync.

**Environment isolation** is handled by the custom `generate_schema_name` macro: in `prod` each
layer gets its own schema (`staging`, `intermediate`, `marts`); in `dev` the layer is appended to the
developer's own schema (`<user>_analytics_staging`, …), so the whole project can be built by several
people simultaneously without collisions.

The aggregates already net out cancellations, returns and ICMS/IPI taxes — check each `.yml` before
re-deriving a "net" figure in DAX, or you will double-count.

---

## 3. Orchestration — `databricks/orchestration`

The `LH Nautical Orchestration` job runs ingestion, then transformation (`dbt deps` + `dbt build`),
each with up to 2 retries, daily at **01:00 America/Sao_Paulo** (Quartz `0 0 1 * * ?`, `UNPAUSED`).
The ingestion job has no schedule of its own.

Both child jobs are referenced by `job_id`, which is workspace-specific — see
[Setup](#setup) step 7.

---

## 4. Demand forecasting — `forecasting/`

Analysis code, not a deployed bundle: 36 scripts and 33 write-ups across 11 numbered stages plus
five model experiments. Nothing here runs on a schedule, and `fact_demand_daily` is a disposable
scratch table rather than a committed dbt model.

* **Baselines and segmented methods (`01`–`09`).** No existing mart is at product × store × day
  grain, so stage `02` defines and builds `fact_demand_daily` (dense, zero-filled, reconciled
  exactly against `fct_sales`). Weekly is the recommended default grain: daily is dominated by
  per-SKU sparsity (73% of observations are legitimately zero) and monthly loses the peak-season
  signal. The catalog classifies as **96.8% Intermittent / 3.2% Lumpy** — no Smooth or Erratic
  series at all — which is what motivated the Croston/SBA, ADIDA and hierarchical-reconciliation
  work. Stage `09` closes the phase with a 91-day **held-static** baseline scored across 4 rolling
  origins (`seasonal_naive_genuine_91d_v2`, the champion).
* **Feature engineering (`10`).** A pooled fixed-effects panel regression (product × location ×
  ISO-week, entity effects + linear trend control) tests calendar, holiday, weather, economic,
  marketing and inventory features one cumulative layer at a time against a >2% MAPE-improvement
  threshold. **Only calendar survives** into the frozen feature set — and via an accuracy ablation,
  not its own threshold, because nothing individually cleared 2%.
* **Model comparison (`experiments/`, `11`).** Panel regression, Random Forest, XGBoost, Prophet and
  SARIMA, every one scored through the same `experiments/_shared/evaluate.py`: predictions summed per
  (`product_variant_id`, `location_id`) over four non-overlapping 91-day windows, trained only on
  data up to each cutoff. MAPE/sMAPE are deliberately excluded as structurally uninformative on this
  catalog.

**Result: no model beats the seasonal-naive champion in 3 or more of the 4 windows.** XGBoost is the
best trained model. Every model sits at 77–132% WAPE at SKU × location, because 25–59% of series sell
nothing in a 91-day window and the annual cycle is already what the champion encodes. Forecasts are
only usable at **category × location** level (11–39% WAPE). The report is explicit about the fact
that the committed baseline counts only series with sales in the window while models are scored on
all 6,054 series, and about what that does to the ranking.

Read: [`forecasting/README.md`](forecasting/README.md) (run order) ·
[`forecasting/OVERVIEW.md`](forecasting/OVERVIEW.md) (stage-by-stage narrative) ·
[`forecasting/11_model_comparison/11_model_comparison_report.md`](forecasting/11_model_comparison/11_model_comparison_report.md)
(headline findings) ·
[`forecasting/experiments/README.md`](forecasting/experiments/README.md) (evaluation protocol).

```bash
uv sync --dev --group forecasting
uv run python forecasting/02_schema_design/build_prototype.py       # fact_demand_daily in your scratch schema
uv run python forecasting/11_model_comparison/build_comparison.py   # final comparison from committed outputs
```

---

## 5. RAG assistant — `databricks/rag`

A retrieval-augmented generation system that answers natural-language questions about the project —
architecture, warehouse schema and grain, engineering decisions, and generated forecast cards — in
Brazilian Portuguese, served as a Streamlit app on Databricks Apps.

It is built around one invariant: **the language model never produces a fact.** Every number,
identifier and date that can appear in an answer is rendered by a deterministic Spark template from a
Delta column *before* indexing. The model only selects passages and phrases them, and is instructed
to refuse when the retrieved evidence does not answer the question.

| | |
|---|---|
| **Pattern** | Document-level RAG over a generated corpus (not Text-to-SQL, not Genie) |
| **Index** | Databricks Vector Search — Delta Sync, managed embeddings, `TRIGGERED` sync |
| **Embeddings** | `databricks-qwen3-embedding-0-6b` |
| **Retrieval** | Hybrid (ANN + BM25, Reciprocal Rank Fusion) → relative-score cut-off → per-document cap → top-5 |
| **Generation** | `databricks-llama-4-maverick` via Foundation Model APIs, `temperature=0` |
| **Pipeline** | 6-task idempotent job: `setup → build_documents → build_chunks → ensure_index → sync_index → grant_app` |
| **Observability** | Append-only Delta table `query_logs`, fire-and-forget |
| **Secrets** | None — identity comes from the runtime (notebook token / app service principal) |

Why not Text-to-SQL: there, the model authors the query, and a wrong join yields a *plausible,
incorrect* figure in front of an executive. Here the figures are pre-rendered and the model cannot
alter them — and curated architecture/decision documents sit in the same index, so "why dbt?" is
answerable too.

Full design, configuration reference, security model and decision log:
[`databricks/rag/README.md`](databricks/rag/README.md).

---

## 6. BI layer

[`docs/dashboard_guide.md`](docs/dashboard_guide.md) documents the Power BI executive dashboard:
KPI dictionary (gross vs. net revenue, average ticket, rolling-12M active customers, return rate),
the four report pages, RLS model and refresh schedule. The report itself lives in the training
program's tenant and is not publicly reachable.

---

## Setup

**Requirements**

* Python **>=3.10, <3.13** and [uv](https://docs.astral.sh/uv/)
* [Databricks CLI](https://docs.databricks.com/dev-tools/cli/)
* A Databricks workspace with **Unity Catalog**, serverless compute, and permission to create
  catalogs, schemas, jobs and apps
* A SQL Warehouse named exactly **`Serverless Starter Warehouse`** — the `transformation` and `rag`
  bundles resolve `warehouse_id` by name lookup. Rename yours or edit the lookup.
* Workspace groups `stakeholders`, `engenharia_dados` and `time_negocios` must exist before running
  `apply_rbca.ipynb`
* A reachable SQL Server holding the 24 source tables (see
  [What is *not* reproducible](#what-is-not-reproducible))

```bash
# 1. Install dependencies (repository root)
uv sync --dev

# 2. Authenticate
databricks auth login

# 3. Create the required local config files (neither is versioned)
cp databricks.local.yml.example databricks.local.yml   # then edit host + dev_schema
cp profiles.yml.example ~/.dbt/profiles.yml            # then edit host + http_path + schema

# 4. Store the SQL Server credentials in a Databricks secret scope
databricks secrets create-scope sqlserver
databricks secrets put-secret sqlserver username <value>
databricks secrets put-secret sqlserver password <value>
databricks secrets put-secret sqlserver host <value>
databricks secrets put-secret sqlserver port <value>
databricks secrets put-secret sqlserver db <value>

# 5. Deploy the bundles, each from its own directory
(cd databricks/ingestion      && databricks bundle deploy --target dev)   # or --target prod
(cd databricks/transformation && databricks bundle deploy --target dev)
(cd databricks/rag            && databricks bundle deploy --target dev)

# 6. Run ingestion, then dbt, then the RAG pipeline
(cd databricks/ingestion && databricks bundle run ingestion)
(cd dbt && dbt deps && dbt build)
(cd databricks/rag && databricks bundle run rag_ingestion)

# 7. Orchestration LAST: read the two job ids and paste them into
#    databricks/orchestration/resources/orchestration_job.yml, then deploy
(cd databricks/ingestion      && databricks bundle summary | grep -i 'job\|url')
(cd databricks/transformation && databricks bundle summary | grep -i 'job\|url')
(cd databricks/orchestration  && databricks bundle deploy --target dev)
```

`databricks.local.yml` is an `include:` of the `ingestion`, `orchestration` and `rag` bundles — until
it exists, `databricks bundle validate` fails with a missing-file error. The `transformation` bundle
does not include it: it takes its host from the CLI profile and its schema from
`${workspace.current_user.short_name}`.

### Checks

There is no CI for the Databricks side. Run these before considering a change done:

```bash
uv run ruff check .                        # lint (line-length 120)
uv run pytest databricks/rag/tests         # pure-Python unit tests, no workspace needed
cd dbt && dbt parse                        # validate refs/macros without touching the warehouse
cd dbt && dbt build -s <model_name>        # after any model change: the model + its up/downstream
cd databricks/ingestion && uv run pytest   # requires a live workspace (Databricks Connect)
```

---

## What is *not* reproducible

Being direct about this, because a `git clone` will not get you a running pipeline:

1. **The source SQL Server does not exist outside the training environment.** It was provided by the
   program. This repository contains no DDL, no mock-data generator and no sample extract, so the
   ingestion layer cannot be run against anything. Everything downstream of Bronze (`dbt/`,
   `forecasting/`, `databricks/rag/`) is readable and reviewable, and the committed
   `forecasting/**/output/*.csv` files hold the actual results every report is based on.
2. **The ingestion tests need a live workspace.** `databricks/ingestion/tests/conftest.py` initializes
   Databricks Connect against real (or serverless) compute; there is no local Spark mock. Only
   `databricks/rag/tests` runs offline.
3. **The RAG corpus expects two tables that were never materialized.** `fct_forecast` and
   `model_evaluation` are flagged as hypotheses in `databricks/rag/config/rag_config.yaml`. The
   document builder unions whichever builders succeed, so the pipeline still works without them — it
   just indexes the data dictionary and the curated docs, with no forecast cards.
4. **The Power BI report and the MLflow runs are internal** to the original workspace. The dashboard
   is documented in `docs/dashboard_guide.md`; the MLflow run ids are listed in each experiment's
   README for traceability, but the runs themselves are not public.
5. **Mock-data quirks are intentional.** The source contains future-dated records; the team chose to
   filter them with `filter_valid_orders()` and friends in
   `dbt/macros/data_quality_and_dedupe.sql` rather than silently correct them. Similarly, `fct_sales`
   falls back to `date_id = 19000101` when `placed_at` is null, which lands those rows in Power BI's
   blank relationship row by design.

---

## Team

Built over six months by Group 10 of the Lighthouse program:

**Eva Carrasquel** · **Gabriel Henrique Silva** · **Leonardo Pinho** · **Eros Gonçalves**

There were no fixed roles. Tech lead rotated at every checkpoint of the program, so each of the four
held it at some point, and everyone worked full-stack across the whole platform — data engineering,
analytics engineering, AI engineering and machine learning engineering. The five model experiments
under `forecasting/experiments/` are one visible trace of that: every member of the team owns at
least one.

[`CLAUDE.md`](CLAUDE.md) holds the working architecture notes the team kept alongside the code.

## License

[MIT](LICENSE).
