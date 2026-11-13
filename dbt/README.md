# dbt project — `desafio_lh`

Transformation layer of the LH Nautical pipeline: reads the Bronze tables landed by
`databricks/ingestion` and builds a star schema plus a set of pre-aggregated tables for BI.

Runs either locally (`dbt build`) or as the `dbt_build` task of the `transformation` Databricks
bundle. Profile name: `desafio_lh` — see [`../profiles.yml.example`](../profiles.yml.example) for
the expected shape of `~/.dbt/profiles.yml`.

## Layers

| Layer | Path | Materialization | Schema suffix | Role |
| :--- | :--- | :--- | :--- | :--- |
| Staging | `models/staging/` | view | `staging` | 1:1 cleanup, renaming and typing of the Bronze sources. Sources declared in `models/staging/src_bronze.yml`. |
| Intermediate | `models/intermediate/` | view | `intermediate` | Cross-entity business logic: product categorization, enriched sales / purchases / returns, supplier delivery delay. |
| Marts | `models/marts/` | table | `marts` | Star schema: 7 dimensions (`dim_*`) and 4 facts (`fct_*`). |
| Aggregates | `models/marts/aggregates/` | table | `marts` | 6 pre-aggregated tables consumed directly by Power BI, already net of cancellations, returns and ICMS/IPI taxes. |

Grain and key notes for every mart and aggregate are in [`../CLAUDE.md`](../CLAUDE.md#marts-reference-for-bi--dax-work-grain-and-keys)
and in each model's `.yml`. Two of them are worth calling out:

- `dim_customer_addresses` is **SCD Type 2** — filter on `is_current` unless you want history.
- `fct_daily_stock_levels` is **incremental with deferred surrogate-key backfill**: `product_sk` /
  `location_sk` are filled best-effort on insert and repaired by a `post_hook` MERGE once the
  late-arriving dimension row shows up. Keep the `merge` strategy and that post-hook together.

## Environment isolation

`macros/generate_schema_name.sql` overrides dbt's default naming:

- **`prod`** → the layer name is used as the schema directly: `staging`, `intermediate`, `marts`.
- **any other target (`dev`)** → the layer is appended to the developer's own schema:
  `<user>_analytics_staging`, `<user>_analytics_intermediate`, `<user>_analytics_marts`.

So two developers can build the whole project at the same time without overwriting each other.

## Shared macros

| Macro | File | What it does |
| :--- | :--- | :--- |
| `dedupe_latest()` | `macros/data_quality_and_dedupe.sql` | `qualify row_number()` over a key, keeping the latest ingested version (`_ingested_at`, `_batch_id`). |
| `keep_required_rows()` | same file | Drops rows with nulls in mandatory columns. |
| `filter_valid_orders()` | same file | Keeps only rows whose `order_id` still exists in `stg_orders`, which drops rows tied to the invalid/future-dated orders the source mock database contains. |
| `convert_currency()` | `macros/convert_currency.sql` | USD/BRL conversion via `stg_exchange_rates` (BCB rates). |
| `days_between()` | `macros/days_between.sql` | Date-diff arithmetic used by the purchases/delivery-delay models. |

Use these instead of repeating the pattern inline — the staging and intermediate models all go
through them.

## Snapshots and seeds

- `snapshots/snap_stock_levels.sql` — SCD2 history of `stg_stock_levels` (timestamp strategy,
  keyed on `stock_level_id`). It is what feeds `fct_daily_stock_levels`.
- `seeds/seed_category_fix.csv` — manual lookup by `product_type` that `int_products_categorized`
  joins against to correct miscategorized/misspelled product types coming from the source system.

## Tests

Schema tests live in each model's `.yml`. Singular assertions live in `tests/`, one directory per
fact table (`fct_sales/`, `fct_purchases/`, `fct_returns/`, `fct_daily_stock_levels/`): no negative
quantities or refunds, no future dates, received quantity never exceeding ordered, non-negative
lead time, and 7-day reconciliation of the stock fact against the source snapshot and movements.

## Commands

```bash
dbt deps                       # install dbt_utils + codegen (packages.yml)
dbt build                      # run + test everything
dbt build -s dim_customers     # one model with its upstream/downstream — use this after any change
dbt parse                      # validate refs/macros without touching the warehouse
dbt snapshot                   # refresh snap_stock_levels
dbt docs generate && dbt docs serve
```
