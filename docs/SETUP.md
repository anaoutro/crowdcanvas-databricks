# CrowdCanvas setup and demonstration

## Local reference

Use Python 3.10+ and the commands in [the README](../README.md). No third-party package is required by the reference model. The generator writes the raw CSV, reference JSON, curated streaming fixtures and browser demo data. The visual demo is offline and does not execute Spark.

## Workspace prerequisites

A Unity Catalog workspace and compatible PySpark/Delta compute, with Databricks Runtime 15.4 LTS or later compatible compute as an unverified baseline. The execution principal needs catalog/schema access, volume read/write for source/checkpoint files and table creation/write access.

## Corrected history

1. Upload data/festival_events.csv to a UC volume.
2. Import notebooks/01_corrected_history.py.
3. Set catalog, schema and input_path widgets.
4. Run all cells; inspect quarantine, Silver, zone windows and before/after summary.
5. Run the same input again. Bronze payload count and Gold metrics should remain unchanged.

Bronze ingestion merges hashes. Silver and Gold are rebuilt from the full Bronze history so a new conflicting version can remove a previously accepted identity. This is a small-data corrected-history implementation, not a fully incremental or concurrent production pipeline. Multi-table writes are not one transaction.

## Separate streaming experiment

1. Upload the contents of data/stream_input as a directory of JSON files. These are unique valid snapshots, not raw corrupt source rows.
2. Import notebooks/02_watermarked_stream.py and configure stream_path and checkpoint_path.
3. Use a fresh stream output table and fresh checkpoint for the first experiment. Restart subsequent runs with the same checkpoint.
4. Run availableNow processing and inspect finalized stream windows.
5. Run the comparison query in sql/dashboard.sql. Missing or reduced finalized windows are expected under different lateness/finalization behavior.

File processing order and micro-batch boundaries affect watermark progress. The last windows may remain unfinalized without newer event-time observations. Ten-minute arrival delay in the batch diagnostic is not identical to Spark's event-time watermark state. The stream assumes upstream uniqueness and does not promise permanent deduplication or live conflict resolution.

## Official references

- [Watermark thresholds and late events](https://docs.databricks.com/aws/en/structured-streaming/watermarks)
- [Delta streaming reads and writes](https://docs.databricks.com/aws/en/structured-streaming/delta-lake)
