# CrowdCanvas — A crowd moves. The data arrives later.

## Scenario

At 18:30 UTC, a cancelled fictional set changes the festival's occupancy pattern. Main occupancy falls; Harbor queue wait rises. The telemetry includes delayed delivery, missing samples, repeated payloads, invalid values and one conflicted event identity.

## Design

I treated observation time and arrival time as separate fields. Bronze preserves payloads, validation isolates invalid/conflicting identities, and Silver contains anonymous zone snapshots. Gold produces five-minute windows, sample completeness and zone-specific before/after averages.

The corrected historical analysis retains valid late arrivals. A separate watermarked streaming example illustrates bounded state and finalized output. Comparing those products is part of the case: a streaming window can legitimately differ from corrected history.

## Evidence

A deterministic fixture reconciles 314 rows into 294 accepted snapshots, twelve duplicate payloads and eight quarantined payloads. Twenty-seven valid observations arrived more than ten minutes after their event time. Six of sixty zone windows are incomplete. Fourteen portable business tests passed.

Notebook execution, actual watermark output, Delta integration and workspace permissions remain pending. The browser dashboard displays portable reference-model results, not a live Databricks connection.

## Tradeoffs and next iteration

Corrected history rebuilds Silver/Gold and scans the demo archive. The streaming example starts from curated unique data; production ingest needs authoritative snapshot identity, quality routing and observed source ordering. Pressure is a synthetic operational heuristic rather than a safety determination. Next steps include workspace verification, scheduled reconciliation and a real anonymous aggregate sensor contract.

[Editorial case JPG](images/case-board.jpg) · [Implementation](../README.md) · [Validation](../VALIDATION.md)
