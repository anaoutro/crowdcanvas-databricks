# CrowdCanvas architecture decisions

## Immutability and conflict handling

An event_id identifies one immutable snapshot. Exact payload replays deduplicate; multiple different payloads for one ID are all quarantined. A rebuilt Silver table can retract a previously accepted ID after a later conflict is observed.

## Two clocks

Event time determines analytical windows. Arrival time diagnoses source delay. The historical late flag means delay strictly greater than 600 seconds. A Spark watermark is relative to event-time progress and micro-batches, so it is not the same as that flag.

## Completeness

The fixture contract expects one observation per zone per minute, or five in each full window. The historical notebook asserts unique zone-minute observations. The pressure flag uses peak occupancy ratio ≥85% or rounded mean wait ≥12 minutes. It is an illustrative operational heuristic.

## Source compatibility

The Python reference accepts broader valid Decimal/ISO input text than the notebook's strict plain-text regex contract. The shared fixture is within both contracts. Equivalent behavior is not asserted for arbitrary input encodings.

## Corrected history versus streaming

Historical Gold is rebuilt using all valid snapshots, including late ones. Streaming uses curated unique files, a ten-minute watermark and append-only finalized windows. It deliberately has no stateful deduplication chained before aggregation. Newer event times may be needed to finalize the tail.
