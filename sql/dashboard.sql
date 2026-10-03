-- Change main.crowdcanvas to match notebook widgets.
SELECT zone, capacity, samples, peak_occupancy, before_wait, after_wait,
       after_wait - before_wait AS wait_change_minutes
FROM main.crowdcanvas.gold_zone_summary ORDER BY wait_change_minutes DESC;

SELECT zone, window_start, peak_ratio, mean_wait_minutes, samples, complete, late_samples, pressure
FROM main.crowdcanvas.gold_zone_windows ORDER BY window_start, zone;

SELECT quality_reason, COUNT(*) AS rejected_payloads
FROM main.crowdcanvas.quarantine_snapshots GROUP BY quality_reason;

-- Run after notebook 02. A difference is expected: streaming has different lateness/finalization semantics.
SELECT h.zone, h.window_start, h.samples AS corrected_samples, s.samples AS finalized_stream_samples
FROM main.crowdcanvas.gold_zone_windows h
LEFT JOIN main.crowdcanvas.gold_stream_windows s
ON h.zone=s.zone AND h.window_start=s.window_start
ORDER BY h.window_start,h.zone;
