import unittest,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts")]
from flow_model import analyze,validate
from generate_data import generate

class FlowTests(unittest.TestCase):
    def setUp(self): self.rows=generate();self.good=next(r for r in self.rows if r["event_id"]=="OBS-MAIN-000")
    def test_counts_reconcile(self):
        s=analyze(self.rows)["summary"];self.assertEqual(s["raw_rows"],314);self.assertEqual(s["accepted_samples"],294)
        self.assertEqual(s["duplicates"],12);self.assertEqual(s["quarantined"],8)
        self.assertEqual(s["raw_rows"],s["duplicates"]+s["quarantined"]+s["accepted_samples"])
    def test_replay_keeps_decisions(self):
        a=analyze(self.rows);b=analyze(self.rows+self.rows);self.assertEqual(a["windows"],b["windows"])
    def test_out_of_order_is_invariant(self): self.assertEqual(analyze(self.rows)["windows"],analyze(list(reversed(self.rows)))["windows"])
    def test_identity_conflicts_quarantine_both(self):
        result=analyze([dict(self.good,event_id="C",occupancy="100"),dict(self.good,event_id="C",occupancy="101")])
        self.assertEqual(result["summary"]["accepted_samples"],0);self.assertEqual(result["summary"]["quarantined"],2)
    def test_exact_payload_duplicates_count_once(self): self.assertEqual(analyze([self.good,self.good])["summary"]["accepted_samples"],1)
    def test_late_samples_retained_in_history(self):
        late=dict(self.good,arrival_time="2026-09-26T18:15:00Z");s=analyze([late])["summary"]
        self.assertEqual(s["accepted_samples"],1);self.assertEqual(s["late_samples"],1)
    def test_ten_minutes_is_not_late(self):
        s=analyze([dict(self.good,arrival_time="2026-09-26T18:10:00Z")])["summary"];self.assertEqual(s["late_samples"],0)
    def test_invalid_values(self):
        for c in [dict(occupancy="-1"),dict(occupancy="1001"),dict(queue_wait_minutes="NaN"),dict(queue_wait_minutes="1.001"),dict(event_time="bad"),dict(zone="UNKNOWN")]:
            with self.subTest(c=c):self.assertIsNotNone(validate(dict(self.good,**c))[1])
    def test_arrival_cannot_precede_event(self): self.assertIsNotNone(validate(dict(self.good,arrival_time="2026-09-26T17:59:00Z"))[1])
    def test_event_minute_alignment(self): self.assertIsNotNone(validate(dict(self.good,event_time="2026-09-26T18:00:01Z"))[1])
    def test_incomplete_windows_are_visible(self): self.assertEqual(analyze(self.rows)["summary"]["incomplete_windows"],6)
    def test_cancellation_effect_is_zone_specific(self):
        zones={r["zone"]:r for r in analyze(self.rows)["zones"]}
        self.assertGreater(zones["HARBOR"]["after_wait"],zones["HARBOR"]["before_wait"])
        self.assertLess(zones["MAIN"]["after_mean_occupancy"],zones["MAIN"]["before_mean_occupancy"])
    def test_pressure_threshold(self):
        result=analyze([dict(self.good,occupancy="850",queue_wait_minutes="0")]);self.assertTrue(result["windows"][0]["pressure"])
    def test_windows_are_five_minutes(self):
        self.assertTrue(all(int(w["window_start"][14:16])%5==0 for w in analyze(self.rows)["windows"]))

if __name__=="__main__":unittest.main()
