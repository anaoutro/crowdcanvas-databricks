"""Corrected historical analysis of synthetic anonymous snapshots; not a Spark watermark simulator."""
from datetime import datetime, timedelta
from collections import defaultdict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import hashlib

ZONES={"MAIN":1000,"HARBOR":500,"GROVE":700,"FOOD":300}
FIELDS=("event_id","zone","event_time","arrival_time","occupancy","queue_wait_minutes")
START=datetime.fromisoformat("2026-09-26T18:00:00+00:00")
CANCEL=START+timedelta(minutes=30)

def stamp(value):
    d=datetime.fromisoformat(value.replace("Z","+00:00"))
    if d.tzinfo is None or d.utcoffset().total_seconds()!=0: raise ValueError("UTC required")
    return d
def validate(row):
    try:
        if any(not str(row.get(k,"") or "").strip() for k in FIELDS): return None,"missing_field"
        zone=row["zone"]
        if zone not in ZONES: return None,"unknown_zone"
        event=stamp(row["event_time"]); arrival=stamp(row["arrival_time"])
        if arrival<event or event.second or event.microsecond: return None,"invalid_timestamp"
        occupancy=Decimal(row["occupancy"]); wait=Decimal(row["queue_wait_minutes"])
        if not occupancy.is_finite() or occupancy!=occupancy.to_integral_value() or occupancy<0 or occupancy>ZONES[zone]: return None,"invalid_occupancy"
        if not wait.is_finite() or wait<0 or wait>120 or wait!=wait.quantize(Decimal("0.01")): return None,"invalid_wait"
        return dict(row,observed_at=event,arrived_at=arrival,occupancy=int(occupancy),queue_wait_minutes=wait),None
    except (ValueError,TypeError,InvalidOperation): return None,"invalid_type"

def average(values):
    return float((sum(values,Decimal(0))/len(values)).quantize(Decimal("0.01"),rounding=ROUND_HALF_UP)) if values else 0
def analyze(rows):
    unique={}; duplicate_rows=0
    for row in rows:
        serialized=json.dumps({k:row.get(k,"") for k in FIELDS},sort_keys=True)
        if serialized in unique: duplicate_rows+=1
        else: unique[serialized]=row
    groups=defaultdict(list)
    for row in unique.values(): groups[row.get("event_id","")].append(row)
    conflicts={key for key,values in groups.items() if key and len(values)>1}
    quarantine=[]; accepted=[]
    for row in unique.values():
        a,reason=validate(row)
        if not reason and row["event_id"] in conflicts: reason="identity_conflict"
        if reason: quarantine.append({"event_id":row.get("event_id",""),"reason":reason})
        else: accepted.append(a)
    accepted.sort(key=lambda a:(a["observed_at"],a["zone"],a["event_id"]))
    buckets=defaultdict(list)
    for a in accepted:
        t=a["observed_at"]; beginning=t.replace(minute=(t.minute//5)*5)
        buckets[(a["zone"],beginning)].append(a)
    windows=[]
    for (zone,beginning),items in sorted(buckets.items(),key=lambda p:(p[0][1],p[0][0])):
        mean=average([a["queue_wait_minutes"] for a in items]); peak=max(a["occupancy"] for a in items)
        ratio=peak/ZONES[zone]
        windows.append({"zone":zone,"window_start":beginning.isoformat(),"samples":len(items),"complete":len(items)==5,
            "peak_occupancy":peak,"peak_ratio":round(ratio,4),"mean_wait_minutes":mean,
            "late_samples":sum((a["arrived_at"]-a["observed_at"]).total_seconds()>600 for a in items),
            "pressure":ratio>=.85 or mean>=12})
    zone_summary=[]
    for zone,capacity in ZONES.items():
        items=[a for a in accepted if a["zone"]==zone]
        before=[a for a in items if a["observed_at"]<CANCEL]; after=[a for a in items if a["observed_at"]>=CANCEL]
        zone_summary.append({"zone":zone,"capacity":capacity,"samples":len(items),
            "before_wait":average([a["queue_wait_minutes"] for a in before]),"after_wait":average([a["queue_wait_minutes"] for a in after]),
            "before_mean_occupancy":average([Decimal(a["occupancy"]) for a in before]),"after_mean_occupancy":average([Decimal(a["occupancy"]) for a in after]),
            "peak_occupancy":max((a["occupancy"] for a in items),default=0)})
    summary={"raw_rows":len(rows),"duplicates":duplicate_rows,"quarantined":len(quarantine),"accepted_samples":len(accepted),
        "conflicting_ids":len(conflicts),"late_samples":sum((a["arrived_at"]-a["observed_at"]).total_seconds()>600 for a in accepted),
        "windows":len(windows),"incomplete_windows":sum(not w["complete"] for w in windows),"pressure_windows":sum(w["pressure"] for w in windows)}
    # Keep only simple scalar inputs for the offline demo.
    observations=[{k:a[k] for k in FIELDS if k!="queue_wait_minutes"}|{"queue_wait_minutes":float(a["queue_wait_minutes"])} for a in accepted]
    return {"policy":"flow-v1","cancellation_time":CANCEL.isoformat(),"summary":summary,"zones":zone_summary,"windows":windows,"quarantine":quarantine,"observations":observations}
