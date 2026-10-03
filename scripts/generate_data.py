"""Deterministic synthetic telemetry with duplicates, invalid data, conflicts and late arrivals."""
from pathlib import Path
from datetime import timedelta
import csv,json,random,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from flow_model import ZONES,FIELDS,START,analyze

def generate():
    rng=random.Random(91); rows=[]
    # Six deliberate missing observations create incomplete historical windows.
    missing={("MAIN",7),("HARBOR",33),("GROVE",51),("FOOD",24),("FOOD",42),("HARBOR",62)}
    for minute in range(75):
        for zone,capacity in ZONES.items():
            if (zone,minute) in missing: continue
            post=minute>=30
            ratio={"MAIN":.76 if not post else .27,"HARBOR":.39 if not post else .88,"GROVE":.46 if not post else .73,"FOOD":.40 if not post else .77}[zone]
            wait={"MAIN":4 if not post else 2,"HARBOR":5 if not post else 18,"GROVE":5 if not post else 10,"FOOD":6 if not post else 14}[zone]
            event=START+timedelta(minutes=minute)
            delay=15 if (minute+len(zone))%11==0 else rng.randint(0,6)
            rows.append(dict(event_id=f"OBS-{zone}-{minute:03d}",zone=zone,event_time=event.isoformat().replace("+00:00","Z"),
                arrival_time=(event+timedelta(minutes=delay)).isoformat().replace("+00:00","Z"),
                occupancy=str(round(capacity*(ratio+rng.uniform(-.03,.03)))),queue_wait_minutes=f"{wait+rng.uniform(-1,1):.2f}"))
    rows.extend(dict(rows[i]) for i in range(12))
    for key,changes in [("BAD-1",{"occupancy":"-1"}),("BAD-2",{"queue_wait_minutes":"NaN"}),
        ("BAD-3",{"zone":"UNKNOWN"}),("BAD-4",{"event_time":"bad-date"}),
        ("BAD-5",{"arrival_time":"2026-09-26T17:00:00Z"}),("BAD-6",{"event_id":""})]:
        rows.append(dict(rows[0],event_id=key,**{k:v for k,v in changes.items() if k!="event_id"}))
        if "event_id" in changes: rows[-1]["event_id"]=""
    rows.extend([dict(rows[0],event_id="CONFLICT-1",occupancy="700"),dict(rows[0],event_id="CONFLICT-1",occupancy="701")])
    rng.shuffle(rows)
    return rows

if __name__=="__main__":
    rows=generate(); data=ROOT/"data"; data.mkdir(exist_ok=True)
    with (data/"festival_events.csv").open("w",newline="",encoding="utf-8") as h:
        writer=csv.DictWriter(h,fieldnames=FIELDS);writer.writeheader();writer.writerows(rows)
    result=analyze(rows)
    (data/"reference_results.json").write_text(json.dumps(result,indent=2)+"\n",encoding="utf-8",newline="\n")
    for base in [ROOT/"docs/demo"]:
        if base.exists(): (base/"data.js").write_text("window.FESTIVAL_DATA = "+json.dumps(result)+";\n",encoding="utf-8",newline="\n")
    # Streaming fixture: unique, valid snapshots split by arrival-time bucket. Raw batch source remains unchanged.
    stream=data/"stream_input";stream.mkdir(exist_ok=True)
    chunks={}
    for row in result["observations"]:
        arrival=row["arrival_time"];bucket=arrival[:15] # group by tens of minutes of arrival
        chunks.setdefault(bucket,[]).append(row)
    for index,key in enumerate(sorted(chunks)):
        items=sorted(chunks[key],key=lambda r:(r["arrival_time"],r["event_id"]))
        (stream/f"arrival-{index:02d}.json").write_text("".join(json.dumps(a)+"\n" for a in items),encoding="utf-8",newline="\n")
    print(json.dumps(result["summary"],indent=2))
