"""Actual HTTP replay against a separately running central process."""
import argparse,json,time,tempfile
from pathlib import Path
import httpx
from core import Outbox
p=argparse.ArgumentParser();p.add_argument('--server',default='http://127.0.0.1:8022');p.add_argument('--output',default='artifacts/demo.json');a=p.parse_args()
before=httpx.get(a.server+'/state').json()['total_events'];start=time.perf_counter();trace=[]
with tempfile.TemporaryDirectory() as tmp:
    boxes=[]
    for s in ['alpha','beta','gamma']:
        o=Outbox(Path(tmp)/(s+'.sqlite'),s)
        # Reuse central station sequence only in a clean demo DB, explicitly checked.
        if before:raise SystemExit('Use a fresh RELAY_DB for this reproducible scenario.')
        for i in range(20):o.enqueue({'sample':i,'station_value':s})
        boxes.append(o)
    trace.append({'phase':'offline_local_acceptance','pending':sum(o.count() for o in boxes)})
    try:boxes[0].sync(a.server,'demo-alpha',lose_reply=True)
    except TimeoutError:trace.append({'phase':'reply_lost_after_commit','pending':boxes[0].count()})
    # Reopen local durable state, modeling a restarted client process.
    boxes[0]=Outbox(boxes[0].path,'alpha')
    for o in boxes:
        while o.count():o.sync(a.server,'demo-'+o.station)
    after=httpx.get(a.server+'/state').json()['total_events'];pending=sum(o.count() for o in boxes)
    report={'scenario':'HTTP commit-before-reply with durable client reopen','attempted':60,'locally_accepted':60,'centrally_committed':after-before,'pending':pending,
            'accepted_event_loss':60-(after-before)-pending,'duration_seconds':time.perf_counter()-start,'trace':trace,
            'limitations':['Single host','Client reopen is not an OS-kill experiment','No disk-loss guarantee','No multiwriter CRDT or cursor reconciliation yet']}
    assert report['centrally_committed']==60 and pending==0
    output=Path(a.output);output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2));print(json.dumps(report))
