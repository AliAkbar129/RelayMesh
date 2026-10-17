"""Repeat actual HTTP + SIGKILL recovery checkpoints; no simulated success counts."""
import argparse, concurrent.futures, hashlib, json, os, platform, random, signal, socket, subprocess, sys, tempfile, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import httpx
from core import Outbox, Central, connect

def worker(folder, mode, seed, count):
    rng=random.Random(seed); queues={s:[] for s in ('alpha','beta','gamma')}
    for s in queues:
        box=Outbox(Path(folder)/f'{s}.sqlite',s) if mode=='durable' else None
        for i in range(count):
            payload={'reading':rng.random(),'index':i,'seed':seed}
            if box:box.enqueue(payload)
            else:queues[s].append(payload)
    print(json.dumps({'accepted':3*count}),flush=True)
    while True:time.sleep(1)

def start_server(folder,port):
    env={**os.environ,'RELAY_DB':str(Path(folder)/'central.sqlite')}
    p=subprocess.Popen([sys.executable,'-m','uvicorn','server:app','--host','127.0.0.1','--port',str(port),'--log-level','error'],cwd=ROOT,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    for _ in range(200):
        if p.poll() is not None:raise RuntimeError(p.stderr.read().decode())
        try:
            if httpx.get(f'http://127.0.0.1:{port}/health',timeout=.2).status_code==200:return p
        except httpx.HTTPError:pass
        time.sleep(.025)
    p.kill();p.wait();raise TimeoutError('server startup')

def kill(p):
    p.kill();p.wait(timeout=10)
    if p.returncode != -signal.SIGKILL:raise AssertionError('Expected real SIGKILL')

def trial(seed,count):
    began=time.perf_counter();trace=[]
    with tempfile.TemporaryDirectory() as folder:
        with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
        url=f'http://127.0.0.1:{port}'
        p=subprocess.Popen([sys.executable,__file__,'--worker',folder,'--seed',str(seed),'--count',str(count)],stdout=subprocess.PIPE,text=True)
        try:accepted=json.loads(p.stdout.readline())['accepted']
        finally:kill(p)
        trace.append({'checkpoint':'client_SIGKILL_after_local_commit','accepted':accepted})
        boxes=[Outbox(Path(folder)/f'{s}.sqlite',s) for s in ('alpha','beta','gamma')]
        expected=[e for b in boxes for e in b.pending(count)]
        assert len(expected)==accepted
        unavailable=0
        for b in boxes:
            try:b.sync(url,'demo-'+b.station)
            except httpx.TransportError:unavailable+=1
        assert unavailable==3 and sum(b.count() for b in boxes)==accepted
        trace.append({'checkpoint':'central_offline','failed_attempts':unavailable,'pending':accepted})
        server=start_server(folder,port)
        try:
            try:boxes[0].sync(url,'demo-alpha',lose_reply=True)
            except TimeoutError:pass
            assert boxes[0].count()==count
            committed_before=Central(Path(folder)/'central.sqlite').snapshot()['total_events']
            assert committed_before==count
            kill(server)
            trace.append({'checkpoint':'central_SIGKILL_after_commit_before_local_ack','committed':committed_before})
            server=start_server(folder,port)
            # Three concurrent synchronizers for each outbox deliberately race/replay.
            with concurrent.futures.ThreadPoolExecutor(max_workers=9) as pool:
                futures=[pool.submit(b.sync,url,'demo-'+b.station) for b in boxes for _ in range(3)]
                acknowledgements=sum(f.result() for f in futures)
            with connect(Path(folder)/'central.sqlite') as c:
                rows=[dict(r) for r in c.execute('SELECT station,id,seq,payload FROM events ORDER BY station,seq')]
                integrity=c.execute('PRAGMA integrity_check').fetchone()[0]
            actual={(r['station'],r['id']):(r['seq'],json.loads(r['payload'])) for r in rows}
            payload_match=all(actual.get((e['station'],e['id']))==(e['seq'],e['payload']) for e in expected)
            pending=sum(b.count() for b in boxes)
            assert integrity=='ok' and len(rows)==accepted and pending==0 and payload_match
            trace.append({'checkpoint':'concurrent_replay','acknowledgements_including_retries':acknowledgements,'committed_unique':len(rows)})
        finally:
            if server.poll() is None:server.terminate();server.wait(timeout=10)
        # Explicitly defined volatile-queue reference: accepts in RAM, then same OS kill.
        p=subprocess.Popen([sys.executable,__file__,'--worker',folder,'--mode','memory','--seed',str(seed),'--count',str(count)],stdout=subprocess.PIPE,text=True)
        try:volatile_accepted=json.loads(p.stdout.readline())['accepted']
        finally:kill(p)
        return {'seed':seed,'attempted':accepted,'locally_accepted':accepted,'centrally_committed':len(rows),'accepted_event_loss':accepted-len(rows),'pending':pending,'payloads_match':payload_match,'sqlite_integrity':integrity,'elapsed_seconds':time.perf_counter()-began,'volatile_queue_reference':{'accepted':volatile_accepted,'recoverable_after_process_kill':0,'lost':volatile_accepted},'synchronous_offline_reference':{'attempted':accepted,'accepted':0,'rejected':accepted,'accepted_event_loss':0},'trace':trace}

def main():
    p=argparse.ArgumentParser();p.add_argument('--worker');p.add_argument('--mode',default='durable');p.add_argument('--seed',type=int,default=42);p.add_argument('--count',type=int,default=40);p.add_argument('--trials',type=int,default=5);p.add_argument('--output',default='evidence/reliability_matrix.json');a=p.parse_args()
    if a.worker:return worker(a.worker,a.mode,a.seed,a.count)
    if a.count<1 or a.count>100 or a.trials<1:raise ValueError('count must be 1..100; trials positive')
    results=[]
    for i in range(a.trials):
        r=trial(a.seed+i,a.count);results.append(r);print(json.dumps(r),flush=True)
    report={'schema_version':1,'python':sys.version,'platform':platform.platform(),'experiment_source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'protocol':'Three stations; offline delivery; real client SIGKILL after enqueue; central SIGKILL after commit and injected lost reply; concurrent replay; full payload equality and SQLite integrity check. Each trial uses fresh databases.','trials':results,'limitations':['Checkpoint kills are not arbitrary instruction-level power failures','No disk loss, multi-host network or long-duration test','No throughput or exactly-once delivery claim','Volatile and synchronous references are deliberately simple policies, not production competitors','No kill inside an in-progress SQLite transaction in this experiment']}
    out=Path(a.output);out.parent.mkdir(parents=True,exist_ok=True);out.write_text(json.dumps(report,indent=2))
if __name__=='__main__':main()
