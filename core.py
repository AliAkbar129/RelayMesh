"""Durable local outbox and central deduplication reference implementation."""
import hashlib,json,sqlite3,uuid
from pathlib import Path
import httpx

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def connect(path):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    con=sqlite3.connect(path,timeout=10);con.row_factory=sqlite3.Row
    con.execute('PRAGMA journal_mode=WAL');con.execute('PRAGMA synchronous=FULL');return con

class Outbox:
    def __init__(self,path,station):
        self.path=str(path);self.station=station
        with connect(path) as c:
            c.execute('CREATE TABLE IF NOT EXISTS identity (station TEXT PRIMARY KEY)')
            existing=c.execute('SELECT station FROM identity').fetchone()
            if existing and existing['station']!=station:raise ValueError('Outbox belongs to another station')
            c.execute('INSERT OR IGNORE INTO identity VALUES (?)',(station,))
            c.execute('CREATE TABLE IF NOT EXISTS outbox (seq INTEGER PRIMARY KEY AUTOINCREMENT,id TEXT UNIQUE,payload TEXT NOT NULL,acked INTEGER NOT NULL DEFAULT 0)')
    def enqueue(self,payload):
        raw=canonical(payload);identifier=str(uuid.uuid4())
        with connect(self.path) as c:
            cur=c.execute('INSERT INTO outbox(id,payload) VALUES (?,?)',(identifier,raw));seq=cur.lastrowid
        return {'id':identifier,'seq':seq,'station':self.station,'payload':payload}
    def pending(self,limit=100):
        with connect(self.path) as c:rows=c.execute('SELECT * FROM outbox WHERE acked=0 ORDER BY seq LIMIT ?',(limit,)).fetchall()
        return [{'id':r['id'],'seq':r['seq'],'station':self.station,'payload':json.loads(r['payload'])} for r in rows]
    def count(self):
        with connect(self.path) as c:return c.execute('SELECT COUNT(*) FROM outbox WHERE acked=0').fetchone()[0]
    def sync(self,url,key,lose_reply=False):
        batch=self.pending()
        if not batch:return 0
        response=httpx.post(url+'/sync/push',json={'events':batch},headers={'X-Station-Key':key},timeout=5)
        response.raise_for_status();received=response.json()['accepted']
        if lose_reply:raise TimeoutError('Injected lost reply after central commit')
        expected={r['id'] for r in batch}
        if not set(received)<=expected:raise ValueError('Unknown acknowledgement IDs')
        with connect(self.path) as c:c.executemany('UPDATE outbox SET acked=1 WHERE id=?',[(x,) for x in received])
        return len(received)

class Central:
    def __init__(self,path):
        self.path=str(path)
        with connect(path) as c:
            c.execute('CREATE TABLE IF NOT EXISTS events (id TEXT,station TEXT,seq INTEGER,payload TEXT,hash TEXT,PRIMARY KEY(station,id),UNIQUE(station,seq))')
            c.execute('CREATE TABLE IF NOT EXISTS orders (id TEXT PRIMARY KEY,body TEXT,revision INTEGER NOT NULL)')
            c.execute('CREATE TABLE IF NOT EXISTS conflicts (id INTEGER PRIMARY KEY AUTOINCREMENT,order_id TEXT,base_revision INTEGER,proposal TEXT,resolved INTEGER NOT NULL DEFAULT 0)')
    def push(self,events,station):
        accepted=[]
        with connect(self.path) as c:
            c.execute('BEGIN IMMEDIATE')
            for e in events:
                if e['station']!=station:raise PermissionError('Cross-station event')
                raw=canonical(e['payload']);digest=hashlib.sha256(canonical(e).encode()).hexdigest()
                old=c.execute('SELECT hash FROM events WHERE station=? AND id=?',(station,e['id'])).fetchone()
                if old:
                    if old['hash']!=digest:raise ValueError('Existing ID has different content')
                else:c.execute('INSERT INTO events VALUES (?,?,?,?,?)',(e['id'],station,e['seq'],raw,digest))
                accepted.append(e['id'])
        return accepted
    def edit(self,identifier,body,base_revision):
        with connect(self.path) as c:
            c.execute('BEGIN IMMEDIATE');row=c.execute('SELECT * FROM orders WHERE id=?',(identifier,)).fetchone()
            revision=row['revision'] if row else 0
            if base_revision!=revision:
                cur=c.execute('INSERT INTO conflicts(order_id,base_revision,proposal) VALUES (?,?,?)',(identifier,base_revision,canonical(body)))
                return {'conflict_id':cur.lastrowid,'current_revision':revision}
            revision+=1;c.execute('INSERT INTO orders VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET body=excluded.body,revision=excluded.revision',(identifier,canonical(body),revision))
            return {'id':identifier,'revision':revision,'body':body}
    def resolve(self,conflict_id,body,revision):
        with connect(self.path) as c:
            c.execute('BEGIN IMMEDIATE');conflict=c.execute('SELECT * FROM conflicts WHERE id=?',(conflict_id,)).fetchone()
            if not conflict or conflict['resolved']:raise ValueError('Missing or resolved conflict')
            order=c.execute('SELECT * FROM orders WHERE id=?',(conflict['order_id'],)).fetchone()
            if not order or order['revision']!=revision:raise ValueError('Stale resolution')
            c.execute('UPDATE orders SET body=?,revision=revision+1 WHERE id=?',(canonical(body),order['id']))
            c.execute('UPDATE conflicts SET resolved=1 WHERE id=?',(conflict_id,))
            return {'id':order['id'],'revision':revision+1}
    def snapshot(self):
        with connect(self.path) as c:
            counts=[dict(r) for r in c.execute('SELECT station,COUNT(*) AS events,MAX(seq) AS last_seq FROM events GROUP BY station')]
            orders=[dict(r) for r in c.execute('SELECT * FROM orders')]
            conflicts=[dict(r) for r in c.execute('SELECT * FROM conflicts ORDER BY id DESC')]
            return {'stations':counts,'orders':orders,'conflicts':conflicts,'total_events':sum(r['events'] for r in counts)}
