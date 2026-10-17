import argparse,json
from core import Outbox
p=argparse.ArgumentParser();p.add_argument('--db',required=True);p.add_argument('--station',required=True)
p.add_argument('--server',default='http://127.0.0.1:8022');p.add_argument('--key');p.add_argument('--enqueue');p.add_argument('--sync',action='store_true');p.add_argument('--lose-reply',action='store_true');a=p.parse_args()
o=Outbox(a.db,a.station)
if a.enqueue:print(json.dumps(o.enqueue(json.loads(a.enqueue))))
if a.sync:print(json.dumps({'acknowledged':o.sync(a.server,a.key,a.lose_reply)}))
print(json.dumps({'pending':o.count()}))
