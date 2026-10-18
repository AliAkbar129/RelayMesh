import pytest,sqlite3
from core import Outbox,Central

def test_durable_reopen_and_identity(tmp_path):
    p=tmp_path/'edge';o=Outbox(p,'a');e=o.enqueue({'x':1});assert Outbox(p,'a').pending()==[e]
    with pytest.raises(ValueError):Outbox(p,'b')
def test_dedup_conflicts_and_station_isolation(tmp_path):
    c=Central(tmp_path/'central');e={'id':'1','station':'a','seq':1,'payload':{'x':1}}
    assert c.push([e],'a')==['1'];assert c.push([e],'a')==['1'];assert c.snapshot()['total_events']==1
    with pytest.raises(ValueError):c.push([{**e,'payload':{'x':2}}],'a')
    with pytest.raises(PermissionError):c.push([e],'b')
def test_batch_rollback_on_sequence_conflict(tmp_path):
    c=Central(tmp_path/'central');one={'id':'1','station':'a','seq':1,'payload':{}}
    with pytest.raises(sqlite3.IntegrityError):c.push([one,{**one,'id':'2'}],'a')
    assert c.snapshot()['total_events']==0

def test_concurrent_edit_preserved_and_stale_resolution_rejected(tmp_path):
    c=Central(tmp_path/'central');assert c.edit('order',{'title':'first'},0)['revision']==1
    conflict=c.edit('order',{'title':'second'},0);assert 'conflict_id' in conflict
    assert len(c.snapshot()['conflicts'])==1
    with pytest.raises(ValueError):c.resolve(conflict['conflict_id'],{'title':'merged'},0)
    assert c.resolve(conflict['conflict_id'],{'title':'merged'},1)['revision']==2
    with pytest.raises(ValueError):c.resolve(conflict['conflict_id'],{},2)

def test_actual_concurrent_order_edits(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    c=Central(tmp_path/'central')
    with ThreadPoolExecutor(max_workers=8) as pool:
        results=list(pool.map(lambda i:c.edit('shared',{'writer':i},0),range(20)))
    assert sum('revision' in r for r in results)==1
    assert sum('conflict_id' in r for r in results)==19
    state=c.snapshot()
    assert len(state['orders'])==1 and len(state['conflicts'])==19


@pytest.mark.parametrize('body', [{}, {'accepted': 'bad'}, {'accepted': [None]}, {'accepted': ['unknown']}, []])
def test_invalid_ack_never_discards_pending(tmp_path, monkeypatch, body):
    import core
    class Reply:
        def raise_for_status(self): pass
        def json(self): return body
    monkeypatch.setattr(core.httpx, 'post', lambda *a, **kw: Reply())
    box=Outbox(tmp_path/'outbox','a');event=box.enqueue({'value':1})
    with pytest.raises(ValueError): box.sync('http://central','key')
    assert box.pending()==[event]

def test_partial_ack_and_duplicate_ack(tmp_path, monkeypatch):
    import core
    box=Outbox(tmp_path/'outbox','a');a=box.enqueue({'v':1});b=box.enqueue({'v':2})
    class Reply:
        def raise_for_status(self): pass
        def json(self): return {'accepted':[a['id'],a['id']]}
    monkeypatch.setattr(core.httpx,'post',lambda *args,**kw:Reply())
    with pytest.raises(ValueError):box.sync('http://central','key')
    assert box.count()==2
    Reply.json=lambda self:{'accepted':[a['id']]}
    assert box.sync('http://central','key')==1
    assert box.pending()==[b]
