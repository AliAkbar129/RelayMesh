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
