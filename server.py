import json,os,sqlite3
from pathlib import Path
from fastapi import FastAPI,HTTPException,Header
from fastapi.responses import FileResponse
from pydantic import BaseModel,Field
from core import Central
app=FastAPI(title='RelayMesh failure laboratory')
KEYS=json.loads(os.environ.get('RELAY_KEYS','{"alpha":"demo-alpha","beta":"demo-beta","gamma":"demo-gamma"}'))
def central():return Central(os.environ.get('RELAY_DB','artifacts/central.sqlite'))
class Event(BaseModel):
    id:str=Field(min_length=1,max_length=100)
    station:str=Field(min_length=1,max_length=50)
    seq:int=Field(gt=0)
    payload:dict
class Batch(BaseModel):events:list[Event]=Field(min_length=1,max_length=100)
class Edit(BaseModel):
    body:dict
    revision:int=Field(ge=0)
def admin(key):
    if key!=os.environ.get('RELAY_ADMIN_KEY','local-admin-only'):raise HTTPException(403,'Lab admin key required')
@app.get('/')
def home():return FileResponse(Path(__file__).parent/'index.html')
@app.get('/health')
def health():return {'status':'alive'}
@app.post('/sync/push')
def push(batch:Batch,x_station_key:str=Header(default='')):
    station=next((s for s,k in KEYS.items() if k==x_station_key),None)
    if not station:raise HTTPException(403,'Unknown station key')
    try:return {'accepted':central().push([e.model_dump() for e in batch.events],station)}
    except PermissionError as e:raise HTTPException(403,str(e))
    except (ValueError,sqlite3.IntegrityError) as e:raise HTTPException(409,str(e))
@app.get('/state')
def state():return central().snapshot()
@app.put('/orders/{identifier}')
def edit(identifier:str,value:Edit,x_admin_key:str=Header(default='')):
    admin(x_admin_key)
    return central().edit(identifier,value.body,value.revision)
@app.post('/conflicts/{identifier}/resolve')
def resolve(identifier:int,value:Edit,x_admin_key:str=Header(default='')):
    admin(x_admin_key)
    try:return central().resolve(identifier,value.body,value.revision)
    except ValueError as e:raise HTTPException(409,str(e))
