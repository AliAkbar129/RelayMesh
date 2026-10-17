# RelayMesh — executable synchronization reference

A Python first milestone for a distributed-systems portfolio project. It is not the complete proposed Go/PostgreSQL system or a production synchronization service.

## Run

```bash
python -m venv .venv
# Activate .venv.
python -m pip install -r requirements.txt
python -m pytest -q tests
python -m uvicorn server:app --host 127.0.0.1 --port 8022
```

Open http://127.0.0.1:8022 for station counts and work-order conflict resolution. In another terminal, run `python demo.py` against a fresh central DB. It creates 60 offline events across three persistent outboxes, submits one batch but deliberately discards its reply, reopens the client state and retries. The result is saved in `artifacts/demo.json`. Run a new server with a new `RELAY_DB` file for each fresh experiment; the demo refuses an already populated central DB.

Individual station process:

```bash
python agent.py --db artifacts/alpha.sqlite --station alpha --enqueue '{"sample":1}'
python agent.py --db artifacts/alpha.sqlite --station alpha --key demo-alpha --sync
```

Configure `RELAY_KEYS` as a JSON station-to-token mapping and `RELAY_ADMIN_KEY` for the work-order API. Defaults are clearly labelled local demo credentials, not production authentication. Agents use HTTP; central and local state use SQLite WAL with FULL synchronization. Keep the default deployment on localhost.

## Contract and evidence

Local acceptance occurs after a committed outbox insert. Central insertion and deduplication occur in one transaction; retries of identical IDs do not duplicate effects. Conflicting ID content and cross-station writes are rejected. Per-station sequence uniqueness is enforced, but no global order is promised. Work-order edits carry a base revision; competing proposals are retained as conflicts and resolutions check the current revision.

`evidence/http_recovery.json` records an executed HTTP experiment with a separate central server process. It is one single-host recovery scenario, not an availability SLA. Client reopen models restart; it is not an OS-level kill experiment. Tests cover reopen, identities, duplicate content, transaction rollback and conflicting edits.

Remaining: baseline comparison against synchronous/in-memory delivery, repeated fault schedules, OS-kill tests, pull cursors, tombstones, snapshots, PostgreSQL, Go implementation if warranted, device registration, deployment hardening, and throughput/load characterization. No exactly-once or disk-destruction guarantee.

Original reference implementation with AI assistance. Uses Python, SQLite, FastAPI, Pydantic and HTTPX; their licenses remain applicable. No external source application was copied or rebranded.
