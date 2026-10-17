# Recovery experiment

Run `python experiments/reliability_matrix.py --trials 5 --count 40` from the repository root on Linux. The experiment launches real processes and localhost HTTP servers, allocates temporary databases and uses SIGKILL. Do not run against an existing deployment.

Seeds 42–46 each generate 40 JSON observations per station across three stations. The acceptance boundary is a successful committed SQLite insert. We kill the producer after all inserts, reconstruct its outboxes, attempt delivery with no central service, start the central service, discard the first successful batch reply, kill the central process, restart it, and race three synchronizers per station. We compare every recovered payload and sequence to its accepted event, count unique central rows, count pending records and run SQLite integrity_check. Each trial has fresh state.

The references illustrate acceptance semantics: a volatile queue acknowledges RAM insertion and loses those records when the process is killed; synchronous-only delivery during an outage rejects attempts and therefore has no accepted-event loss. They are simple policies, not external products. Retries may produce multiple acknowledgements for the same ID; unique stored effects, not exactly-once network delivery, are measured.

This checkpoint experiment does not test disk destruction, arbitrary power failure, a kill in the middle of SQLite commit, WAN latency, multiple machines, long-duration operation or production load. All 600 accepted events across the five executed trials recovered with identical payloads and no pending records. See raw evidence/reliability_matrix.json and the script hash recorded there. Timing includes setup and is not a throughput benchmark.
