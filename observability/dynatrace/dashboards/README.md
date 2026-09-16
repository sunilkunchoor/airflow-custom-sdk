# Airflow Monitoring Dashboards (Dynatrace)

This folder contains three Dynatrace dashboards that let anyone — engineer or not —
see how our Airflow data pipelines are running. This document explains what each
dashboard is for, what the sections and tiles mean, and where the underlying numbers
(metrics and traces) come from, in plain language.

If you just want the short version: **start with Dashboard 1** for the overall
fleet health, **drop into Dashboard 2** when you want to look at one specific
pipeline (DAG), and **use Dashboard 3** when you need to reconstruct exactly what
happened during one specific run of a pipeline (for example, to investigate a
failure).

## A few terms, explained simply

- **Airflow** — the system that runs our scheduled data pipelines (things like
  "every night, pull data from A, transform it, load it into B").
- **DAG** ("Directed Acyclic Graph") — the definition of one pipeline: a named
  sequence of steps (tasks) with dependencies between them, e.g. "clean data" must
  finish before "load data" starts. Think of a DAG as a recipe.
- **DAG run** — one execution of a DAG, at a particular scheduled or manually
  triggered time. If the recipe is a DAG, a DAG run is "making the dish on
  Tuesday at 2am."
- **Task / Task instance** — one step within a DAG run (e.g. "clean data" for
  Tuesday's run).
- **Dynatrace** — the monitoring tool these dashboards live in. It receives two
  kinds of data from Airflow:
  - **Metrics** — numbers over time (counts, durations, gauges) such as "how many
    tasks failed in the last 5 minutes" or "how many free worker slots are left."
    Metrics are good for spotting trends and setting alerts, but they are
    *aggregated* — they can't tell you about one specific run, only about totals
    or groups.
  - **Traces** — a detailed, step-by-step record of one specific DAG run: which
    tasks ran, in what order, how long each took, and whether it succeeded. Traces
    are the only place that can answer "what exactly happened during *this*
    9:03am run on Tuesday."
- **Tile** — one chart, table, or number box on a dashboard.
- **Variable / filter** — a dropdown at the top of a dashboard (e.g. "choose a
  DAG") that narrows every tile below it down to just what you picked.
- **p50 / p90 / p99** — percentile durations. p50 is the "typical" (median)
  duration; p90 and p99 show how bad the slow outliers get (90% and 99% of runs
  finished within this time). Useful for spotting "it usually works fine, but the
  worst 1% are really slow."

## Our environment, in brief

- Airflow 3.x, both metrics and traces are switched on.
- We run a mix of two execution engines (Celery and Kubernetes), plus separate
  "Edge" workers for remote/offline task execution.
- OpenLineage (data-lineage tracking) is enabled.
- Large scale: 200+ pipelines (DAGs) with many running at once. Because of this,
  several tiles show "top 20/25" results or need a DAG picked from the dropdown
  rather than showing everything at once — otherwise the dashboard would be
  overwhelmed with data.

---

## Dashboard 1 — Fleet-Wide Metrics
**File:** `01-airflow-fleet-metrics.json`

**What it's for:** the single "is everything OK?" dashboard for the whole Airflow
platform — every pipeline, every worker, every component — in one place. This is
the one to check first each morning, or to put on a wall monitor.

**Filters available at the top:** DAG, Task, Pool, Executor type, Worker name,
Host — pick any of these to narrow the whole dashboard down.

It's organized into 15 sections, each covering one part of the Airflow system:

1. **Cluster Health / Heartbeats** — is the core Airflow engine (scheduler, DAG
   processor, triggerer) still alive and checking in on time? Includes an alert
   for tasks that got killed because their heartbeat (a periodic "I'm still
   alive" signal) stopped arriving.
2. **Scheduler Performance** — how fast is the scheduler (the component that
   decides what runs next) doing its job? Includes how long it takes to plan the
   next batch of work, and whether tasks are piling up waiting to run
   ("starving").
3. **DAG Processing / Parsing** — how long it takes Airflow to read and interpret
   our pipeline definition files, whether any files have errors, and whether any
   files have gone stale (haven't been successfully re-read in a while).
4. **DAG Runs** — how long full pipeline runs take to succeed or fail, and how
   much delay there is between a run being scheduled and actually starting.
5. **Task Instances** — the busiest section: how individual pipeline steps are
   performing — success/failure rates, durations, how long tasks wait in queues.
   Because this can combine "pipeline × step × status" it's the highest-volume
   data in the dashboard, so it's shown as a top-25 table — use the DAG/Task
   filters at the top for a specific pipeline.
6. **Pools** — Airflow "pools" are shared capacity limits (e.g. only 10 tasks can
   hit a database at once). This shows how full each pool is.
7. **Executor** — the engines that actually run tasks (Celery, Kubernetes, Edge).
   Shows available capacity and, for Kubernetes, whether pod creation/deletion is
   succeeding (a non-200 status here is flagged as an alert).
8. **Triggerer** — the component that handles tasks waiting on external events
   (e.g. "wait until a file appears"). Shows capacity and how many triggers are
   blocked, failed, or succeeded.
9. **Edge Workers** — status of our remote/offline workers: are they connected,
   how busy are they, are their heartbeats arriving.
10. **Celery** — errors specific to the Celery execution engine (timeouts, failed
    command execution).
11. **API Server** — the internal cache Airflow's API uses for pipeline
    definitions: hit rate, size, and how often it gets cleared.
12. **Connection Tests** — when someone tests a data connection (e.g. "can we
    reach this database?") from the Airflow UI, this tracks whether those tests
    succeed, how long they take, and how many are queued.
13. **Assets & Deadlines** — Airflow's data-asset tracking (when a dataset
    updates, what pipelines that triggers) and deadline alerts (pipelines that
    were supposed to finish by a certain time).
14. **OpenLineage** — whether our data-lineage tracking system is successfully
    receiving events from Airflow (marked for review — see note in the dashboard
    itself about confirming the exact metric names on our setup).
15. **Operators** — success rates broken down by the *type* of step (e.g. "Python
    script" vs "SQL query" vs "run a container").

**How to read a tile:** single numbers ("stat tiles") show a current state (e.g.
how many free pool slots right now). Line charts show trends over time (rates,
durations). Tables are used wherever a section can involve many pipelines/tasks at
once, so you can sort and scan rather than looking at 200 tiny charts. Anything
colored red/amber is an actionable problem (e.g. import errors, failed pod
creation, pools nearly full).

---

## Dashboard 2 — Per-DAG Drill-Down
**File:** `02-airflow-dag-level.json`

**What it's for:** once you know *which* pipeline you're interested in (from
Dashboard 1, or because someone reported an issue with a specific pipeline), this
dashboard focuses on just that one DAG and shows everything about it: every run,
every task, every step.

**Filters:** pick one or more DAGs from the dropdown at the top — every tile below
updates to show only those pipelines. There's also a Task filter, but it only
applies to one specific "trend" chart — the rest intentionally show *every* task
in the DAG at once so you get the full picture, not a partial one.

Sections:

1. **DAG Run Overview** — for the selected pipeline(s): how long runs take to
   succeed or fail, scheduling delay, and an overall success rate.
2. **Task Breakdown Within This DAG** — every step in the pipeline, side by side:
   which steps fail most, which are slowest, which are currently queued/running,
   and recent activity. This is the section to check when a pipeline is slow or
   failing and you need to know *which step* is the culprit.
3. **Task Lifecycle / DAG Structure Changes** — tracks changes to the pipeline
   itself over time: steps added or removed, and any errors in custom
   "callback" code attached to the pipeline.
4. **Associated File-Processing Health** — how long it takes Airflow to read the
   file that defines this pipeline. Note: this is tracked per *file*, not per
   pipeline — if several pipelines share one definition file, these numbers
   reflect the whole file, not just this one DAG.
5. **Edge Worker Tasks For This DAG** — if any steps of this pipeline run on our
   remote/offline workers, this shows their activity specifically for this DAG.

---

## Dashboard 3 — Single Run Investigator (Metrics + Traces)
**File:** `03-airflow-dagrun-metrics-traces.json`

**What it's for:** the deepest level of detail — reconstructing exactly what
happened during **one specific run** of one pipeline (e.g. "why did last night's
2am run of `daily_sales_load` fail?"). This is the dashboard to use for incident
investigation.

This one works differently from the other two because it mixes two types of data:
metrics (which can narrow to a pipeline, but not to one single run) and traces
(which can pinpoint one exact run, second by second). It's designed as a
three-step workflow:

**Step 1 — Anomaly Timeline (metrics).** Pick a DAG. This section charts run
durations, scheduling delay, and task failures over time so you can visually spot
*roughly when* something went wrong.

**Step 2 — Run Lookup (traces).** Once you've narrowed the time window (using the
dashboard's time range picker) to around when the anomaly happened, this section
lists the actual runs of that pipeline in that window — worst/failed runs listed
first — so you can identify the exact Run ID you're interested in.

**Step 3 — Run Inspector (traces).** Enter that Run ID into the dropdown, and this
section shows everything about that one run: total duration, how many steps it
had, and a full "waterfall" table of every step in the order it happened —
duration, whether it succeeded, whether it was retried, and a direct link to that
step's logs. If a more detailed tracing option is enabled on our setup, an
additional table shows sub-steps within each task.

**Step 4 — Cross-Check Against Metrics.** Finally, this section pulls the
aggregate metrics for that same pipeline, narrowed to that run's time window, as a
sanity check against what the trace showed. This is labeled "best-effort" because
metrics genuinely cannot be filtered down to one exact run — only traces can do
that — so this cross-check is an approximation, most reliable when the pipeline
isn't running multiple overlapping instances at the same time.

**Note:** a few technical details of our specific Dynatrace/Airflow setup were not
confirmed at the time this dashboard was built (e.g. exact naming of some
attributes). These are flagged directly inside the dashboard (as "REVIEW" notes)
rather than guessed at, so the numbers can be trusted once verified.

---

## Where the numbers come from (metrics glossary)

Every metric name below is automatically prefixed with `airflow.` by our OpenTelemetry
configuration (e.g. Airflow's internal `scheduler_heartbeat` metric shows up in
Dynatrace as `airflow.scheduler_heartbeat`). Grouped by dashboard section:

**Heartbeats & core health**
`scheduler_heartbeat`, `scheduler_heartbeat_failure`, `triggerer_heartbeat`,
`triggerer_heartbeat_failure`, `scheduler_job_start`/`scheduler_job_end`,
`triggerer_job_start`/`triggerer_job_end`, `dag_processor_heartbeat`,
`task_instances_without_heartbeats_killed` — is the system alive and checking in?

**Scheduler internals**
`scheduler.critical_section_duration`, `scheduler.critical_section_query_duration`,
`scheduler.critical_section_busy`, `scheduler.scheduler_loop_duration`,
`scheduler.executor_heartbeat_duration`, `scheduler.tasks.starving`,
`scheduler.tasks.executable`, `scheduler.dagruns.running`,
`scheduler.orphaned_tasks.cleared`/`.adopted`, `scheduler.tasks.killed_externally`
— how efficiently the scheduler is planning work.

**DAG file parsing**
`dag_processing.total_parse_time`, `dag_processing.last_duration`,
`dag_processing.last_run.seconds_ago`, `dag_processing.import_errors`,
`dag_processing.file_path_queue_size`, `dag_processing.processes`,
`dag_processing.processor_timeouts`, `dag_processing.manager_stalls`,
`dag_file_refresh_error` — health of reading pipeline definition files.

**DAG runs (per pipeline)**
`dagrun.duration.success`, `dagrun.duration.failed`, `dagrun.schedule_delay`,
`dagrun.dependency_check`, `dagrun.first_task_scheduling_delay`,
`dagrun.first_task_start_delay` — how full pipeline runs perform.

**Tasks (per pipeline step)**
`ti.start`, `ti.finish`, `ti.queued`, `ti.running`, `ti.scheduled`, `ti.deferred`,
`ti_successes`, `ti_failures`, `task.duration`, `task.scheduled_duration`,
`task.queued_duration`, `task_instance_created`, `task_removed_from_dag`,
`task_restored_to_dag`, `previously_succeeded`, `dag.callback_exceptions` — how
individual steps perform.

**Pools (shared capacity limits)**
`pool.open_slots`, `pool.queued_slots`, `pool.running_slots`,
`pool.deferred_slots`, `pool.scheduled_slots`, `pool.starving_tasks`.

**Executors (Celery / Kubernetes)**
`executor.open_slots`, `executor.queued_tasks`, `executor.running_tasks`,
`kubernetes_executor.pod_creation`, `kubernetes_executor.pod_creation_status`,
`kubernetes_executor.pod_deletion_status`, `kubernetes_executor.pod_patching_status`,
`kubernetes_executor.adopt_task_instances.duration`,
`batch_executor.adopt_task_instances.duration`,
`edge_executor.sync.duration`, `celery.task_timeout_error`,
`celery.execute_command.failure`.

**Triggerer**
`triggerer.capacity_left`, `triggers.running`, `triggers.blocked_main_thread`,
`triggers.failed`, `triggers.succeeded`.

**Edge workers**
`edge_worker.status`, `edge_worker.connected`, `edge_worker.maintenance`,
`edge_worker.jobs_active`, `edge_worker.concurrency`,
`edge_worker.free_concurrency`, `edge_worker.num_queues`,
`edge_worker.heartbeat_count`, `edge_worker.ti.start`, `edge_worker.ti.finish`.

**API server, assets, deadlines, lineage, operators**
`api_server.dag_bag.cache_hit`/`.cache_miss`/`.cache_clear`/`.cache_size`,
`connection_test.success`/`.failed`/`.active`/`.pending`/`.reaped`/`.dispatch_duration`,
`asset.updates`, `asset.triggered_dagruns`, `asset.orphaned`,
`deadline_alerts.deadline_created`/`.deadline_missed`/`.deadline_not_missed`,
`ol.emit.attempts`/`.emit.failed`/`.extract`/`.event.size` (OpenLineage — flagged
for confirmation of exact naming on our pipeline), `operator_successes`,
`operator_failures`, `resumable_job.count` (flagged — not a standard Airflow
metric name, needs confirming against our Airflow/provider version).

## Where the numbers come from (trace glossary — Dashboard 3 only)

Traces record one DAG run as a tree of "spans." The top-level span is the run
itself (`dag_run.{dag_id}`); each pipeline step is a `task_run` child span
carrying:

- `airflow.dag_id`, `airflow.dag_run.run_id` (or `airflow.run_id`) — which
  pipeline and which run.
- `airflow.task_id`, `try_number`, `map_index` — which step, which attempt.
- `hostname`, `operator`, `pool`, `job_id`, `executor_state` — where and how it
  ran.
- `log_url` — a direct link to that step's logs.

If a more detailed tracing setting (`task_span_detail_level`) is turned on, each
step can also have nested sub-step spans underneath it, which Dashboard 3 will
show automatically if present (and will simply appear empty if the setting is
off — that's expected, not a bug).

## A note on "why does this only show the top 20/25?" or "why do I have to pick a DAG?"

With 200+ pipelines running, some at the same time, showing every pipeline × every
step × every status in one chart would be unreadable and could overload the
dashboard. So the fleet-wide dashboard (1) intentionally caps the busiest tables to
a top-N view, and points you toward Dashboard 2 (pick one pipeline) or Dashboard 3
(pick one run) for the full detail. This is by design, not a limitation to work
around.
