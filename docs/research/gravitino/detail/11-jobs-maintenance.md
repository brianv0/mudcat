> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Jobs, table maintenance, optimizer, plugins (ref prefix JM)

Scope read: `core/.../job/**`, `core/.../connector/job/**`, `core/.../maintenance/policy/**`, `maintenance/**` (jobs, optimizer, optimizer-api, updaters), `plugins/k8s-job-executor`, `plugins/maintenance`, the four design docs, `docs/*job*`, `docs/table-maintenance-service/*`, `scripts/postgresql/schema-2.0.0-postgresql.sql`. Gravitino commit 9aa78cf. All paths are repo-relative to `/home/user/apache/gravitino`. Items marked `(inferred)` are not stated in code.

## 1. Decomposition

Gravitino has three layers that stack. mudcat should see them as three separable problems.

**Layer A. Generic Job framework (core, SPI).**
- `core/src/main/java/org/apache/gravitino/job/JobManager.java` (1933 lines). Owns job templates, job runs, the status-pull thread, the staging-dir cleanup thread, cancel, and recovery of half-submitted jobs.
- `core/.../job/JobTemplateResolver.java`, `JobTemplatePlaceholderUtils.java`. Template placeholders `{{name}}`, `{{name:-default}}`; escape `\{{`. A required placeholder with no value fails the run before anything is created.
- `core/.../connector/job/JobExecutor.java`. The executor SPI (`@DeveloperApi`): `initialize`, `submitJob(JobContext, JobTemplate)`, `findSubmittedJob`, `getJobExecutionInfo`, `cancelJob`, `ownsJob`, `isJobStateNodeLocal`, `getJobStdout/Stderr`. `JobContext` carries only `jobId`, `metalake`, `stagingDir` (`connector/job/JobContext.java:36-60`). It carries no principal, no credential, no data scope.
- `core/.../job/JobExecutorFactory.java`. Config `gravitino.job.executor=<name>` and `gravitino.jobExecutor.<name>.class`. Built-ins: `local` (`job/local/LocalJobExecutor.java`) and `k8s` (plugin, loaded by class name). It checks at load time that a custom class implements the required methods (`checkJobExecutorClass`, line 101).
- Job entity: `job_run_meta` (id, template id, metalake id, `job_execution_id`, status, started/finished, `runtime_job_template`, audit, versions, `deleted_at`). `UNIQUE(metalake_id, job_execution_id, deleted_at)` (schema-2.0.0-postgresql.sql:827-843). Template entity: `job_template_meta` with `current_version/last_version`.
- Config keys: `gravitino.job.stagingDir`, `gravitino.job.stagingDirKeepTimeInMs` (default 7 days, `Configs.java:642`), `gravitino.job.statusPullIntervalInMs` (default 5 min, `Configs.java:653`), `gravitino.job.outputMaxLines/outputMaxBytes`, local executor `maxRunningJobs`, `waitingQueueSize`, `cancelForceKillDelayInMs`, `jobStatusKeepTimeInMs`.
- REST: `server/.../web/rest/JobOperations.java`. Privileges `REGISTER_JOB_TEMPLATE`, `USE_JOB_TEMPLATE`, `RUN_JOB` (`docs/jobs.md:63-69`). Run requires `METALAKE::OWNER || (METALAKE::RUN_JOB && (ANY_USE_JOB_TEMPLATE || JOB_TEMPLATE::OWNER))` (JobOperations.java:424-425).
- Events: about 30 pre/post/failure events under `core/.../listener/api/event/job/`.

**Layer B. Built-in maintenance jobs (Spark programs plus templates).**
- `maintenance/jobs/.../iceberg/`: `IcebergRewriteDataFilesJob` (compaction, binpack or sort), `IcebergRewriteManifestsJob`, `IcebergExpireSnapshotsJob`, `IcebergRemoveOrphanFilesJob`, `IcebergUpdateStatsAndMetricsJob`, `IcebergUpdateManifestStatsJob`, and the TMS multi-task driver `IcebergMaintenanceJob` (`builtin-iceberg-maintenance-task`). All wrap Iceberg Spark procedures. `BuiltInJobTemplateProvider` registers them through the `JobTemplateProvider` SPI.
- `core/.../job/BuiltInJobTemplateEventListener.java`. Registers built-in templates into each metalake on create/enable and on startup. A `customFields.version=vN` decides replacement.
- `MaintenanceActionHttpClient`. Plain `HttpURLConnection` callback client the Spark job uses to report `metrics/before`, `metrics/after`, `finish`. Auth types `none|simple|basic|oauth`.

**Layer C. Policy-driven automation.**
- Policies (`api/.../policy/Iceberg*Content.java`): `system_iceberg_compaction`, `..._rewrite_manifests`, `..._snapshot_expiration`, `..._orphan_file_removal`. Content = thresholds + `trigger-expr` + `score-expr` + `job.options.*`; the TMS-era content adds `commitTrigger`, `timeTrigger` (cron), `minIntervalMs`, `jobOptions`, `updaterOptions`, `rewriteOptions`. Policies attach to catalog, schema, table, or via tags. Nearest level wins (`plugins/maintenance/.../policy/NearestMaintenancePolicyResolver.java:55-90`).
- Optimizer (CLI era, `maintenance/optimizer*`): `Updater` (statistics/metrics into Gravitino or JDBC), `Recommender` (`StrategyProvider` + `StatisticsProvider` + `TableMetadataProvider` + `JobSubmitter`, all SPI by ServiceLoader), `StrategyHandler` per type (`BaseExpressionStrategyHandler`, QLExpress `trigger-expr`/`score-expr`), `Monitor` (before/after metric windows).
- TMS (`plugins/maintenance`, in-process plugin on the main server): db-scheduler with three pools and three tables `scheduled_expand_tasks`, `scheduled_cron_tasks`, `scheduled_commit_tasks`; `policy-expand` fans out to `cron-job`; `PostCommitMaintenanceListener` (an `EventListenerPlugin`, `Mode.ASYNC_ISOLATED`) turns an Iceberg REST `IcebergUpdateTableEvent` into a `commit-job`; `table_maintenance_action` (soft slot per table and policy); `table_snapshot_metrics` (JSON per table, policy, Iceberg snapshot id); callback REST `metalakes/{m}/maintenance/actions/{id}/{metrics/before|metrics/after|finish}`.
- Auth for jobs is cluster-static: `gravitino.maintenance.gravitinoAuth.*` (job to Gravitino) and `gravitino.maintenance.ircAuth.*` (Spark to Iceberg REST, passthrough of `rest.auth.*`). `core/.../maintenance/policy/MaintenanceAuthConfigs.java`.

**Layer D. Async hard delete in the Iceberg REST server** (`design-docs/async-iceberg-rest-hard-deletion.md`, code under `iceberg/iceberg-rest-server/.../service/cleanup/`). Table `iceberg_cleanup_job` with PENDING/RUNNING/SUCCEEDED/FAILED, heartbeat CAS, tombstone against name reuse. It is a second, unrelated job system next to Layer A.

**Extension points.** `JobExecutor` (SPI), `JobTemplateProvider` (SPI), optimizer `StrategyProvider/StatisticsProvider/TableMetadataProvider/JobSubmitter/MetricsProvider/MetricsEvaluator/TableJobRelationProvider/MonitorCallback` (ServiceLoader), `gravitino.optimizer.strategyHandler.<type>.className`, `EventListenerPlugin`, `extensionPackages` for REST features.

**Neighbours.** Catalog entity store (job rows, policy rows, tables by numeric entity id); Iceberg REST service (commit events, credential vending, purge); policy and tag subsystems (attachment, inheritance); authorization (job privileges); SecretManager (conf references). No lineage module call exists in any file under `maintenance/` or `core/.../job/` (grep for `lineage|openlineage|prov` found only test files and a Javadoc "provider" hit).

## 2. Candidate requirements

Notation: kind IMPL = Gravitino implements it; GAP = Gravitino omits or fails it. Evidence paths are repo-relative.

### 2.1 Job state machine, persistence, restart, retry

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| JM-R1 | IMPL | mudcat shall commit a job record before it hands the job to an executor. | Gravitino registers the job with a placeholder execution id `pending-submission:<id>`, so a crash after submit cannot leave a running execution that the catalog does not know. `JobManager.java:611-614`, `:137`. | OP | T |
| JM-R2 | IMPL | mudcat shall record the owner of a job before it submits the job. | Recovery by another node does not rerun the request's post hook; ownership written after submit would be lost. `JobManager.java:663-664`. | AC | T |
| JM-R3 | IMPL | When a submission has an unknown outcome, mudcat shall keep the job record, mark it cancelling, and look the execution up by job ID in the runner before it settles the job. | `JobExecutor.findSubmittedJob` contract: "Do not classify a lost response as a confirmed rejection"; empty result only when the runner confirms no execution. `connector/job/JobExecutor.java:62-80,165`; `JobManager.java:724-775,1839-1900`. | OP | T |
| JM-R4 | IMPL | mudcat shall never replay a submission whose outcome is unknown. | Replay could start two executions. `JobManager.java:713` ("Never replay the submission"). K8s executor makes the resource name deterministic from the job id so a retried create conflicts instead of duplicating: `K8sJobResourceUtils.java:87`, `K8sJobExecutor.java:209-236`. | OP | T |
| JM-R5 | IMPL | mudcat shall require a job executor to make cancel idempotent and to report a cancelling state once cancel is acknowledged. | The core may repeat cancel after a failure or while the runner still says queued or started. `JobExecutor.java:213-221`. | OP | T |
| JM-R6 | IMPL | mudcat shall not move a job out of a terminal state, and shall not move a cancelling job back to a non-terminal state, whatever a stale status poll reports. | Poll results race with cancel; the updater re-reads the latest row and applies the rule. `JobManager.java:1469-1480,1604-1610`. | OP | T |
| JM-R7 | IMPL | mudcat shall ignore or correct runner-reported timestamps that are in the far future, before the queue time, or inconsistent with the status. | Runner clocks differ; an `Instant.MAX` sentinel would stop retention cleanup. `JobManager.java:1502-1590` (`usableTime`, `notBeforeQueuedAt`, MAX_REPORTED_TIME_AHEAD one day, line 99). | OP | T |
| JM-R8 | IMPL | mudcat shall let any node settle a job whose submitter node died, within a bounded multiple of the status-poll interval. | Three poll intervals for an unrecorded submission and for an unanswered cancel. `JobManager.java:122-130,148-152,1807-1912`. | OP | T |
| JM-R9 | IMPL | mudcat shall allow an executor to declare that job state is local to one node, and shall route status and cancel for such jobs to the owner node only. | `ownsJob`, `isJobStateNodeLocal`. `JobExecutor.java:225-262`; `JobManager.java:1388-1396`. | OP | T |
| JM-R10 | GAP | mudcat shall make a job record survive a node restart with a defined final status within a time that an operator sets, without waiting for a retention timeout. | Gravitino's local executor loses the process on restart; the job stays "started" until `stagingDirKeepTimeInMs` (default 7 days, about 7.7 days worst case) expires, and then is marked FAILED even if it is still running. `docs/local-job-executor.md:125-145`; `JobManager.java:1735-1766`. | OP | T |
| JM-R11 | GAP | mudcat shall keep the record of a finished maintenance run for at least as long as the data it changed exists. | Gravitino deletes a finished job row `stagingDirKeepTimeInMs` (7 days) after it finishes, and deleting a job template deletes all its finished jobs. `JobManager.java:1737-1760,356`. The audit trail of what compaction or expiry did is gone while the files it removed are also gone. | PR | T |
| JM-R12 | GAP | mudcat shall query only non-terminal jobs when it polls job status. | Gravitino lists every job of the metalake on every poll and filters in memory; the code comments it as a known inefficiency. `JobManager.java:1362-1370`. | PF | T |
| JM-R13 | IMPL | mudcat shall run each job execution once, and shall start a new job record for a retry. | K8s `restartPolicy: Never` because "Gravitino never changes a job's final status". `plugins/k8s-job-executor/.../spark/SparkApplicationUtils.java:222-227`. Retry policy belongs to the scheduler, not the runner. | OP | I |
| JM-R14 | GAP | mudcat shall accept an idempotency key on a request to run a job and shall return the existing job for a repeated key. | Gravitino's `runJob` takes a template name and configuration only and allocates a fresh id each time (`JobManager.java:560-640`). The optimizer CLI path submits without any in-flight check (`optimizer/.../GravitinoJobSubmitter.java:91`), so two invocations run two jobs on one table. Only the TMS path has a slot (see JM-R20). | ID | T |

### 2.2 Job templates and executor SPI

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| JM-R15 | IMPL | mudcat shall store with each job run the fully resolved template that the run used. | Template can change or be deleted after the run; `runtime_job_template` column plus serialisation before submit. `JobManager.java:578-590`; schema line 835. | PR | T |
| JM-R16 | IMPL | mudcat shall reject a job run that lacks a required template parameter before it creates any resource. | `checkJobConf` runs before staging dir or row creation, and reports all missing names at once. `JobTemplateResolver.java:80-100,170-180`; `JobManager.java:565-568`. | OP | T |
| JM-R17 | IMPL | mudcat shall refuse to delete a job template that has a non-terminal job. | `InUseException`. `JobManager.java:343-352`. | OP | T |
| JM-R18 | IMPL | mudcat shall reject an executor plugin at load time if it lacks a method that the core needs. | A class compiled against an older SPI loads but throws `AbstractMethodError` later; the check fails early. `JobExecutorFactory.java:101-148`. | IN | T |
| JM-R19 | IMPL | mudcat shall ignore a configured class name for a built-in executor name and log a warning. | Prevents a config key from silently replacing a built-in. `JobExecutorFactory.java:78-84`. | OP | T |

### 2.3 Scheduling, triggers, concurrency control

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| JM-R20 | IMPL | mudcat shall allow at most one in-flight maintenance run for each pair of table and policy, enforced by a unique key in the database. | Soft slot: `finished_at = 0` means in flight, `UNIQUE(table_id, policy_id, finished_at)` (schema-2.0.0-postgresql.sql:1325; `TableMaintenanceActionMetaService.java:56-66`). Same trick as `deleted_at`. Relevant to PR-7: the unit is table (entity id) and policy, not name. | OP | T |
| JM-R21 | IMPL | mudcat shall claim the slot before it submits the job, and shall fence the later bind of the job ID by the claim lease. | `tryAcquireInFlight`, DB-clock `created_at` as fence; bind CAS matches zero rows after a reclaim. `TableMaintenanceActionMetaService.java:50-110,170-200`. | OP | T |
| JM-R22 | GAP | mudcat shall release the in-flight slot of a run when the job record reaches a terminal state, without help from the job. | Gravitino releases a bound slot only when the Spark job calls `finish`; the call is "best-effort" and the manager "does not reclaim on Gravitino job terminal state". If the pod is killed, OOMs, or cannot reach the server, the slot stays occupied and the table is never maintained again for that policy. `MaintenanceActionManager.java:30-35`; `IcebergMaintenanceJob.java:183-196`. The design checklist promises "reconcile closes stale occupancy" (`design-docs/table-maintenance-service-design.md:624`) but no reconcile for bound rows exists (only `PolicyExpandReconciler`). | OP | T |
| JM-R23 | IMPL | mudcat shall make a scheduler task claim exclusive across nodes, with a heartbeat that frees a hung short callback but not a long job. | db-scheduler CAS plus heartbeat; scheduler rows are one-shot and deleted after the short `runJob`; Spark lifetime is tracked in `table_maintenance_action`. Design doc §5.2. | OP | T |
| JM-R24 | IMPL | mudcat shall apply a cooldown after the last finished run of the same type on the same table, taken from the policy, then the server default, then a built-in default. | `minIntervalMs` resolution. `MaintenanceMinIntervalConfigs.resolve` (`:116-141`); defaults 1 h for compaction, manifest rewrite, expiry; 1 day for orphan cleanup (`:40-44`). | OP | T |
| JM-R25 | GAP | mudcat shall reject a cooldown of zero for a policy that a commit event triggers. | Gravitino allows `minIntervalMs >= 0` and `isCoolingDown` returns false at `<= 0`; a maintenance commit goes through the Iceberg REST service, raises `IcebergUpdateTableEvent` again, and the commit trigger schedules the next run. Nothing marks a commit as made by maintenance. `MaintenanceMinIntervalConfigs.java:70`; `MaintenanceActionManager.java:111`; `PostCommitMaintenanceListener.java:96-103`. (loop is inferred; each guard is read.) | OP | T |
| JM-R26 | IMPL | mudcat shall bound the work that one trigger creates, and shall merge a burst of commit events for one table into one scheduled run. | `scheduleIfNotExists` keyed `table:{id}`, existing row keeps its data; `expand.scheduleBatchSize` default 100. Design §5.2.3, §7.2. | PF | T |
| JM-R27 | IMPL | mudcat shall make a scheduled-run request carry a snapshot of its decision inputs, and the run shall not re-read policy. | Schedule-time job snapshot in `task_data`; pick "does not re-collect policies or rebuild jobConf". Design §5 intro. Policy changes between schedule and run do not change what runs. | ID | T |
| JM-R28 | IMPL | mudcat shall bind a job parameter that came from a decision (for example partition spec id) at decision time and pass the bound value to the job. | "Do not resolve the default again between these steps: partition evolution could otherwise make the job target a different spec." `design-docs/iceberg-rewrite-manifests-job.md:108,129`. | ID | T |
| JM-R29 | IMPL | mudcat shall end a maintenance run without rewriting data if the table head moved after the decision sample. | In-Spark drift gate compares the sampled snapshot id to the head. `IcebergMaintenanceJob.java:295-310`; design §4.3 chose "gate before" rather than fail at commit (Amoro). It is disabled for expiry and orphan removal (`IcebergExpireSnapshotsJob.java:231`, orphan `IcebergRemoveOrphanFilesJob.java:251`) because those never conflict with writers by design. | ID | T |
| JM-R30 | GAP | mudcat shall resolve a policy conflict at attach time and not at run time. | `resolveWinnerId` throws `IllegalArgumentException` when two policies of one type attach at the nearest level; this runs in the commit listener and in expand, so the table silently drops out of maintenance. `NearestMaintenancePolicyResolver.java:84-89`. Attach time is not checked in the code I read. (attach-time omission inferred) | ST | T |
| JM-R31 | IMPL | mudcat shall not run an expensive full-storage scan type on every commit. | Orphan cleanup rejects `commitTrigger=true` and is time-trigger only. Design §5.1.1. | PF | T |

### 2.4 Credentials, identity, authority of jobs

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| JM-R32 | GAP | mudcat shall run a job with a credential that is scoped to the entities of that run, and not with a standing credential of a service identity. | Gravitino runs every TMS job with the cluster-static identity from `gravitino.maintenance.gravitinoAuth.*` and `ircAuth.*`; that identity needs `MODIFY_TABLE`, `USE_SECRET`, `RUN_JOB` on the whole metalake (design §6). A submitter with `RUN_JOB` and template use can point a built-in job at any table the identity can write. `docs/k8s-job-executor.md:285-311` says jobs run "with their service accounts and the credentials of the job". This matches mudcat's run-token model; the gap is the confused deputy. | AC | A |
| JM-R33 | GAP | mudcat shall authorise a job callback by the run that the callback names, and not by a metalake-wide privilege. | The callbacks `metrics/before`, `metrics/after`, `finish` require `METALAKE::OWNER || METALAKE::RUN_JOB`. Any holder can report metrics for or close any action in the metalake; the check that the row belongs to the caller is only table and policy match. `TableMaintenanceActionOperations.java:93,127,158`; `MaintenanceMetricsManager.java:84,126,166` call `loadInFlight(metalake, actionId, tableId, policyId)`. | AC | T |
| JM-R34 | GAP | mudcat shall keep secrets out of stored job records and of the job's visible launch configuration. | Gravitino's mask is display-only: "This is not encryption: the stored runtime template and the command list ... stay plaintext" (`JobRuntimeTemplateMaskUtils.java:38-41`). OAuth client secret travels in `updater_options` and `spark_conf`; K8s docs: values "are readable in the `sparkConf` and the `driverArgs` of the SparkApplication" (`docs/k8s-job-executor.md:578-581`). Async purge also stores `file_io_props` with credentials in `iceberg_cleanup_job` (design `async-iceberg-rest-hard-deletion.md:227,665`). | AC | I |
| JM-R35 | IMPL | mudcat shall keep secrets out of policy content, and shall reject credential-shaped keys in policy job options. | Policies are versioned and readable with `VIEW_POLICY`. Schedule snapshot stores conf-key references and resolves secret values only just before `runJob`. Design §5.1.2; `MaintenanceJobConfBuilder.java:60,168-190`. | AC | T |
| JM-R36 | IMPL | mudcat shall let a job obtain storage credentials only through the catalog's credential issuing path. | Spark reaches data through the Iceberg REST service; docs say to set `X-Iceberg-Access-Delegation: vended-credentials` under `ircAuth.header.*`; TMS does not inject it. `docs/table-maintenance-service/optimizer-configuration.md:154-167`. Aligned with RD-1 and RD-3. | RD | D |
| JM-R37 | IMPL | mudcat shall reject a job parameter set that changes infrastructure placement or identity (cluster, namespace, service account, secrets, volumes) when the caller is only allowed to run the job. | Built-in jobs ignore `spark.kubernetes.*` in `spark_conf`. `SparkApplicationUtils.java:105-110`. Docs list what a template author (not runner) can still do, for example `secretKeyRef` read-out (`k8s-job-executor.md:285-300`). | AC | T |
| JM-R38 | IMPL | mudcat shall record the principal that requested a job, and a recovered or scheduled job shall keep the submitter's owner. | `AuditInfo.creator`; TMS runs `runJob` as a configured submitter user so group-owned metalakes get a stable owner. `JobManager.java:624`; `MaintenanceJobManager.java:44-50,168-170`. | AC | T |

### 2.5 What the catalog records about rewritten data (PR-7)

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| JM-R39 | GAP | mudcat shall record each compaction, manifest rewrite, snapshot expiry and orphan removal as an activity with its inputs and outputs, not only before and after statistics. | Gravitino records: job row (template, status, times, resolved template), `table_maintenance_action` (table id, policy id, job id, before/after metrics ids), `table_snapshot_metrics` (a JSON of counters per Iceberg snapshot id). It does not record which files were replaced by which files, the resulting snapshot id of a rewrite, or the files deleted. The after-metrics row is keyed by the snapshot current after the job, which may include another writer's commit. `schema-2.0.0-postgresql.sql:1316-1352`; `IcebergExpireSnapshotsJob.java:260-270`. Sizes (`custom-expired-file-size`) are computed by set difference on file listings (`:265-268`), not stored as lists. No lineage call exists. | PR | I |
| JM-R40 | IMPL | mudcat shall key maintenance records by the entity ID of the table and not by its name. | `table_maintenance_action.table_id`, `table_snapshot_metrics.table_id` reference `table_meta` ids, so a rename keeps history. Compare: the optimizer CLI path keys metrics and jobs by `NameIdentifier` (`MetricPoint.forTable`, `DataScope`; `LocalTableJobRelationProvider` maps name to job names), which breaks on rename. | ID | T |
| JM-R41 | GAP | mudcat shall tie each statistic that drives a decision to the version it describes, and shall refuse a statistic of another version. | Optimizer statistics in `statistic_meta` have no snapshot id; the code explicitly avoids using table-level stats for partitioned tables "so partition-aware strategies cannot be driven by stale statistic_meta values" (`BaseExpressionStrategyHandler.java:70-73,140-145`, issues #14674/#14677). The TMS path later fixed this by keying `table_snapshot_metrics` on the Iceberg snapshot id and sampling inside the job. | ST | T |
| JM-R42 | IMPL | mudcat shall compute decision metrics at the moment of the decision, from the data's own metadata, and store them with the version they describe. | "Sampling is inside the Spark job (not on the control plane)"; metrics JSON keyed `(table_id, policy_id, snapshot_id)` with a before and an after row per run. Design §5.2.2; `MaintenanceMetricsManager.reportBeforeMetrics`. | ST | T |
| JM-R43 | GAP | mudcat shall make the claimed effect of a maintenance run verifiable against a second source. | Orphan cleanup reports counts and bytes from a dry-run listing, then calls the procedure again to delete; the reported set and the deleted set can differ. `IcebergRemoveOrphanFilesJob.java:182-217`. Open question 3 in the design ("Dry-run result persistence") is unanswered (`iceberg-remove-orphan-files-maintenance-job.md:590`). | PR | T |

### 2.6 Safety of destructive maintenance (ID-9, UP)

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| JM-R44 | IMPL | mudcat shall not delete an unreferenced file younger than a minimum age, and shall reject a minimum age under one day. | Orphan policy `olderThanDays` in 1..36500, default 3; the Iceberg procedure rejects cutoffs under 24 hours also in dry run. Reason: "A file written by a transaction that has not yet committed looks identical to an orphan file." `IcebergOrphanFileRemovalContent.java:59,141-147`; design `iceberg-remove-orphan-files-maintenance-job.md:220-240`. | UP | T |
| JM-R45 | GAP | mudcat shall learn the set of in-flight writers from the catalog and shall not infer it from file age. | Gravitino's only guard for in-flight writes is a wall-clock window (3 days default). A writer that stages longer than the window loses data or corrupts the table ("produces a table that references a missing file"). The catalog has no writer registry. `iceberg-remove-orphan-files-maintenance-job.md:222-235`. mudcat's staged parts (OP-17, OP-22) and run tokens can close this. | UP | A |
| JM-R46 | IMPL | mudcat shall reject a scan root for orphan removal unless it lies inside the storage location of the target entity, after lexical normalisation of scheme, authority, dots and slashes. | One wrong location deletes another table's files. Checked both at submit and inside the job. `OrphanFileLocationUtils.java:38-52`; job `IcebergRemoveOrphanFilesJob.java:330`; design §6.1. | UP | T |
| JM-R47 | GAP | mudcat shall make a scan for unreferenced files consider every entity, in every tenant, that may reference a file under the scan root. | Gravitino's check is per table: "deletes every file under the location that the target table does not reference". Two tables that share a prefix (a re-created table at the same default location, clones, `register_table`, `add_files`) make the other table's files orphans of this one. The async-purge design records the same-prefix hazard for re-created tables and answers it only with a 409 tombstone (`async-iceberg-rest-hard-deletion.md:550-565`). mudcat shares data by reference (ID-13, OP-15), so this applies to every maintenance scan. | UP | A |
| JM-R48 | GAP | mudcat shall not expire a snapshot or delete a file because of an age and count rule if any ref, resolution, citation or other catalog record names it. | Gravitino's expiry policy has `olderThanDays` and `retainLast` only; protection of branch and tag heads is whatever Iceberg's `expire_snapshots` does (`history.expire.*`). The policy has no field for Gravitino-side references, and grep over the policy content, adapter and job finds no "branch", "tag" or "ref" handling. `IcebergSnapshotExpirationContent` (docs `iceberg-snapshot-expiration-policy.md:20-27`); ID-9 requires the stronger rule. | ID | T |
| JM-R49 | GAP | mudcat shall tell apart an explicit retention setting that equals the default from an unset retention setting. | The adapter sends `older_than=""` when the policy value is 5 and `retain_last=""` when it is 1, so table properties win; a policy that sets exactly 5 days cannot override a table that sets `history.expire.max-snapshot-age-ms` to 30 days. The design doc says the adapter "always submits explicit" values (`iceberg-expire-snapshots-maintenance-job.md:171-175`); the code says the opposite. `GravitinoSnapshotExpirationJobAdapter.java:100-111`. | ST | T |
| JM-R50 | IMPL | mudcat shall turn a relative retention ("older than N days") into an absolute cutoff when the job executes and not when it is scheduled. | Spark job expands `N` to a UTC timestamp at run time, so a job that waits in a queue does not use a stale cutoff. `IcebergJobUtils.normalizeOlderThan` (`:588-606`); adapter comment `GravitinoSnapshotExpirationJobAdapter.java:30-34`. | UP | T |
| JM-R51 | IMPL | mudcat shall require an explicit predicate for a data rewrite and shall not default it to the whole table. | Template parameter `where_clause` has no default "on purpose: an empty where clause rewrites the whole table". `IcebergRewriteDataFilesJob.java:360-363`. | UP | T |
| JM-R52 | IMPL | mudcat shall make a deletion of storage objects safe to retry, order it so that every crash is recoverable, and treat "already gone" as success. | Async purge deletes leaves first (data, manifests, manifest lists, statistics, ancestor metadata) and the root pointer last, because a missing root means "table already gone". `async-iceberg-rest-hard-deletion.md:340-365` (§5.5), crash table §5.6. | UP | T |
| JM-R53 | IMPL | mudcat shall keep the lease heartbeat of a long deletion on a thread other than the thread that deletes. | "If that same thread were responsible for writing heartbeat_at, any slow storage round trip would stall the heartbeat ... a peer would wrongly reclaim a job that is in fact making progress." Heartbeat is a compare-and-swap on the value last written, so ownership transfers without an owner column. Design §5.5 "Heartbeat is decoupled" (`:431`). | OP | T |
| JM-R54 | IMPL | mudcat shall block creation of a new entity at an identifier while a purge for that identifier is still running. | `409` for create or register at the same identifier until cleanup is terminal; the cleanup row is the tombstone; key by catalog id, not name. Design §5.7, §5.4 (`:527-575`, `:256-262`). | UP | T |
| JM-R55 | GAP | mudcat shall lift a name tombstone for a failed purge only together with a record of the files that the purge left, and shall keep that record until an operator closes it. | A FAILED job stops blocking the name, "the cost is that any files the job did not delete are now an orphaned leak"; recovery is out-of-band and "there is no automatic re-drive". Row is pruned after `retention-hours`. Design `:496-505`. For UP-7 a deletion receipt per replica must stay even on failure. | UP | T |
| JM-R56 | GAP | mudcat shall support undrop, or a hold, for a requested purge. | Gravitino async purge is hard delete only: "once a drop is accepted the files are destroyed with no undrop or recovery path". Non-goal in the design (`:83`). Legal hold (UP-8) cannot be honoured by this path; no hold check exists in it. | UP | T |
| JM-R57 | GAP | mudcat shall refresh or avoid snapshotting storage credentials in a purge record. | Credentials are "snapshotted at enqueue time" and stored in `file_io_props`; the doc says they are "refreshed from the catalog on each heartbeat" while the table is already dropped. The stored plaintext is a standing risk. Design §5.11. (refresh behaviour read from doc, not from code) | AC | I |

### 2.7 Optimizer, statistics, strategies

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| JM-R58 | IMPL | mudcat shall let an operator set a trigger and a priority of a maintenance rule as expressions over named metrics, with the expression engine behind an interface. | `trigger-expr` and `score-expr` evaluated by QLExpress behind `ExpressionEvaluator`; compaction trigger default `file-mse >= minFileMse || delete-files >= minDeleteFiles`, score `mse*w1 + deletes*w2`. `IcebergDataCompactionContent.java:151-166`; `QLExpressionEvaluator.java`. | PF | T |
| JM-R59 | IMPL | mudcat shall rank candidate tables or partitions by score and cap the number of partitions in one run. | `maxPartitionNum` default 50, table score = avg/sum/max of top partitions. `BaseExpressionStrategyHandler.java:157-215`; `IcebergDataCompactionContent.java:102`. | PF | T |
| JM-R60 | GAP | mudcat shall treat an expression that fails to evaluate as a visible failure, and shall not turn it into a number that sorts. | On error the handler logs a warning and returns `-1` for a score (and false for a trigger), so a typo in `score-expr` silently disables maintenance for every table. `BaseExpressionStrategyHandler.java:222-245`. | OP | T |
| JM-R61 | IMPL | mudcat shall skip maintenance for a partitioned table when it has no partition statistics, and shall not substitute table-level statistics. | `BaseExpressionStrategyHandler.java:138-147`. | ST | T |
| JM-R62 | IMPL | mudcat shall inherit a maintenance policy by nearest level (entity, then each parent), with exactly one winner per type at a level. | `NearestMaintenancePolicyResolver.java:55-90`; policies attach through tags with effective-tag inheritance (`docs/iceberg-snapshot-expiration-policy.md:99-103`). This is the same inheritance rule as ST-17. | ST | T |
| JM-R63 | IMPL | mudcat shall validate every field of a maintenance policy when the policy is written, with bounds. | `olderThanDays` 1..36500, `retainLast >= 1`, option key pattern `[A-Za-z0-9._-]+`, rewrite options in a fixed whitelist with typed values, location a valid URI without query or fragment. `IcebergDataCompactionContent.java:114-146`; `IcebergOrphanFileRemovalContent.java:141-200`. | ST | T |
| JM-R64 | IMPL | mudcat shall let an enricher or statistics updater be replaced through a provider interface, including the source of statistics. | `StatisticsCalculator`, `StatisticsUpdater`, `SupportsCalculate*` interfaces in `optimizer-api`; local file/payload importers; Gravitino and JDBC sinks (H2, MySQL, PostgreSQL dialects). Relevant to ST-13..ST-21: each provider has a name and `initialize(OptimizerEnv)` but no per-value record of which provider and version wrote a value (see JM-R66). | ST | I |
| JM-R65 | GAP | mudcat shall keep a time-series metric store bounded by a retention rule that is on by default. | The JDBC metrics repository has `DELETE FROM table_metrics WHERE metric_ts < ?` but no scheduler calls it that I found; TMS `table_snapshot_metrics` has `created_at` and a design checklist item for TTL/N purge ("Validation reads metrics ... purge unreferenced history by TTL/N") without code. `JdbcMetricsRepository.java:356,378`; design checklist `:618`. (absence of caller is inferred from grep) | PF | T |
| JM-R66 | GAP | mudcat shall record, for each statistic or metric value, which enricher or job wrote it, its version, and the run. | `StatisticEntry`/`MetricPoint` carry scope, name, value, time only; no producer or version (`MetricPoint.java:30-50`). `table_snapshot_metrics` rows have table, policy, snapshot, JSON; the writer is implied by `policy_id`. Links the value to ST-14 and ST-21. | ST | T |
| JM-R67 | IMPL | mudcat shall store a statistic for a partitioned object under one key per partition, and merge a partial update into the existing value without loss. | Manifest statistics design: one row per statistic name, value an object keyed by spec id; "Coordinate concurrent collectors ... so one read/merge/write does not overwrite another spec's update". `iceberg-rewrite-manifests-job.md:106-123` (design only; not checked in code). | ST | T |

### 2.8 Scheduling hygiene and operations

| Ref | Kind | Requirement | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| JM-R68 | IMPL | mudcat shall make a scheduling plugin start inside the control-plane process and use the PostgreSQL database for its queue and its locks. | Design §4.1-4.2 rejects a second deployable and a second listener; picks db-scheduler (Apache 2.0, DB CAS and heartbeat) over JobRunr (LGPL), ShedLock (lock only), Quartz (heavy). Aligns with OP-1. | OP | I |
| JM-R69 | IMPL | mudcat shall resynchronise scheduler rows from stored policies at start-up. | `reconcileExistingPolicies` on feature start, so upgrades and restored databases get expand rows. `TableMaintenanceRESTFeature.java:66-72,148,188`. | OP | T |
| JM-R70 | GAP | mudcat shall not drop change events that trigger work, or shall make the trigger recoverable from state. | The commit trigger rides on an `ASYNC_ISOLATED` event listener (`PostCommitMaintenanceListener.java:86`) and only for commits that go through Gravitino's Iceberg REST service; design non-goal 3: "Commit reports from engines that bypass Gravitino IRC". A lost event means no maintenance until the next cron. | EV | T |
| JM-R71 | IMPL | mudcat shall make a failed scheduling decision fail closed when two merged tasks would run with different effective credentials. | `MaintenanceAuthConsistency.hasSameAuth` blocks a chain whose members disagree on effective gravitino or IRC auth. `CommitJobPlanner.java:192`. | AC | T |

## 3. Quirks and gotchas ledger

- **Job row exists before the execution does**
  - What happens: a job is persisted with `job_execution_id = "pending-submission:<jobId>"`, then the executor is called, then the real id replaces the placeholder. The placeholder must be unique because the execution id is in a unique key.
  - Cause: CONCURRENCY.
  - Evidence: `JobManager.java:137` `"pending-submission:"`; schema `UNIQUE (metalake_id, job_execution_id, deleted_at)`.
  - Mudcat implication: JM-R1, JM-R3. Use a job ID that mudcat mints as the correlation key to the runner (K8s label `gravitino.apache.org/job-id`), not the runner's id.

- **Executor SPI is a contract about failure, not only about success**
  - What happens: `JobSubmissionRejectedException` and `IllegalArgumentException` mean "nothing was created, delete the row"; any other exception means "unknown, keep the row, cancel, and search". An executor that cannot search must clean up before it throws.
  - Cause: SPEC (SPI contract).
  - Evidence: `connector/job/JobExecutor.java:62-80`.
  - Mudcat implication: JM-R3, JM-R4. State the three-way outcome in the executor SPI of mudcat.

- **Multi-node status polling: only the owning node can ask**
  - What happens: every server has its own executor instance; the local executor's process table is per node; `ownsJob()` gates pull; a cancel on another node is carried out by the owner at its next pull, and a stuck cancel is settled by anyone after 3 intervals.
  - Cause: CONCURRENCY.
  - Evidence: `JobManager.java:1388-1396`; `JobExecutor.java:225-262`.
  - Mudcat implication: JM-R9, JM-R8.

- **Local executor restart orphans jobs for about a week**
  - What happens: new server instance gets a new identity; old jobs are not recognised; they remain started and block deletion of their template until `stagingDirKeepTimeInMs` (7 days default) expires. A genuinely running job older than the keep time is marked FAILED and never corrected when it finishes.
  - Cause: OTHER (design of in-memory executor).
  - Evidence: `docs/local-job-executor.md:125-145`; `JobManager.java:1768-1800` (expireStaleActiveJob).
  - Mudcat implication: JM-R10.

- **Finished-job retention equals staging-dir retention**
  - What happens: one knob controls both the staging files and the job row. After 7 days the row is deleted, and with it the only record of which template and arguments ran.
  - Cause: MODEL (conflates a temp-file lifetime with an audit lifetime).
  - Evidence: `JobManager.java:1737-1760`; `Configs.java:642-651`.
  - Mudcat implication: JM-R11. Keep audit and scratch lifetimes separate.

- **Slot released only by the job itself**
  - What happens: the unique soft slot (`finished_at=0`) is set before submit and cleared by Spark's `finish` call. Failure to call is logged as "Best-effort". No server-side reconcile on terminal job state.
  - Cause: CONCURRENCY / MODEL.
  - Evidence: `IcebergMaintenanceJob.java:183-196`; `MaintenanceActionManager.java:30-35` ("does not reclaim on Gravitino job terminal state"); design checklist `table-maintenance-service-design.md:624` promises a reconcile.
  - Mudcat implication: JM-R22.

- **Soft-slot uniqueness trick reused from tombstones**
  - What happens: `finished_at=0` means "open"; the unique index on `(table_id, policy_id, finished_at)` allows exactly one open row and any number of closed rows, provided closed rows get distinct millisecond times. The design admits two rows that close in the same millisecond would collide, and argues only one open row exists so they cannot.
  - Cause: STORE (portable unique-constraint trick, same as `deleted_at`).
  - Evidence: `schema-2.0.0-postgresql.sql:1324-1325`; `TableMaintenanceActionMetaService.java:411-415`.
  - Mudcat implication: JM-R20. A partial unique index is available in PostgreSQL and is clearer; mudcat only needs PostgreSQL (OP-1).

- **Claim lease uses the database clock**
  - What happens: unbound claims abandon after a TTL measured with DB `now`, not JVM time, so node clock skew does not matter. The claim returns `(action_id, created_at)` as a fence; a stale holder's bind updates zero rows.
  - Cause: CONCURRENCY.
  - Evidence: `TableMaintenanceActionBaseSQLProvider.java:88-102`; `TableMaintenanceActionMetaService.java:56-110,176-190`.
  - Mudcat implication: JM-R21.

- **A successful `runJob` cannot be undone by a failed bind**
  - What happens: the slot claim, the Spark submit and the bind of `job_id` are three steps; if bind fails after submit, the manager cancels the orphan job and deletes the claims. The service doc says the fence "cannot undo a runJob that already succeeded".
  - Cause: CONCURRENCY.
  - Evidence: `MaintenanceJobManager.java:129-158`; `TableMaintenanceActionMetaService.java:69-73`.
  - Mudcat implication: JM-R3, JM-R21; make the job ID and the claim the same row if possible.

- **Maintenance commit may re-trigger maintenance**
  - What happens: Spark jobs commit through the Iceberg REST service, which raises the event that the post-commit listener turns into a `commit-job`. The only brake is the cooldown, and `minIntervalMs = 0` is a legal value that disables it.
  - Cause: CONCURRENCY / OTHER. (loop is inferred; the guards are read)
  - Evidence: `PostCommitMaintenanceListener.java:96-103`; `MaintenanceActionManager.java:111`; `MaintenanceMinIntervalConfigs.java:70`.
  - Mudcat implication: JM-R25. Mark commits by a maintenance run so triggers ignore them.

- **Design doc and code disagree on names and shape**
  - What happens: design names `table_maintenance_job` with `before_snapshot_id`/`after_snapshot_id`; the schema has `table_maintenance_action` with `before_metrics_id`/`after_metrics_id` and a `created_at`. Design says cooldown key `gravitino.maintenance.task.<type>.minIntervalMs`; code has `gravitino.maintenance.compaction.minIntervalMs` etc. Design shows `authType=oauth2`, `oauth2ServerUri`, `kerberos`; code accepts `none|simple|basic|oauth` and `oauthServerUri`; docs mark Kerberos "Not supported yet".
  - Cause: OTHER (design drift).
  - Evidence: `design-docs/table-maintenance-service-design.md:169,557,578`; `schema-2.0.0-postgresql.sql:1316`; `MaintenanceMinIntervalConfigs.java:46-55`; `MaintenanceAuthConfigs.java:95`.
  - Mudcat implication: design warning. Generate docs from the schema and config registry; test that doc keys exist.

- **Expiry retention: policy default is invisible**
  - What happens: a policy value equal to the default (5 days, retain 1) is sent as empty so that the table's own `history.expire.*` wins. The design doc says the opposite ("always submits explicit").
  - Cause: SPEC (Iceberg table properties) + MODEL.
  - Evidence: `GravitinoSnapshotExpirationJobAdapter.java:100-111`; `design-docs/iceberg-expire-snapshots-maintenance-job.md:171-175`.
  - Mudcat implication: JM-R49.

- **Relative cutoffs are resolved late**
  - What happens: `older_than` is carried as "N days" through jobConf and expanded at run time with the executor's clock.
  - Cause: CONCURRENCY (queue delay).
  - Evidence: `IcebergJobUtils.java:588-606`.
  - Mudcat implication: JM-R50. Also note the clock is the job host's, not the catalog's; skew shifts the window (inferred).

- **Orphan removal lists twice**
  - What happens: the TMS path calls `remove_orphan_files` with `dry_run=true`, measures sizes with `FileIO`, then calls it again with `dry_run=false`. The reported count and bytes come from the first listing; the second call decides what is deleted.
  - Cause: CONCURRENCY.
  - Evidence: `IcebergRemoveOrphanFilesJob.java:182-217`.
  - Mudcat implication: JM-R43.

- **Orphan scan is bounded only by lexical prefix**
  - What happens: scan root must be the table location or below, after normalising `.`/`..`, trailing slashes, scheme and authority. Symlinks are explicitly out of scope. A file under the prefix that another table references is still deleted.
  - Cause: MODEL (file ownership is per table, storage layout is per prefix).
  - Evidence: `OrphanFileLocationUtils.java:38-52`; design `iceberg-remove-orphan-files-maintenance-job.md:484-520`.
  - Mudcat implication: JM-R46, JM-R47.

- **Retention minimum is enforced by the engine, not the catalog**
  - What happens: the 24-hour floor for orphan cutoffs is enforced by the Spark procedure ("including for dry runs"); the policy also checks `>= 1` day. A different engine would not enforce it.
  - Cause: ENGINE.
  - Evidence: design `iceberg-remove-orphan-files-maintenance-job.md:236-239`; `IcebergOrphanFileRemovalContent.java:141-147`.
  - Mudcat implication: JM-R44. Enforce the floor in the catalog.

- **Unresolved placeholder reaches the job as literal text**
  - What happens: if a template placeholder has no value and no default, the server may leave `{{name}}` in the argument; the job must map that back to "not supplied" (`nullIfUnresolvedPlaceholder`). Templates use `{{name:-}}` for optional parameters.
  - Cause: SPEC (own template language) + MODEL.
  - Evidence: `IcebergJobUtils.java:181-189`; `IcebergRemoveOrphanFilesJob.java:340-345`.
  - Mudcat implication: JM-R16. Resolve before submit and fail on any leftover `{{`.

- **Masking is a display feature**
  - What happens: sensitive keys in two JSON arguments (`--updater-options`, `--spark-conf`) and `--conf k=v` are masked in logs and API output. Other arguments (`--password`, `-c`), environment maps, custom fields and the stored runtime template remain plaintext.
  - Cause: SECURITY.
  - Evidence: `JobRuntimeTemplateMaskUtils.java:38-60`.
  - Mudcat implication: JM-R34.

- **Anyone who may run a job may influence its code**
  - What happens: with `RUN_JOB`, a caller can set non-Kubernetes Spark configs of a built-in job, such as `spark.jars`, which adds code that runs "with the service accounts and the credentials of the job". `spark.kubernetes.*` is blocked for built-ins, but a custom template that passes configuration through a placeholder is not.
  - Cause: SECURITY.
  - Evidence: `docs/k8s-job-executor.md:285-300`; `SparkApplicationUtils.java:105-110`.
  - Mudcat implication: JM-R32, JM-R37; enrichers from third parties run only where ST-16 puts them.

- **Callback authority is metalake-wide**
  - What happens: any principal with `METALAKE::RUN_JOB` can post metrics or finish for any action id in the metalake; only table id and policy id on the request must match the row.
  - Cause: SECURITY.
  - Evidence: `TableMaintenanceActionOperations.java:93,127,158`; `MaintenanceMetricsManager.java:60-175`.
  - Mudcat implication: JM-R33. A run token names one run and one scope.

- **Status of a vanished execution**
  - What happens: if the runner says "no such job" at poll time, the job becomes FAILED (or CANCELLED if it was cancelling). Gravitino does not tell apart "TTL deleted the SparkApplication" from "never ran".
  - Cause: OTHER.
  - Evidence: `JobManager.java:1441-1456`; K8s `ttlAfterStopMillis` and `resourceRetainDurationMillis` in `SparkApplicationUtils.java:228-231`.
  - Mudcat implication: JM-R3, JM-R15; a succeeded run whose execution was garbage collected before the first poll may be recorded as failed (inferred).

- **Statistics without version binding cause wrong decisions**
  - What happens: optimizer statistics are per table or partition and carry no snapshot id; pull requests #14674, #14677, #14675 added special cases so table-level stats do not drive partitioned decisions. The TMS path samples inside the job and keys by snapshot.
  - Cause: MODEL.
  - Evidence: `BaseExpressionStrategyHandler.java:70-73,140-145,253`; design §4.3.
  - Mudcat implication: JM-R41, JM-R42.

- **Typos in score expressions disable work silently**
  - What happens: any exception in `score-expr` or `trigger-expr` evaluation is caught, logged as warn, and mapped to `-1` or false.
  - Cause: OTHER.
  - Evidence: `BaseExpressionStrategyHandler.java:222-245`.
  - Mudcat implication: JM-R60.

- **Ambiguous nearest policy throws at run time**
  - What happens: two same-type policies at the nearest level cause `IllegalArgumentException("ambiguous ...")` inside the resolver that expand and the commit listener call.
  - Cause: MODEL (inheritance without precedence).
  - Evidence: `NearestMaintenancePolicyResolver.java:84-89`.
  - Mudcat implication: JM-R30; same as tag-conflict handling for ST-17.

- **Snapshot-drift choices have a history**
  - What happens: the design surveys Amoro (gate on drift and fail at commit with `validateFromSnapshot`), Floe and OpenHouse (no gate). Gravitino chose to stop before maintenance, once, without re-sampling, and leaves final conflict detection to Iceberg's optimistic commit. Expiry and orphan jobs skip the gate.
  - Cause: SPEC (Iceberg OCC) + CONCURRENCY.
  - Evidence: design `table-maintenance-service-design.md:99-118`; `IcebergMaintenanceJob.java:295-310`; `IcebergExpireSnapshotsJob.java:231`.
  - Mudcat implication: JM-R29. For mudcat a rewrite is a commit with requirements (ID-7); the requirement is the drift gate.

- **Async purge is a second job system**
  - What happens: Gravitino has `job_run_meta` and `iceberg_cleanup_job`, each with its own state machine (QUEUED/STARTED/... vs PENDING/RUNNING/...), claim, heartbeat, retry count and retention. The design rejected reuse ("share patterns, not code").
  - Cause: OTHER.
  - Evidence: `async-iceberg-rest-hard-deletion.md:96-108`.
  - Mudcat implication: design warning. One durable-work table type with lease, attempts and last error serves jobs, purge, enrichers (ST-21) and registration cleanup (OP-22).

- **Purge row stores credentials and a table of the same name can be created meanwhile**
  - What happens: see JM-R34, JM-R54, JM-R57. The tombstone is keyed by `catalog_id`, namespace and table name, so it survives a catalog rename but not a table that moves across namespaces.
  - Cause: SECURITY / MODEL.
  - Evidence: `async-iceberg-rest-hard-deletion.md:225-262`.
  - Mudcat implication: JM-R54 (key by entity ID).

## 4. Open questions, inconsistencies, half-implemented

1. **Status poll lists all jobs.** Comment: "we need to improve the code to only list the active jobs." `JobManager.java:1362-1364`. Every poll reads every job row of every in-use metalake.
2. **Reconcile of stale bound slots is promised but absent.** Design checklist: "reconcile closes stale occupancy without inventing metrics" (`table-maintenance-service-design.md:624`); `PolicyExpandReconciler` reconciles policies, not slots; slot release for bound rows is only through `finish`.
3. **Metrics purge.** Checklist item "purge unreferenced history by TTL/N" (`table-maintenance-service-design.md:618`) has no implementation I could find for `table_snapshot_metrics`; the optimizer JDBC repo has delete methods (`JdbcMetricsRepository.java:356,378`) with no scheduler (inferred from grep).
4. **Dry-run persistence.** Orphan design open question 3: "Should dry-run results be stored somewhere ... before actual deletion?" unanswered. `iceberg-remove-orphan-files-maintenance-job.md:590`. Open question 6 (uniform minimum interval) was later solved by `minIntervalMs`.
5. **Expiry trigger is `true`.** The design rejected a snapshot-count gate (`iceberg-expire-snapshots-maintenance-job.md:323`) and left "skip if fewer than N snapshots" as an open question, so every scheduled run starts a Spark job even when nothing can expire. Spark start-up cost is paid for a no-op. (cost is inferred)
6. **Authority split.** Design §6 says operators must grant `MODIFY_TABLE`, `USE_SECRET`, `RUN_JOB`, `USE_JOB_TEMPLATE` to the token identity and that no built-in user is created. No separation exists between "may compact" and "may expire or delete" (one privilege, `MODIFY_TABLE`); expiry and orphan removal are destructive but need the same grant as compaction.
7. **Optimizer-CLI path has no in-flight guard.** Only TMS claims a slot; `GravitinoJobSubmitter.runJob` (`optimizer/.../GravitinoJobSubmitter.java:91`) submits without a check, and `docs` say to run the CLI "from cron". Two overlapping runs can start two rewrites of one table (inferred from absence).
8. **Job table linkage in the Monitor is a local file.** `LocalTableJobRelationProvider` reads a JSON-lines file mapping table names to job names; the Monitor compares metrics in a time window around the action (`Monitor.java:25-50`). The TMS path links by ids and snapshot, so the two evaluation paths do not agree on what "this action" means.
9. **Kerberos.** Design shows `kerberos`; code and docs say not supported (`optimizer-configuration.md:148-153`).
10. **Which snapshot the "after" row names.** `after_snapshot_id` is taken after the procedure; a concurrent writer's commit between the rewrite commit and the sample is attributed to the maintenance run (inferred; the rewrite's own resulting snapshot id is not captured from the procedure result).
11. **Retention of `job_run_meta` vs policy history.** Policy content is versioned, but the job row stores only the resolved template; the policy version that decided the run is not stored. `table_maintenance_action` has `policy_id` and no policy version (`schema-2.0.0-postgresql.sql:1316-1326`). Reproducing why a run happened needs the policy version at that time (UP-3 analogue).
12. **Spark job output is not stored.** K8s output is the driver pod log read on request; after pod retention expires it is empty (`docs/k8s-job-executor.md` "Job Output and Times"). Local executor keeps output by job id under staging dir for the same 7 days. Output of a destructive job (file lists) is not a durable record.
13. **`ASYNC_ISOLATED` listener drop semantics** are in the event subsystem, not read here. The commit trigger depends on them.
14. **Expand enumerates tables from the underlying catalog** and imports missing Gravitino entities by `loadSchema`/`loadTable` (`docs/table-maintenance-service/service.md`, "Cron Path" step 2). Listing cost and import side effects at scale are not bounded in the code I read (`scheduleBatchSize` bounds rows per pick, not listing).

## 5. Relevance map to mudcat requirements

- **PR-7 (compaction as activity).** Gravitino keeps only counters before and after plus job row. mudcat needs the activity record to contain: input versions, output versions, the commit (snapshot) it made, the run token, the enricher or job template and version, the policy version, and the deleted file list or a hash of it. JM-R39, R40, R42, R43, R66.
- **ID-9 (expire only if unreferenced).** Gravitino delegates entirely to Iceberg's ref handling and an age and count rule; no catalog-side reference check. JM-R48, R47, R45. The scan for orphans must consult every entity that shares data (ID-13, OP-15).
- **UP (deletion, purge, hold).** Gravitino's purge is hard delete, async, no hold, no undrop, credentials stored, tombstone by name. Positive patterns to copy: leaf-first deletion, NotFound as success, decoupled heartbeat, tombstone lifted only on terminal state, FAILED stays queryable. JM-R52 to R57.
- **Enrichers (ST-13..21).** The optimizer provider SPI and the job SPI are the closest Gravitino analogue to an enricher host. Gaps to avoid: no per-value producer/version (R66), no namespace ownership of written statistics (any updater writes any `custom-` statistic), settings through shared cluster-static identity (R32), no per-run record of last result and error beyond the job row (ST-21 asks for it per entity and enricher; Gravitino's nearest is `table_maintenance_action` with job id).
- **Jobs authenticating and getting credentials.** Positive: data access goes through the catalog's own credential vending (`X-Iceberg-Access-Delegation`). Negative: one standing identity per cluster, secrets in launch config, callbacks authorised by metalake-wide privilege. mudcat run tokens, RD-3 prefix-scoped credentials and ST-16 solve this; JM-R32, R33, R34, R36.
- **Retry and idempotency.** Gravitino's job runs once, retries are new rows, an in-flight slot prevents overlap, deterministic runner resource names prevent duplicate submit, and a placeholder row prevents orphaned runs. It has no client idempotency key on `runJob` (JM-R14). mudcat should key a run request by (entity, policy, input version) so a repeated trigger returns the same run.
