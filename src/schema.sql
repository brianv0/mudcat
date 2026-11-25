-- mudcat catalog schema, version 1.
--
-- Rules (architecture.md, "Catalog database" and "Tenants"):
--   * The tenant ID is the first column of every primary key and index of a tenant table.
--   * The tenant ID column defaults to the tenant of the session (mc_tenant()), so code
--     below the tenant layer never passes it. A session with no tenant cannot insert.
--   * Row-level security, forced also for the owner, filters every tenant table.
--   * The schema is the same for one tenant and for many tenants (OP-13).
--   * Foreign keys inside one tenant include the tenant ID. References between tenants,
--     references to more than one kind of entity, and references in the largest tables
--     have no foreign key (model.md, "Cheap copies").

create extension if not exists pg_trgm;

create or replace function mc_tenant() returns uuid
  language sql stable as $$ select nullif(current_setting('mudcat.tenant', true), '')::uuid $$;

create sequence if not exists commit_seq;

-- ------------------------------------------------------------------ global (instance) tables

create table if not exists tenant (
  id          uuid primary key,
  name        text not null unique,
  endpoint    text not null unique,
  status      text not null default 'active',      -- active | moving
  location    text not null default 'shared',      -- the database that holds the rows
  created_at  timestamptz not null
);

create table if not exists schema_version (version int primary key);

-- ------------------------------------------------------------------ commits, entities, identifiers

create table if not exists commit_log (
  tenant_id  uuid not null default mc_tenant(),
  commit_id  bigint not null,
  actor      text not null,
  at         timestamptz not null,
  primary key (tenant_id, commit_id)
);
create index if not exists commit_log_at on commit_log (tenant_id, at);

create table if not exists entity (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  kind            text not null,
  revision        int  not null default 1,
  created_commit  bigint not null,
  erased          boolean not null default false,
  primary key (tenant_id, id)
);

create table if not exists erasure_record (
  tenant_id     uuid not null default mc_tenant(),
  id            uuid not null,
  kind          text not null,
  erased_at     timestamptz not null,
  reason        text not null,
  content_hash  text,
  commit_id     bigint not null,
  primary key (tenant_id, id)
);

create table if not exists identifier (
  tenant_id   uuid not null default mc_tenant(),
  scheme      text not null,
  value       text not null,
  entity_id   uuid not null,
  valid_from  bigint not null,
  valid_to    bigint,
  primary key (tenant_id, scheme, value, valid_from)
);
create unique index if not exists identifier_current on identifier (tenant_id, scheme, value) where valid_to is null;
create index if not exists identifier_entity on identifier (tenant_id, entity_id);

-- ------------------------------------------------------------------ namespace and storage

create table if not exists storage_resource (
  tenant_id      uuid not null default mc_tenant(),
  id             uuid not null,
  name           text not null,
  location       text not null,             -- compute location, for example cloud-a, eu, cn, hpc
  jurisdiction   text not null,
  mode           text not null,             -- managed | external
  protocol       text not null default 's3',-- s3 | posix
  versioned      boolean not null default false,
  egress_allowed boolean not null default true,
  bucket         text not null,             -- the bucket or file system root in the store
  attrs          jsonb not null default '{}',
  primary key (tenant_id, id),
  unique (tenant_id, name)
);

create table if not exists collection (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  path            text not null,
  parent_id       uuid,
  has_acl         boolean not null default false,
  acl_root_id     uuid not null,
  resource_id     uuid,                     -- mount: the storage resource of this subtree
  prefix          text,                     -- the prefix on that resource
  created_commit  bigint not null,
  primary key (tenant_id, id)
);
create unique index if not exists collection_path on collection (tenant_id, path text_pattern_ops);

create table if not exists artifact (
  tenant_id         uuid not null default mc_tenant(),
  id                uuid not null,
  collection_id     uuid not null,
  name              text not null,
  kind              text not null default 'data',   -- data | model | document | run-record | manifest
  latest_version_id uuid,
  created_commit    bigint not null,
  primary key (tenant_id, id),
  foreign key (tenant_id, collection_id) references collection (tenant_id, id)
);
create index if not exists artifact_collection on artifact (tenant_id, collection_id, name);

create table if not exists version (
  tenant_id         uuid not null default mc_tenant(),
  id                uuid not null,
  artifact_id       uuid not null,
  number            int not null,
  label             text,
  content_hash      text,                    -- sha256 of the content; null for a delete marker
  checksums         jsonb not null default '[]',
  size              bigint not null default 0,
  element_count     bigint not null default 1,
  structure         jsonb not null default '{"kind":"opaque"}',
  format            text,
  reader            text,
  source_id         uuid,
  is_delete_marker  boolean not null default false,
  element_manifest  uuid,                    -- manifest file of the element list, if the structure enumerates elements
  stats             jsonb not null default '{}',
  created_commit    bigint not null,
  primary key (tenant_id, id),
  unique (tenant_id, artifact_id, number)
);
-- idempotency key of registration (OP-5, INV-13)
create unique index if not exists version_content on version (tenant_id, artifact_id, content_hash) where content_hash is not null;
create index if not exists version_source on version (tenant_id, source_id);
create index if not exists version_hash on version (tenant_id, content_hash);
create index if not exists version_size on version (tenant_id, size);

create table if not exists replica (
  tenant_id          uuid not null default mc_tenant(),
  id                 uuid not null,
  version_id         uuid not null,
  resource_id        uuid not null,
  representation_id  uuid,
  url                text not null,
  state              text not null,          -- registered | copying | verified | suspect | lost | retired
  observed_checksum  text,
  verified_at        timestamptz,
  lock_until         timestamptz,
  created_commit     bigint not null,
  primary key (tenant_id, id)
);
create index if not exists replica_version on replica (tenant_id, version_id);
create index if not exists replica_resource on replica (tenant_id, resource_id);

create table if not exists replica_history (
  tenant_id   uuid not null default mc_tenant(),
  replica_id  uuid not null,
  commit_id   bigint not null,
  state       text not null,
  primary key (tenant_id, replica_id, commit_id)
);

create table if not exists verification (
  tenant_id    uuid not null default mc_tenant(),
  replica_id   uuid not null,
  at           timestamptz not null,
  algorithm    text not null,
  expected     text not null,
  observed     text,
  ok           boolean not null,
  agent        text not null,
  primary key (tenant_id, replica_id, at)
);

create table if not exists deletion_receipt (
  tenant_id        uuid not null default mc_tenant(),
  id               uuid not null,
  replica_id       uuid not null,
  version_id       uuid not null,
  resource_id      uuid not null,
  url              text not null,
  object_versions  int not null,
  at               timestamptz not null,
  agent            text not null,
  reason           text,
  commit_id        bigint not null,
  primary key (tenant_id, id)
);

-- ------------------------------------------------------------------ metadata

create table if not exists metadata_namespace (
  tenant_id  uuid not null default mc_tenant(),
  name       text not null,
  owner      text,
  writers    text[] not null default '{}',
  primary key (tenant_id, name)
);

create table if not exists attribute_def (
  tenant_id    uuid not null default mc_tenant(),
  id           uuid not null,               -- the field ID; it does not change with a new name (ST-8)
  namespace    text not null,
  name         text not null,
  datatype     text not null,               -- string | number | datetime | boolean
  unit         text,
  custody      text not null default 'vendor-held',
  indexed      boolean not null default false,
  is_tag       boolean not null default false,
  permitted    jsonb,
  propagate    boolean not null default false,
  access_condition text,                    -- for example "embargo": the value is a condition for access
  description  text,
  primary key (tenant_id, id),
  unique (tenant_id, namespace, name)
);

create table if not exists metadata_payload (
  tenant_id         uuid not null default mc_tenant(),
  entity_id         uuid not null,
  namespace         text not null,
  payload           jsonb not null,
  origin            text not null,           -- person | enricher | model | plugin
  asserted_by       text not null,
  enricher          text,
  enricher_version  text,
  confidence        real,
  status            text not null default 'visible',  -- visible | proposed | rejected
  reviewed_by       text,
  reviewed_at       timestamptz,
  commit_id         bigint not null,
  primary key (tenant_id, entity_id, namespace)
);
create index if not exists metadata_payload_gin on metadata_payload using gin (payload jsonb_path_ops);

create table if not exists metadata_history (
  tenant_id   uuid not null default mc_tenant(),
  entity_id   uuid not null,
  namespace   text not null,
  commit_id   bigint not null,
  payload     jsonb,
  actor       text not null,
  primary key (tenant_id, entity_id, namespace, commit_id)
);

-- the typed attribute index (architecture.md, "Metadata indexes"); derived from payloads
create table if not exists attr_index (
  tenant_id   uuid not null default mc_tenant(),
  attr_id     uuid not null,
  entity_id   uuid not null,
  num_value   double precision,
  text_value  text,
  ts_value    timestamptz,
  primary key (tenant_id, attr_id, entity_id)
);
create index if not exists attr_index_num on attr_index (tenant_id, attr_id, num_value);
create index if not exists attr_index_text on attr_index (tenant_id, attr_id, text_value);
create index if not exists attr_index_ts on attr_index (tenant_id, attr_id, ts_value);

create table if not exists tag (
  tenant_id       uuid not null default mc_tenant(),
  entity_id       uuid not null,
  name            text not null,
  value           text not null,
  propagated_from uuid,
  commit_id       bigint not null,
  primary key (tenant_id, entity_id, name)
);
create index if not exists tag_value on tag (tenant_id, name, value);

create table if not exists tag_policy (
  tenant_id  uuid not null default mc_tenant(),
  name       text not null,
  value      text not null,
  kind       text not null,                 -- compartment | jurisdiction | duty
  config     jsonb not null default '{}',
  primary key (tenant_id, name, value, kind)
);

create table if not exists enricher (
  tenant_id     uuid not null default mc_tenant(),
  name          text not null,
  version       text not null,
  namespace     text not null,
  needs_review  boolean not null default false,
  place         text not null default 'site-agent',
  primary key (tenant_id, name, version)
);

-- ------------------------------------------------------------------ datasets, snapshots, manifests

create table if not exists dataset (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  path            text not null,
  collection_id   uuid,
  retention_days  int not null default 30,
  is_series       boolean not null default false,
  follow          jsonb,
  created_commit  bigint not null,
  primary key (tenant_id, id)
);
create unique index if not exists dataset_path on dataset (tenant_id, path);

create table if not exists snapshot (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  dataset_id      uuid not null,
  parent_id       uuid,
  created_commit  bigint not null,
  created_at      timestamptz not null,
  summary         jsonb not null,
  status          text,                      -- published | deprecated | withdrawn; null if not published
  merkle_hash     text,
  member_count    bigint not null default 0,
  warnings        jsonb not null default '[]',
  equivalences    uuid[] not null default '{}',
  published_at    timestamptz,
  published_by    text,
  expired_at      timestamptz,
  primary key (tenant_id, id),
  foreign key (tenant_id, dataset_id) references dataset (tenant_id, id)
);
create index if not exists snapshot_dataset on snapshot (tenant_id, dataset_id, created_commit);

create table if not exists manifest (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  sealed          boolean not null default false,
  entry_count     bigint not null default 0,
  total_size      bigint not null default 0,
  sources         uuid[] not null default '{}',   -- the source bitmap of the manifest summary
  file            text,
  created_commit  bigint not null,
  primary key (tenant_id, id)
);

create table if not exists manifest_entry (
  tenant_id      uuid not null default mc_tenant(),
  manifest_id    uuid not null,
  member_id      uuid not null,
  member_kind    text not null,             -- version | annotation-set | snapshot
  member_hash    text not null,
  source_id      uuid,
  size           bigint not null default 0,
  element_count  bigint not null default 1,
  primary key (tenant_id, manifest_id, member_id)
);
create index if not exists manifest_entry_member on manifest_entry (tenant_id, member_id);

-- a snapshot holds manifest references, not manifests (model.md, "Cheap copies")
create table if not exists manifest_ref (
  tenant_id        uuid not null default mc_tenant(),
  snapshot_id      uuid not null,
  ordinal          int not null,
  manifest_tenant  uuid not null,           -- the tenant of the manifest: a tenant mapping if it differs
  manifest_id      uuid not null,
  excluded         uuid[] not null default '{}',   -- members that this reference leaves out
  primary key (tenant_id, snapshot_id, ordinal)
);
create index if not exists manifest_ref_manifest on manifest_ref (manifest_tenant, manifest_id);

-- references from one tenant to the records of another tenant have no foreign key; this
-- table lets the owner of the records find them (OP-15)
create table if not exists cross_tenant_ref (
  owner_tenant   uuid not null,
  target_id      uuid not null,
  holder_tenant  uuid not null,
  holder_entity  uuid not null,
  primary key (owner_tenant, target_id, holder_tenant, holder_entity)
);

create table if not exists ref (
  tenant_id    uuid not null default mc_tenant(),
  dataset_id   uuid not null,
  name         text not null,
  kind         text not null,               -- branch | tag
  snapshot_id  uuid not null,
  primary key (tenant_id, dataset_id, name)
);

create table if not exists ref_history (
  tenant_id    uuid not null default mc_tenant(),
  dataset_id   uuid not null,
  name         text not null,
  commit_id    bigint not null,
  snapshot_id  uuid,
  primary key (tenant_id, dataset_id, name, commit_id)
);

create table if not exists annotation_set (
  tenant_id         uuid not null default mc_tenant(),
  id                uuid not null,
  dataset_id        uuid not null,
  name              text not null,
  version_no        int not null,
  enricher          text,
  enricher_version  text,
  key_kind          text not null,
  manifest_file     text not null,
  entry_count       bigint not null,
  content_hash      text not null,
  activity_id       uuid,
  created_commit    bigint not null,
  primary key (tenant_id, id)
);

create table if not exists representation (
  tenant_id    uuid not null default mc_tenant(),
  id           uuid not null,
  snapshot_id  uuid not null,
  name         text not null,
  transform    jsonb not null default '{}',
  activity_id  uuid,
  members      uuid[] not null default '{}', -- versions of this representation
  created_commit bigint not null,
  primary key (tenant_id, id)
);

create table if not exists selection (
  tenant_id     uuid not null default mc_tenant(),
  id            uuid not null,
  name          text not null,
  dataset_id    uuid not null,
  snapshot_id   uuid,
  predicate     jsonb not null,
  element_hash  text not null,
  count         bigint not null,
  created_commit bigint not null,
  primary key (tenant_id, id)
);

create table if not exists equivalence (
  tenant_id    uuid not null default mc_tenant(),
  id           uuid not null,
  method_a     text not null,
  method_b     text not null,
  declared_by  text not null,
  at           timestamptz not null,
  reason       text not null,
  after        text,
  primary key (tenant_id, id)
);

-- ------------------------------------------------------------------ sources and usage policy

create table if not exists source (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  name            text not null,
  kind            text not null,            -- crawl | licensed | human | user | generated | instrument | computed | external
  attrs           jsonb not null default '{}',
  method          jsonb,
  method_hash     text,
  terms_status    text not null default 'none',  -- none | compiled | partly-compiled
  created_commit  bigint not null,
  primary key (tenant_id, id),
  unique (tenant_id, name)
);

create table if not exists terms_document (
  tenant_id  uuid not null default mc_tenant(),
  id         uuid not null,
  source_id  uuid not null,
  format     text not null,                 -- spdx | odrl | text
  content    jsonb not null,
  commit_id  bigint not null,
  primary key (tenant_id, id)
);

create table if not exists usage_grant (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  source_id       uuid not null,
  action          text not null,
  purpose         text not null,
  not_after       timestamptz,
  clause          text,
  terms_id        uuid,
  granted_by      text not null,
  created_commit  bigint not null,
  revoked_commit  bigint,
  primary key (tenant_id, id)
);
create index if not exists usage_grant_source on usage_grant (tenant_id, source_id, action, purpose);

create table if not exists duty (
  tenant_id  uuid not null default mc_tenant(),
  id         uuid not null,
  source_id  uuid not null,
  kind       text not null,
  clause     text,
  terms_id   uuid,
  params     jsonb not null default '{}',
  primary key (tenant_id, id)
);

create table if not exists review_item (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  kind            text not null,            -- clause | enricher-output | model-flag
  target_id       uuid not null,
  clause          text,
  details         jsonb not null default '{}',
  status          text not null default 'open',
  decision        jsonb,
  reviewer        text,
  decided_at      timestamptz,
  created_commit  bigint not null,
  primary key (tenant_id, id)
);

create table if not exists invalidation (
  tenant_id     uuid not null default mc_tenant(),
  id            uuid not null,
  target_kind   text not null,              -- version | artifact | source | snapshot | fingerprint
  target        text not null,
  reason        text not null,
  requested_by  text not null,
  commit_id     bigint not null,
  at            timestamptz not null,
  primary key (tenant_id, id)
);
create index if not exists invalidation_target on invalidation (tenant_id, target_kind, target);

create table if not exists legal_hold (
  tenant_id        uuid not null default mc_tenant(),
  id               uuid not null,
  name             text not null,
  reason           text not null,
  placed_by        text not null,
  placed_at        timestamptz not null,
  released_by      text,
  released_reason  text,
  released_at      timestamptz,
  primary key (tenant_id, id)
);

create table if not exists hold_scope (
  tenant_id  uuid not null default mc_tenant(),
  hold_id    uuid not null,
  entity_id  uuid not null,
  primary key (tenant_id, hold_id, entity_id)
);
create index if not exists hold_scope_entity on hold_scope (tenant_id, entity_id);

create table if not exists model_flag (
  tenant_id          uuid not null default mc_tenant(),
  id                 uuid not null,
  model_version_id   uuid not null,
  kind               text not null,         -- invalidated-input | licence-ended
  cause              text not null,
  affected_elements  bigint not null,
  decision           text,
  decided_by         text,
  decided_at         timestamptz,
  created_commit     bigint not null,
  primary key (tenant_id, id)
);

-- the effective-policy cache (derived): the (action, purpose) pairs that all sources of a snapshot permit
create table if not exists policy_cache (
  tenant_id        uuid not null default mc_tenant(),
  snapshot_id      uuid not null,
  permitted        text[] not null,
  earliest_expiry  timestamptz,
  computed_at      timestamptz not null,
  primary key (tenant_id, snapshot_id)
);

-- ------------------------------------------------------------------ access control

create table if not exists relation (
  tenant_id         uuid not null default mc_tenant(),
  object_type       text not null,
  object_id         text not null,
  relation          text not null,
  subject_type      text not null,
  subject_id        text not null,
  subject_relation  text not null default '',
  condition         text,                  -- a condition that the catalog evaluates on the object, for example embargo
  membership_source uuid,                  -- the membership source that wrote this tuple
  synced_at         timestamptz,
  created_commit    bigint not null,
  primary key (tenant_id, object_type, object_id, relation, subject_type, subject_id, subject_relation)
);
create index if not exists relation_subject on relation (tenant_id, subject_type, subject_id);

create table if not exists relation_history (
  tenant_id   uuid not null default mc_tenant(),
  commit_id   bigint not null,
  op          text not null,
  tuple       text not null,
  primary key (tenant_id, commit_id, tuple)
);

create table if not exists membership_source (
  tenant_id     uuid not null default mc_tenant(),
  id            uuid not null,
  group_id      text not null,
  provider      text not null,
  org           text not null,
  team          text not null,
  max_age_secs  bigint not null,
  last_sync_at  timestamptz,
  primary key (tenant_id, id)
);

create table if not exists linked_identity (
  tenant_id    uuid not null default mc_tenant(),
  provider     text not null,
  external_id  text not null,
  principal    text not null,
  primary key (tenant_id, provider, external_id)
);

create table if not exists trust_rule (
  tenant_id   uuid not null default mc_tenant(),
  id          uuid not null,
  issuer      text not null,
  repository  text not null,
  branch      text not null,
  principal   text not null,
  primary key (tenant_id, id)
);

create table if not exists audit (
  tenant_id  uuid not null default mc_tenant(),
  id         bigserial,
  at         timestamptz not null,
  principal  text not null,
  point      text not null,                 -- registration | resolution | materialisation | export | deletion | read | search | write
  action     text not null,
  object     text not null,
  decision   text not null,                 -- allow | deny
  reason     text,
  cached     boolean not null default false,
  details    jsonb not null default '{}',
  primary key (tenant_id, id)
);

create or replace function mc_audit_append_only() returns trigger language plpgsql as $$
begin
  if current_setting('mudcat.moving', true) = 'on' then
    return old;  -- a tenant move removes the rows of the tenant from its old database
  end if;
  raise exception 'the audit log is append-only';
end $$;
drop trigger if exists audit_append_only on audit;
create trigger audit_append_only before update or delete on audit
  for each row execute function mc_audit_append_only();

-- ------------------------------------------------------------------ provenance, resolution, evaluation

create table if not exists activity (
  tenant_id            uuid not null default mc_tenant(),
  id                   uuid not null,
  kind                 text not null,       -- training | evaluation | ingest | transform | takedown | transfer | answer | enrichment
  code                 text,
  image                text,
  params               jsonb not null default '{}',
  started_at           timestamptz,
  ended_at             timestamptz,
  location             text,
  run_record_version   uuid,
  resolution_id        uuid,
  run_token            text,
  capture              text not null default 'declared',
  primary key (tenant_id, id)
);

create table if not exists usage (
  tenant_id    uuid not null default mc_tenant(),
  activity_id  uuid not null,
  entity_id    uuid not null,
  role         text not null,
  capture      text not null,               -- observed | declared | imported
  selector     jsonb,
  primary key (tenant_id, activity_id, entity_id, role)
);
create index if not exists usage_entity on usage (tenant_id, entity_id);

create table if not exists generation (
  tenant_id    uuid not null default mc_tenant(),
  activity_id  uuid not null,
  entity_id    uuid not null,
  primary key (tenant_id, activity_id, entity_id)
);
create index if not exists generation_entity on generation (tenant_id, entity_id);

create table if not exists derivation (
  tenant_id  uuid not null default mc_tenant(),
  entity_id  uuid not null,
  from_id    uuid not null,
  kind       text not null,                 -- wasDerivedFrom | fineTunedFrom | materializedFrom | ...
  primary key (tenant_id, entity_id, from_id)
);
create index if not exists derivation_from on derivation (tenant_id, from_id);

create table if not exists resolution (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  principal       text not null,
  request         jsonb not null,
  inputs          jsonb not null,
  action          text,
  purpose         text,
  place           text,
  decision        jsonb not null,
  run_token       text,
  created_commit  bigint not null,
  at              timestamptz not null,
  primary key (tenant_id, id)
);

create table if not exists credential_log (
  tenant_id      uuid not null default mc_tenant(),
  id             uuid not null,
  resolution_id  uuid,
  run_token      text,
  principal      text not null,
  kind           text not null,             -- prefix | signed-urls | none
  scope          jsonb not null,
  issued_at      timestamptz not null,
  primary key (tenant_id, id)
);

create table if not exists similarity_criterion (
  tenant_id  uuid not null default mc_tenant(),
  name       text not null,
  version    int not null,
  kind       text not null,                 -- exact-checksum | near-duplicate | domain-similarity | time-range-overlap
  params     jsonb not null default '{}',
  primary key (tenant_id, name, version)
);

create table if not exists leakage_assertion (
  tenant_id          uuid not null default mc_tenant(),
  id                 uuid not null,
  criterion          text not null,
  criterion_version  int not null,
  train              jsonb not null,
  test               jsonb not null,
  result             text not null,         -- satisfied | violated
  related_count      bigint not null,
  evidence           jsonb not null,
  at                 timestamptz not null,
  primary key (tenant_id, id)
);

-- content-identity index (derived, rebuildable): fingerprint or checksum -> occurrence
create table if not exists identity_index (
  tenant_id    uuid not null default mc_tenant(),
  key_kind     text not null,               -- checksum | fingerprint | domain-key
  key          text not null,
  version_id   uuid not null,
  element_id   text not null default '',
  primary key (tenant_id, key_kind, key, version_id, element_id)
);
create index if not exists identity_index_version on identity_index (tenant_id, version_id);

-- ------------------------------------------------------------------ placement

create table if not exists replication_rule (
  tenant_id       uuid not null default mc_tenant(),
  id              uuid not null,
  subject_id      uuid not null,
  resources       uuid[] not null,
  copies          int not null,
  expires_at      timestamptz,
  owner           text not null,
  state           text not null default 'active',
  created_commit  bigint not null,
  primary key (tenant_id, id)
);

create table if not exists transfer (
  tenant_id           uuid not null default mc_tenant(),
  id                  uuid not null,
  rule_id             uuid not null,
  version_id          uuid not null,
  source_replica_id   uuid not null,
  target_resource_id  uuid not null,
  target_replica_id   uuid not null,
  state               text not null,        -- requested | done | failed
  requested_at        timestamptz not null,
  finished_at         timestamptz,
  primary key (tenant_id, id)
);

create table if not exists completeness (
  tenant_id    uuid not null default mc_tenant(),
  subject_id   uuid not null,
  resource_id  uuid not null,
  complete     boolean not null,
  verified     bigint not null,
  total        bigint not null,
  at           timestamptz not null,
  commit_id    bigint not null,
  primary key (tenant_id, subject_id, resource_id)
);

create table if not exists agent_task (
  tenant_id      uuid not null default mc_tenant(),
  id             bigserial,
  kind           text not null,              -- verify | delete | inventory
  resource_id    uuid not null,
  payload        jsonb not null,
  state          text not null default 'queued',  -- queued | leased | done | failed
  lease_owner    text,
  lease_expires  timestamptz,
  attempts       int not null default 0,
  result         jsonb,
  primary key (tenant_id, id)
);
create index if not exists agent_task_queue on agent_task (tenant_id, resource_id, state);

-- ------------------------------------------------------------------ change stream

-- The commit order of a writing transaction is a number that it takes at the end, while it
-- holds the commit lock of its tenant until the commit completes; the relay publishes in this
-- order, below a horizon that it reads under the same lock (see stream.rs).
create sequence if not exists outbox_ord_seq;
create table if not exists outbox (
  tenant_id  uuid not null default mc_tenant(),
  seq        bigserial,
  xid        xid8 not null default pg_current_xact_id(),
  commit_id  bigint not null,
  ord        bigint,
  event      jsonb not null,
  primary key (tenant_id, seq)
);
alter table outbox add column if not exists ord bigint;
create index if not exists outbox_ord on outbox (tenant_id, ord, seq);

create table if not exists event (
  tenant_id  uuid not null default mc_tenant(),
  position   bigint not null,
  commit_id  bigint not null,
  type       text not null,
  subject    uuid not null,
  revision   int not null,
  at         timestamptz not null,
  actor      text not null,
  acl_root   uuid,
  removal    boolean not null default false,
  data       jsonb not null,
  primary key (tenant_id, position)
);
create index if not exists event_subject on event (tenant_id, subject, position);

create table if not exists stream_state (
  tenant_id      uuid not null default mc_tenant(),
  last_position  bigint not null default 0,
  floor          bigint not null default 0,  -- positions at or below the floor are no longer kept
  primary key (tenant_id)
);

create table if not exists subscription (
  tenant_id   uuid not null default mc_tenant(),
  id          uuid not null,
  principal   text not null,
  scope       text not null default '/',
  stream      text not null default 'state',  -- change | state
  created_at  timestamptz not null,
  primary key (tenant_id, id)
);

-- ------------------------------------------------------------------ search (derived)

create table if not exists search_doc (
  tenant_id      uuid not null default mc_tenant(),
  index_version  int not null,
  entity_id      uuid not null,
  kind           text not null,
  parent_id      uuid,                        -- for a chunk: the document version
  doc            jsonb not null,
  body           text not null,
  tsv            tsvector not null,
  embedding      real[],
  acl_root       uuid,
  revision       int not null,
  primary key (tenant_id, index_version, entity_id)
);
create index if not exists search_doc_tsv on search_doc using gin (tsv);
create index if not exists search_doc_trgm on search_doc using gin (body gin_trgm_ops);

create table if not exists index_state (
  tenant_id       uuid not null default mc_tenant(),
  name            text not null,
  active_version  int not null,
  position        bigint not null default 0,
  primary key (tenant_id, name)
);

-- ------------------------------------------------------------------ row-level security

do $$
declare t text;
begin
  foreach t in array array[
    'commit_log','entity','erasure_record','identifier','storage_resource','collection','artifact',
    'version','replica','replica_history','verification','deletion_receipt','metadata_namespace',
    'attribute_def','metadata_payload','metadata_history','attr_index','tag','tag_policy','enricher',
    'dataset','snapshot','manifest','manifest_entry','manifest_ref','ref','ref_history',
    'annotation_set','representation','selection','equivalence','source','terms_document',
    'usage_grant','duty','policy_cache','review_item','invalidation','legal_hold','hold_scope','model_flag',
    'relation','relation_history','membership_source','linked_identity','trust_rule','audit',
    'activity','usage','generation','derivation','resolution','credential_log',
    'similarity_criterion','leakage_assertion','identity_index','replication_rule','transfer',
    'completeness','agent_task','outbox','event','stream_state','subscription','search_doc',
    'index_state']
  loop
    execute format('alter table %I enable row level security', t);
    execute format('alter table %I force row level security', t);
    execute format('drop policy if exists tenant_isolation on %I', t);
    execute format('create policy tenant_isolation on %I using (tenant_id = mc_tenant()) with check (tenant_id = mc_tenant())', t);
  end loop;
end $$;

insert into schema_version values (1) on conflict do nothing;
