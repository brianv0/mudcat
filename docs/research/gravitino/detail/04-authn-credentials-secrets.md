> **Research note, not a requirement document.** Drafted by a code-reading agent from Apache Gravitino commit `9aa78cf` (2026-11-26), then spot-checked by the lead. Line numbers can drift by a few lines; search by file and symbol. Items marked `(inferred)` were not confirmed in code. The text does not follow Simplified Technical English. See [../README.md](../README.md) for the synthesis.

# Authentication, credential vending, secrets, encryption (Gravitino) — findings for mudcat

Gravitino checkout: `/home/user/apache/gravitino` (commit 9aa78cf). All paths below are repo-relative to it. "(inferred)" marks a conclusion I drew rather than read directly.

## 1. Decomposition

### 1.1 Credential vending (the part closest to mudcat's "issue credentials, never proxy reads")

```
Client (Java/Python/GVFS/Spark/Flink/Trino/Iceberg engine)
  -> GET /api/metalakes/{m}/objects/{catalog|fileset}/{name}/credentials     (server/.../rest/MetadataObjectCredentialOperations.java)
  -> GET /v1/{prefix}/namespaces/{ns}/tables/{t}/credentials  (IRC)          (iceberg/iceberg-rest-server/.../IcebergTableOperations.java:519)
  -> loadTable/createTable/registerTable/planTableScan with X-Iceberg-Access-Delegation: vended-credentials
        |
        v   authorization decides READ vs WRITE (never the caller)
CredentialOperationDispatcher (core/.../credential/CredentialOperationDispatcher.java)
  -> builds CredentialContext: CatalogCredentialContext(user) | PathBasedCredentialContext(user, writePaths, readPaths)
  -> CatalogCredentialManager (core/.../credential/CatalogCredentialManager.java)  one per catalog, lazily built (BaseCatalog.java:477)
       -> CredentialCache (Caffeine, key = (credentialType, context incl. user + paths); core/.../CredentialCache.java)
       -> CredentialProvider (SPI, ServiceLoader; common/.../CredentialProvider.java)
            lightweight provider + reflective CredentialGenerator (CredentialProviderDelegator) to isolate cloud SDK classpaths
```

- **Credential model** (`api/.../credential/Credential.java`): `credentialType` string + `expireTimeInMs` (epoch ms; `0` = never expires) + `Map<String,String> credentialInfo`. Types: `s3-token`, `s3-secret-key`, `aws-irsa`, `aws-secret-key` (Glue), `oss-token`, `oss-secret-key`, `gcs-token`, `adls-token`, `azure-account-key`, `cos-token`, `cos-secret-key`, `dlf-secret-key`, `jdbc-user-password`. Client-side classes are created by `CredentialFactory` (ServiceLoader, `common/.../CredentialFactory.java`).
- **Dynamic vs static**: `SupportsDynamicCredential` marker (`common/.../SupportsDynamicCredential.java`) = provider issues scoped, short-lived credentials without exposing stored keys. Static types (secret keys, account keys, JDBC) need extra privileges (`USE_SECRETS` + `INCLUDE_CREDENTIAL_SECRETS`) — `MetadataObjectCredentialOperations.java:134`, `CredentialAuthorizationExecutor.java:62-80`, `docs/security/credential-vending.md:148-166`.
- **Per-cloud generators** (all in `bundles/<cloud>/`):
  - AWS `S3TokenGenerator` (STS AssumeRole + inline session policy, `externalId`, default TTL 3600), `AwsIrsaCredentialGenerator` (AssumeRoleWithWebIdentity from a projected token file), `S3SecretKeyProvider` (returns the stored key).
  - GCP `GCSTokenGenerator` (CredentialAccessBoundary downscoping of the server's own token, CEL `availabilityCondition`).
  - Azure `ADLSTokenGenerator` (user-delegation key via Entra client secret, one directory-scoped SAS).
  - Aliyun `OSSTokenGenerator` (RAM AssumeRole + JSON policy), Tencent `COSTokenGenerator` (CAM AssumeRole + policy v2.0).
  - Shared: `RoleSessionNameUtils` (`common/.../RoleSessionNameUtils.java`).
- **Selection among providers**: catalog property `credential-providers` (comma list). Order of precedence for fileset: fileset props, then schema props, then catalog conf; first non-empty list wins, no merge (`FilesetCatalogOperations.java:1974-1984`, `CredentialUtils.java:68-80`). Per path: the provider whose `supportsScheme(scheme)` matches; two matches = `UnsupportedOperationException` (`CatalogCredentialManager.java:92-133`). If the property is omitted, providers are inferred from which static-key properties exist (`BaseCatalog.java:625-645`, `docs/security/credential-vending.md:128-144`).
- **Iceberg REST translation**: `CredentialPropertyUtils` (`common/.../CredentialPropertyUtils.java`) maps Gravitino credentials to Iceberg keys (`s3.session-token`, `gcs.oauth2.token`, `adls.sas-token.<host>`, `client.security-token`...), adds `*-expires-at-ms`, and adds `*.refresh-credentials-endpoint` pointing back at the IRC (`buildRefreshProps` :277). Federated REST backends: `FederatedCatalogWrapper` strips remote credential keys (`REMOTE_CREDENTIAL_KEY_PREFIXES` :102) and rewrites refresh endpoints (`IcebergRESTUtils.java:176-334`).
- **Client refresh**: Java Hadoop bundles (`bundles/aws/.../S3CredentialsRefresher.java`, `bundles/gcp/.../GCSCredentialsProvider.java`, Azure/OSS/COS equivalents) call back to `GravitinoFileSystemCredentialsProvider` at 50% of remaining life; Python GVFS has a per-(fileset, location) cache with ratio 0.5 (`clients/client-python/gravitino/filesystem/gvfs_base_operations.py:781-867`, `gvfs_config.py:58`). Spark/Flink take only non-expiring credentials at catalog init (`CredentialInfos.nonExpiringCredentialInfo`, `api/.../CredentialInfos.java:44`); expiring ones come through the IRC.
- **Config keys that matter**: `credential-providers`, `credential-cache-expire-ratio` (default 0.15), `credential-cache-max-size` (10000), `s3-token-expire-in-secs` (3600) and `oss-/cos-/adls-token-expire-in-secs`, `s3-role-arn`, `s3-external-id`, `s3-token-service-endpoint`, `gcs-service-account-file`, `azure-tenant-id/client-id/client-secret`, `gravitino.catalog.credential.backfillToProperties` (legacy leak switch, `Configs.java:719`).

### 1.2 Secrets and encryption

- **Secret references** (design `design-docs/gravitino-entity-secrets.md`): property values for secret keys are stored as `urn:gravitino-secret:<provider>:<type-specific-id>`; a property is "a secret" iff the value starts with `urn:gravitino-secret` and ends with the key (`core/.../secret/SecretPropertyUtils.java:134`). REST create takes `secretBindings` (write-through plaintext) and `secretReferences` (external locators); alter has `setSecretBinding`/`setSecretReference`. Classes: `SecretManager` (staging/commit/rollback of secret versions around entity create/alter), `SecretProviderRegistry` (`gravitino.secret.providers`, `gravitino.secret.provider.<name>.className`), SPI `SecretProvider` (`common/.../secret/SecretProvider.java`; version methods default to `UnsupportedOperationException`), `SupportsSecrets` client API, `StaticCredentialSecretKeys`.
- **Shipped provider**: only an in-memory provider (design §3 item 3, `design-docs/gravitino-entity-secrets.md:100`). No production secret store is implemented.
- **Redaction**: `HiddenPropertyMaskUtils` (`core/.../connector/HiddenPropertyMaskUtils.java`) masks values as `******` on read; `AuditLogRedactor` (`core/.../audit/AuditLogRedactor.java`) masks audit `customInfo`; `SensitivePropertyKeyMatcher` is a key-name substring list (`secret,password,token,credential,access,account`, config `gravitino.secret.sensitiveKeyKeywords`).
- **Encryption**: no encryption code exists in `core`, `server-common` or `common` (grep for `javax.crypto`/`Cipher` returns none). `encryption/kms/` is an SPI that only reads key properties (`KmsClient.getKeyProperties`; "does not perform cryptographic operations", `common/.../encryption/kms/KmsClient.java`) and is referenced by no feature code (inferred from grep: only `catalog-glue` mentions `KmsClient`).

### 1.3 Authentication

- `server-common/.../server/authentication/`: `AuthenticationFilter` (first authenticator whose `supportsToken` is true wins, :195), `SimpleAuthenticator` (trusts the claimed user), `OAuth2TokenAuthenticator` + `OAuthTokenValidator` (`StaticSignKeyValidator` = one shared key, jjwt; `JwksTokenValidator` = Nimbus + JWKS URI), `KerberosAuthenticator` (SPNEGO, `HTTP/` principal + keytab), `BasicAuthenticator` (in `plugins/idp-basic`, passwords in the entity DB).
- `core/.../auth/`: `PrincipalMapper` (`RegexPrincipalMapper`, `KerberosPrincipalMapper`), `GroupMapper`. Config `gravitino.authenticators` (default `simple`, `Configs.java:310`), `gravitino.authenticator.oauth.*` (`OAuthConfig.java`).
- `UserPrincipal` carries name, groups, active roles and the raw `Authorization` header value (`core/.../UserPrincipal.java`), so a federated Iceberg REST backend can receive the caller's token (`UserPrincipalForwardingAuthManager.java:62-75`).
- Role narrowing (`X-Gravitino-Active-Roles`) is applied in the filter and feeds authorization and credential privilege (`AuthenticationFilter.java:218-226`; design `design-docs/gravitino-role-assumption.md:214-217`).
- Downstream impersonation: Hive (`HiveClientFactory.java:200-207`) and HDFS (`ImpersonationHDFSFileSystemProxy.java:54-66`) create Hadoop proxy users from the Gravitino user name.

### 1.4 Extension points
`CredentialProvider` (ServiceLoader), `CredentialGenerator` (reflective), `SupportsPathBasedCredentials` + `PathContext` (catalog tells core which paths and which credential type), `SecretProvider`, `KmsClientFactory`, `Authenticator` (class name in `gravitino.authenticators`, `AuthenticatorFactory.java:60-66`), `OAuthTokenValidator`, `PrincipalMapper`/`GroupMapper` (class name), `WebIdentityTokenSource`.

## 2. Candidate requirements

Kind: IMPL = Gravitino does it; GAP = Gravitino fails or omits it. Ver: T test, A analysis, I inspection, D demonstration. Provider limits quoted from cloud docs (not from the repo) are marked TBR.

| Ref | Kind | Requirement (mudcat style) | Rationale / Gravitino evidence | Area | Ver |
|---|---|---|---|---|---|
| CR-R1 | GAP | mudcat shall issue a storage credential only for a prefix that lies inside a storage root registered for the replica's storage resource. | Gravitino builds the credential scope from table metadata the client supplied: `location`, `write.data.path`, `write.metadata.path` (`CatalogWrapperForREST.java:403-425`). The only location checks are scheme and non-local (`IcebergCatalogWrapper.java:892-915`); there is no containment check against the warehouse (no "allowed locations" anywhere: grep). A table creator can name any bucket path the vending role can reach. | RD | T |
| CR-R2 | GAP | mudcat shall reject or escape wildcard and expression metacharacters in a location before it builds a cloud access policy. | GCS: the path is spliced into a CEL string with `'%s'` and no escaping (`GCSTokenGenerator.java:100,169,177`); a quote in the path can alter the condition (inferred). S3/OSS/COS: the path is placed in an ARN, where `*` and `?` are wildcards (`S3TokenGenerator.java:231`). No test covers this (grep in `bundles/*/src/test`). | RD | T |
| CR-R3 | IMPL | mudcat shall grant write permission on a prefix only when the caller holds write permission on that prefix, and read permission otherwise. | Dispatcher sets `writePaths` only for WRITE (`CredentialOperationDispatcher.java:151`); privilege comes from an authorization check, `FILTER_WRITE_FILESET_AUTHORIZATION_EXPRESSION` (`MetadataObjectCredentialOperations.java:123-130`) and `FILTER_MODIFY_TABLE_AUTHORIZATION_EXPRESSION` (`IcebergTableOperationExecutor.java:218-231`). | RD | T |
| CR-R4 | GAP | mudcat shall let a caller request read-only access, a narrower prefix and a shorter lifetime, and shall issue the intersection of the request and the caller's grant. | Gravitino gives write whenever the caller may write; the client cannot ask for less (`IcebergTableOperationExecutor.java:231`: `writable ? WRITE : READ`). A job that only reads gets a write credential. Run tokens (mudcat) need this to bound a job. | RD | T |
| CR-R5 | GAP | When a storage provider cannot express the requested scope exactly, mudcat shall refuse the request instead of issuing a wider credential. | ADLS: one directory-scoped SAS for all paths; it merges read and write paths (`ADLSTokenGenerator.java:78`), grants write on the whole root if any write path exists (:89), and a parent write path widens the scope (`docs/security/credential-vending.md:401`). Independent roots are rejected (:168-174), which is the right behaviour for that case. | RD | T |
| CR-R6 | GAP | mudcat shall check the size of a generated cloud policy against the provider limit before the call and shall return an error that names the limit. | S3 policy has one resource per location and per bucket statement, passed inline (`S3TokenGenerator.java:140`); GCS emits up to 3 rules per bucket and joins all conditions with `\|\|` (:227). No size or rule-count check exists (grep for "PackedPolicy", "2048": none). AWS inline session policy limit and GCS rule limit are TBR. | RD | T |
| CR-R7 | IMPL | mudcat shall set the lifetime of each issued credential from a per-storage-resource setting, with a default of TBD seconds. | Per-cloud `*-token-expire-in-secs`, default 3600 (`S3CredentialConfig.java:77`). GCS has no setting: lifetime is whatever the source token gives (`GCSTokenGenerator.java:80-81`). | RD | T |
| CR-R8 | GAP | mudcat shall state the longest delay between a revocation (legal hold, takedown, grant removal) and the end of every credential already issued, and shall limit credential lifetime for affected data to that delay. | Cloud STS/SAS tokens cannot be revoked and Gravitino has no revoke path (grep: none). The server cache serves a credential until 15% of its remaining life is used (`CredentialCache.java:62`), so a revoked caller's token stays valid up to the TTL (default 3600 s). | UP | A |
| CR-R9 | IMPL | mudcat shall serve a cached credential only while at least TBR percent of its lifetime remains. | Cache entry expires after `ratio * remaining` with ratio 0.15 (`CredentialConfig.java:36`, `CredentialCache.java:56-63`), so callers get at least 85% of the lifetime. Absent results are never cached (:52, test `TestCredentialCache.testAbsentCredentialIsNotCached`). | RD | T |
| CR-R10 | IMPL | mudcat shall key its credential cache by caller and exact scope, never by object alone. | `CredentialCacheKey(type, context)` where context holds user name plus read and write path sets (`CredentialCacheKey.java:31`, `PathBasedCredentialContext.java:77-94`). | RD | T |
| CR-R11 | GAP | mudcat shall return with each credential its issue time and its lifetime in seconds as well as its absolute expiry. | Only absolute epoch ms is sent (`Credential.java:30,46`). Clients compute refresh time with their own clock (`S3CredentialsRefresher.java:41`, Python `gvfs_base_operations.py:802`). Server STS expiry, server clock and client clock can all differ; the 0.15 and 0.5 ratios are the only skew defence. | RD | T |
| CR-R12 | IMPL | mudcat shall name, in each credential response, the endpoint at which the client can renew that credential. | Iceberg `client.refresh-credentials-endpoint`, `gcs.oauth2.refresh-credentials-endpoint`, `adls.refresh-credentials-endpoint` set to the table credentials path (`CredentialPropertyUtils.java:277-305`). Gap inside the IMPL: no endpoint for OSS (see quirk Q9). | RD | T |
| CR-R13 | GAP | mudcat clients shall ignore a credential of an unknown type and shall use the credentials they understand. | `CredentialFactory.create` throws `RuntimeException("No credential found for: ...")` (`CredentialFactory.java:84`) from `DTOConverters.fromDTO` (:1316), so one new type in a response breaks old clients for all types. | IN | T |
| CR-R14 | IMPL | mudcat shall refuse a credential request when more than one issuer can serve the location and no explicit choice exists. | Two matching providers for one scheme raise `UnsupportedOperationException` (`CatalogCredentialManager.java:116-121`). Gap: the conflict appears at request time, not at configuration time. | RD | T |
| CR-R15 | GAP | mudcat shall not fall back from a scoped credential to a long-lived account key when scoped issuance is configured. | Setting `s3-role-arn` without `credential-providers` silently vends the static key (`docs/security/credential-vending.md:144`); inference picks static providers (`BaseCatalog.java:625-645`). "Explicit list wins" (:636) is the only guard. | RD | T |
| CR-R16 | IMPL | mudcat shall require a separate, explicit grant before it returns any long-lived storage key, and shall return scoped short-lived credentials to holders of the plain read or write grant. | `SupportsDynamicCredential` marker; static credentials need `USE_SECRETS` and `INCLUDE_CREDENTIAL_SECRETS` (`MetadataObjectCredentialOperations.java:134,160-175`, `CredentialAuthorizationExecutor.java:62-80`, `SupportsDynamicCredential.java:22-32`). | AC | T |
| CR-R17 | GAP | mudcat shall not return the credentials of its own PostgreSQL database, or any other credential of its control plane, to a client. | Iceberg JDBC-backed catalogs list `jdbc-user-password` as a provider (`IcebergCatalog.java:113-130`) and `applyIcebergCredentials` copies `jdbc.user`/`jdbc.password` into engine config (`CredentialPropertyUtils.java:149`). The backing metadata DB password is vended to holders of the static grant. | AC | I |
| CR-R18 | IMPL | mudcat shall put a unique, provider-valid identifier of the caller into the session name of each issued cloud credential. | `RoleSessionNameUtils` cleans to `[A-Za-z0-9_@.-]`, caps at 64, adds a stable 16-char UUID suffix of the user name (`RoleSessionNameUtils.java:38-50`), so cloud audit logs name the caller. Gap: Tencent builds its own name and truncates (`COSTokenGenerator.java:274-277`), which drops uniqueness. | RD | T |
| CR-R19 | GAP | mudcat shall record each credential issuance with the caller, the run token, the object, the scope, the expiry and the cloud session identifier. | Iceberg path emits `LOAD_TABLE_CREDENTIAL` events (`IcebergLoadTableCredentialEvent.java`); the fileset/catalog endpoint emits no event (`MetadataObjectCredentialOperations.java`, whole file: no dispatch) and no event carries the STS session or scope. | PR | T |
| CR-R20 | GAP | mudcat shall let the credential-minting component run with no long-lived cloud key, using the host's workload identity, for every supported cloud. | AWS: default credential chain when no key is set (`S3TokenGenerator.java:102-109`) and IRSA (`AwsIrsaCredentialGenerator`). Azure needs an Entra client secret (`ADLSTokenGenerator.java:113-118`), OSS and COS need an access key pair (`OSSTokenGenerator.java:92-93`, `COSTokenGenerator.java:104`); the key is stored in the catalog properties. | OP | I |
| CR-R21 | IMPL | When an operator selects a replica or location by name, mudcat shall reject a name that is not a registered location of the object, and shall use the object's default when no name is sent. | `Current-Location-Name` header is validated against `fileset.storageLocations()` (`FilesetCatalogOperations.java:1999-2009`); multiple locations without a header use the stored default (:2018-2026). Fits replica-aware resolution. | RD | T |
| CR-R22 | GAP | mudcat shall check access to the path in a credential request against the requested scope, not against a stored default of the object. | Catalog-level credential requests cannot carry a path: path-scoped providers return nothing (`SupportsCredentials.java:38-47`, generators return `null` for non-path context e.g. `S3TokenGenerator.java:78`). The object endpoint accepts only CATALOG and FILESET (`MetadataObjectCredentialOperations.java:67`); tables go only through the IRC. Callers cannot request a sub-prefix. | AC | T |
| CR-R23 | IMPL | mudcat shall apply the caller's active-role narrowing to credential issuance. | Design: credential vending goes through the same check as direct access (`design-docs/gravitino-role-assumption.md:214-217`); the privilege checks above call `MetadataAuthzHelper.checkAccess`, which the narrowed principal flows through (inferred). Run tokens in mudcat play the same part. | AC | T |
| CR-R24 | IMPL | When mudcat federates a remote catalog, it shall let the operator choose to forward the caller's token or to use a service account, and with a service account it shall require local authorization and issue storage credentials locally. | `FederatedCatalogWrapper.java:144` (`vendCredentialsLocally`), :162 (refuses a service account without authorization), `forwardsUserToken()` :175; `UserPrincipalForwardingAuthManager.java:62-75`. Remote storage credentials are stripped when vending locally (:102-122). | PL | T |
| CR-R25 | GAP | mudcat shall apply access control to every catalog definition, whatever the source of the definition. | Catalogs in `gravitino.conf` for the IRC "are not registered in a metalake, so Gravitino access control does not apply" (`docs/security/credential-vending.md:96`). | AC | I |
| CR-R26 | GAP | mudcat shall reject a bearer token that has no expiry claim. | `DefaultJWTClaimsVerifier(aud, exact, null, null)` passes no required claims (`JwksTokenValidator.java:147`); Nimbus checks `exp` only when present (inferred). jjwt `parseClaimsJws` behaves the same (`StaticSignKeyValidator.java:104`). Tests only cover expired tokens (`TestJwksTokenValidator.java:729`). | AC | T |
| CR-R27 | GAP | mudcat shall check the issuer of every bearer token against a configured issuer. | JWKS validator checks the issuer only if `authority` is set (`JwksTokenValidator.java:141`); the static-key validator never checks it (`StaticSignKeyValidator.java:100-143`). | AC | T |
| CR-R28 | GAP | mudcat shall derive the principal identifier from the token issuer and subject together, so that equal subjects from different issuers or realms are different principals. | Default principal field is `sub` alone (`OAuthConfig.java`, `PRINCIPAL_FIELDS` default `["sub"]`). Kerberos default mapper takes everything before `@` (`KerberosAuthenticator.java` comment above `PRINCIPAL_MAPPER`; `KerberosPrincipalMapper.java:59-68`), so `alice@R1` and `alice@R2` collapse to `alice`. | ID | T |
| CR-R29 | GAP | mudcat shall reject a token or ticket whose principal the configured mapping rule does not match. | `RegexPrincipalMapper` returns the unmapped principal when the pattern does not match (`RegexPrincipalMapper.java:81`); a test pins it (`TestStaticSignKeyValidator.java:304-322`: pattern `([^@]+)@.*`, subject `plainuser` accepted). A rule meant as a domain filter does not filter. | AC | T |
| CR-R30 | GAP | mudcat shall refuse to start with an authenticator that accepts unverified identities enabled together with an authenticator that verifies identities. | `simple` and `basic` both claim the `Basic ` prefix; `simple` also claims requests with no header (`SimpleAuthenticator.java:79`); the filter takes the first match (`AuthenticationFilter.java:195`) and does not try the next. With `simple` first, any `Basic` request becomes an unverified user. | AC | T |
| CR-R31 | IMPL | mudcat shall leave only the health endpoints without authentication. | `AuthenticationFilter.java:105`, `HealthCheckPathMatcher`. | OP | T |
| CR-R32 | GAP | mudcat shall store local passwords with a standard password KDF with a memory or work cost that it records in each hash, and shall re-hash at login when the stored cost is below the current cost. | SHA3-512 iterated 5,000 times (`Sha3512Defaults.java:36`), chosen to avoid third-party crypto (export-control note in `Sha3512PasswordHasher.java` header). Cost was cut from 100,000 and old hashes are refused (`Sha3512Defaults.java:30-35`, `Sha3512PasswordHasher.java:142`); the design promises parameter upgrades (`gravitino-local-authentication.md:152`) but the code has no upgrade path. Hash is recomputed on every Basic request (no session). | AC | A |
| CR-R33 | GAP | mudcat shall take the same time to reject an unknown user as to reject a wrong password. | `authenticate` returns early for an unknown user or disabled user before hashing (`IdpUserGroupManager.java:186-196`) (timing difference inferred). | AC | T |
| CR-R34 | IMPL | mudcat shall never return the stored value of a secret property in an API response, and shall return a fixed placeholder that it rejects as a written value. | `HiddenPropertyMaskUtils.MASKED_VALUE = "******"` (:74), `validateNoMaskedPlaceholders` (:104), alter validators (`SecretPropertyUtils.java:286,300`). Also masks `jdbc-url` that embeds credentials (:142) and secrets inside nested JSON, rejecting duplicate keys and trailing tokens (:80-90). | AC | T |
| CR-R35 | GAP | mudcat shall decide that a property is secret from a declared schema or an explicit marker, not from substrings of its name. | Default list `secret,password,token,credential,access,account` matched as substrings of the key (`SensitivePropertyKeyKeywords.java:33`); docs admit false positives (`access`, `account`) and the list can be replaced or emptied (`docs/gravitino-server-config.md:489-498`). An undeclared key named `api-key` is not masked. | ST | T |
| CR-R36 | GAP | mudcat shall use one redaction rule set for API responses, logs and audit records. | `AuditLogRedactor` has its own list (`SENSITIVE_KEY_SUBSTRINGS`, :59: includes `auth`, `signature`, `apikey`) different from the property list above (`SensitivePropertyKeyKeywords.java:33`); the class comment names drift as the risk (:30-34). | OP | I |
| CR-R37 | GAP | mudcat shall store no secret in plaintext in its PostgreSQL database; it shall store secrets encrypted under a key held outside the database, or as references to an external secret manager. | Legacy plaintext in `catalog_meta`, `schema_meta`, `fileset_version_info` stays valid ("no migration", design §2 goal 4); only an in-memory provider ships (design §3, line 100); no cipher code in core; KMS SPI only inspects key properties (`KmsClient.java`). | OP | I |
| CR-R38 | IMPL | mudcat shall reject a request that supplies a secret reference as a plain property value. | Else one entity could read another's secret: `SecretManager.checkSecretKeys` (`SecretManager.java:90-103`) with the recognition rule (`SecretPropertyUtils.java:134`). | AC | T |
| CR-R39 | IMPL | mudcat shall rotate a secret by writing a new version, making it current only after the entity commit succeeds, and deleting the old version only after that. | Stage, commit, rollback (design `gravitino-entity-secrets.md:460-510`, `SecretManager`). Known gap in the design: two servers rotating one key can switch versions in a different order from their entity commits (:505). | OP | T |
| CR-R40 | GAP | mudcat shall use the same reserved scheme for external secret references as for stored ones and shall validate a reference against the provider before it commits the entity. | In-memory provider "rejects" external references (design :434,:550) and `SecretProvider.buildReferenceUrn` throws `UnsupportedOperationException` by default (`SecretProvider.java`); only the write-through path was exercised. | OP | T |
| CR-R41 | IMPL | When a stored secret cannot be resolved, mudcat shall still load, list, disable and drop the object that holds it. | `toPlaintextOrMasked` so a deleted or rotated secret does not block catalog load (`BaseCatalog.java:629-633`). Side effect: the masked placeholder may then be used as a value (see Q18). | OP | T |
| CR-R42 | GAP | mudcat clients shall renew a bearer token before it expires, with a leeway. | `DefaultOAuth2TokenProvider.java:40`: renews only when `expires <= now`; a token can expire in flight, and a client clock behind the server makes every request fail. | IN | T |
| CR-R43 | GAP | mudcat shall pass the caller's identity to a downstream system as a fully qualified principal and shall key every downstream client pool by that principal. | HDFS: user name plus the configured realm (`ImpersonationHDFSFileSystemProxy.java:60`); Hive pool keyed by user name only (`CachedClientPool.java:118`). Combined with CR-R28, two realms share one downstream identity. | PL | T |
| CR-R44 | IMPL | mudcat shall exclude credentials and secret values from its logs and from the string form of its principal. | `UserPrincipal.toString` prints `token=***` (`UserPrincipal.java:87`); debug log prints credential context (user and paths), not values (`CatalogCredentialManager.java:170`). | OP | I |

## 3. Quirks and gotchas ledger

**Q1. The credential scope comes from client-controlled table metadata**
- What happens: the vended policy covers `location`, `write.data.path`, `write.metadata.path` of the loaded metadata. The REST credential's `prefix` is only `tableMetadata.location()`, so a data path outside the location is covered by the policy but the Iceberg client will not choose that credential for it (prefix match; inferred).
- Cause: SECURITY / SPEC.
- Evidence: `CatalogWrapperForREST.java:403-408` (`Stream.of(tableMetadata.location(), tableMetadata.property(WRITE_DATA_LOCATION...`), `IcebergRESTUtils.java:185` (`toRESTCredential(tableMetadata.location(), config)`).
- Mudcat implication: CR-R1; the credential `prefix` and the policy scope must be the same set.

**Q2. Provider is chosen from the table location only; other paths are silently dropped**
- What happens: `getCredentialByPath(tableMetadata.location(), ctx)` picks the provider by the location's scheme; `filterContextByProvider` drops paths of other schemes. A write path on another store gets no credential and no error.
- Cause: MODEL.
- Evidence: `CatalogWrapperForREST.java:423`; `CredentialOperationDispatcher.java:162-185`.
- Mudcat implication: CR-R14; a request with mixed storage resources should fail or return one credential per resource.

**Q3. Read vs write is decided by the server from authorization, not requested**
- What happens: a principal that can modify a table always gets write credentials, even for a read-only job; a principal that cannot modify gets read.
- Cause: MODEL.
- Evidence: `IcebergTableOperationExecutor.java:231` `return writable ? CredentialPrivilege.WRITE : CredentialPrivilege.READ;`.
- Mudcat implication: CR-R4.

**Q4. Setting a role ARN without `credential-providers` vends a long-lived key**
- What happens: providers are inferred from the properties; `s3-token`, `oss-token`, `adls-token`, `aws-irsa` have no inference rule, so the catalog falls back to `s3-secret-key`.
- Cause: OTHER (usability trap that became a secret leak).
- Evidence: `docs/security/credential-vending.md:144`: "setting `s3-role-arn` without `credential-providers` does not enable `s3-token`. The catalog falls back to `s3-secret-key`".
- Mudcat implication: CR-R15; no inference between security classes.

**Q5. Static JDBC credentials of the backing database are vendable**
- What happens: an Iceberg catalog with a JDBC backend adds `jdbc-user-password` to its providers; clients with the static grant receive the DB user and password.
- Cause: SECURITY.
- Evidence: `IcebergCatalog.java:122-128` (`ensureCredentialProviderListed(props, JdbcCredential.JDBC_CREDENTIAL_TYPE)`); `CredentialPropertyUtils.java:149`.
- Mudcat implication: CR-R17.

**Q6. S3 `ListBucket` needs a bare-prefix escape hatch for Hadoop**
- What happens: by default the vended list condition requires a trailing slash so siblings sharing a string prefix (`table_new` vs `table`) cannot be listed. Hadoop `getFileStatus` does a HEAD on the bare key and gets 403 instead of 404 unless the bare prefix is allowed. Filesets turn the escape hatch on; Iceberg forces it off. The flag is internal, not user-set.
- Cause: ENGINE (Hadoop FileSystem semantics) / SECURITY.
- Evidence: `S3TokenGenerator.java:235-258`; `CredentialConstants.java:33-41`; `FilesetCatalogImpl.java:84-86`; `IcebergCatalog.java:115-117`; tests `TestS3PolicyPrefix`.
- Mudcat implication: scoping rules are per client class; document that a prefix grant lists names that share the string prefix unless a trailing slash is enforced.

**Q7. S3 partition is guessed in two different ways**
- What happens: `S3TokenGenerator` picks `aws-cn`/`aws-us-gov` by substring of the role ARN; `AwsIrsaCredentialGenerator` picks it by substring of the region (`cn-`, `us-gov-`). The two can disagree for the same catalog.
- Cause: OTHER.
- Evidence: `S3TokenGenerator.java:223-228` (`roleArn.contains("aws-cn")`); `AwsIrsaCredentialGenerator.java:234-238` (`region.contains("cn-")`).
- Mudcat implication: store the partition as an explicit field of the storage resource.

**Q8. Per-cloud policy shapes and quirks that the generators encode**
- What happens: AWS needs `GetObject`+`GetObjectVersion` on the resource, `ListBucket` with `s3:prefix` conditions per bucket, `GetBucketLocation`; read-only gets no `Put`/`Delete`; write paths also get read. OSS uses `oss:ListObjects` with a prefix condition and falls back to an unconditioned `oss:ListBucket` statement when there is no bucket entry. COS needs different ARN forms per action (`bucket/*` for `GetBucket`, `bucket/` for `HeadBucket`/`GetBucketLocation`), the app-id suffix on the bucket, and `HeadBucket` because hadoop-cos calls it at initialize. GCS needs `roles/storage.insightsCollectorService` on the bucket (bucket-level metadata read, not path-scoped), and an `objectListPrefix` condition plus equality conditions on every parent path.
- Cause: SPEC (each provider's IAM grammar).
- Evidence: `S3TokenGenerator.java:149-216`; `OSSTokenGenerator.java:154-167`; `COSTokenGenerator.java:142-165,220-242`; `GCSTokenGenerator.java:96-110,186-196`.
- Mudcat implication: per-cloud issuers need a conformance test suite against the real service or a faithful emulator; the same logical scope yields different grants (bucket metadata is always readable under GCS).

**Q9. Refresh endpoints exist for S3, GCS, ADLS only**
- What happens: `buildRefreshProps` sets refresh endpoints when `s3.session-token`, `gcs.oauth2.token` or `adls.sas-token.*` is present. OSS tokens carry an `expires-at` but no refresh endpoint. COS has no Iceberg mapping at all: `toIcebergProperties` falls through to `credential.toProperties()` (raw Gravitino keys).
- Cause: COMPAT (Iceberg client support per cloud).
- Evidence: `CredentialPropertyUtils.java:277-305`, :223-238.
- Mudcat implication: the IRC conformance gateway must list per cloud which keys Iceberg clients honour.

**Q10. Azure SAS is one directory-scoped token, signed with a user-delegation key per call**
- What happens: read and write sets are merged; any write path adds write, delete, create, add on the whole root; paths in different containers or accounts, or with no common root, fail. A new `ClientSecretCredential` and `DataLakeServiceClient` are built on every call; the key's start time is `now()` with no back-dating.
- Cause: SPEC (SAS grammar) / OTHER.
- Evidence: `ADLSTokenGenerator.java:78-95,113-125,128-176`; `docs/security/credential-vending.md:401`.
- Mudcat implication: CR-R5; clock skew between issuer and storage can make a fresh token "not yet valid" (inferred).

**Q11. AWS limits the generator does not guard**
- What happens: the session policy is passed inline; AWS limits its packed size, and the role session duration is capped by the role's setting (docs say "cannot exceed the role's maximum session duration"). With many locations (table plus data and metadata paths) the call fails with a cloud error that surfaces as `RuntimeException("Failed to generate credential using ...")`.
- Cause: SPEC.
- Evidence: `S3TokenGenerator.java:134-146`; `CredentialProviderDelegator.java:66-70`; `docs/security/credential-vending.md` table for `s3-token-expire-in-secs`.
- Mudcat implication: CR-R6; check limits up front, report the offending limit. Limit values TBR.

**Q12. Wrapped errors lose their type**
- What happens: `IllegalArgumentException` passes through; every other generator failure becomes a bare `RuntimeException`, so STS throttling, bad role and bad policy are one 5xx. `getTableCredentials` returns an empty body on `ServiceUnavailableException`.
- Cause: OTHER.
- Evidence: `CredentialProviderDelegator.java:63-72`; `CatalogWrapperForREST.java:263-265`.
- Mudcat implication: distinguish retryable (throttle), configuration (bad role) and policy (too large) errors in the response.

**Q13. Credentials and clocks**
- What happens: server cache and clients both compute with absolute epoch ms from the cloud. Server evicts at 15% of remaining life; Hadoop clients refresh at 50%; Python GVFS caches at 50%. Static credentials carry `expire = 0`, and `0` is both "never expires" and "do not cache" (`expireAfterCreate` returns 0 for `timeToExpire <= 0`).
- Cause: OTHER.
- Evidence: `CredentialCache.java:56-63`; `S3CredentialsRefresher.java:41`; `GCSCredentialsProvider.java` (`EXPIRATION_TIME_FACTOR = 0.5D`); `Credential.java:41`; `S3SecretKeyCredential.java:110`.
- Mudcat implication: CR-R11; use an explicit "no expiry" flag, not a magic zero.

**Q14. Python GVFS holds a write lock during the REST call**
- What happens: on a cache miss the code takes the write lock and calls the server inside it, so readers wait for a network call.
- Cause: CONCURRENCY.
- Evidence: `clients/client-python/gravitino/filesystem/gvfs_base_operations.py:844-849`.
- Mudcat implication: refresh with single-flight outside the lock.

**Q15. Hadoop S3A has two SDK generations**
- What happens: a credential provider class must implement the AWS SDK v1 or v2 interface depending on the Hadoop version on the classpath; Gravitino ships two providers sharing a refresher and detects the SDK by probing for a class name.
- Cause: ENGINE.
- Evidence: `S3Utils.java:35-41,60-75`; `S3CredentialsRefresher.java:26-31`.
- Mudcat implication: client connectors are a compatibility matrix; keep the credential wire format independent of the SDK.

**Q16. Client picks among several credentials with a hard-coded order**
- What happens: the client prefers a token, then IRSA, then a static key (`getSuitableCredential`); a catalog with both returns both.
- Cause: MODEL.
- Evidence: `S3Utils.java:119-137`; `CatalogCredentialManager.java:98-110` (server returns every provider's credential).
- Mudcat implication: the server should state which credential is for which prefix so that clients do not guess.

**Q17. Unknown credential type breaks the whole response on the Java client**
- What happens: see CR-R13.
- Cause: COMPAT.
- Evidence: `CredentialFactory.java:84`; `DTOConverters.java:1316`.
- Mudcat implication: CR-R13.

**Q18. Masked placeholder can become a value**
- What happens: unresolved secrets are replaced by `******` for catalog load, and the same placeholder is also what read responses show. Clients filter it out (`BaseGVFSOperations.java:1143`); the server rejects it on write. A deleted secret leaves the object loadable with `******` as the password.
- Cause: MODEL.
- Evidence: `BaseCatalog.java:629-633`; `HiddenPropertyMaskUtils.java:74,104`; `SecretPropertyUtils.java:286`.
- Mudcat implication: use a distinct "redacted" marker type, not a value in the same field; failure to resolve a secret should fail the use, not substitute.

**Q19. Raw secret reference as a property value**
- What happens: any value that starts with `urn:gravitino-secret` and ends with the key is resolved server-side; without the create-time check a user could point a property at another entity's secret.
- Cause: SECURITY.
- Evidence: `SecretManager.java:90-103`; `SecretPropertyUtils.java:134`.
- Mudcat implication: CR-R38; never infer "this is a reference" from a string shape in user-writable fields.

**Q20. Secret key detection by name hits unrelated keys and misses others**
- What happens: any key containing `access` or `account` is masked and recovered through `getSecrets`; a secret named `api-key` is neither.
- Cause: MODEL.
- Evidence: `SensitivePropertyKeyKeywords.java:33`; `HiddenPropertyMaskUtils.java:140-145`; `docs/gravitino-server-config.md:487-498`.
- Mudcat implication: CR-R35.

**Q21. Nested JSON masking and duplicate keys**
- What happens: masking walks JSON values; a duplicate key (`{"config":{"password":"x"},"config":{}}`) could hide the first object from the walk while the raw string keeps it, so the mapper fails on duplicate keys and trailing tokens.
- Cause: SECURITY.
- Evidence: `HiddenPropertyMaskUtils.java:80-90`.
- Mudcat implication: if mudcat masks structured values, reject ambiguous JSON at write time.

**Q22. Legacy leak switch**
- What happens: `gravitino.catalog.credential.backfillToProperties=true` returns plaintext hidden credentials in catalog properties, for old connectors during a rolling upgrade.
- Cause: COMPAT.
- Evidence: `Configs.java:719-727`; `BaseCatalog.java:610-615`.
- Mudcat implication: no secret-bearing compatibility switch; version the response shape instead.

**Q23. Static vs dynamic access check has a catalog-level special case**
- What happens: for catalog-level requests the authorizer allows a caller without static grants only when every configured provider is dynamic; otherwise it demands the static privileges. Unmarked custom providers count as static, so older provider jars stay safe.
- Cause: COMPAT / SECURITY.
- Evidence: `CredentialAuthorizationExecutor.java:62-80`; `SupportsDynamicCredential.java:30-31`.
- Mudcat implication: default-deny for an issuer that does not declare itself scoped.

**Q24. Two authenticators can claim the same header**
- What happens: `simple` (any user name, no check) and `basic` (database users) both handle `Basic `; `simple` also handles "no header". First match wins; exceptions are not caught to try the next authenticator.
- Cause: OTHER.
- Evidence: `AuthenticationFilter.java:195-199`; `SimpleAuthenticator.java:79-81`; `BasicAuthenticator.java:93-97`.
- Mudcat implication: CR-R30.

**Q25. Malformed Kerberos header returns 500**
- What happens: `Base64.getDecoder().decode(token)` runs before the `try` block and before the blank check; bad base64 throws `IllegalArgumentException`, which the filter maps to 500, not 401.
- Cause: OTHER.
- Evidence: `KerberosAuthenticator.java:119` vs the `try` at :123; `AuthenticationFilter.java:117-122`.
- Mudcat implication: every authenticator parse error is a 401 with a challenge.

**Q26. JWT validation defaults**
- What happens: clock skew allowance defaults to 0 s; audience required; issuer optional; no required `exp`; static-key mode uses one shared key and an algorithm from config; the code has a `TODO` for multiple signing servers. JWKS keys are cached 5 minutes with refresh-ahead and at most one fetch per 30 s (Nimbus defaults, per comment).
- Cause: SPEC / OTHER.
- Evidence: `OAuthConfig.java:40-45`; `JwksTokenValidator.java:95-101,126,141-147`; `OAuth2TokenAuthenticator.java:72`.
- Mudcat implication: CR-R26, CR-R27; a key-rotation test is needed (new `kid` appears after a cached JWKS).

**Q27. Same-subject collision across realms**
- What happens: Kerberos default mapping and OAuth `sub` do not include the issuer or realm, so equal names are the same user in Gravitino's metadata and grants.
- Cause: MODEL / SECURITY.
- Evidence: `KerberosPrincipalMapper.java:59-68`; `OAuthConfig.java` (`PRINCIPAL_FIELDS` default `sub`); `ImpersonationHDFSFileSystemProxy.java:60`.
- Mudcat implication: CR-R28, CR-R43; tenant-qualified principal IDs (mudcat ID area) cover this by design.

**Q28. Basic IdP password hashing**
- What happens: SHA3-512, 5,000 iterations, 16-byte salt, PHC string; iteration count must equal the default exactly, so the parameter cannot be raised without invalidating every hash. Chosen to avoid third-party crypto for export-control scope.
- Cause: OTHER (policy constraint).
- Evidence: `Sha3512Defaults.java:30-36`; `Sha3512PasswordHasher.java:140-143`.
- Mudcat implication: CR-R32; select the KDF by licensing and policy early.

**Q29. Principal keeps the raw `Authorization` header**
- What happens: `UserPrincipal.accessToken` holds the full header (for `Basic`, the base64 of user:password) so it can be forwarded to a remote catalog; `toString` masks it.
- Cause: MODEL.
- Evidence: `SimpleAuthenticator.java` (`new UserPrincipal(userInformation[0], authData)`); `OAuth2TokenAuthenticator.java:80-84`; `UserPrincipal.java:87`.
- Mudcat implication: forward tokens only when the operator chooses "forward token" (CR-R24); never forward a password header.

**Q30. Federated catalogs: three credential sources**
- What happens: with a user-token connection, remote storage credentials pass through, rewritten so refresh points at this server; with a service account or `storage-credential-source=local`, remote credentials are removed and local providers vend.
- Cause: MODEL.
- Evidence: `FederatedCatalogWrapper.java:100-122,144-148,175`; `IcebergRESTUtils.java:307-334`.
- Mudcat implication: CR-R24; the gateway must say whose permissions a credential carries.

**Q31. Tencent session name differs from the shared helper**
- What happens: `gravitino_` + cleaned name, hard-truncated to 64 characters; the shared helper adds a hash suffix, so Tencent can give two users one session name.
- Cause: OTHER.
- Evidence: `COSTokenGenerator.java:274-277` vs `RoleSessionNameUtils.java:38-50`; test `TestRoleSessionNameUtils.sessionNamesAreValidAndDistinguishable`.
- Mudcat implication: CR-R18; one helper, one test, all clouds.

**Q32. Context sets are mutable and merged in place**
- What happens: `mergeContexts` adds the new paths into the first context's `HashSet`; `CredentialCacheKey` hashes the same objects. If a cached context is merged after use, its hash changes (not shown to happen in the code paths read; inferred risk).
- Cause: CONCURRENCY.
- Evidence: `CredentialOperationDispatcher.java:203-210`; `PathBasedCredentialContext.java:77-80`.
- Mudcat implication: immutable scope value objects.

**Q33. Catalog credential manager is built once**
- What happens: `CatalogCredentialManager` is created from the properties on first use and closed only with the catalog; a rotated source key (the key used to call STS) is picked up only after a reload (inferred from lazy init plus close-only lifecycle).
- Cause: CONCURRENCY / OTHER.
- Evidence: `BaseCatalog.java:472-482`, :443-454.
- Mudcat implication: a rotation test (CR-R39) must include the issuer's own key.

**Q34. Spark/Flink take only non-expiring credentials at init**
- What happens: catalog initialization copies credentials with `expireTimeInMs == 0`; expiring ones go through IRC refresh.
- Cause: ENGINE.
- Evidence: `CredentialInfos.java:44-57`; `BaseCatalog.java` (spark) :909-910.
- Mudcat implication: the gateway is the only path with refresh; native connectors must not cache vended keys.

## 4. Open questions, inconsistencies, half-implemented

1. **Doc vs code, password-hash upgrades**: design says self-describing hashes allow "future upgrades of SHA3-512 cost parameters" (`gravitino-local-authentication.md:152`); code rejects any iteration count other than 5,000 (`Sha3512PasswordHasher.java:142`).
2. **Doc vs code, malformed Basic header**: design says decoding failure returns 400 (`gravitino-local-authentication.md:383`); `BasicAuthenticator.java` throws `UnauthorizedException` (401) for invalid base64.
3. **Secrets store never finished**: only an in-memory provider is in scope of the design (`gravitino-entity-secrets.md:100`); external references are refused by it (:434,:550); `SecretProvider` version methods default to `UnsupportedOperationException`. No production-grade secret store ships.
4. **KMS SPI without a user**: `KmsClient` inspects keys but does no cryptographic operation, and nothing in the repo reads it (`KmsClient.java`; `docs/gravitino-server-config.md:698-716`). Table encryption, if intended, is not started (inferred).
5. **Identity-bound token (role narrowing that a client cannot override)** is a later phase (`gravitino-role-assumption.md:283,299`); today the narrowing header is client-set, so a compromised client can widen it back (inferred).
6. **Multi-node secret rotation ordering** is documented as a residual race (`gravitino-entity-secrets.md:505-509`).
7. **TODO: multiple OAuth servers** with separate signing keys (`OAuth2TokenAuthenticator.java:72`).
8. **`CredentialConstants.CREDENTIAL_PROVIDER_TYPE`** is `@Deprecated` in favour of `credential-providers` (`CredentialConstants.java:24-26`), but the IRC wrapper keeps a rename table for old property names (`CatalogWrapperForREST.java:118-123`, `checkForCompatibility` :131), so the legacy key stays live in the IRC path.
9. **Catalogs defined in `gravitino.conf`** bypass access control (`docs/security/credential-vending.md:96`).
10. **Credential issuance audit** exists for the Iceberg endpoint only; the fileset/catalog endpoint has no event (see CR-R19).
11. **Catalog-level credential request** returns only static keys whenever token providers need a path (`docs/security/credential-vending.md:160-164`): a caller with both privileges asking at catalog level "typically returns only the static key".
12. **Impersonation scope**: Iceberg paths need explicit care so that metadata building runs inside the proxy user's context (`IcebergCatalogWrapper.java:532-561` comments), which shows how easily impersonation is bypassed by moving a call outside a wrapper.
13. **Gravitino has no usage-policy gate in front of vending** (licence, embargo, hold). Credential issuance checks metadata access and read/write privilege only; mudcat's UP area must sit before issuance.
