# Execution artifact byte adapters

This internal MIT crate supplies filesystem/S3 bytes and local transfer spools
behind the orchestration API ports. The catalog and service remain authoritative
for access, generation selection, acceptance and cleanup eligibility. Program
ZIP publication and its cache are separate.

The default build has no AWS dependency. Enable `s3` for the optional AWS SDK
adapter. AWS credentials are resolved only on the server through its standard
credential chain. Fixed nonsecret credentials can be explicitly selected for a
literal-loopback test endpoint. Other endpoints require HTTPS.

## Resource ownership

Create one `FileArtifactSpoolFactory` per orchestrator. Clones share two upload
and four download slots. Admission occurs before file creation. Spool data is
bounded to 128 MiB of uploads and 256 MiB of downloads; an instance of the local
durable store can additionally stage 128 MiB. These limits apply per composed
instance, not across independent replicas or independently opened factories.

Each transfer has an absolute deadline capped at 120 seconds. A finished spool
is immutable and rewound; finishing does **not** attest to its digest. The
service checks actual size and SHA-256. Ownership passes from writer to reader
and remains until drop or expiry, including after EOF so the service can keep it
through catalog finalization. An independent timer expires unpolled readers.
Dropped futures do not release capacity ahead of pending blocking file IO.
Removal failures retain capacity and retry cleanup. `drain` closes admission;
a timeout reports incomplete cleanup and never proves physical IO stopped.
Keep the runtime alive until draining succeeds.

Spool directories are exclusively locked. Startup deletes only recognized stale
spool files and rejects failed cleanup. Concurrent orchestrator/maintenance
processes require distinct spool directories; a process cannot clean another
live process's namespace. No spool recovery resumes a remote write.

## Durable byte stores

Filesystem writes hash and count a bounded stream, synchronize a private stage,
and publish using an atomic hard link without replacement, followed by parent
directory synchronization. A durable random marker identifies the store; its
path is diagnostic, not identity. Per-process staging locks protect live writers
from restart cleanup. This backend is qualified for one orchestrator and a
persistent local volume, not arbitrary network filesystems or multiple hosts.
Maintenance can independently open the store without sweeping live staging.

S3 writes use one conditional single-object PUT with `If-None-Match: *`, an
explicit whole-object SHA-256 and exact length. SDK retries are disabled even if
`AWS_MAX_ATTEMPTS` requests more attempts. The adapter never uses an ETag as a
content digest, multipart uploads, or a full-artifact in-memory request buffer.
GET streams are bounded by their original deadline and checked against the
manifest's size and SHA-256. Successful request metadata alone is insufficient.
Post-dispatch connection failures/timeouts remain uncertain; an absent-object
probe does not establish that a delayed PUT cannot complete.

## Verification

`cargo test -p ledgence-adapter-blob --locked` exercises the filesystem/spool
lifecycle. `cargo test -p ledgence-adapter-blob --all-features --locked` also
exercises deterministic faults against the production S3 transport.

`python3 tools/check-artifact-s3.py --evidence /absolute/evidence/directory`
(from the repository root) runs the production adapter against a pinned,
unchanged external RustFS fixture, replaces its server while preserving data,
and verifies recovery. It creates only labeled, disposable Docker resources,
uses fake credentials, isolates fixture egress, and removes its resources.
The fixture is not shipped as part of Ledgence and retains its own licenses.
This gate establishes that local compatibility profile, not AWS or every
S3-compatible deployment. The explicit external-loopback mode reports only
conformance and does not claim container restart validation.
