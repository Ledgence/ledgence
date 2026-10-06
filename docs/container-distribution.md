# Container distribution and release qualification

One Ledgence runtime image supplies the orchestrator, worker and explicit
migration job, including Console, the Python helper and CPython 3.14. PostgreSQL
remains separate. Programs still come from the program store; deploying a program
does not require rebuilding the image.

This distribution is implemented on the development branch. Published 0.3.1
native bundles do not contain `ledgence local` or the deployment kit. Keep using
their manual installation guide until a release includes these artifacts. The
presence of a workflow is not evidence that images have been published.

## Standalone kit

Generate a complete Compose kit from `deploy/distribution` with an exact workspace
version and an already qualified immutable image reference:

```sh
python3 tools/release/local_distribution.py \
  --version VERSION \
  --image docker.io/ledgence/ledgence@sha256:QUALIFIED_INDEX_DIGEST \
  --output /path/to/new-local-kit
```

Replace the version and digest placeholders. The kit contains its manifest,
pull-only Compose files, optional examples, guide, license and full checksums.
It needs no checkout, Git variables or Docker build and can be used without the
CLI. The base has PostgreSQL, migration, orchestrator and worker; only the API and
Console port is exposed on host loopback. Examples add a callback receiver and
prepare programs inside the selected Linux runtime.

Named volumes survive `down`/`up`. One worker owns its cache; do not share that
cache between worker processes. The CLI saves the kit, version, image, port,
concurrency, Docker context and unique project identity on first use. A new CLI
does not upgrade that saved installation. Schema upgrades require a separate
operator procedure with backups and stopped writers.

The source-built recipe, including ElasticMQ, remains in `deploy/local`; see
[local deployment](local-deployment.md).

## Compatibility

Container variants cover `linux/amd64` and `linux/arm64`, each tested on a native
runner. A configured matrix is not a passing result. Native CLI bundle targets
remain Linux x86_64 GNU and macOS ARM64; container support does not create another
native executable distribution.

Program packages identify one platform and exact Python major/minor. Prepare
dependencies in the target runtime. A package prepared on macOS or another
architecture is not automatically usable in the container. This distribution
does not add multi-platform artifact selection for a `(program_id, version)`.

## Qualification

The **Container distribution** workflow qualifies relevant source pushes and can
also be manually dispatched. With `publish=false` it uses read-only repository
permissions and never authenticates to Docker Hub. Publication additionally
requires the Rust/Python CI and Console's dependency, component, browser and
live-backend checks to pass on the same source. Independently on each architecture it:

1. Builds an OCI archive with pinned BuildKit, SBOM generator and base images.
2. Verifies blobs, descriptors, source/version/platform, SBOM and provenance.
3. Copies the complete archive into an owned loopback registry without changing
   digests, then pulls that exact reference.
4. Runs the kit outside the checkout: Console/API, worker observations, real
   tasks/workflows/callbacks, repeated publication, persistence and relocated CLI.
5. Collects exact corresponding sources for the base components.
6. Retains the tested archive, checksums, acceptance report and sources.

Internal CLI dotfiles stay on the disposable runner; the acceptance report and
service logs are retained. An independent job downloads both artifacts and runs
the publication preflight without credentials, verifying the complete inventory
and checksums of the evidence transferred for publication. Failed image jobs
retain available reports and service logs for seven days in separate
`image-failure-*` artifacts; these are never inputs to publication.

Restart qualification records all existing worker sessions before shutdown and
waits for a fresh, accepting, unexpired session with a new identity after startup. Persisted
observations from the previous process may remain fresh briefly; their presence
does not establish that the restarted worker is ready.

Any failed gate prevents publication. Run the acceptance locally with
`python3 tools/check-distribution.py --directory KIT --evidence NEW --cli BINARY`.
It creates unique projects and removes only its own resources. This does not
qualify AWS SQS, production capacity or untested architectures.

Skopeo runs on CI only; its version is recorded and it is not shipped in the
product. `--all --preserve-digests` copies images and attestations without a rebuild.

## Docker Hub configuration

Create the public repository `ledgence/ledgence`. Configure GitHub repository
settings or the `docker-hub` environment:

| Setting | Purpose |
| --- | --- |
| Variable `DOCKERHUB_NAMESPACE` | Namespace; defaults to `ledgence`. |
| Variable `DOCKERHUB_USERNAME` | Account authenticating to Docker Hub. |
| Secret `DOCKERHUB_TOKEN` | Repository write credential. Never commit it. |

Authentication uses standard input, followed by logout. Public availability is
checked without the publisher's registry session. Users need no Ledgence account;
standalone kits can reference another compatible registry.

## Complete release procedure

Publishing requires an explicit `publish=true` dispatch on the matching annotated
version tag already contained in `main`. Workspace and SDK versions must agree,
and all qualification gates must pass. Do not reuse an existing version.

Create the version's GitHub release as a **public prerelease**, leaving the last
complete stable release as latest. This makes corresponding sources available
before the runtime image is distributed. The workflow then:

1. Uploads verified source archives and manifests for both architectures. Existing
   assets are accepted only when their bytes match.
2. Copies qualified archives to architecture tags and assembles
   `VERSION-python3.14`. Existing tags must match the qualified content; neither
   `latest` nor different existing bytes are overwritten.
3. Checks the combined descriptors and public access, then publishes the kit
   pinned to the final index digest, `install.sh` and individual SHA256 files.
4. Retains a small `local-distribution` artifact for native packaging and the
   complete `container-release` artifact.

Dispatch **Candidate packaging** on that same commit with
`local_distribution_run` identifying the successful container run. It verifies
and includes the kit under `local/` in each candidate. Local packaging accepts
`--local-distribution DIRECTORY`. Native promotion preserves those exact bytes.

Finish native bundle qualification/promotion, combined native `SHA256SUMS`, SDK
and crate publication, and documentation. Verify installation from the public
downloads before marking the release stable/latest. The installer defaults to
the latest stable release; an explicit version can select the prerelease.

After a partial publication, retry the failed publication job using the same
qualified artifacts. Rebuilding everything may produce different digests and is
not an idempotent publication retry. Retain the artifacts until completion.

## Third-party components

Ledgence-owned code remains MIT. Runtime OS components retain their licenses.
The image keeps Rust/toolchain, Console, CPython and Debian notices. Do not
describe the whole image as MIT or omit base-component source obligations.

`deploy/distribution/base-image.json` identifies the immutable official Python
base, upstream Dockerfile and CPython source by checksum. The image records actual
binary/source versions in `/opt/ledgence/legal/image-runtime.json`. The host-side
collector retrieves the exact Debian source descriptors and checksummed source
files, CPython source and upstream recipe. Sources accompany the published image
as separate downloads, without increasing every runtime download:

```sh
python3 deploy/distribution/image_sources.py \
  --image REGISTRY/IMAGE@sha256:DIGEST --platform linux/arm64 --output NEW_DIRECTORY
python3 deploy/distribution/image_sources.py \
  --verify NEW_DIRECTORY --expected-image REGISTRY/IMAGE@sha256:DIGEST --platform linux/arm64
```

Missing source versions, incomplete inventories and checksum mismatches fail
publication. Review base updates explicitly and retain source assets while the
corresponding images are distributed. See [dependency policy](dependencies.md).

References: [Docker multi-platform builds](https://docs.docker.com/build/ci/github-actions/multi-platform/),
[attestation storage](https://docs.docker.com/build/metadata/attestations/attestation-storage/),
[Skopeo digest-preserving copy](https://github.com/containers/skopeo/blob/main/docs/skopeo-copy.1.md).
