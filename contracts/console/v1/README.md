# Console contract v1

Portable DTOs are defined in `ledgence-orchestration-api::console`. Existing
client/worker HTTP contracts retain their previous serialization.

The canonical cross-language fixture is
[`console-v1.json`](../../../crates/ledgence-orchestration-api/tests/fixtures/console-v1.json).
Rust constructs it from the public DTOs and verifies the committed bytes; Console
runtime-decoder tests consume that same file. It is also included in the Rust
source archive so its tests remain independent of the monorepo.

Regenerate explicitly after an intentional contract change:

```sh
cargo run -p ledgence-orchestration-api --example console-fixtures -- --write
cargo test -p ledgence-orchestration-api --test console_contracts
pnpm --dir console contracts:check
```

All `/v1/console/*` responses identify the fixed instance with
`Ledgence-Instance-Id` and `Ledgence-Console-Contract: 1`. The browser rejects
responses that disagree with the configuration used for its query keys.
Queries and commands reject unknown and duplicate fields. The internal binding
is never selected by a Console request or exposed in its response DTOs.

New revisions, sequences and unsigned metadata counters are canonical decimal
strings. Timestamps are UTC epoch milliseconds within the existing four-digit
year range. User JSON keeps the task API's exact integer and finite floating
value categories; clients must not round-trip it through JavaScript Number.

Pages are live keyset reads. Their opaque cursor binds endpoint, internal
instance scope, parent and filters. A page is internally consistent; consecutive
pages can observe later committed changes. The worker slot cursor uses the
stable slot position, not a changing snapshot sequence.
