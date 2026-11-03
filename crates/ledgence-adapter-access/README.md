# Ledgence local access adapter

Optional opaque credentials for a single Ledgence installation. The portable
verifier contract belongs to `ledgence-orchestration-api`; this adapter has no
HTTP, identity-provider SDK, network or database dependency.

`AccessConfig` binds an authority to an instance and scope. Each credential has a
stable subject, explicit operator/service/worker class, expiry and capabilities.
The configuration contains SHA-256 verifiers, never reusable bearer secrets.
`AccessConfig::issue` obtains 256 secret bits from the operating system and
returns a redacted provisioning value. Callers must write it privately and must
not print it. `OpaqueCredentialVerifier` validates tokens with one bounded
in-memory lookup and constant-time digest equality, rechecking expiry each time.

A verifier is an immutable startup snapshot. To change permissions or revoke a
credential, replace configuration and restart every serving replica. Worker
credentials authorize the trusted worker plane for the instance; they do not
isolate individual worker sessions from each other.

Authentication does not establish durable approval attribution or read-audit
receipts. Those are separate orchestration capabilities. Deployment guidance and
supported release availability live in the repository's public documentation.
