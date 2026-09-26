# Self-hosted instance binding

A self-hosted installation has one immutable compatibility binding. The browser does not select a tenant or namespace. Existing CLI, SDK and worker commands retain their scope fields and must match the server binding.

Create an instance configuration file (it contains no credentials):

```json
{
  "instance_id": "local",
  "name": "Local instance",
  "scope": { "tenant_id": "acme", "namespace": "demo" },
  "suggested_queues": ["demo"],
  "allowed_origins": []
}
```

For a new database, omitting `scope` selects the fixed internal `default/default` binding. To adopt existing data, explicitly configure its existing binding. Run the explicit database migration, then supply `--instance-config instance.json` to `ledgence-orchestrator serve`. This flag is required on every subsequent start, including headless operation. Turning off assets does not turn off the binding. Display names and queue suggestions may change; the persisted instance ID and scope cannot change.

Stop every orchestrator and other writer before migrating or binding an existing database. A database containing work, sessions, dispatch destinations, completion destinations or retention state from another binding is rejected. No records are deleted or reassigned. Use a separate database or an explicitly planned offline migration. Older orchestrators must not share a bound database. Rolling back to a version that ignores the binding requires a coordinated offline operational plan; simply removing the configuration flag does not restore legacy operation.

Retention keeps its explicit scope arguments and also enforces the database binding. Unbound databases remain compatible with existing commands. The binding is an installation boundary, not operator authentication; remote access still needs an access-controlled TLS reverse proxy protecting both console assets and APIs.

The startup scope audit reads the first and last byte-ordered scope through full indexes for every scoped table, including expired sessions, retiring execution records, and terminal completion subscriptions. A matching pair proves all rows in that table use the same scope; it does not scan execution payloads or count records. Initial index construction remains part of the explicit maintenance migration. Stop legacy writers before binding an existing database.
