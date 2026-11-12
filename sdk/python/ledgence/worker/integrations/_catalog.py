"""Declared read-only tool identities and durable discovery snapshots (MIT)."""
from dataclasses import dataclass
import inspect

from ..workflow import OperationKind, _callable_name, WorkflowError
from ._core import PreparedCall, freeze, text
from ._schema import validate_schema
from ._types import AgentProfileError


def callable_identity(function):
    try:
        identity = _callable_name(function, label="read tool")
        if "<lambda>" in identity:
            raise AgentProfileError("named_read_callable_required")
        return identity
    except WorkflowError as exc:
        raise AgentProfileError("named_read_callable_required") from exc


@dataclass(frozen=True, slots=True)
class ReadSpec:
    identity: str
    version: str
    function: object
    callable_name: str

    @classmethod
    def create(cls, identity, function, version):
        return cls(text(identity), text(version), function, callable_identity(function))

    def binding(self):
        return {"identity": self.identity, "version": self.version, "callable": self.callable_name}


def validate_descriptors(value, registry):
    descriptors = freeze(value, 8192)
    if type(descriptors) is not list or len(descriptors) > 16:
        raise AgentProfileError("invalid_catalog_descriptors")
    identities, names = set(), set()
    for item in descriptors:
        if type(item) is not dict or set(item) != {"id", "name", "version", "input_schema", "executor_id", "target"}:
            raise AgentProfileError("invalid_catalog_descriptor")
        for field in ("id", "name", "version", "executor_id"):
            text(item[field])
        if (item["id"] in identities or item["name"] in names or item["executor_id"] not in registry
                or type(item["target"]) is not dict):
            raise AgentProfileError("ambiguous_catalog_descriptor")
        item["input_schema"] = validate_schema(item["input_schema"])
        identities.add(item["id"]); names.add(item["name"])
    return descriptors


class CatalogSpec:
    def __init__(self, state, identity, *, version, discover, registry):
        self.state, self.identity, self.version = state, text(identity), text(version)
        self.discover = discover
        self.discover_identity = callable_identity(discover)
        if type(registry) is not dict or not registry or len(registry) > 64:
            raise AgentProfileError("invalid_catalog_registry")
        self.registry = {text(key): fn for key, fn in registry.items()}
        self.registry_identities = {key: callable_identity(fn) for key, fn in self.registry.items()}

    async def snapshot(self, parent="START"):
        prepared = PreparedCall(self.state.slot("CATALOG", parent, self.identity), OperationKind.TOOL,
            {"kind": "discovery", "identity": self.identity, "version": self.version,
             "discover": self.discover_identity, "registry": self.registry_identities},
            {"configuration": self.state.configuration}, "ledgence-catalog-v1")
        async def discover(*, request):
            result = self.discover(configuration=request["arguments"]["configuration"])
            if inspect.isawaitable(result):
                result = await result
            return validate_descriptors(result, self.registry)
        try:
            return validate_descriptors(await self.state.execute(prepared, discover, category="discovery"), self.registry)
        except AgentProfileError as exc:
            raise self.state.poison(exc)
