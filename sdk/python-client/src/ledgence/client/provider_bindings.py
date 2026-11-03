"""Immutable, approval-bound provider configuration; no vendor SDK types (MIT)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re
from typing import TYPE_CHECKING

from . import codec
from .errors import InputError

if TYPE_CHECKING:
    from .effect_models import EffectProgramDescriptor


class ProviderLookup(StrEnum):
    BY_BINDING = "by_binding"
    BY_REFERENCE = "by_reference"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class ProviderBusiness:
    namespace: str
    key: str

    def to_dict(self):
        return {"namespace": self.namespace, "key": self.key}


@dataclass(frozen=True, slots=True)
class ProviderContract:
    adapter_id: str
    version: str
    digest: str
    completion_criterion: str
    dispatch_callable: str
    reconcile_callable: str | None
    lookup: ProviderLookup

    def to_dict(self):
        return {
            name: self.lookup.value if name == "lookup" else getattr(self, name)
            for name in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class ProviderTarget:
    provider: str
    environment: str
    account: str
    api_variant: str
    operation: str
    endpoint_identity: str
    dimensions: tuple[tuple[str, str], ...]

    def to_dict(self):
        return {
            name: dict(self.dimensions) if name == "dimensions" else getattr(self, name)
            for name in self.__dataclass_fields__
        }


@dataclass(frozen=True, slots=True)
class ProviderDomain:
    provider: str
    namespace: str
    dimensions: tuple[tuple[str, str], ...]

    def to_dict(self):
        return {
            "provider": self.provider,
            "namespace": self.namespace,
            "dimensions": dict(self.dimensions),
        }


class ProviderHorizonKind(StrEnum):
    UNKNOWN = "unknown"
    MINIMUM_DURATION = "minimum_duration"
    RESOURCE_LIFETIME = "resource_lifetime"


@dataclass(frozen=True, slots=True)
class ProviderHorizon:
    kind: ProviderHorizonKind
    duration_ms: int | None = None

    def to_dict(self):
        return {
            "kind": self.kind.value,
            **(
                {"duration_ms": self.duration_ms}
                if self.kind == ProviderHorizonKind.MINIMUM_DURATION
                else {}
            ),
        }


@dataclass(frozen=True, slots=True)
class ProviderDeduplication:
    domain: ProviderDomain
    key: str
    horizon: ProviderHorizon

    def to_dict(self):
        return {
            "domain": self.domain.to_dict(),
            "key": self.key,
            "horizon": self.horizon.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class ProviderReconciliationPolicy:
    queue: str
    initial_delay_ms: int
    interval_ms: int
    maximum_interval_ms: int
    max_checks: int
    max_elapsed_ms: int
    check_timeout_ms: int

    def to_dict(self):
        return {name: getattr(self, name) for name in self.__dataclass_fields__}


@dataclass(frozen=True, slots=True)
class ProviderAdmissionRequirement:
    resource: str

    def to_dict(self):
        return {"resource": self.resource}


@dataclass(frozen=True, slots=True)
class ProviderBinding:
    business: ProviderBusiness
    contract: ProviderContract
    target: ProviderTarget
    deduplication: ProviderDeduplication | None
    credential_reference: str
    reconciliation: ProviderReconciliationPolicy
    program: EffectProgramDescriptor
    v: int = 1
    admission: ProviderAdmissionRequirement | None = None

    def to_dict(self):
        return {
            "v": self.v,
            **({"admission": self.admission.to_dict()} if self.admission is not None else {}),
            "business": self.business.to_dict(),
            "contract": self.contract.to_dict(),
            "target": self.target.to_dict(),
            "deduplication": self.deduplication.to_dict()
            if self.deduplication
            else None,
            "credential_reference": self.credential_reference,
            "reconciliation": self.reconciliation.to_dict(),
            "program": {
                "program": {
                    "id": self.program.program.id,
                    "version": self.program.program.version,
                },
                "digest": self.program.digest,
                "size": self.program.size,
            },
        }


def _digest(raw):
    if type(raw) is not str or not re.fullmatch(r"sha256:[0-9a-f]{64}", raw):
        raise InputError("invalid provider digest")
    return raw


def _dimensions(raw):
    if type(raw) is not dict or len(raw) > 8:
        raise InputError("provider dimensions must be an object of at most eight pairs")
    return tuple(
        sorted(
            (
                codec.text(key, "dimension key"),
                codec.text(value, "dimension value", 256),
            )
            for key, value in raw.items()
        )
    )


def parse_provider_target(raw):
    codec.fields(raw, set(ProviderTarget.__dataclass_fields__))
    return ProviderTarget(
        codec.text(raw["provider"], "provider"),
        codec.text(raw["environment"], "environment", 64),
        codec.text(raw["account"], "account", 256),
        codec.text(raw["api_variant"], "api_variant"),
        codec.text(raw["operation"], "operation"),
        codec.text(raw["endpoint_identity"], "endpoint_identity", 512),
        _dimensions(raw["dimensions"]),
    )


def parse_provider_binding(raw):
    try:
        return _binding(raw)
    except (ValueError, TypeError, KeyError, UnicodeError) as exc:
        raise InputError("invalid provider binding") from exc


def _binding(raw):
    # Local imports avoid a model dependency cycle: approval owns the binding,
    # while the established descriptor model belongs to effect evidence.
    from .effect_models import EffectProgramDescriptor, _program

    codec.fields(raw, set(ProviderBinding.__dataclass_fields__) - {"admission"}, {"admission"})
    version = codec.integer(raw["v"], "provider binding version", 1, 2)
    admission = None
    if "admission" in raw:
        from .admission_models import name
        codec.fields(raw["admission"], {"resource"})
        admission = ProviderAdmissionRequirement(name(raw["admission"]["resource"]))
    if (version == 2) != (admission is not None):
        raise InputError("invalid provider admission version")
    codec.validate_authoritative(raw, 16 * 1024, max_depth=96)
    codec.validate_authoritative(
        {key: value for key, value in raw.items() if key != "program"},
        12 * 1024,
        max_depth=96,
    )
    business = raw["business"]
    codec.fields(business, {"namespace", "key"})
    business = ProviderBusiness(
        codec.text(business["namespace"], "business namespace"),
        codec.text(business["key"], "business key", 256),
    )
    contract = raw["contract"]
    codec.fields(contract, set(ProviderContract.__dataclass_fields__))
    criterion = codec.text(
        contract["completion_criterion"], "completion criterion", 1024
    )
    if not criterion.strip():
        raise InputError("completion criterion must not be blank")
    dispatch = codec.text(contract["dispatch_callable"], "dispatch callable", 512)
    lookup = ProviderLookup(contract["lookup"])
    reconcile = contract["reconcile_callable"]
    if reconcile is not None:
        codec.text(reconcile, "reconcile callable", 512)
    if (lookup == ProviderLookup.UNAVAILABLE) != (
        reconcile is None
    ) or reconcile == dispatch:
        raise InputError("provider lookup and registered callables disagree")
    contract = ProviderContract(
        codec.text(contract["adapter_id"], "adapter_id"),
        codec.text(contract["version"], "version"),
        _digest(contract["digest"]),
        criterion,
        dispatch,
        reconcile,
        lookup,
    )
    target = parse_provider_target(raw["target"])
    deduplication = raw["deduplication"]
    if deduplication is not None:
        codec.fields(deduplication, {"domain", "key", "horizon"})
        domain = deduplication["domain"]
        codec.fields(domain, {"provider", "namespace", "dimensions"})
        domain = ProviderDomain(
            codec.text(domain["provider"], "domain provider"),
            codec.text(domain["namespace"], "domain namespace"),
            _dimensions(domain["dimensions"]),
        )
        if domain.provider != target.provider:
            raise InputError("deduplication domain belongs to another provider")
        horizon = deduplication["horizon"]
        if type(horizon) is not dict:
            raise InputError("invalid provider horizon")
        kind = ProviderHorizonKind(horizon.get("kind"))
        codec.fields(
            horizon,
            {"kind", "duration_ms"}
            if kind == ProviderHorizonKind.MINIMUM_DURATION
            else {"kind"},
        )
        duration = (
            codec.integer(horizon["duration_ms"], "horizon duration", 1, 31_536_000_000)
            if kind == ProviderHorizonKind.MINIMUM_DURATION
            else None
        )
        deduplication = ProviderDeduplication(
            domain,
            codec.text(deduplication["key"], "provider key", 256),
            ProviderHorizon(kind, duration),
        )
    policy = raw["reconciliation"]
    codec.fields(policy, set(ProviderReconciliationPolicy.__dataclass_fields__))
    initial = codec.integer(
        policy["initial_delay_ms"], "initial delay", 1000, 86_400_000
    )
    interval = codec.integer(policy["interval_ms"], "interval", 1000, 86_400_000)
    maximum = codec.integer(
        policy["maximum_interval_ms"], "maximum interval", interval, 86_400_000
    )
    elapsed = codec.integer(
        policy["max_elapsed_ms"], "reconciliation duration", 60_000, 604_800_000
    )
    if initial >= elapsed:
        raise InputError(
            "first provider check must precede the reconciliation deadline"
        )
    policy = ProviderReconciliationPolicy(
        codec.text(policy["queue"], "probe queue"),
        initial,
        interval,
        maximum,
        codec.integer(policy["max_checks"], "max_checks", 1, 256),
        elapsed,
        codec.integer(policy["check_timeout_ms"], "check timeout", 1000, 30_000),
    )
    descriptor = raw["program"]
    codec.fields(descriptor, {"program", "digest", "size"})
    descriptor = EffectProgramDescriptor(
        _program(descriptor["program"]),
        _digest(descriptor["digest"]),
        codec.integer(descriptor["size"], "size", 1),
    )
    return ProviderBinding(
        business,
        contract,
        target,
        deduplication,
        codec.text(raw["credential_reference"], "credential_reference", 256),
        policy,
        descriptor,
        version,
        admission,
    )
