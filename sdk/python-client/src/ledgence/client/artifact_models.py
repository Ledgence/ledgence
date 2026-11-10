"""Bounded execution-artifact identities and immutable metadata."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
import re

from . import codec
from .errors import InputError, LedgenceError, ProtocolError
from .models import Scope, _scope

MAX_BYTES = 64 * 1024 * 1024
CHUNK_BYTES = 256 * 1024
TRANSFER_TIMEOUT = 120.0


def media_type(value):
    codec.text(value, "media_type")
    if not value.isascii() or "/" not in value:
        raise InputError("media_type must contain an ASCII type/subtype")
    return value


def filename(value):
    if value is not None:
        codec.text(value, "filename", 255)
        if value in (".", "..") or "/" in value or "\\" in value:
            raise InputError("filename must be a display basename")
    return value


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    id: str
    digest: str
    size: int
    media_type: str
    schema: str = "ledgence.artifact.v1"

    def __post_init__(self):
        if self.schema != "ledgence.artifact.v1":
            raise InputError("unsupported artifact reference")
        codec.text(self.id, "artifact id")
        if type(self.digest) is not str or re.fullmatch(r"sha256:[0-9a-f]{64}", self.digest) is None:
            raise InputError("invalid artifact digest")
        codec.integer(self.size, "artifact size", 0, MAX_BYTES)
        media_type(self.media_type)
        codec.encode(asdict(self), 1024)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        codec.fields(value, {"schema", "id", "digest", "size", "media_type"})
        return cls(**value)


@dataclass(frozen=True, slots=True)
class ArtifactSelection:
    artifact_id: str
    upload_id: str
    revision: int

    def __post_init__(self):
        codec.text(self.artifact_id, "artifact_id")
        codec.text(self.upload_id, "upload_id")
        codec.integer(self.revision, "revision", 1, (1 << 63) - 1)

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_dict(cls, value):
        codec.fields(value, {"artifact_id", "upload_id", "revision"})
        return cls(**value)


@dataclass(frozen=True, slots=True)
class ArtifactTarget:
    kind: str
    id: str

    def __post_init__(self):
        if type(self.kind) is not str or self.kind not in ("task", "workflow"):
            raise InputError("artifact target must be task or workflow")
        codec.text(self.id, "execution id")

    def to_dict(self):
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ArtifactProducer:
    kind: str
    task_id: str | None = None
    workflow_id: str | None = None
    activation_id: str | None = None
    instance_id: str | None = None
    issuer: str | None = None
    subject: str | None = None


@dataclass(frozen=True, slots=True)
class ArtifactManifest:
    reference: ArtifactRef
    scope: Scope
    root: ArtifactTarget
    producer: ArtifactProducer
    key: str
    filename: str | None
    created_at: int
    accepted_at: int
    first_attempt_id: str | None


class ArtifactUploadStage(StrEnum):
    PREPARED = "prepared"
    RECEIVING = "receiving"
    WRITING = "writing"
    UNCERTAIN = "uncertain"
    READY = "ready"
    EXPIRED_NO_WRITE = "expired_no_write"
    RETIRED = "retired"


class ArtifactRetryGuidance(StrEnum):
    UPLOAD = "upload"
    WAIT = "wait"
    RECONCILE = "reconcile"
    REPLACE = "replace"
    COMPLETE = "complete"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class ArtifactUploadStatus:
    selection: ArtifactSelection
    selected: bool
    stage: ArtifactUploadStage
    prepare_expires_at: int
    ingress_expires_at: int | None
    dispatch_expires_at: int | None
    successor: ArtifactSelection | None
    reference: ArtifactRef | None
    retry: ArtifactRetryGuidance


@dataclass(frozen=True, slots=True)
class ArtifactReplacementReceipt:
    replaced: ArtifactSelection
    replacement: ArtifactSelection
    current: ArtifactUploadStatus


@dataclass(frozen=True, slots=True)
class ArtifactPage:
    items: tuple[ArtifactManifest, ...]
    next_cursor: str | None


class ArtifactUploadUncertain(LedgenceError):
    """No second PUT is issued. Inspect selection, then reconcile or replace explicitly."""
    def __init__(self, selection, cause, *, operation=None):
        super().__init__("artifact upload acceptance is uncertain; inspect and reconcile before retrying",
                         request_id=getattr(cause, "request_id", None))
        self.selection = selection
        self.operation = operation or {"kind": "put_file"}
        self.cause = cause


class ArtifactRecoveryRequired(LedgenceError):
    """An existing generation needs explicit operator recovery before sending bytes."""
    def __init__(self, status):
        super().__init__("artifact upload requires " + status.retry.value)
        self.status = status
        self.selection = status.selection


def parse_manifest(value, scope):
    codec.fields(value, {"reference", "scope", "root", "producer", "key", "filename", "created_at", "accepted_at", "first_attempt_id"})
    codec.encode(value, 4096)
    if _scope(value["scope"]) != scope:
        raise ProtocolError("artifact scope mismatch")
    codec.fields(value["root"], {"kind", "id"})
    root = ArtifactTarget(**value["root"])
    producer = value["producer"]
    if type(producer) is not dict:
        raise InputError("invalid artifact producer")
    kind = producer.get("kind")
    if type(kind) is not str:
        raise InputError("invalid producer kind")
    required = {"task": {"task_id"}, "workflow_activation": {"workflow_id", "activation_id"},
                "verified_actor": {"identity"}, "trusted_instance": {"instance_id"}}.get(kind)
    if required is None:
        raise InputError("unknown artifact producer")
    codec.fields(producer, {"kind"} | required)
    fields = {key: val for key, val in producer.items() if key != "kind"}
    if kind == "verified_actor":
        codec.fields(producer["identity"], {"issuer", "subject"})
        fields = dict(producer["identity"])
    for key, item in fields.items():
        codec.text(item, key, 256 if key == "issuer" else 128)
    created = codec.integer(value["created_at"], "created_at", 1, codec.MAX_TIMESTAMP)
    accepted = codec.integer(value["accepted_at"], "accepted_at", created, codec.MAX_TIMESTAMP)
    if value["first_attempt_id"] is not None:
        codec.text(value["first_attempt_id"], "first_attempt_id")
    return ArtifactManifest(ArtifactRef.from_dict(value["reference"]), scope, root,
                            ArtifactProducer(kind, **fields), codec.text(value["key"], "key"),
                            filename(value["filename"]), created, accepted, value["first_attempt_id"])


def parse_status(value):
    codec.fields(value, {"selection", "selected", "stage", "prepare_expires_at", "ingress_expires_at", "dispatch_expires_at", "successor", "reference", "retry"})
    codec.encode(value, 4096)
    selection = ArtifactSelection.from_dict(value["selection"])
    if type(value["selected"]) is not bool:
        raise InputError("selected must be a boolean")
    try:
        stage = ArtifactUploadStage(value["stage"])
        retry = ArtifactRetryGuidance(value["retry"])
    except (ValueError, TypeError) as exc:
        raise InputError("unknown artifact upload state") from exc
    for field in ("prepare_expires_at", "ingress_expires_at", "dispatch_expires_at"):
        if value[field] is not None:
            codec.integer(value[field], field, 1, codec.MAX_TIMESTAMP)
    if value["prepare_expires_at"] is None:
        raise InputError("missing preparation deadline")
    successor = ArtifactSelection.from_dict(value["successor"]) if value["successor"] is not None else None
    reference = ArtifactRef.from_dict(value["reference"]) if value["reference"] is not None else None
    if successor is not None and (successor.artifact_id != selection.artifact_id or successor.upload_id == selection.upload_id
                                  or successor.revision != selection.revision + 1 or value["selected"]):
        raise InputError("invalid successor selection")
    if reference is not None and (reference.id != selection.artifact_id or not value["selected"] or stage != ArtifactUploadStage.READY):
        raise InputError("invalid accepted artifact reference")
    if ((stage == ArtifactUploadStage.RECEIVING) != (value["ingress_expires_at"] is not None)
            or (value["selected"] and stage == ArtifactUploadStage.READY) != (reference is not None)):
        raise InputError("inconsistent artifact upload state")
    if stage in (ArtifactUploadStage.WRITING, ArtifactUploadStage.UNCERTAIN, ArtifactUploadStage.READY) and value["dispatch_expires_at"] is None:
        raise InputError("missing dispatch provenance")
    allowed = {
        ArtifactUploadStage.PREPARED: {ArtifactRetryGuidance.UPLOAD, ArtifactRetryGuidance.REPLACE},
        ArtifactUploadStage.RECEIVING: {ArtifactRetryGuidance.WAIT, ArtifactRetryGuidance.REPLACE},
        ArtifactUploadStage.WRITING: {ArtifactRetryGuidance.RECONCILE, ArtifactRetryGuidance.REPLACE},
        ArtifactUploadStage.UNCERTAIN: {ArtifactRetryGuidance.RECONCILE, ArtifactRetryGuidance.REPLACE},
        ArtifactUploadStage.READY: {ArtifactRetryGuidance.COMPLETE},
        ArtifactUploadStage.EXPIRED_NO_WRITE: {ArtifactRetryGuidance.REPLACE},
        ArtifactUploadStage.RETIRED: {ArtifactRetryGuidance.UNAVAILABLE},
    }
    if value["selected"] and retry not in allowed[stage]:
        raise InputError("retry guidance disagrees with upload stage")
    if not value["selected"] and retry != ArtifactRetryGuidance.UNAVAILABLE:
        raise InputError("historical selection cannot grant retry permission")
    return ArtifactUploadStatus(selection, value["selected"], stage, value["prepare_expires_at"],
                                value["ingress_expires_at"], value["dispatch_expires_at"], successor, reference, retry)


def parse_upload(value):
    codec.fields(value, {"upload"})
    return parse_status(value["upload"])
