"""The deliberately small, enforced JSON schema subset for read tools (MIT)."""
import math

from ..workflow import _encode, _freeze, _text, WorkflowError
from ._types import AgentProfileError


def validate_schema(schema):
    try:
        result = _freeze(schema, 8 * 1024)
        properties = 0

        def visit(node, depth):
            nonlocal properties
            if type(node) is not dict or depth > 8:
                raise ValueError
            kind = node.get("type")
            allowed = {"type", "description", "enum"}
            if kind == "object":
                allowed |= {"properties", "required", "additionalProperties"}
            elif kind == "array":
                allowed.add("items")
            elif kind not in ("string", "integer", "number", "boolean"):
                raise ValueError
            if set(node) - allowed:
                raise ValueError
            if "description" in node:
                _text(node["description"], "description", 1024)
            if "enum" in node:
                values = node["enum"]
                if type(values) is not list or not 1 <= len(values) <= 32 or kind in ("object", "array"):
                    raise ValueError
                for value in values:
                    check_type(kind, value)
                encoded = [_encode(value, 8 * 1024) for value in values]
                if len(set(encoded)) != len(values):
                    raise ValueError
            if kind == "object":
                fields = node.get("properties")
                required = node.get("required", [])
                if (type(fields) is not dict or node.get("additionalProperties") is not False
                        or type(required) is not list or any(type(k) is not str for k in required)
                        or len(required) != len(set(required)) or set(required) - set(fields)):
                    raise ValueError
                properties += len(fields)
                if properties > 32:
                    raise ValueError
                for key, child in fields.items():
                    _text(key, "property", 128)
                    visit(child, depth + 1)
            elif kind == "array":
                visit(node.get("items"), depth + 1)

        if type(result) is not dict or result.get("type") != "object":
            raise ValueError
        visit(result, 0)
        return result
    except (ValueError, TypeError, WorkflowError) as exc:
        raise AgentProfileError("tool_schema_unsupported") from exc


def check_type(kind, value):
    if kind == "object": valid = type(value) is dict
    elif kind == "array": valid = type(value) is list
    elif kind == "string": valid = type(value) is str
    elif kind == "boolean": valid = type(value) is bool
    elif kind in ("number", "integer"):
        valid = type(value) in (int, float) and math.isfinite(value)
        if valid and kind == "integer":
            valid = type(value) is int or value.is_integer()
    else: valid = False
    if not valid:
        raise ValueError


def validate_arguments(schema, arguments):
    """Validate final transformed arguments; preserve their numeric representation."""
    try:
        result = _freeze(arguments, 16 * 1024)

        def visit(node, value):
            kind = node["type"]
            check_type(kind, value)
            if "enum" in node and not any(
                (type(value) is bool) == (type(member) is bool) and value == member
                for member in node["enum"]
            ):
                raise ValueError
            if kind == "object":
                if set(value) - set(node["properties"]) or set(node.get("required", [])) - set(value):
                    raise ValueError
                for key, child in value.items():
                    visit(node["properties"][key], child)
            elif kind == "array":
                for child in value:
                    visit(node["items"], child)
        visit(schema, result)
        return result
    except (ValueError, TypeError, KeyError, WorkflowError) as exc:
        raise AgentProfileError("tool_arguments_invalid") from exc


def schema_for_callable(function):
    """Infer the common static read-tool subset; use catalogs for richer schemas."""
    import inspect
    import typing
    try:
        signature = inspect.signature(function)
        hints = typing.get_type_hints(function)
        def annotation_schema(annotation):
            primitives = {str: "string", int: "integer", float: "number", bool: "boolean"}
            if annotation in primitives:
                return {"type": primitives[annotation]}
            if typing.get_origin(annotation) is list:
                args = typing.get_args(annotation)
                if len(args) == 1:
                    return {"type": "array", "items": annotation_schema(args[0])}
            raise ValueError
        properties, required = {}, []
        for name, parameter in signature.parameters.items():
            if parameter.kind not in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY):
                raise ValueError
            properties[name] = annotation_schema(hints.get(name, parameter.annotation))
            if parameter.default is inspect.Parameter.empty:
                required.append(name)
            else:
                # Invalid/non-JSON defaults must fail before a tool can be exposed.
                scalar = {"type": "object", "additionalProperties": False,
                          "properties": {name: properties[name]}, "required": [name]}
                validate_arguments(scalar, {name: parameter.default})
        return validate_schema({"type": "object", "properties": properties,
                                "required": required, "additionalProperties": False})
    except (ValueError, TypeError, NameError, AgentProfileError) as exc:
        raise AgentProfileError("static_tool_signature_unsupported") from exc


def effective_arguments(function, arguments):
    """Bind defaults before journaling the exact final tool request."""
    import inspect
    try:
        schema = schema_for_callable(function)
        supplied = validate_arguments(schema, arguments)
        bound = inspect.signature(function).bind(**supplied)
        bound.apply_defaults()
        return validate_arguments(schema, dict(bound.arguments))
    except (TypeError, ValueError) as exc:
        raise AgentProfileError("tool_arguments_invalid") from exc
