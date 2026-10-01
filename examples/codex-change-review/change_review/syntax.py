"""Small, dependency-free Python/diff presentation tokens (MIT).

Token text is data, never executable markup. Joining it reproduces the source.
"""
import builtins
import io
import keyword
import tokenize


def python_tokens(source):
    try:
        tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return [{"kind": "plain", "text": source}]
    offsets = [0]
    for line in source.split("\n")[:-1]:
        offsets.append(offsets[-1] + len(line) + 1)
    result = []
    cursor = 0
    for index, token in enumerate(tokens):
        kind = None
        if token.type == tokenize.STRING:
            kind = "string"
        elif token.type == tokenize.COMMENT:
            kind = "comment"
        elif token.type == tokenize.NUMBER:
            kind = "number"
        elif token.type == tokenize.NAME:
            name = token.string
            if name in {"True", "False", "None"}:
                kind = "constant"
            elif keyword.iskeyword(name):
                kind = "keyword"
            elif name.isupper():
                kind = "constant"
            elif hasattr(builtins, name) or name[0].isupper():
                kind = "type"
            elif index + 1 < len(tokens) and tokens[index + 1].string == "(":
                kind = "call"
        if kind is None:
            continue
        start = offsets[token.start[0] - 1] + token.start[1]
        end = offsets[token.end[0] - 1] + token.end[1]
        if start > cursor:
            result.append({"kind": "plain", "text": source[cursor:start]})
        result.append({"kind": kind, "text": source[start:end]})
        cursor = end
    if cursor < len(source):
        result.append({"kind": "plain", "text": source[cursor:]})
    return result


def highlight_snippet(source, *, diff=False):
    if diff:
        rows = []
        for line in source.splitlines(keepends=True):
            if line.startswith(("--- ", "+++ ", "@@", "\\")):
                rows.append({"kind": "meta", "tokens": [{"kind": "comment", "text": line}]})
            elif line.startswith(("+", "-", " ")):
                kind = {"+": "add", "-": "remove", " ": "plain"}[line[0]]
                rows.append({"kind": kind, "tokens": [{"kind": "marker", "text": line[0]}] + python_tokens(line[1:])})
            else:
                rows.append({"kind": "plain", "tokens": [{"kind": "plain", "text": line}]})
        return rows
    rows = []
    row = {"kind": "plain", "tokens": []}
    for token in python_tokens(source):
        for part in token["text"].splitlines(keepends=True):
            row["tokens"].append({"kind": token["kind"], "text": part})
            if part.endswith(("\n", "\r")):
                rows.append(row)
                row = {"kind": "plain", "tokens": []}
    if row["tokens"]:
        rows.append(row)
    return rows
