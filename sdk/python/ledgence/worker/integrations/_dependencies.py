"""Exact installed SDK qualification without implicit installation (MIT)."""
import importlib
import importlib.metadata
from pathlib import Path

from ._types import AgentDependencyError


def load_dependencies(dependencies):
    """Validate versions before imports, then bind origins to installed RECORDs."""
    distributions = []
    for name, version, module in dependencies:
        try:
            dist = importlib.metadata.distribution(name)
        except importlib.metadata.PackageNotFoundError as exc:
            raise AgentDependencyError("dependency_missing") from exc
        if dist.version != version or not dist.files:
            raise AgentDependencyError("dependency_version_unsupported")
        distributions.append((dist, module))
    loaded = {}
    for dist, name in distributions:
        try:
            module = importlib.import_module(name)
            origin = Path(module.__file__).resolve(strict=True)
            owned = {Path(dist.locate_file(file)).resolve() for file in dist.files}
            if origin not in owned:
                raise AgentDependencyError("dependency_origin_mismatch")
        except AgentDependencyError:
            raise
        except Exception as exc:
            raise AgentDependencyError("dependency_import_failed") from exc
        loaded[name] = module
    return loaded
