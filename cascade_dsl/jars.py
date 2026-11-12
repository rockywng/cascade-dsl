"""Connector jars for local planning and tests: resolve a pipeline's artifacts and fetch them from
Maven Central into a cache, so `cascade plan` / `cascade test` work for any connector without
hand-copying jars into FLINK_HOME/lib (put the result on CASCADE_EXTRA_JARS)."""
from __future__ import annotations

import os
import urllib.request

import zipfile

from .connectors import PATCHES, TESTED_VERSIONS

MAVEN = os.environ.get("CASCADE_MAVEN", "https://repo1.maven.org/maven2")
CACHE = os.environ.get("CASCADE_JAR_CACHE", os.path.join(os.path.expanduser("~"), ".cache", "cascade", "jars"))


class FetchError(Exception):
    pass


def resolve(coord: str, flink_minor: str, versions: dict | None = None) -> tuple[str, str, str]:
    """'group:artifact[:version]' (artifact may contain {flink_minor}) -> (group, artifact, version)."""
    parts = coord.format(flink_minor=flink_minor).split(":")
    group, artifact = parts[0], parts[1]
    version = parts[2] if len(parts) == 3 else ((versions or {}).get(f"{group}:{artifact}")
                                                or TESTED_VERSIONS.get(f"{group}:{artifact}"))
    if not version:
        raise FetchError(f"no version for {group}:{artifact}; set it under deploy.artifacts")
    return group, artifact, version


def fetch(coords, flink_minor: str, versions: dict | None = None, cache: str = CACHE) -> list[str]:
    os.makedirs(cache, exist_ok=True)
    paths = []
    for coord in sorted(set(coords)):
        group, artifact, version = resolve(coord, flink_minor, versions)
        name = f"{artifact}-{version}.jar"
        path = os.path.join(cache, name)
        if not os.path.exists(path):
            url = f"{MAVEN}/{group.replace('.', '/')}/{artifact}/{version}/{name}"
            tmp = path + ".part"
            try:
                with urllib.request.urlopen(url, timeout=300) as r, open(tmp, "wb") as f:
                    while chunk := r.read(1 << 20):
                        f.write(chunk)
            except OSError as e:
                raise FetchError(f"downloading {url}: {e}") from e
            os.replace(tmp, path)
        fixes = PATCHES.get(f"{group}:{artifact}")
        if fixes:
            path = patch(path, fixes)
        paths.append(path)
    return paths


def patch(path: str, resources: list[str]) -> str:
    """A copy of the jar with comment lines removed from the given resource files (connectors.PATCHES)."""
    out = path[:-4] + "-cascade-patched.jar"
    if os.path.exists(out):
        return out
    tmp = out + ".part"
    with zipfile.ZipFile(path) as src, zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as dst:
        for info in src.infolist():
            data = src.read(info.filename)
            if info.filename in resources:
                data = b"".join(line for line in data.splitlines(keepends=True)
                                if not line.lstrip().startswith(b"#"))
            dst.writestr(info, data)
    os.replace(tmp, out)
    return out
