"""Where the pipeline writes what it produces, and how.

Two rules are enforced here rather than left to each task:

- a run writes under `data/<ds>/`, named by the logical date, so replaying a run
  rewrites the same files instead of piling up new ones;
- every write is atomic — a temporary file then a rename — so a task killed in
  the middle never leaves a half-written JSON behind.
"""

import json
import os
import re
import tempfile
from pathlib import Path

# Default: the project's own data/ folder, which .gitignore excludes.
# BULLETIN_DATA_DIR overrides it, which is how the server will point somewhere else.
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def data_dir() -> Path:
    """
    Returns the root folder the pipeline writes into.

    Returns:
        Path: BULLETIN_DATA_DIR if set, the project's data/ folder otherwise.
    """
    return Path(os.environ.get("BULLETIN_DATA_DIR", DEFAULT_DATA_DIR))


def run_slug(logical_date) -> str:
    """
    Names one run: its date, then its hour.

    The DAG runs twice a day, so a folder per date would have the evening run
    overwrite the morning one. "2026-10-23/07h" keeps both, grouped by day.

    Args:
        logical_date: The run's logical date, timezone-aware.

    Returns:
        str: The relative folder of this run.
    """
    return logical_date.strftime("%Y-%m-%d/%Hh")


def run_dir(slug: str, *parts: str) -> Path:
    """
    Returns (and creates) the folder of one run, optionally a subfolder of it.

    Args:
        slug (str): What run_slug returned, e.g. "2026-10-23/07h".
        *parts (str): Optional subfolders, e.g. "raw".

    Returns:
        Path: The folder, created if needed.
    """
    folder = data_dir().joinpath(slug, *parts)
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def slugify(name: str) -> str:
    """
    Turns a source name into a safe file name.

    "20 minutes High-tech" becomes "20-minutes-high-tech".

    Args:
        name (str): The source name.

    Returns:
        str: A lowercase name made of letters, digits and dashes.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "source"


def write_json(path: Path | str, payload) -> str:
    """
    Writes JSON atomically, keeping accents readable.

    The content goes to a temporary file in the same folder, then replaces the
    target in one operation: readers see either the old file or the new one,
    never a truncated one.

    Args:
        path (Path | str): Where to write.
        payload: Anything json.dump can serialize.

    Returns:
        str: The path written, as a string — that is what travels through XCom.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    handle, temporary = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as file:
            # ensure_ascii=False: "économie" stays readable instead of économie
            json.dump(payload, file, ensure_ascii=False, indent=2)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise

    return str(path)


def write_text(path: Path | str, content: str) -> str:
    """
    Writes text atomically, like write_json but without the JSON layer.

    Args:
        path (Path | str): Where to write.
        content (str): The text to write.

    Returns:
        str: The path written, as a string.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    handle, temporary = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(handle, "w", encoding="utf-8") as file:
            file.write(content)
        os.replace(temporary, path)
    except BaseException:
        Path(temporary).unlink(missing_ok=True)
        raise

    return str(path)


def read_json(path: Path | str):
    """
    Reads back a file written by write_json.

    Args:
        path (Path | str): The file to read.

    Returns:
        The deserialized content.
    """
    return json.loads(Path(path).read_text(encoding="utf-8"))


def read_text(path: Path | str) -> str:
    """
    Reads back a file written by write_text.

    Args:
        path (Path | str): The file to read.

    Returns:
        str: Its content.
    """
    return Path(path).read_text(encoding="utf-8")
