"""YAML profile loading, validation, and first-run initialization.

Handles reading the user's ``profile.yaml``, validating it against the
``Profile`` model, and bootstrapping a new config directory with the
bundled example profile when the user runs ``jobhaul config init``.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import yaml
from pydantic import ValidationError

from jobhaul.log import get_logger
from jobhaul.models import Profile

logger = get_logger(__name__)

CONFIG_DIR = Path.home() / ".config" / "jobhaul"
DATA_DIR = Path.home() / ".local" / "share" / "jobhaul"
PROFILE_PATH = CONFIG_DIR / "profile.yaml"
EXAMPLE_PROFILE = Path(__file__).resolve().parent.parent.parent / "config" / "profile.example.yaml"


def load_profile(path: Path | None = None) -> Profile:
    """Load and validate the YAML profile.

    Args:
        path: Explicit path to a profile YAML file.  When ``None`` the
            default location (``~/.config/jobhaul/profile.yaml``) is used.

    Returns:
        A fully validated ``Profile`` instance.

    Raises:
        FileNotFoundError: If the profile file does not exist.
        ValueError: If the YAML content is not a mapping or fails
            Pydantic validation.
    """
    profile_path = path or PROFILE_PATH

    if not profile_path.exists():
        raise FileNotFoundError(
            f"Profile not found at {profile_path}. "
            f"Run 'jobhaul config init' to create one."
        )

    with open(profile_path) as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Invalid profile: expected a YAML mapping, got {type(data).__name__}")

    try:
        return Profile(**data)
    except ValidationError as e:
        raise ValueError(f"Invalid profile at {profile_path}: {e}") from e


def init_profile(target: Path | None = None) -> Path:
    """Copy the bundled example profile into the user's config directory.

    Args:
        target: Destination path for the new profile.  Defaults to
            ``~/.config/jobhaul/profile.yaml``.

    Returns:
        The ``Path`` where the profile was written.

    Raises:
        FileExistsError: If a profile already exists at the target path.
    """
    target = target or PROFILE_PATH
    target.parent.mkdir(parents=True, exist_ok=True)

    if target.exists():
        raise FileExistsError(f"Profile already exists at {target}")

    shutil.copy2(EXAMPLE_PROFILE, target)
    logger.info("Created profile at %s", target)
    return target


def ensure_data_dir() -> Path:
    """Create the data directory if it does not exist and return its path.

    The data directory (``~/.local/share/jobhaul``) is where the SQLite
    database and other runtime artifacts are stored.

    Returns:
        The ``Path`` to the data directory.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR
