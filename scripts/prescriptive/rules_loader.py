from pathlib import Path
import yaml

_RULES = None


def _find_rules_yaml(start: Path) -> Path:
    current = start
    while current != current.parent:
        candidate = current / "rules.yaml"
        if candidate.exists():
            return candidate
        current = current.parent
    raise FileNotFoundError("rules.yaml not found in any parent directory")


def load_rules():
    global _RULES
    if _RULES is None:
        here = Path(__file__).resolve()
        path = _find_rules_yaml(here)
        with open(path, "r") as f:
            _RULES = yaml.safe_load(f)
    return _RULES
