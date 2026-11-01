from pathlib import Path
import yaml

CONFIG_PATH = Path("/app/config/settings.yaml")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)