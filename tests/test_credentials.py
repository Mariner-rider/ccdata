from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")


def _walk_values(value):
    if isinstance(value, dict):
        for nested in value.values():
            yield from _walk_values(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_values(nested)
    else:
        yield value


def test_compose_has_no_known_hardcoded_credentials():
    forbidden = {"minio123", "admin", "change-me"}
    for file_name in ("docker-compose.production.yml", "docker-compose.local-lite.yml"):
        data = yaml.safe_load(Path(file_name).read_text())
        for value in _walk_values(data):
            assert value not in forbidden


def test_service_api_key_is_required_env_interpolation():
    data = yaml.safe_load(Path("docker-compose.production.yml").read_text())
    crawler_env = data["services"]["core-api"]["environment"]
    assert crawler_env["SERVICE_API_KEY"].startswith("${")
