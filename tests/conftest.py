"""Pytest configuration and shared fixtures."""

import subprocess
import time
import pytest
import requests


def _wait_for_minio(url: str, retries: int = 20, delay: float = 1.0) -> bool:
    """Poll the MinIO health endpoint until it responds or retries are exhausted."""
    health_url = f"{url}/minio/health/live"
    for _ in range(retries):
        try:
            r = requests.get(health_url, timeout=3)
            if r.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(delay)
    return False


@pytest.fixture(scope="session")
def minio_endpoint(tmp_path_factory):
    """Start a containerised MinIO instance for the session and return its endpoint URL.

    Skips automatically if Docker is not available or the compose file is absent.
    """
    import shutil
    from pathlib import Path

    repo_root = Path(__file__).parent.parent
    compose_file = repo_root / "docker-compose.yml"

    if not shutil.which("docker"):
        pytest.skip("docker not available")

    if not compose_file.exists():
        pytest.skip("docker-compose.yml not found")

    endpoint = "http://localhost:9000"

    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "up", "-d", "--wait", "minio"],
        check=True,
        capture_output=True,
    )

    if not _wait_for_minio(endpoint):
        subprocess.run(
            ["docker", "compose", "-f", str(compose_file), "down"],
            capture_output=True,
        )
        pytest.fail("MinIO did not become healthy in time")

    yield endpoint

    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down"],
        capture_output=True,
    )
