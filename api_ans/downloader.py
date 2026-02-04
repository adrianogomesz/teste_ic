# Imports
import requests
import zipfile
from pathlib import Path


def download_zip(url: str, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)

    filename = url.split("/")[-1]
    file_path = dest_dir / filename

    with requests.get(url, stream=True, timeout=30) as response:
        response.raise_for_status()

        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    return file_path


def extract_zip(zip_path: Path, extract_to: Path) -> None:
    extract_to.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_to)


def download_and_extract(url: str, base_dir: Path) -> Path:
    zip_path = download_zip(url, base_dir / "zips")

    extract_dir = base_dir / zip_path.stem
    extract_zip(zip_path, extract_dir)

    return extract_dir


def download_file(url: str, dest_path: Path) -> Path:
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=30) as response:
        response.raise_for_status()

        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    return dest_path

