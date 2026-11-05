from pathlib import Path
import hashlib
import json
import os
import shutil
import tempfile
import zipfile


def calculate_checksum(file_path: Path) -> str:
    """Calculate the SHA-256 checksum of a file."""

    sha256 = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def validate_archive(zip_path: Path) -> None:
    """Validate ZIP integrity and prevent unsafe archive paths."""

    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Invalid ZIP archive: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as archive:

        bad_file = archive.testzip()

        if bad_file:
            raise ValueError(
                f"Corrupted file inside archive: {bad_file}"
            )

        for member in archive.infolist():

            member_path = Path(member.filename)

            if member_path.is_absolute() or ".." in member_path.parts:
                raise ValueError(
                    f"Unsafe archive path: {member.filename}"
                )

            # Reject symbolic links inside ZIP archives.
            unix_mode = member.external_attr >> 16

            if (unix_mode & 0o170000) == 0o120000:
                raise ValueError(
                    f"Symbolic links are not allowed: {member.filename}"
                )


def extract_archive(
    zip_path: Path,
    staging_dir: Path
) -> dict:
    """
    Extract one GTFS ZIP archive into a staging directory.

    Skip extraction if the same source archive was already extracted.
    """

    borough = zip_path.stem.replace("google_transit_", "")

    destination = staging_dir / borough

    checksum = calculate_checksum(zip_path)

    marker = destination / ".extraction_metadata.json"

    # Check whether this exact archive was already extracted.
    if destination.exists() and marker.exists():

        metadata = json.loads(marker.read_text())

        if metadata.get("source_checksum") == checksum:

            return {
                "borough": borough,
                "status": "skipped",
                "destination": str(destination),
            }

    validate_archive(zip_path)

    staging_dir.mkdir(parents=True, exist_ok=True)

    # Extract into a temporary directory first.
    with tempfile.TemporaryDirectory(
        dir=staging_dir
    ) as temp_dir:

        temp_path = Path(temp_dir)

        with zipfile.ZipFile(zip_path, "r") as archive:

            archive.extractall(temp_path)

        # Write extraction metadata.
        extracted_files = [
            str(path.relative_to(temp_path))
            for path in temp_path.rglob("*")
            if path.is_file()
        ]

        metadata = {
            "source_file": zip_path.name,
            "source_checksum": checksum,
            "borough": borough,
            "extracted_files": extracted_files,
        }

        (temp_path / ".extraction_metadata.json").write_text(
            json.dumps(metadata, indent=4)
        )

        # Replace previous extraction only after successful extraction.
        backup = staging_dir / f".{borough}_backup"

        if backup.exists():
            shutil.rmtree(backup)

        if destination.exists():
            os.replace(destination, backup)

        try:
            os.replace(temp_path, destination)

        except Exception:
            if backup.exists():
                os.replace(backup, destination)
            raise

        if backup.exists():
            shutil.rmtree(backup)

    return {
        "borough": borough,
        "status": "extracted",
        "destination": str(destination),
    }


def extract_all(
    raw_dir: Path,
    staging_dir: Path
) -> list:
    """Extract all GTFS Static ZIP archives."""

    zip_files = sorted(raw_dir.glob("google_transit_*.zip"))

    if not zip_files:
        raise FileNotFoundError(
            f"No GTFS ZIP archives found in {raw_dir}"
        )

    results = []

    for zip_path in zip_files:

        print(f"\nExtracting: {zip_path.name}")

        result = extract_archive(zip_path, staging_dir)

        results.append(result)

        print(f"Status: {result['status']}")
        print(f"Destination: {result['destination']}")

    return results


if __name__ == "__main__":

    project_root = Path("/app")

    raw_dir = (
        project_root / "data/raw/gtfs_static"
    )

    staging_dir = (
        project_root / "data/staging/gtfs_static"
    )

    results = extract_all(raw_dir, staging_dir)

    print("\n========== EXTRACTION SUMMARY ==========")

    for result in results:
        print(
            f"{result['borough']}: {result['status']}"
        )

    print("\nExtraction completed.")
