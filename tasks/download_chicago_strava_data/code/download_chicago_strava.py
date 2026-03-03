"""Download Strava Metro export ZIPs from Metroview Data-page links.

This script handles the post-processing step after exports are queued in Metroview:
- download one or more export ZIP URLs,
- validate ZIP integrity,
- extract contents,
- write a manifest of downloaded and extracted files.
"""

from __future__ import annotations

import csv
import os
import re
import zipfile
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

import requests

OUTPUT_DIR = Path("../output")
DOWNLOAD_DIR = OUTPUT_DIR / "downloads"
EXTRACT_DIR = OUTPUT_DIR / "extracted"

URLS = os.getenv("STRAVA_METRO_DOWNLOAD_URLS", "")
URLS_FILE = os.getenv("STRAVA_METRO_URLS_FILE", "")
REQUEST_TIMEOUT = int(os.getenv("STRAVA_REQUEST_TIMEOUT", "300"))

BEARER_TOKEN = os.getenv("STRAVA_METRO_BEARER_TOKEN", "") or os.getenv("STRAVA_BEARER_TOKEN", "")
COOKIE = os.getenv("STRAVA_METRO_COOKIE", "")
USER_AGENT = os.getenv("STRAVA_METRO_USER_AGENT", "crime-in-space/1.0")


def _headers() -> dict[str, str]:
    headers = {"User-Agent": USER_AGENT}
    if BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    if COOKIE:
        headers["Cookie"] = COOKIE
    return headers


def _load_urls() -> list[str]:
    urls: list[str] = []
    if URLS.strip():
        urls.extend([u.strip() for u in URLS.split(",") if u.strip()])

    if URLS_FILE:
        p = Path(URLS_FILE)
        if not p.exists():
            raise ValueError(f"STRAVA_METRO_URLS_FILE does not exist: {p}")
        for line in p.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)

    deduped: list[str] = []
    seen: set[str] = set()
    for u in urls:
        if u not in seen:
            deduped.append(u)
            seen.add(u)

    if not deduped:
        raise ValueError(
            "Provide at least one URL via STRAVA_METRO_DOWNLOAD_URLS or STRAVA_METRO_URLS_FILE."
        )
    return deduped


def _sanitize_filename(name: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._")
    return clean or "strava_metro_export.zip"


def _filename_from_response(url: str, response: requests.Response) -> str:
    cd = response.headers.get("Content-Disposition", "")
    m = re.search(r"filename\*=UTF-8''([^;]+)", cd)
    if m:
        return _sanitize_filename(unquote(m.group(1)))

    m = re.search(r'filename="?([^";]+)"?', cd)
    if m:
        return _sanitize_filename(unquote(m.group(1)))

    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    for key in ("filename", "file", "name"):
        if key in qs and qs[key]:
            return _sanitize_filename(unquote(qs[key][0]))

    basename = Path(parsed.path).name
    if basename:
        return _sanitize_filename(unquote(basename))

    return "strava_metro_export.zip"


def _ensure_zip_suffix(name: str) -> str:
    return name if name.lower().endswith(".zip") else f"{name}.zip"


def _download_file(url: str) -> Path:
    print(f"Downloading: {url}")
    with requests.get(url, headers=_headers(), stream=True, timeout=REQUEST_TIMEOUT, allow_redirects=True) as resp:
        resp.raise_for_status()
        filename = _ensure_zip_suffix(_filename_from_response(url, resp))
        out_path = DOWNLOAD_DIR / filename

        with out_path.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=1_048_576):
                if chunk:
                    f.write(chunk)

    if not zipfile.is_zipfile(out_path):
        raise ValueError(
            f"Downloaded file is not a valid ZIP: {out_path}. Check Metro URL/authentication."
        )

    return out_path


def _extract_zip(zip_path: Path) -> list[Path]:
    target_dir = EXTRACT_DIR / zip_path.stem
    target_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(target_dir)

    extracted = [p for p in target_dir.rglob("*") if p.is_file()]
    print(f"Extracted {len(extracted)} files to {target_dir}")
    return extracted


def _write_manifest(rows: list[dict[str, str]]) -> Path:
    manifest = OUTPUT_DIR / "strava_metro_download_manifest.csv"
    fieldnames = ["source_url", "zip_path", "extracted_path"]

    with manifest.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return manifest


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

    urls = _load_urls()
    manifest_rows: list[dict[str, str]] = []

    for url in urls:
        zip_path = _download_file(url)
        extracted = _extract_zip(zip_path)
        for file_path in extracted:
            manifest_rows.append(
                {
                    "source_url": url,
                    "zip_path": str(zip_path),
                    "extracted_path": str(file_path),
                }
            )

    manifest = _write_manifest(manifest_rows)
    print(f"Saved manifest: {manifest}")


if __name__ == "__main__":
    main()
