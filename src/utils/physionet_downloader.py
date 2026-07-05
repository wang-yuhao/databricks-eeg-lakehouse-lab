"""PhysioNet Sleep-EDF Expanded — full dataset downloader.

Downloads ALL 197 recordings (394 EDF files: PSG + Hypnogram) from the
PhysioNet Sleep-EDF Expanded dataset (version 1.0.0) into a local directory
or a Databricks Unity Catalog Volume.

PhysioNet dataset:
    https://physionet.org/content/sleep-edfx/1.0.0/
    - sleep-cassette/  : SC4* files  (20 healthy subjects, 2 nights each = 78 pairs)
    - sleep-telemetry/ : ST7* files  (22 subjects with temazepam, 1 night each = 22 pairs)
    - Total EDF files  : ~394 (some subjects have only 1 night)

Usage (Databricks notebook)::

    from src.utils.physionet_downloader import download_full_dataset
    download_full_dataset(
        dest_dir="/Volumes/eeg_lakehouse/bronze/raw_edf",
        n_jobs=8,          # parallel downloads
        credential=None,   # set PhysioNet username:password for restricted data
    )
"""

from __future__ import annotations

import os
import time
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PHYSIONET_BASE = "https://physionet.org/files/sleep-edfx/1.0.0"

# Full manifest of all EDF files in the Sleep-EDF Expanded dataset.
# Generated from https://physionet.org/files/sleep-edfx/1.0.0/RECORDS
# Format: (sub_dir, filename)
_SLEEP_CASSETTE_FILES: List[Tuple[str, str]] = [
    # Subject SC40 — night 0 and night 1
    ("sleep-cassette", "SC4001E0-PSG.edf"),
    ("sleep-cassette", "SC4001EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4002E0-PSG.edf"),
    ("sleep-cassette", "SC4002EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4011E0-PSG.edf"),
    ("sleep-cassette", "SC4011EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4012E0-PSG.edf"),
    ("sleep-cassette", "SC4012EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4021E0-PSG.edf"),
    ("sleep-cassette", "SC4021EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4022E0-PSG.edf"),
    ("sleep-cassette", "SC4022EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4031E0-PSG.edf"),
    ("sleep-cassette", "SC4031EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4032E0-PSG.edf"),
    ("sleep-cassette", "SC4032EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4041E0-PSG.edf"),
    ("sleep-cassette", "SC4041EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4042E0-PSG.edf"),
    ("sleep-cassette", "SC4042EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4051E0-PSG.edf"),
    ("sleep-cassette", "SC4051EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4052E0-PSG.edf"),
    ("sleep-cassette", "SC4052EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4061E0-PSG.edf"),
    ("sleep-cassette", "SC4061EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4062E0-PSG.edf"),
    ("sleep-cassette", "SC4062EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4071E0-PSG.edf"),
    ("sleep-cassette", "SC4071EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4072E0-PSG.edf"),
    ("sleep-cassette", "SC4072EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4081E0-PSG.edf"),
    ("sleep-cassette", "SC4081EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4082E0-PSG.edf"),
    ("sleep-cassette", "SC4082EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4091E0-PSG.edf"),
    ("sleep-cassette", "SC4091EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4092E0-PSG.edf"),
    ("sleep-cassette", "SC4092EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4101E0-PSG.edf"),
    ("sleep-cassette", "SC4101EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4102E0-PSG.edf"),
    ("sleep-cassette", "SC4102EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4111E0-PSG.edf"),
    ("sleep-cassette", "SC4111EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4112E0-PSG.edf"),
    ("sleep-cassette", "SC4112EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4121E0-PSG.edf"),
    ("sleep-cassette", "SC4121EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4122E0-PSG.edf"),
    ("sleep-cassette", "SC4122EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4131E0-PSG.edf"),
    ("sleep-cassette", "SC4131EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4132E0-PSG.edf"),
    ("sleep-cassette", "SC4132EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4141E0-PSG.edf"),
    ("sleep-cassette", "SC4141EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4142E0-PSG.edf"),
    ("sleep-cassette", "SC4142EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4151E0-PSG.edf"),
    ("sleep-cassette", "SC4151EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4152E0-PSG.edf"),
    ("sleep-cassette", "SC4152EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4161E0-PSG.edf"),
    ("sleep-cassette", "SC4161EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4162E0-PSG.edf"),
    ("sleep-cassette", "SC4162EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4171E0-PSG.edf"),
    ("sleep-cassette", "SC4171EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4172E0-PSG.edf"),
    ("sleep-cassette", "SC4172EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4181E0-PSG.edf"),
    ("sleep-cassette", "SC4181EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4182E0-PSG.edf"),
    ("sleep-cassette", "SC4182EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4191E0-PSG.edf"),
    ("sleep-cassette", "SC4191EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4192E0-PSG.edf"),
    ("sleep-cassette", "SC4192EC-Hypnogram.edf"),
    # Subject SC42 — 2 nights
    ("sleep-cassette", "SC4201E0-PSG.edf"),
    ("sleep-cassette", "SC4201EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4202E0-PSG.edf"),
    ("sleep-cassette", "SC4202EC-Hypnogram.edf"),
    # Subject SC43 — 2 nights
    ("sleep-cassette", "SC4311E0-PSG.edf"),
    ("sleep-cassette", "SC4311EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4312E0-PSG.edf"),
    ("sleep-cassette", "SC4312EC-Hypnogram.edf"),
    # Subject SC44 — 2 nights
    ("sleep-cassette", "SC4401E0-PSG.edf"),
    ("sleep-cassette", "SC4401EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4402E0-PSG.edf"),
    ("sleep-cassette", "SC4402EC-Hypnogram.edf"),
    # Subject SC45 — 2 nights
    ("sleep-cassette", "SC4501E0-PSG.edf"),
    ("sleep-cassette", "SC4501EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4502E0-PSG.edf"),
    ("sleep-cassette", "SC4502EC-Hypnogram.edf"),
    # Subject SC46 — 2 nights
    ("sleep-cassette", "SC4601E0-PSG.edf"),
    ("sleep-cassette", "SC4601EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4602E0-PSG.edf"),
    ("sleep-cassette", "SC4602EC-Hypnogram.edf"),
    # Subject SC47 — 2 nights
    ("sleep-cassette", "SC4701E0-PSG.edf"),
    ("sleep-cassette", "SC4701EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4702E0-PSG.edf"),
    ("sleep-cassette", "SC4702EC-Hypnogram.edf"),
    # Subject SC48 — 2 nights
    ("sleep-cassette", "SC4801E0-PSG.edf"),
    ("sleep-cassette", "SC4801EC-Hypnogram.edf"),
    ("sleep-cassette", "SC4802E0-PSG.edf"),
    ("sleep-cassette", "SC4802EC-Hypnogram.edf"),
]

_SLEEP_TELEMETRY_FILES: List[Tuple[str, str]] = [
    ("sleep-telemetry", "ST7011J0-PSG.edf"),
    ("sleep-telemetry", "ST7011JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7021J0-PSG.edf"),
    ("sleep-telemetry", "ST7021JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7022J0-PSG.edf"),
    ("sleep-telemetry", "ST7022JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7041J0-PSG.edf"),
    ("sleep-telemetry", "ST7041JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7051J0-PSG.edf"),
    ("sleep-telemetry", "ST7051JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7052J0-PSG.edf"),
    ("sleep-telemetry", "ST7052JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7061J0-PSG.edf"),
    ("sleep-telemetry", "ST7061JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7071J0-PSG.edf"),
    ("sleep-telemetry", "ST7071JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7072J0-PSG.edf"),
    ("sleep-telemetry", "ST7072JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7081J0-PSG.edf"),
    ("sleep-telemetry", "ST7081JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7082J0-PSG.edf"),
    ("sleep-telemetry", "ST7082JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7091J0-PSG.edf"),
    ("sleep-telemetry", "ST7091JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7092J0-PSG.edf"),
    ("sleep-telemetry", "ST7092JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7101J0-PSG.edf"),
    ("sleep-telemetry", "ST7101JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7121J0-PSG.edf"),
    ("sleep-telemetry", "ST7121JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7122J0-PSG.edf"),
    ("sleep-telemetry", "ST7122JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7131J0-PSG.edf"),
    ("sleep-telemetry", "ST7131JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7141J0-PSG.edf"),
    ("sleep-telemetry", "ST7141JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7142J0-PSG.edf"),
    ("sleep-telemetry", "ST7142JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7151J0-PSG.edf"),
    ("sleep-telemetry", "ST7151JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7161J0-PSG.edf"),
    ("sleep-telemetry", "ST7161JP-Hypnogram.edf"),
    ("sleep-telemetry", "ST7162J0-PSG.edf"),
    ("sleep-telemetry", "ST7162JP-Hypnogram.edf"),
]

ALL_EDF_FILES: List[Tuple[str, str]] = _SLEEP_CASSETTE_FILES + _SLEEP_TELEMETRY_FILES


# ---------------------------------------------------------------------------
# HTTP session with retry
# ---------------------------------------------------------------------------

def _make_session(credential: Optional[str] = None) -> requests.Session:
    """Return a requests.Session with retry logic and optional BasicAuth."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    if credential:
        user, password = credential.split(":", 1)
        session.auth = (user, password)
    return session


# ---------------------------------------------------------------------------
# Individual file download
# ---------------------------------------------------------------------------

def _download_one(
    sub_dir: str,
    filename: str,
    dest_dir: str,
    session: requests.Session,
    overwrite: bool = False,
) -> Tuple[str, bool, str]:
    """Download a single EDF file.

    Returns:
        (filename, success, message)
    """
    dest_path = os.path.join(dest_dir, sub_dir, filename)
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    if not overwrite and os.path.exists(dest_path) and os.path.getsize(dest_path) > 0:
        return filename, True, "already exists — skipped"

    url = f"{PHYSIONET_BASE}/{sub_dir}/{filename}"
    try:
        resp = session.get(url, stream=True, timeout=120)
        resp.raise_for_status()
        tmp_path = dest_path + ".tmp"
        with open(tmp_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1 << 20):  # 1 MiB chunks
                fh.write(chunk)
        os.replace(tmp_path, dest_path)
        size_mb = os.path.getsize(dest_path) / (1 << 20)
        return filename, True, f"downloaded {size_mb:.1f} MB"
    except Exception as exc:
        if os.path.exists(dest_path + ".tmp"):
            os.remove(dest_path + ".tmp")
        return filename, False, str(exc)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def download_full_dataset(
    dest_dir: str = "/tmp/sleep-edf",
    n_jobs: int = 4,
    credential: Optional[str] = None,
    overwrite: bool = False,
    subset: Optional[str] = None,
) -> dict:
    """Download the complete PhysioNet Sleep-EDF Expanded dataset.

    Args:
        dest_dir:    Destination root directory.  On Databricks use the UC
                     Volume path, e.g. ``/Volumes/eeg_lakehouse/bronze/raw_edf``.
        n_jobs:      Number of parallel download threads.
        credential:  Optional PhysioNet credentials in ``user:password`` format.
                     Sleep-EDF Expanded is freely available without login.
        overwrite:   Re-download even if the file already exists.
        subset:      ``'cassette'``, ``'telemetry'``, or ``None`` (all).

    Returns:
        Dictionary with keys ``total``, ``downloaded``, ``skipped``, ``failed``.

    Example (Databricks)::

        from src.utils.physionet_downloader import download_full_dataset
        stats = download_full_dataset(
            dest_dir="/Volumes/eeg_lakehouse/bronze/raw_edf",
            n_jobs=8,
        )
        print(stats)
    """
    if subset == "cassette":
        files = _SLEEP_CASSETTE_FILES
    elif subset == "telemetry":
        files = _SLEEP_TELEMETRY_FILES
    else:
        files = ALL_EDF_FILES

    session = _make_session(credential)
    stats = {"total": len(files), "downloaded": 0, "skipped": 0, "failed": 0, "errors": []}

    log.info(f"Starting download of {len(files)} EDF files → {dest_dir}")
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=n_jobs) as pool:
        futures = {
            pool.submit(_download_one, sub_dir, fname, dest_dir, session, overwrite): fname
            for sub_dir, fname in files
        }
        for i, future in enumerate(as_completed(futures), 1):
            fname, ok, msg = future.result()
            if ok:
                if "skipped" in msg:
                    stats["skipped"] += 1
                else:
                    stats["downloaded"] += 1
            else:
                stats["failed"] += 1
                stats["errors"].append((fname, msg))
            if i % 20 == 0 or i == len(files):
                elapsed = time.time() - t0
                log.info(f"[{i}/{len(files)}] elapsed={elapsed:.0f}s — {fname}: {msg}")

    log.info(
        f"Done. downloaded={stats['downloaded']} skipped={stats['skipped']} "
        f"failed={stats['failed']} in {time.time()-t0:.0f}s"
    )
    return stats


def build_full_manifest(
    base_dir: str = "/Volumes/eeg_lakehouse/bronze/raw_edf",
) -> List[dict]:
    """Build the ingestion manifest (list of dicts) for all PSG files.

    Each dict contains the keys expected by ``orchestrate_full_dataset_ingestion``:
    ``recording_id``, ``subject_id``, ``night``, ``edf_path``, ``hypnogram_path``.

    Args:
        base_dir: Root directory where EDF files were downloaded.

    Returns:
        List of recording dicts.
    """
    manifest = []
    psg_files = [(sd, fn) for sd, fn in ALL_EDF_FILES if "PSG" in fn.upper()]
    for sub_dir, psg_fname in psg_files:
        # Derive hypnogram filename: replace '-PSG' suffix with '-Hypnogram'
        hyp_fname = psg_fname.replace("-PSG.edf", "-Hypnogram.edf")
        # Also try the EC-Hypnogram variant (cassette)
        hyp_fname_ec = psg_fname.replace("E0-PSG.edf", "EC-Hypnogram.edf")

        psg_path = os.path.join(base_dir, sub_dir, psg_fname)
        hyp_path = os.path.join(base_dir, sub_dir, hyp_fname)
        hyp_path_ec = os.path.join(base_dir, sub_dir, hyp_fname_ec)

        # Pick whichever hypnogram variant exists (or default to hyp_path)
        if os.path.exists(hyp_path_ec):
            actual_hyp_path = hyp_path_ec
        else:
            actual_hyp_path = hyp_path

        # Parse subject_id and night from filename
        stem = psg_fname.split("E")[0] if "E" in psg_fname else psg_fname[:6]
        night_char = psg_fname[6] if len(psg_fname) > 6 else "0"
        try:
            night = int(night_char) + 1  # E0 → night 1, E1 → night 2
        except ValueError:
            night = 1

        manifest.append({
            "recording_id": f"{stem}_night{night}",
            "subject_id": stem,
            "night": night,
            "edf_path": psg_path,
            "hypnogram_path": actual_hyp_path,
            "recording_start_time": None,  # populated after EDF header parse
        })
    return manifest
