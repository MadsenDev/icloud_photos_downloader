"""Handles file downloads with retries and error handling"""

import base64
import datetime
import hashlib
import logging
import os
import shutil
import time
from functools import partial
from typing import Callable

from requests import Response
from tzlocal import get_localzone

from icloudpd import constants
from icloudpd.limiter import AdaptiveDownloadLimiter
from icloudpd.metrics import RunMetrics
from icloudpd.retry_utils import (
    RetryConfig,
    is_session_invalid_error,
    is_throttle_error,
    is_transient_error,
)
from pyicloud_ipd.asset_version import AssetVersion, calculate_version_filename
from pyicloud_ipd.base import PyiCloudService
from pyicloud_ipd.exceptions import PyiCloudAPIResponseException
from pyicloud_ipd.services.photos import PhotoAsset
from pyicloud_ipd.version_size import VersionSize

_RETRY_CONFIG: RetryConfig | None = None
_DOWNLOAD_CHUNK_BYTES: int = 262144
_VERIFY_SIZE: bool = True
_VERIFY_CHECKSUM: bool = False
_DOWNLOAD_LIMITER: AdaptiveDownloadLimiter | None = None
_METRICS: RunMetrics | None = None
_URL_REFRESH_NEEDED: bool = False


def set_retry_config(retry_config: RetryConfig) -> None:
    global _RETRY_CONFIG
    _RETRY_CONFIG = retry_config


def set_download_chunk_bytes(chunk_bytes: int) -> None:
    global _DOWNLOAD_CHUNK_BYTES
    _DOWNLOAD_CHUNK_BYTES = chunk_bytes


def get_download_chunk_bytes() -> int:
    return _DOWNLOAD_CHUNK_BYTES


def set_download_limiter(limiter: AdaptiveDownloadLimiter | None) -> None:
    global _DOWNLOAD_LIMITER
    _DOWNLOAD_LIMITER = limiter


def get_download_limiter() -> AdaptiveDownloadLimiter | None:
    return _DOWNLOAD_LIMITER


def set_metrics_collector(metrics: RunMetrics | None) -> None:
    global _METRICS
    _METRICS = metrics


def get_metrics_collector() -> RunMetrics | None:
    return _METRICS


def consume_url_refresh_needed_signal() -> bool:
    global _URL_REFRESH_NEEDED
    value = _URL_REFRESH_NEEDED
    _URL_REFRESH_NEEDED = False
    return value


def set_download_verification(*, verify_size: bool, verify_checksum: bool) -> None:
    global _VERIFY_SIZE
    global _VERIFY_CHECKSUM
    _VERIFY_SIZE = verify_size
    _VERIFY_CHECKSUM = verify_checksum


def get_download_verification() -> tuple[bool, bool]:
    return (_VERIFY_SIZE, _VERIFY_CHECKSUM)


def get_retry_config() -> RetryConfig:
    if _RETRY_CONFIG is not None:
        return _RETRY_CONFIG
    return RetryConfig(
        max_retries=constants.MAX_RETRIES,
        backoff_base_seconds=float(constants.WAIT_SECONDS),
        backoff_max_seconds=300.0,
        respect_retry_after=True,
        throttle_cooldown_seconds=60.0,
    )


def update_mtime(created: datetime.datetime, download_path: str) -> None:
    """Set the modification time of the downloaded file to the photo creation date"""
    if created:
        created_date = None
        try:
            created_date = created.astimezone(get_localzone())
        except (ValueError, OSError):
            # We already show the timezone conversion error in base.py,
            # when generating the download directory.
            # So just return silently without touching the mtime.
            return
        set_utime(download_path, created_date)


def set_utime(download_path: str, created_date: datetime.datetime) -> None:
    """Set date & time of the file"""
    try:
        ctime = time.mktime(created_date.timetuple())
    except OverflowError:
        ctime = time.mktime(datetime.datetime(1970, 1, 1, 0, 0, 0).timetuple())
    os.utime(download_path, (ctime, ctime))


def mkdirs_for_path(logger: logging.Logger, download_path: str) -> bool:
    """Creates hierarchy of folders for file path if it needed"""
    try:
        # get back the directory for the file to be downloaded and create it if
        # not there already
        download_dir = os.path.dirname(download_path)
        os.makedirs(name=download_dir, exist_ok=True)
        return True
    except OSError:
        logger.error(
            "Could not create folder %s",
            download_dir,
        )
        return False


def mkdirs_for_path_dry_run(logger: logging.Logger, download_path: str) -> bool:
    """DRY Run for Creating hierarchy of folders for file path"""
    download_dir = os.path.dirname(download_path)
    if not os.path.exists(download_dir):
        logger.debug(
            "[DRY RUN] Would create folder hierarchy %s",
            download_dir,
        )
    return True


def download_response_to_path(
    response: Response,
    temp_download_path: str,
    append_mode: bool,
    download_path: str,
    created_date: datetime.datetime,
) -> bool:
    """Saves response content into file with desired created date"""
    with open(temp_download_path, ("ab" if append_mode else "wb")) as file_obj:
        for chunk in response.iter_content(chunk_size=get_download_chunk_bytes()):
            if chunk:
                file_obj.write(chunk)
    os.rename(temp_download_path, download_path)
    update_mtime(created_date, download_path)
    return True


def _calculate_digest(path: str, hash_name: str) -> bytes:
    hash_obj = hashlib.new(hash_name)
    with open(path, "rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            hash_obj.update(chunk)
    return hash_obj.digest()


def _matches_checksum(path: str, expected_checksum: bytes) -> bool:
    digest_length_to_algorithm = {
        16: "md5",
        20: "sha1",
        32: "sha256",
        48: "sha384",
        64: "sha512",
    }
    preferred_algorithm = digest_length_to_algorithm.get(len(expected_checksum))
    if preferred_algorithm is not None:
        return _calculate_digest(path, preferred_algorithm) == expected_checksum

    for hash_name in ["md5", "sha1", "sha256", "sha384", "sha512"]:
        if _calculate_digest(path, hash_name) == expected_checksum:
            return True
    return False


def verify_download_integrity(
    logger: logging.Logger,
    download_path: str,
    *,
    expected_size: int,
    expected_checksum: bytes,
    verify_size: bool,
    verify_checksum: bool,
) -> bool:
    if verify_size:
        actual_size = os.path.getsize(download_path)
        if actual_size != expected_size:
            logger.error(
                "File size mismatch for %s (expected %d bytes, got %d)",
                download_path,
                expected_size,
                actual_size,
            )
            return False

    if verify_checksum and not _matches_checksum(download_path, expected_checksum):
        logger.error("Checksum mismatch for %s", download_path)
        return False

    return True


def download_response_to_path_dry_run(
    logger: logging.Logger,
    _response: Response,
    _temp_download_path: str,
    _append_mode: bool,
    download_path: str,
    _created_date: datetime.datetime,
) -> bool:
    """Pretends to save response content into a file with desired created date"""
    logger.info(
        "[DRY RUN] Would download %s",
        download_path,
    )
    return True


def download_media(
    logger: logging.Logger,
    dry_run: bool,
    icloud: PyiCloudService,
    photo: PhotoAsset,
    download_path: str,
    version: AssetVersion,
    size: VersionSize,
    filename_builder: Callable[[PhotoAsset], str],
    refresh_version: Callable[[], AssetVersion | None] | None = None,
) -> bool:
    """Download the photo to path, with retries and error handling"""
    retry_config = get_retry_config()
    limiter = get_download_limiter()
    metrics = get_metrics_collector()

    mkdirs_local = mkdirs_for_path_dry_run if dry_run else mkdirs_for_path
    if not mkdirs_local(logger, download_path):
        return False

    checksum = base64.b64decode(version.checksum)
    checksum32 = base64.b32encode(checksum).decode()
    download_dir = os.path.dirname(download_path)
    temp_download_path = os.path.join(download_dir, checksum32) + ".part"

    download_local = (
        partial(download_response_to_path_dry_run, logger) if dry_run else download_response_to_path
    )

    retries = 0
    exhausted_retries = False
    download_failed = False
    refreshed_url_once = False
    range_restart_attempted = False
    global _URL_REFRESH_NEEDED
    _URL_REFRESH_NEEDED = False
    verify_size, verify_checksum = get_download_verification()
    while True:
        retry_after_header: str | None = None
        try:
            append_mode = os.path.exists(temp_download_path)
            current_size = os.path.getsize(temp_download_path) if append_mode else 0
            if append_mode:
                logger.debug(f"Resuming downloading of {download_path} from {current_size}")
            if (
                not dry_run
                and version.size is not None
                and version.size > 0
                and not append_mode
                and not has_disk_space_for_download(download_path, version.size)
            ):
                logger.error(
                    "Low disk space for %s (required: %d bytes). Skipping download.",
                    download_path,
                    version.size,
                )
                if metrics is not None:
                    metrics.on_low_disk()
                    metrics.on_download_failed()
                return False

            if metrics is not None:
                metrics.on_download_attempt()
            if limiter is None:
                photo_response = photo.download(icloud.photos.session, version.url, current_size)
            else:
                with limiter.slot():
                    photo_response = photo.download(icloud.photos.session, version.url, current_size)
            if photo_response.ok:
                if append_mode and photo_response.status_code != 206:
                    logger.warning(
                        "Range resume unsupported for %s (HTTP %d). Restarting partial download.",
                        download_path,
                        photo_response.status_code,
                    )
                    append_mode = False

                saved = download_local(
                    photo_response, temp_download_path, append_mode, download_path, photo.created
                )
                if not saved:
                    return False

                if dry_run:
                    return True

                if not verify_download_integrity(
                    logger,
                    download_path,
                    expected_size=version.size,
                    expected_checksum=checksum,
                    verify_size=verify_size,
                    verify_checksum=verify_checksum,
                ):
                    try:
                        os.remove(download_path)
                    except OSError:
                        logger.error("Could not remove failed download %s", download_path)
                    return False

                if limiter is not None:
                    limiter.on_success()
                if metrics is not None:
                    try:
                        metrics.on_download_success(os.path.getsize(download_path))
                    except OSError:
                        metrics.on_download_success(0)
                return True
            else:
                status_code = str(photo_response.status_code)
                if append_mode and status_code == "416":
                    logger.warning(
                        "Range resume rejected for %s (HTTP 416). Restarting partial download.",
                        download_path,
                    )
                    if range_restart_attempted:
                        break
                    range_restart_attempted = True
                    try:
                        os.remove(temp_download_path)
                    except OSError:
                        logger.error("Could not remove stale partial download %s", temp_download_path)
                        break
                    continue
                if status_code in {"401", "403", "410"}:
                    if refresh_version is not None and not refreshed_url_once:
                        logger.info(
                            "Download URL may be expired for %s. Refreshing asset metadata and retrying once.",
                            filename_builder(photo),
                        )
                        try:
                            refreshed = refresh_version()
                        except Exception:
                            refreshed = None
                        if refreshed is not None and refreshed.url and refreshed.url != version.url:
                            version = refreshed
                            refreshed_url_once = True
                            continue
                    _URL_REFRESH_NEEDED = True
                    raise PyiCloudAPIResponseException(
                        f"Download URL expired or denied (HTTP {status_code})",
                        status_code,
                    )
                if status_code in {"429", "500", "502", "503", "504"}:
                    retry_after_header = photo_response.headers.get("Retry-After")
                    if status_code == "429" and limiter is not None:
                        limiter.on_throttle()
                    if status_code == "429" and metrics is not None:
                        metrics.on_throttle()
                    raise PyiCloudAPIResponseException(
                        f"Download request failed with HTTP {status_code}",
                        status_code,
                    )
                # Use the standard original filename generator for error logging
                from icloudpd.base import lp_filename_original as simple_lp_filename_generator

                # Get the proper filename using filename_builder
                base_filename = filename_builder(photo)
                version_filename = calculate_version_filename(
                    base_filename, version, size, simple_lp_filename_generator, photo.item_type
                )
                logger.error(
                    "Could not find URL to download %s for size %s",
                    version_filename,
                    size.value,
                )
                break

        except PyiCloudAPIResponseException as ex:
            download_failed = True
            if is_session_invalid_error(ex):
                logger.error("Session error, re-authenticating...")
                icloud.authenticate()
            else:
                if limiter is not None and is_throttle_error(ex):
                    limiter.on_throttle()
                if not is_transient_error(ex):
                    break
            # short circuiting 0 retries
            if retries >= retry_config.max_retries:
                exhausted_retries = True
                break

            retries += 1
            if metrics is not None:
                metrics.on_retry()
            wait_time = retry_config.next_delay_seconds(
                retries,
                retry_after=retry_after_header,
                throttle_error=is_throttle_error(ex),
            )
            error_filename = filename_builder(photo)
            logger.error(
                "Error downloading %s, retrying after %.1f seconds... (%d/%d)",
                error_filename,
                wait_time,
                retries,
                retry_config.max_retries,
            )
            time.sleep(wait_time)
            continue

        except OSError:
            download_failed = True
            logger.error(
                "IOError while writing file to %s. "
                + "You might have run out of disk space, or the file "
                + "might be too large for your OS. "
                + "Skipping this file...",
                download_path,
            )
            break

    if exhausted_retries or download_failed:
        if metrics is not None:
            metrics.on_download_failed()
        # Get the proper filename for error messages
        error_filename = filename_builder(photo)
        logger.error(
            "Could not download %s. Please try again later.",
            error_filename,
        )

    return False


def has_disk_space_for_download(download_path: str, required_bytes: int, reserve_bytes: int = 50 * 1024 * 1024) -> bool:
    target_dir = os.path.dirname(download_path) or "."
    try:
        free_bytes = shutil.disk_usage(target_dir).free
    except OSError:
        return True
    return free_bytes >= (required_bytes + reserve_bytes)
