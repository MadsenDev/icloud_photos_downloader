import datetime
import pathlib
from dataclasses import dataclass
from typing import Sequence

from icloudpd.log_level import LogLevel
from icloudpd.mfa_provider import MFAProvider
from icloudpd.password_provider import PasswordProvider
from pyicloud_ipd.file_match import FileMatchPolicy
from pyicloud_ipd.live_photo_mov_filename_policy import LivePhotoMovFilenamePolicy
from pyicloud_ipd.raw_policy import RawTreatmentPolicy
from pyicloud_ipd.version_size import AssetVersionSize, LivePhotoVersionSize


@dataclass(kw_only=True)
class _DefaultConfig:
    directory: str
    auth_only: bool
    cookie_directory: str
    sizes: Sequence[AssetVersionSize]
    live_photo_size: LivePhotoVersionSize
    recent: int | None
    until_found: int | None
    albums: Sequence[str]
    list_albums: bool
    library: str
    list_libraries: bool
    skip_videos: bool
    skip_live_photos: bool
    xmp_sidecar: bool
    force_size: bool
    auto_delete: bool
    folder_structure: str
    set_exif_datetime: bool
    smtp_username: str | None
    smtp_password: str | None
    smtp_host: str
    smtp_port: int
    smtp_no_tls: bool
    notification_email: str | None
    notification_email_from: str | None
    notification_script: pathlib.Path | None
    delete_after_download: bool
    keep_icloud_recent_days: int | None
    dry_run: bool
    keep_unicode_in_filenames: bool
    live_photo_mov_filename_policy: LivePhotoMovFilenamePolicy
    align_raw: RawTreatmentPolicy
    file_match_policy: FileMatchPolicy
    skip_created_before: datetime.datetime | datetime.timedelta | None
    skip_created_after: datetime.datetime | datetime.timedelta | None
    skip_photos: bool
    skip_added_before: datetime.datetime | datetime.timedelta | None = None
    skip_added_after: datetime.datetime | datetime.timedelta | None = None
    download_chunk_bytes: int = 262144
    verify_size: bool = False
    verify_checksum: bool = False
    download_workers: int = 4
    album_page_size: int = 100
    no_remote_count: bool = False
    state_db: str | None = None
    state_db_prune_completed_days: int | None = None
    state_db_vacuum: bool = False


@dataclass(kw_only=True)
class UserConfig(_DefaultConfig):
    username: str
    password: str | None


@dataclass(kw_only=True)
class GlobalConfig:
    help: bool
    version: bool
    use_os_locale: bool
    only_print_filenames: bool
    log_level: LogLevel
    log_format: str = "text"
    no_progress_bar: bool
    threads_num: int
    domain: str
    watch_with_interval: int | None
    password_providers: Sequence[PasswordProvider]
    mfa_provider: MFAProvider
    max_retries: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    respect_retry_after: bool
    throttle_cooldown_seconds: float
    metrics_json: str | None = None
