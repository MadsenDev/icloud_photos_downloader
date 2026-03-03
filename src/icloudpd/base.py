#!/usr/bin/env python
"""Main script that uses Click to parse command-line arguments"""

import datetime
import getpass
import itertools
import json
import logging
import os
import re
import signal
import subprocess
import sys
import time
import typing
import urllib
import uuid
from functools import partial, singledispatch
from logging import Logger
from multiprocessing import freeze_support
from threading import Event, Thread, current_thread, main_thread
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Sequence,
    Tuple,
)

from requests.exceptions import (
    ChunkedEncodingError,
    ContentDecodingError,
    StreamConsumedError,
    UnrewindableBodyError,
)
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from tzlocal import get_localzone

from foundation.core import compose, identity, map_, partial_1_1
from icloudpd import download, exif_datetime
from icloudpd.authentication import authenticator
from icloudpd.autodelete import autodelete_photos
from icloudpd.config import GlobalConfig, UserConfig
from icloudpd.counter import Counter
from icloudpd.email_notifications import send_2sa_notification
from icloudpd.filename_policies import build_filename_with_policies, create_filename_builder
from icloudpd.limiter import AdaptiveDownloadLimiter
from icloudpd.log_level import LogLevel
from icloudpd.metrics import RunMetrics, write_metrics_json
from icloudpd.mfa_provider import MFAProvider
from icloudpd.password_provider import PasswordProvider
from icloudpd.paths import local_download_path, remove_unicode_chars
from icloudpd.retry_utils import (
    RetryConfig,
    is_fatal_auth_config_error,
    is_throttle_error,
    is_transient_error,
)
from icloudpd.server import serve_app
from icloudpd.state_db import (
    checkpoint_wal,
    clear_asset_tasks_need_url_refresh,
    initialize_state_db,
    load_checkpoint,
    mark_asset_tasks_need_url_refresh,
    prune_completed_tasks,
    record_asset_checksum_result,
    requeue_in_progress_tasks,
    requeue_stale_leases,
    resolve_state_db_path,
    save_checkpoint,
    upsert_asset_tasks,
    vacuum_state_db,
)
from icloudpd.status import Status, StatusExchange
from icloudpd.string_helpers import parse_timestamp_or_timedelta, truncate_middle
from icloudpd.xmp_sidecar import generate_xmp_file
from pyicloud_ipd.asset_version import (
    AssetVersion,
    add_suffix_to_filename,
    calculate_version_filename,
)
from pyicloud_ipd.base import PyiCloudService
from pyicloud_ipd.exceptions import (
    PyiCloudAPIResponseException,
    PyiCloudConnectionErrorException,
    PyiCloudFailedLoginException,
    PyiCloudFailedMFAException,
    PyiCloudServiceNotActivatedException,
    PyiCloudServiceUnavailableException,
)
from pyicloud_ipd.file_match import FileMatchPolicy
from pyicloud_ipd.item_type import AssetItemType  # fmt: skip
from pyicloud_ipd.live_photo_mov_filename_policy import LivePhotoMovFilenamePolicy
from pyicloud_ipd.raw_policy import RawTreatmentPolicy
from pyicloud_ipd.services.photos import (
    PhotoAlbum,
    PhotoAsset,
    PhotoLibrary,
)
from pyicloud_ipd.utils import (
    disambiguate_filenames,
    get_password_from_keyring,
    size_to_suffix,
    store_password_in_keyring,
)
from pyicloud_ipd.version_size import AssetVersionSize, LivePhotoVersionSize

freeze_support()  # fmt: skip # fixing tqdm on macos
EXIT_CANCELLED = 130
ENGINE_MODE_LEGACY_STATELESS = "legacy_stateless"
ENGINE_MODE_STATEFUL = "stateful_engine"
THROTTLE_ALERT_THRESHOLD = 5


def determine_engine_mode(state_db_path: str | None) -> str:
    return ENGINE_MODE_STATEFUL if state_db_path else ENGINE_MODE_LEGACY_STATELESS


def emit_throttle_alert_if_needed(
    logger: logging.Logger,
    run_metrics: RunMetrics,
    threshold: int = THROTTLE_ALERT_THRESHOLD,
) -> bool:
    if run_metrics.throttle_events < threshold:
        return False
    logger.warning(
        "Repeated throttling detected for user %s (%d events). Consider lowering --download-workers, increasing --watch-with-interval, and keeping --no-remote-count enabled.",
        run_metrics.username,
        run_metrics.throttle_events,
    )
    return True


class ShutdownController:
    def __init__(self, status_exchange: StatusExchange):
        self._status_exchange = status_exchange
        self._event = Event()
        self._signal_name: str | None = None
        self._installed = False
        self._previous_handlers: dict[int, typing.Any] = {}

    def install(self) -> None:
        if self._installed or current_thread() is not main_thread():
            return
        for sig in (signal.SIGINT, signal.SIGTERM):
            self._previous_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, self._build_handler(sig))
        self._installed = True

    def restore(self) -> None:
        if not self._installed:
            return
        for sig, previous in self._previous_handlers.items():
            signal.signal(sig, previous)
        self._previous_handlers.clear()
        self._installed = False

    def _build_handler(self, sig: int) -> Callable[[int, typing.Any], None]:
        def handler(_signum: int, _frame: typing.Any) -> None:
            self._signal_name = signal.Signals(sig).name
            self._event.set()
            self._status_exchange.get_progress().cancel = True

        return handler

    def request_stop(self, reason: str | None = None) -> None:
        self._signal_name = reason or self._signal_name
        self._event.set()
        self._status_exchange.get_progress().cancel = True

    def requested(self) -> bool:
        return self._event.is_set() or self._status_exchange.get_progress().cancel

    def signal_name(self) -> str | None:
        return self._signal_name

    def sleep_or_stop(self, seconds: float) -> bool:
        if seconds <= 0:
            return not self.requested()
        return not self._event.wait(timeout=seconds)


def build_filename_cleaner(keep_unicode: bool) -> Callable[[str], str]:
    """Build filename cleaner based on unicode preference.

    Args:
        keep_unicode: If True, preserve Unicode characters. If False, remove non-ASCII characters.

    Returns:
        Function that processes filenames according to unicode preference.
        Note: Basic filesystem character cleaning (clean_filename) is always applied in calculate_filename.
    """
    if keep_unicode:
        # Only basic cleaning is needed (already applied in calculate_filename)
        return identity
    else:
        # Apply unicode removal in addition to basic cleaning
        return remove_unicode_chars


def lp_filename_concatinator(filename: str) -> str:
    """Generate concatenator-style live photo filename, adding HEVC suffix for HEIC files"""
    import os

    from foundation.core import compose
    from foundation.string_utils import endswith, lower

    name, ext = os.path.splitext(filename)
    if not ext:
        return filename

    is_heic = compose(endswith(".heic"), lower)(ext)
    return name + ("_HEVC.MOV" if is_heic else ".MOV")


def lp_filename_original(filename: str) -> str:
    """Generate original-style live photo filename by replacing extension with .MOV"""
    from foundation.string_utils import replace_extension

    replace_with_mov = replace_extension(".MOV")
    return replace_with_mov(filename)


def ask_password_in_console(_user: str) -> str | None:
    return getpass.getpass(f"iCloud Password for {_user}:")


def get_password_from_webui(
    logger: Logger, status_exchange: StatusExchange, _user: str
) -> str | None:
    """Request two-factor authentication through Webui."""
    if not status_exchange.replace_status(Status.NO_INPUT_NEEDED, Status.NEED_PASSWORD):
        logger.error("Expected NO_INPUT_NEEDED, but got something else")
        return None

    # wait for input
    while True:
        if status_exchange.get_progress().cancel:
            logger.info("Password input cancelled")
            status_exchange.replace_status(Status.NEED_PASSWORD, Status.NO_INPUT_NEEDED)
            return None
        status = status_exchange.get_status()
        if status == Status.NEED_PASSWORD:
            time.sleep(1)
        else:
            break
    if status_exchange.replace_status(Status.SUPPLIED_PASSWORD, Status.CHECKING_PASSWORD):
        password = status_exchange.get_payload()
        if not password:
            logger.error("Internal error: did not get password for SUPPLIED_PASSWORD status")
            status_exchange.replace_status(
                Status.CHECKING_PASSWORD, Status.NO_INPUT_NEEDED
            )  # TODO Error
            return None
        return password

    return None  # TODO


def update_password_status_in_webui(status_exchange: StatusExchange, _u: str, _p: str) -> None:
    status_exchange.replace_status(Status.CHECKING_PASSWORD, Status.NO_INPUT_NEEDED)


def update_auth_error_in_webui(status_exchange: StatusExchange, error: str) -> bool:
    return status_exchange.set_error(error)


# def get_click_param_by_name(_name: str, _params: List[Parameter]) -> Optional[Parameter]:
#     _with_password = [_p for _p in _params if _name in _p.name]
#     if len(_with_password) == 0:
#         return None
#     return _with_password[0]


def dummy_password_writter(_u: str, _p: str) -> None:
    pass


def keyring_password_writter(logger: Logger) -> Callable[[str, str], None]:
    def _intern(username: str, password: str) -> None:
        try:
            store_password_in_keyring(username, password)
        except Exception:
            logger.warning("Password was not saved to keyring")

    return _intern


def skip_created_generator(
    name: str, formatted: str | None
) -> datetime.datetime | datetime.timedelta | None:
    """Converts ISO dates to datetime and interval in days to timeinterval using supplied name as part of raised exception in case of the error"""
    if formatted is None:
        return None
    result = parse_timestamp_or_timedelta(formatted)
    if result is None:
        raise ValueError(f"{name} parameter did not parse ISO timestamp or interval successfully")
    if isinstance(result, datetime.datetime):
        return ensure_tzinfo(get_localzone(), result)
    return result


def ensure_tzinfo(tz: datetime.tzinfo, input: datetime.datetime) -> datetime.datetime:
    if input.tzinfo is None:
        return input.astimezone(tz)
    return input


# Must import the constants object so that we can mock values in tests.


def create_logger(config: GlobalConfig) -> logging.Logger:
    logger = logging.getLogger("icloudpd")
    logger.handlers.clear()
    logger.propagate = True

    class RunContextFilter(logging.Filter):
        def __init__(self, run_id: str):
            super().__init__()
            self._run_id = run_id

        def filter(self, record: logging.LogRecord) -> bool:
            if not hasattr(record, "run_id"):
                record.run_id = self._run_id  # type: ignore[attr-defined]
            if not hasattr(record, "asset_id"):
                record.asset_id = None  # type: ignore[attr-defined]
            if not hasattr(record, "attempt"):
                record.attempt = None  # type: ignore[attr-defined]
            if not hasattr(record, "http_status"):
                record.http_status = None  # type: ignore[attr-defined]
            return True

    class SensitiveDataRedactionFilter(logging.Filter):
        _KEY_VALUE_PATTERNS = [
            # JSON payload style: "password": "value"
            re.compile(
                r'(?i)"(password|passphrase|session_token|trust_token|token|authorization|cookie|scnt)"\s*:\s*"([^"]+)"'
            ),
            # key=value style in plain logs
            re.compile(
                r"(?i)\b(password|passphrase|session_token|trust_token|token|authorization|cookie|scnt)\b\s*[:=]\s*(Bearer\s+[^\s,;]+|[^\s,;]+)"
            ),
            # Authorization bearer tokens
            re.compile(r"(?i)\bBearer\s+[A-Za-z0-9\-._~+/]+=*"),
        ]

        def _redact(self, message: str) -> str:
            redacted = message
            redacted = self._KEY_VALUE_PATTERNS[0].sub(r'"\1":"REDACTED"', redacted)
            redacted = self._KEY_VALUE_PATTERNS[1].sub(r"\1=REDACTED", redacted)
            redacted = self._KEY_VALUE_PATTERNS[2].sub("Bearer REDACTED", redacted)
            return redacted

        def filter(self, record: logging.LogRecord) -> bool:
            redacted = self._redact(record.getMessage())
            record.msg = redacted
            record.args = ()  # type: ignore[assignment]
            return True

    class JsonLogFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "run_id": getattr(record, "run_id", None),
                "asset_id": getattr(record, "asset_id", None),
                "attempt": getattr(record, "attempt", None),
                "http_status": getattr(record, "http_status", None),
            }
            return json.dumps(payload, ensure_ascii=True)

    handler = logging.StreamHandler(sys.stdout)
    if config.log_format == "json":
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)-8s %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
    logger.addHandler(handler)
    logger.addFilter(RunContextFilter(uuid.uuid4().hex))
    logger.addFilter(SensitiveDataRedactionFilter())

    if config.only_print_filenames:
        logger.disabled = True
    else:
        # Need to make sure disabled is reset to the correct value,
        # because the logger instance is shared between tests.
        logger.disabled = False
        if config.log_level == LogLevel.DEBUG:
            logger.setLevel(logging.DEBUG)
        elif config.log_level == LogLevel.INFO:
            logger.setLevel(logging.INFO)
        elif config.log_level == LogLevel.ERROR:
            logger.setLevel(logging.ERROR)
        else:
            # Developer's error - not an exhaustive match
            raise ValueError(f"Unsupported logging level {config.log_level}")
    return logger


def run_with_configs(global_config: GlobalConfig, user_configs: Sequence[UserConfig]) -> int:
    """Run the application with the new configuration system"""

    # Create shared logger
    logger = create_logger(global_config)

    # Create shared status exchange for web server and progress tracking
    shared_status_exchange = StatusExchange()
    shutdown = ShutdownController(shared_status_exchange)
    shutdown.install()

    # Check if any user needs web server (webui for MFA or passwords)
    needs_web_server = global_config.mfa_provider == MFAProvider.WEBUI or any(
        provider == PasswordProvider.WEBUI for provider in global_config.password_providers
    )

    # Start web server ONCE if needed, outside all loops
    if needs_web_server:
        logger.info("Starting web server for WebUI authentication...")
        server_thread = Thread(target=serve_app, daemon=True, args=[logger, shared_status_exchange])
        server_thread.start()

    # Check if we're in watch mode
    watch_interval = global_config.watch_with_interval

    if not watch_interval:
        # No watch mode - process each user once and exit
        try:
            return _process_all_users_once(
                global_config,
                user_configs,
                logger,
                shared_status_exchange,
                shutdown,
            )
        finally:
            shutdown.restore()
    else:
        # Watch mode - infinite loop processing all users, then wait
        skip_bar = not os.environ.get("FORCE_TQDM") and (
            global_config.only_print_filenames
            or global_config.no_progress_bar
            or not sys.stdout.isatty()
        )

        try:
            while True:
                if shutdown.requested():
                    signal_name = shutdown.signal_name() or "cancellation signal"
                    logger.info("Run cancelled by %s", signal_name)
                    return EXIT_CANCELLED

                # Process all user configs in this iteration
                result = _process_all_users_once(
                    global_config,
                    user_configs,
                    logger,
                    shared_status_exchange,
                    shutdown,
                )
                if result == EXIT_CANCELLED:
                    return EXIT_CANCELLED

                # If any critical operation (auth-only, list commands) succeeded, exit
                if result == 0:
                    first_user = user_configs[0] if user_configs else None
                    if first_user and (
                        first_user.auth_only or first_user.list_albums or first_user.list_libraries
                    ):
                        return 0

                # Wait for the watch interval before next iteration
                # Clear current user during wait period to avoid misleading UI
                shared_status_exchange.clear_current_user()
                logger.info(f"Waiting for {watch_interval} sec...")
                interval: Sequence[int] = range(1, watch_interval)
                iterable: Sequence[int] = (
                    interval
                    if skip_bar
                    else typing.cast(
                        Sequence[int],
                        tqdm(
                            iterable=interval,
                            desc="Waiting...",
                            ascii=True,
                            leave=False,
                            dynamic_ncols=True,
                        ),
                    )
                )
                for counter in iterable:
                    # Update shared status exchange with wait progress
                    shared_status_exchange.get_progress().waiting = watch_interval - counter
                    if shared_status_exchange.get_progress().resume:
                        shared_status_exchange.get_progress().reset()
                        break
                    if not shutdown.sleep_or_stop(1):
                        signal_name = shutdown.signal_name() or "cancellation signal"
                        logger.info("Run cancelled by %s", signal_name)
                        return EXIT_CANCELLED
        finally:
            shutdown.restore()


def _process_all_users_once(
    global_config: GlobalConfig,
    user_configs: Sequence[UserConfig],
    logger: logging.Logger,
    shared_status_exchange: StatusExchange,
    shutdown: ShutdownController,
) -> int:
    """Process all user configs once (used by both single run and watch mode)"""

    # Set global config and all user configs to status exchange once, before processing
    shared_status_exchange.set_global_config(global_config)
    shared_status_exchange.set_user_configs(user_configs)
    user_metrics_snapshots: list[dict[str, float | int | str]] = []

    def write_run_summary(exit_code: int) -> None:
        if not global_config.metrics_json:
            return
        users_with_failures = 0
        for user in user_metrics_snapshots:
            failed = user.get("downloads_failed", 0)
            if isinstance(failed, int) and failed > 0:
                users_with_failures += 1
        if exit_code == 0 and users_with_failures > 0:
            status = "partial_success"
        elif exit_code == 0:
            status = "success"
        elif exit_code == EXIT_CANCELLED:
            status = "cancelled"
        elif exit_code == 2:
            status = "cli_error"
        else:
            status = "runtime_error"
        write_metrics_json(
            global_config.metrics_json,
            {
                "exit_code": exit_code,
                "status": status,
                "users_total": len(user_metrics_snapshots),
                "users_with_failures": users_with_failures,
                "users": user_metrics_snapshots,
            },
        )

    if shutdown.requested():
        write_run_summary(EXIT_CANCELLED)
        return EXIT_CANCELLED

    for user_config in user_configs:
        if shutdown.requested():
            signal_name = shutdown.signal_name() or "cancellation signal"
            logger.info("Stopping before user %s due to %s", user_config.username, signal_name)
            write_run_summary(EXIT_CANCELLED)
            return EXIT_CANCELLED
        with logging_redirect_tqdm():
            # Use shared status exchange instead of creating new ones per user
            status_exchange = shared_status_exchange

            # Set up password providers with proper function replacements
            password_providers_dict: Dict[
                PasswordProvider, Tuple[Callable[[str], str | None], Callable[[str, str], None]]
            ] = {}

            for provider in global_config.password_providers:
                if provider == PasswordProvider.WEBUI:
                    password_providers_dict[provider] = (
                        partial(get_password_from_webui, logger, status_exchange),
                        partial(update_password_status_in_webui, status_exchange),
                    )
                elif provider == PasswordProvider.CONSOLE:
                    password_providers_dict[provider] = (
                        ask_password_in_console,
                        dummy_password_writter,
                    )
                elif provider == PasswordProvider.KEYRING:
                    password_providers_dict[provider] = (
                        get_password_from_keyring,
                        keyring_password_writter(logger),
                    )
                elif provider == PasswordProvider.PARAMETER:

                    def create_constant_password_provider(
                        password: str | None,
                    ) -> Callable[[str], str | None]:
                        def password_provider(_username: str) -> str | None:
                            return password

                        return password_provider

                    password_providers_dict[provider] = (
                        create_constant_password_provider(user_config.password),
                        dummy_password_writter,
                    )

            # Only set current user - global config and user configs are already set
            status_exchange.set_current_user(user_config.username)

            # Web server is now started once outside the user loop - no need to start it here

            # Set up filename processors directly since we don't have click context
            # filename_cleaner was removed from services and should be passed explicitly to functions that need it

            # Set up live photo filename generator directly
            lp_filename_generator = (
                lp_filename_original
                if user_config.live_photo_mov_filename_policy == LivePhotoMovFilenamePolicy.ORIGINAL
                else lp_filename_concatinator
            )

            # Set up filename cleaner based on user preference
            filename_cleaner = build_filename_cleaner(user_config.keep_unicode_in_filenames)

            # Create filename builder with pre-configured policy and cleaner
            filename_builder = create_filename_builder(
                user_config.file_match_policy, filename_cleaner
            )

            # Set up function builders
            state_db_path = resolve_state_db_path(user_config.state_db, user_config.cookie_directory)
            engine_mode = determine_engine_mode(state_db_path)
            if state_db_path:
                logger.info("Initializing state DB at %s", state_db_path)
                initialize_state_db(state_db_path)
                logger.info(
                    "Engine mode: %s (persistent task/checkpoint state enabled)",
                    engine_mode,
                )
            else:
                logger.info(
                    "Engine mode: %s (filesystem skip semantics, no state DB required)",
                    engine_mode,
                )

            download.set_retry_config(
                RetryConfig(
                    max_retries=global_config.max_retries,
                    backoff_base_seconds=global_config.backoff_base_seconds,
                    backoff_max_seconds=global_config.backoff_max_seconds,
                    respect_retry_after=global_config.respect_retry_after,
                    throttle_cooldown_seconds=global_config.throttle_cooldown_seconds,
                )
            )
            download.set_download_chunk_bytes(user_config.download_chunk_bytes)
            download.set_download_verification(
                verify_size=user_config.verify_size,
                verify_checksum=user_config.verify_checksum,
            )
            download.set_download_limiter(
                AdaptiveDownloadLimiter(
                    max_workers=user_config.download_workers,
                    cooldown_seconds=global_config.throttle_cooldown_seconds,
                )
            )
            run_metrics = RunMetrics(username=user_config.username, run_mode=engine_mode)
            run_metrics.start()
            download.set_metrics_collector(run_metrics)
            passer = partial(
                where_builder,
                logger,
                user_config.skip_videos,
                user_config.skip_created_before,
                user_config.skip_created_after,
                user_config.skip_added_before,
                user_config.skip_added_after,
                user_config.skip_photos,
                filename_builder,
            )

            downloader = (
                partial(
                    download_builder,
                    logger,
                    user_config.folder_structure,
                    user_config.directory,
                    user_config.sizes,
                    user_config.force_size,
                    global_config.only_print_filenames,
                    user_config.set_exif_datetime,
                    user_config.skip_live_photos,
                    user_config.live_photo_size,
                    user_config.dry_run,
                    user_config.file_match_policy,
                    user_config.xmp_sidecar,
                    lp_filename_generator,
                    filename_builder,
                    user_config.align_raw,
                )
                if user_config.directory is not None
                else (lambda _s, _c, _p: False)
            )

            notificator = partial(
                notificator_builder,
                logger,
                user_config.username,
                user_config.smtp_username,
                user_config.smtp_password,
                user_config.smtp_host,
                user_config.smtp_port,
                user_config.smtp_no_tls,
                user_config.notification_email,
                user_config.notification_email_from,
                str(user_config.notification_script) if user_config.notification_script else None,
            )

            # Use core_single_run since we've disabled watch at this level
            logger.info(f"Processing user: {user_config.username}")
            result = core_single_run(
                logger,
                status_exchange,
                global_config,
                user_config,
                shutdown,
                password_providers_dict,
                run_metrics,
                passer,
                downloader,
                notificator,
                lp_filename_generator,
            )
            if state_db_path:
                if user_config.state_db_prune_completed_days is not None:
                    pruned = prune_completed_tasks(
                        state_db_path,
                        older_than_days=user_config.state_db_prune_completed_days,
                    )
                    if pruned > 0:
                        logger.info("Pruned %d completed/failed state DB task(s)", pruned)
                checkpoint_wal(state_db_path, mode="PASSIVE")
                if user_config.state_db_vacuum:
                    logger.info("Running state DB VACUUM at %s", state_db_path)
                    vacuum_state_db(state_db_path)
            run_metrics.finish()
            emit_throttle_alert_if_needed(logger, run_metrics)
            user_metrics_snapshots.append(run_metrics.snapshot())

            # If any user config fails and we're not in watch mode, return the error code
            if result != 0:
                if not global_config.watch_with_interval:
                    write_run_summary(result)
                    return result
                else:
                    # In watch mode, log error and continue with next user
                    logger.error(
                        f"Error processing user {user_config.username}, continuing with next user..."
                    )

    write_run_summary(0)
    return 0


def notificator_builder(
    logger: logging.Logger,
    username: str,
    smtp_username: str | None,
    smtp_password: str | None,
    smtp_host: str,
    smtp_port: int,
    smtp_no_tls: bool,
    notification_email: str | None,
    notification_email_from: str | None,
    notification_script: str | None,
) -> None:
    try:
        if notification_script is not None:
            logger.debug("Executing notification script...")
            subprocess.call([notification_script])
        else:
            pass
        if smtp_username is not None or notification_email is not None:
            send_2sa_notification(
                logger,
                username,
                smtp_username,
                smtp_password,
                smtp_host,
                smtp_port,
                smtp_no_tls,
                notification_email,
                notification_email_from,
            )
        else:
            pass
    except Exception as error:
        logger.error("Notification of the required MFA failed")
        logger.debug(error)


@singledispatch
def offset_to_datetime(offset: Any) -> datetime.datetime:
    raise NotImplementedError()


@offset_to_datetime.register(datetime.datetime)
def _(offset: datetime.datetime) -> datetime.datetime:
    return offset


@offset_to_datetime.register(datetime.timedelta)
def _(offset: datetime.timedelta) -> datetime.datetime:
    return datetime.datetime.now(get_localzone()) - offset


def where_builder(
    logger: logging.Logger,
    skip_videos: bool,
    skip_created_before: datetime.datetime | datetime.timedelta | None,
    skip_created_after: datetime.datetime | datetime.timedelta | None,
    skip_added_before: datetime.datetime | datetime.timedelta | None,
    skip_added_after: datetime.datetime | datetime.timedelta | None,
    skip_photos: bool,
    filename_builder: Callable[[PhotoAsset], str],
    photo: PhotoAsset,
) -> bool:
    if skip_videos and photo.item_type == AssetItemType.MOVIE:
        logger.debug(asset_type_skip_message(AssetItemType.IMAGE, filename_builder, photo))
        return False
    if skip_photos and photo.item_type == AssetItemType.IMAGE:
        logger.debug(asset_type_skip_message(AssetItemType.MOVIE, filename_builder, photo))
        return False

    if skip_created_before is not None:
        temp_created_before = offset_to_datetime(skip_created_before)
        if photo.created < temp_created_before:
            logger.debug(skip_created_before_message(temp_created_before, photo, filename_builder))
            return False

    if skip_created_after is not None:
        temp_created_after = offset_to_datetime(skip_created_after)
        if photo.created > temp_created_after:
            logger.debug(skip_created_after_message(temp_created_after, photo, filename_builder))
            return False

    if skip_added_before is not None:
        temp_added_before = offset_to_datetime(skip_added_before)
        try:
            added_date = photo.added_date.astimezone(get_localzone())
        except (KeyError, TypeError, ValueError, OSError):
            added_date = None
        if added_date is not None and added_date < temp_added_before:
            logger.debug(skip_added_before_message(temp_added_before, photo, filename_builder))
            return False

    if skip_added_after is not None:
        temp_added_after = offset_to_datetime(skip_added_after)
        try:
            added_date = photo.added_date.astimezone(get_localzone())
        except (KeyError, TypeError, ValueError, OSError):
            added_date = None
        if added_date is not None and added_date > temp_added_after:
            logger.debug(skip_added_after_message(temp_added_after, photo, filename_builder))
            return False

    return True


def skip_created_before_message(
    target_created_date: datetime.datetime,
    photo: PhotoAsset,
    filename_builder: Callable[[PhotoAsset], str],
) -> str:
    filename = filename_builder(photo)
    return f"Skipping {filename}, as it was created {photo.created}, before {target_created_date}."


def skip_created_after_message(
    target_created_date: datetime.datetime,
    photo: PhotoAsset,
    filename_builder: Callable[[PhotoAsset], str],
) -> str:
    filename = filename_builder(photo)
    return f"Skipping {filename}, as it was created {photo.created}, after {target_created_date}."


def skip_added_before_message(
    target_added_date: datetime.datetime,
    photo: PhotoAsset,
    filename_builder: Callable[[PhotoAsset], str],
) -> str:
    filename = filename_builder(photo)
    return f"Skipping {filename}, as it was added {photo.added_date}, before {target_added_date}."


def skip_added_after_message(
    target_added_date: datetime.datetime,
    photo: PhotoAsset,
    filename_builder: Callable[[PhotoAsset], str],
) -> str:
    filename = filename_builder(photo)
    return f"Skipping {filename}, as it was added {photo.added_date}, after {target_added_date}."


def download_builder(
    logger: logging.Logger,
    folder_structure: str,
    directory: str,
    primary_sizes: Sequence[AssetVersionSize],
    force_size: bool,
    only_print_filenames: bool,
    set_exif_datetime: bool,
    skip_live_photos: bool,
    live_photo_size: LivePhotoVersionSize,
    dry_run: bool,
    file_match_policy: FileMatchPolicy,
    xmp_sidecar: bool,
    lp_filename_generator: Callable[[str], str],
    filename_builder: Callable[[PhotoAsset], str],
    raw_policy: RawTreatmentPolicy,
    icloud: PyiCloudService,
    counter: Counter,
    photo: PhotoAsset,
) -> bool:
    """function for actually downloading the photos"""

    try:
        created_date = photo.created.astimezone(get_localzone())
    except (ValueError, OSError):
        logger.error("Could not convert photo created date to local timezone (%s)", photo.created)
        created_date = photo.created

    from foundation.core import compose
    from foundation.string_utils import eq, lower

    is_none_folder = compose(eq("none"), lower)

    if is_none_folder(folder_structure):
        date_path = ""
    else:
        try:
            date_path = folder_structure.format(created_date)
        except ValueError:  # pragma: no cover
            # This error only seems to happen in Python 2
            logger.error("Photo created date was not valid (%s)", photo.created)
            # e.g. ValueError: year=5 is before 1900
            # (https://github.com/icloud-photos-downloader/icloud_photos_downloader/issues/122)
            # Just use the Unix epoch
            created_date = datetime.datetime.fromtimestamp(0)
            date_path = folder_structure.format(created_date)

    try:
        versions, filename_overrides = disambiguate_filenames(
            photo.versions_with_raw_policy(raw_policy), primary_sizes, photo, lp_filename_generator
        )
    except KeyError as ex:
        print(f"KeyError: {ex} attribute was not found in the photo fields.")
        with open(file="icloudpd-photo-error.json", mode="w", encoding="utf8") as outfile:
            json.dump(
                {
                    "master_record": photo._master_record,
                    "asset_record": photo._asset_record,
                },
                outfile,
            )
        print("icloudpd has saved the photo record to: ./icloudpd-photo-error.json")
        print("Please create a Gist with the contents of this file: https://gist.github.com")
        print(
            "Then create an issue on GitHub: "
            "https://github.com/icloud-photos-downloader/icloud_photos_downloader/issues"
        )
        print("Include a link to the Gist in your issue, so that we can see what went wrong.\n")
        return False

    download_dir = os.path.normpath(os.path.join(directory, date_path))
    success = False

    def refresh_asset_version(target_size: AssetVersionSize | LivePhotoVersionSize) -> AssetVersion | None:
        # Drop cached versions and rebuild from latest asset metadata snapshot.
        if hasattr(photo, "_versions"):
            photo._versions = None  # type: ignore[attr-defined]
        refreshed_versions = photo.versions_with_raw_policy(raw_policy)
        return refreshed_versions.get(target_size)

    for download_size in primary_sizes:
        if download_size not in versions and download_size != AssetVersionSize.ORIGINAL:
            if force_size:
                error_filename = filename_builder(photo)
                logger.error(
                    "%s size does not exist for %s. Skipping...",
                    download_size.value,
                    error_filename,
                )
                continue
            if AssetVersionSize.ORIGINAL in primary_sizes:
                continue  # that should avoid double download for original
            download_size = AssetVersionSize.ORIGINAL

        version = versions[download_size]
        photo_filename = filename_builder(photo)
        filename = calculate_version_filename(
            photo_filename,
            version,
            download_size,
            lp_filename_generator,
            photo.item_type,
            filename_overrides.get(download_size),
        )

        download_path = local_download_path(filename, download_dir)

        original_download_path = None
        file_exists = os.path.isfile(download_path)
        if not file_exists and download_size == AssetVersionSize.ORIGINAL:
            # Deprecation - We used to download files like IMG_1234-original.jpg,
            # so we need to check for these.
            # Now we match the behavior of iCloud for Windows: IMG_1234.jpg
            original_download_path = add_suffix_to_filename("-original", download_path)
            file_exists = os.path.isfile(original_download_path)

        if file_exists:
            if file_match_policy == FileMatchPolicy.NAME_SIZE_DEDUP_WITH_SUFFIX:
                # for later: this crashes if download-size medium is specified
                file_size = os.stat(original_download_path or download_path).st_size
                photo_size = version.size
                if file_size != photo_size:
                    download_path = (f"-{photo_size}.").join(download_path.rsplit(".", 1))
                    logger.debug("%s deduplicated", truncate_middle(download_path, 96))
                    file_exists = os.path.isfile(download_path)
            if file_exists:
                counter.increment()
                logger.debug("%s already exists", truncate_middle(download_path, 96))

        if not file_exists:
            counter.reset()
            if only_print_filenames:
                print(download_path)
            else:
                truncated_path = truncate_middle(download_path, 96)
                logger.debug("Downloading %s...", truncated_path)

                download_result = download.download_media(
                    logger,
                    dry_run,
                    icloud,
                    photo,
                    download_path,
                    version,
                    download_size,
                    filename_builder,
                    refresh_version=partial(refresh_asset_version, download_size),
                )
                success = download_result

                if download_result:
                    from foundation.core import compose
                    from foundation.string_utils import endswith, lower

                    is_jpeg = compose(endswith((".jpg", ".jpeg")), lower)

                    if (
                        not dry_run
                        and set_exif_datetime
                        and is_jpeg(filename)
                        and not exif_datetime.get_photo_exif(logger, download_path)
                    ):
                        # %Y:%m:%d looks wrong, but it's the correct format
                        date_str = created_date.strftime("%Y-%m-%d %H:%M:%S%z")
                        logger.debug("Setting EXIF timestamp for %s: %s", download_path, date_str)
                        exif_datetime.set_photo_exif(
                            logger,
                            download_path,
                            created_date.strftime("%Y:%m:%d %H:%M:%S"),
                        )
                    if not dry_run:
                        download.set_utime(download_path, created_date)
                    logger.info("Downloaded %s", truncated_path)

        if xmp_sidecar:
            generate_xmp_file(logger, download_path, photo._asset_record, dry_run)

    # Also download the live photo if present
    if not skip_live_photos:
        lp_size = live_photo_size
        photo_versions_with_policy = photo.versions_with_raw_policy(raw_policy)
        if lp_size in photo_versions_with_policy:
            version = photo_versions_with_policy[lp_size]
            lp_photo_filename = filename_builder(photo)
            lp_filename = calculate_version_filename(
                lp_photo_filename,
                version,
                lp_size,
                lp_filename_generator,
                photo.item_type,
            )
            if live_photo_size != LivePhotoVersionSize.ORIGINAL:
                # Add size to filename if not original
                lp_filename = add_suffix_to_filename(
                    size_to_suffix(live_photo_size),
                    lp_filename,
                )
            else:
                pass
            lp_download_path = os.path.join(download_dir, lp_filename)

            lp_file_exists = os.path.isfile(lp_download_path)

            if only_print_filenames:
                if not lp_file_exists:
                    print(lp_download_path)
                # Handle deduplication case for only_print_filenames
                if (
                    lp_file_exists
                    and file_match_policy == FileMatchPolicy.NAME_SIZE_DEDUP_WITH_SUFFIX
                ):
                    lp_file_size = os.stat(lp_download_path).st_size
                    lp_photo_size = version.size
                    if lp_file_size != lp_photo_size:
                        lp_download_path = (f"-{lp_photo_size}.").join(
                            lp_download_path.rsplit(".", 1)
                        )
                        logger.debug("%s deduplicated", truncate_middle(lp_download_path, 96))
                        # Print the deduplicated filename but don't download
                        print(lp_download_path)
            else:
                if lp_file_exists:
                    if file_match_policy == FileMatchPolicy.NAME_SIZE_DEDUP_WITH_SUFFIX:
                        lp_file_size = os.stat(lp_download_path).st_size
                        lp_photo_size = version.size
                        if lp_file_size != lp_photo_size:
                            lp_download_path = (f"-{lp_photo_size}.").join(
                                lp_download_path.rsplit(".", 1)
                            )
                            logger.debug("%s deduplicated", truncate_middle(lp_download_path, 96))
                            lp_file_exists = os.path.isfile(lp_download_path)
                    if lp_file_exists:
                        logger.debug("%s already exists", truncate_middle(lp_download_path, 96))
                if not lp_file_exists:
                    truncated_path = truncate_middle(lp_download_path, 96)
                    logger.debug("Downloading %s...", truncated_path)
                    download_result = download.download_media(
                        logger,
                        dry_run,
                        icloud,
                        photo,
                        lp_download_path,
                        version,
                        lp_size,
                        filename_builder,
                        refresh_version=partial(refresh_asset_version, lp_size),
                    )
                    success = download_result and success
                    if download_result:
                        logger.info("Downloaded %s", truncated_path)
    return success


def delete_photo(
    logger: logging.Logger,
    library_object: PhotoLibrary,
    photo: PhotoAsset,
    filename_builder: Callable[[PhotoAsset], str],
) -> None:
    """Delete a photo from the iCloud account."""
    clean_filename_local = filename_builder(photo)
    logger.debug("Deleting %s in iCloud...", clean_filename_local)
    url = (
        f"{library_object.service_endpoint}/records/modify?"
        f"{urllib.parse.urlencode(library_object.params)}"
    )
    post_data = json.dumps(
        {
            "atomic": True,
            "desiredKeys": ["isDeleted"],
            "operations": [
                {
                    "operationType": "update",
                    "record": {
                        "fields": {"isDeleted": {"value": 1}},
                        "recordChangeTag": photo._asset_record["recordChangeTag"],
                        "recordName": photo._asset_record["recordName"],
                        "recordType": "CPLAsset",
                    },
                }
            ],
            "zoneID": library_object.zone_id,
        }
    )
    library_object.session.post(url, data=post_data, headers={"Content-type": "application/json"})
    logger.info("Deleted %s in iCloud", clean_filename_local)


def delete_photo_dry_run(
    logger: logging.Logger,
    library_object: PhotoLibrary,
    photo: PhotoAsset,
    filename_builder: Callable[[PhotoAsset], str],
) -> None:
    """Dry run for deleting a photo from the iCloud"""
    filename = filename_builder(photo)
    logger.info(
        "[DRY RUN] Would delete %s in iCloud library %s",
        filename,
        library_object.zone_id["zoneName"],
    )


def dump_responses(dumper: Callable[[Any], None], responses: List[Mapping[str, Any]]) -> None:
    # dump captured responses
    for entry in responses:
        # compose(logger.debug, compose(json.dumps, response_to_har))(response)
        dumper(json.dumps(entry, indent=2))


def asset_type_skip_message(
    desired_item_type: AssetItemType,
    filename_builder: Callable[[PhotoAsset], str],
    photo: PhotoAsset,
) -> str:
    photo_video_phrase = "photos" if desired_item_type == AssetItemType.IMAGE else "videos"
    filename = filename_builder(photo)
    return f"Skipping {filename}, only downloading {photo_video_phrase}. (Item type was: {photo.item_type})"


def core_single_run(
    logger: logging.Logger,
    status_exchange: StatusExchange,
    global_config: GlobalConfig,
    user_config: UserConfig,
    shutdown: ShutdownController,
    password_providers_dict: Dict[
        PasswordProvider, Tuple[Callable[[str], str | None], Callable[[str, str], None]]
    ],
    run_metrics: RunMetrics,
    passer: Callable[[PhotoAsset], bool],
    downloader: Callable[[PyiCloudService, Counter, PhotoAsset], bool],
    notificator: Callable[[], None],
    lp_filename_generator: Callable[[str], str],
) -> int:
    """Download all iCloud photos to a local directory for a single execution (no watch loop)"""

    skip_bar = not os.environ.get("FORCE_TQDM") and (
        global_config.only_print_filenames
        or global_config.no_progress_bar
        or not sys.stdout.isatty()
    )
    retry_config = RetryConfig(
        max_retries=global_config.max_retries,
        backoff_base_seconds=global_config.backoff_base_seconds,
        backoff_max_seconds=global_config.backoff_max_seconds,
        respect_retry_after=global_config.respect_retry_after,
        throttle_cooldown_seconds=global_config.throttle_cooldown_seconds,
    )
    state_db_path = resolve_state_db_path(user_config.state_db, user_config.cookie_directory)
    retry_count = 0
    stale_requeue_done = False
    while True:  # retry loop (not watch - only for immediate retries)
        if shutdown.requested():
            if state_db_path:
                requeue_in_progress_tasks(state_db_path)
            signal_name = shutdown.signal_name() or "cancellation signal"
            logger.info("Run cancelled before authentication due to %s", signal_name)
            return EXIT_CANCELLED
        captured_responses: List[Mapping[str, Any]] = []

        def append_response(captured: List[Mapping[str, Any]], response: Mapping[str, Any]) -> None:
            captured.append(response)

        try:
            icloud = authenticator(
                logger,
                global_config.domain,
                {
                    provider.value: functions
                    for provider, functions in password_providers_dict.items()
                },
                global_config.mfa_provider,
                status_exchange,
                user_config.username,
                notificator,
                partial(append_response, captured_responses),
                user_config.cookie_directory,
                os.environ.get("CLIENT_ID"),
            )

            # dump captured responses for debugging
            # dump_responses(logger.debug, captured_responses)

            # turn off response capture
            icloud.response_observer = None

            if user_config.auth_only:
                logger.info("Authentication completed successfully")
                return 0

            elif user_config.list_libraries:
                library_names = (
                    icloud.photos.private_libraries.keys() | icloud.photos.shared_libraries.keys()
                )
                print(*library_names, sep="\n")
                return 0

            else:
                # Access to the selected library. Defaults to the primary photos object.
                if user_config.library:
                    if user_config.library in icloud.photos.private_libraries:
                        library_object: PhotoLibrary = icloud.photos.private_libraries[
                            user_config.library
                        ]
                    elif user_config.library in icloud.photos.shared_libraries:
                        library_object = icloud.photos.shared_libraries[user_config.library]
                    else:
                        logger.error("Unknown library: %s", user_config.library)
                        return 1
                else:
                    library_object = icloud.photos

                if user_config.list_albums:
                    print("Albums:")
                    album_titles = [str(a) for a in library_object.albums.values()]
                    print(*album_titles, sep="\n")
                    return 0
                else:
                    if not user_config.directory:
                        # should be checked upstream
                        raise NotImplementedError()
                    else:
                        pass

                    directory = os.path.normpath(user_config.directory)

                    if user_config.skip_photos or user_config.skip_videos:
                        photo_video_phrase = "photos" if user_config.skip_videos else "videos"
                    else:
                        photo_video_phrase = "photos and videos"
                    if len(user_config.albums) == 0:
                        album_phrase = ""
                    elif len(user_config.albums) == 1:
                        album_phrase = f" from album {','.join(user_config.albums)}"
                    else:
                        album_phrase = f" from albums {','.join(user_config.albums)}"

                    logger.debug(f"Looking up all {photo_video_phrase}{album_phrase}...")
                    if state_db_path and not stale_requeue_done:
                        requeued = requeue_stale_leases(state_db_path)
                        stale_requeue_done = True
                        if requeued > 0:
                            logger.info("Requeued %d stale in-progress task(s) from prior run", requeued)

                    albums: Iterable[PhotoAlbum] = (
                        list(map_(library_object.albums.__getitem__, user_config.albums))
                        if len(user_config.albums) > 0
                        else [library_object.all]
                    )
                    for album in albums:
                        album.page_size = user_config.album_page_size

                    should_fetch_remote_count = (
                        not user_config.no_remote_count
                        and user_config.until_found is None
                    )
                    if not should_fetch_remote_count:
                        photos_count = None
                    else:
                        album_lengths: Callable[[Iterable[PhotoAlbum]], Iterable[int]] = partial_1_1(
                            map_, len
                        )

                        def sum_(inp: Iterable[int]) -> int:
                            return sum(inp)

                        photos_count = compose(sum_, album_lengths)(albums)
                    for photo_album in albums:
                        album_name = str(photo_album)
                        library_name = user_config.library
                        if state_db_path:
                            checkpoint = load_checkpoint(
                                state_db_path, library=library_name, album=album_name
                            )
                            if checkpoint is not None:
                                logger.debug(
                                    "Resuming album %s from checkpoint offset %d",
                                    album_name,
                                    checkpoint,
                                )
                                photo_album.offset = checkpoint

                        photos_enumerator: Iterable[PhotoAsset] = photo_album

                        # Optional: Only download the x most recent photos.
                        if user_config.recent is not None:
                            photos_count = user_config.recent
                            photos_top: Iterable[PhotoAsset] = itertools.islice(
                                photos_enumerator, user_config.recent
                            )
                        else:
                            photos_top = photos_enumerator

                        if user_config.until_found is not None:
                            photos_count = None
                            # ensure photos iterator doesn't have a known length
                            # photos_enumerator = (p for p in photos_enumerator)

                        # Skip the one-line progress bar if we're only printing the filenames,
                        # or if the progress bar is explicitly disabled,
                        # or if this is not a terminal (e.g. cron or piping output to file)
                        if skip_bar:
                            photos_bar: Iterable[PhotoAsset] = photos_top
                            # logger.set_tqdm(None)
                        else:
                            photos_bar = tqdm(
                                iterable=photos_top,
                                total=photos_count,
                                leave=False,
                                dynamic_ncols=True,
                                ascii=True,
                            )
                            # logger.set_tqdm(photos_enumerator)

                        if photos_count is not None:
                            plural_suffix = "" if photos_count == 1 else "s"
                            photos_count_str = (
                                "the first" if photos_count == 1 else str(photos_count)
                            )

                            if user_config.skip_photos or user_config.skip_videos:
                                photo_video_phrase = (
                                    "photo" if user_config.skip_videos else "video"
                                ) + plural_suffix
                            else:
                                photo_video_phrase = (
                                    "photo or video" if photos_count == 1 else "photos and videos"
                                )
                        else:
                            photos_count_str = "???"
                            if user_config.skip_photos or user_config.skip_videos:
                                photo_video_phrase = (
                                    "photos" if user_config.skip_videos else "videos"
                                )
                            else:
                                photo_video_phrase = "photos and videos"
                        logger.info(
                            ("Downloading %s %s %s to %s ..."),
                            photos_count_str,
                            ",".join([_s.value for _s in user_config.sizes]),
                            photo_video_phrase,
                            directory,
                        )

                        consecutive_files_found = Counter(0)

                        def should_break(counter: Counter) -> bool:
                            """Exit if until_found condition is reached"""
                            return (
                                user_config.until_found is not None
                                and counter.value() >= user_config.until_found
                            )

                        status_exchange.get_progress().photos_count = (
                            0 if photos_count is None else photos_count
                        )
                        photos_counter = 0

                        now = datetime.datetime.now(get_localzone())
                        # photos_iterator = iter(photos_enumerator)

                        download_photo = partial(downloader, icloud)

                        for item in photos_bar:
                            if shutdown.requested():
                                break
                            try:
                                run_metrics.on_asset_considered()
                                if should_break(consecutive_files_found):
                                    logger.info(
                                        "Found %s consecutive previously downloaded photos. Exiting",
                                        user_config.until_found,
                                    )
                                    break
                                # item = next(photos_iterator)
                                should_delete = False

                                if state_db_path:
                                    upsert_asset_tasks(
                                        state_db_path,
                                        photo=item,
                                        library=library_name,
                                        album=album_name,
                                    )
                                    save_checkpoint(
                                        state_db_path,
                                        library=library_name,
                                        album=album_name,
                                        start_rank=int(photo_album.offset),
                                    )

                                passer_result = passer(item)
                                download_result = passer_result and download_photo(
                                    consecutive_files_found, item
                                )
                                url_refresh_needed = download.consume_url_refresh_needed_signal()
                                if state_db_path and passer_result and url_refresh_needed:
                                    mark_asset_tasks_need_url_refresh(
                                        state_db_path,
                                        asset_id=item.id,
                                        library=library_name,
                                        album=album_name,
                                    )
                                elif state_db_path and passer_result and download_result:
                                    clear_asset_tasks_need_url_refresh(
                                        state_db_path,
                                        asset_id=item.id,
                                        library=library_name,
                                        album=album_name,
                                    )
                                if state_db_path and passer_result and download_result:
                                    record_asset_checksum_result(
                                        state_db_path,
                                        asset_id=item.id,
                                        library=library_name,
                                        album=album_name,
                                        checksum_result=(
                                            "verified"
                                            if user_config.verify_checksum
                                            else "not_checked"
                                        ),
                                    )
                                if download_result and user_config.delete_after_download:
                                    should_delete = True

                                if (
                                    passer_result
                                    and user_config.keep_icloud_recent_days is not None
                                ):
                                    created_date = item.created.astimezone(get_localzone())
                                    age_days = (now - created_date).days
                                    logger.debug(f"Created date: {created_date}")
                                    logger.debug(
                                        f"Keep iCloud recent days: {user_config.keep_icloud_recent_days}"
                                    )
                                    logger.debug(f"Age days: {age_days}")
                                    if age_days < user_config.keep_icloud_recent_days:
                                        # Create filename cleaner for debug message
                                        filename_cleaner_for_debug = build_filename_cleaner(
                                            user_config.keep_unicode_in_filenames
                                        )
                                        debug_filename = build_filename_with_policies(
                                            user_config.file_match_policy,
                                            filename_cleaner_for_debug,
                                            item,
                                        )
                                        logger.debug(
                                            "Skipping deletion of %s as it is within the keep_icloud_recent_days period (%d days old)",
                                            debug_filename,
                                            age_days,
                                        )
                                    else:
                                        should_delete = True

                                if should_delete:
                                    # Create filename cleaner and builder for delete operations
                                    filename_cleaner_for_delete = build_filename_cleaner(
                                        user_config.keep_unicode_in_filenames
                                    )
                                    filename_builder_for_delete = create_filename_builder(
                                        user_config.file_match_policy, filename_cleaner_for_delete
                                    )
                                    if user_config.dry_run:
                                        delete_photo_dry_run(
                                            logger,
                                            library_object,
                                            item,
                                            filename_builder_for_delete,
                                        )
                                    else:
                                        delete_photo(
                                            logger,
                                            library_object,
                                            item,
                                            filename_builder_for_delete,
                                        )

                                    # retrier(delete_local, error_handler)
                                    photo_album.increment_offset(-1)

                                photos_counter += 1
                                status_exchange.get_progress().photos_counter = photos_counter
                                if photos_count is not None:
                                    run_metrics.set_queue_depth(photos_count - photos_counter)

                                if status_exchange.get_progress().cancel:
                                    break

                            except StopIteration:
                                break

                        if global_config.only_print_filenames:
                            return 0
                        else:
                            pass

                        if status_exchange.get_progress().cancel:
                            if state_db_path:
                                requeue_in_progress_tasks(state_db_path)
                            signal_name = shutdown.signal_name() or "cancellation signal"
                            logger.info("Iteration was cancelled by %s", signal_name)
                            status_exchange.get_progress().photos_last_message = (
                                "Iteration was cancelled"
                            )
                            status_exchange.get_progress().reset()
                            return EXIT_CANCELLED
                        else:
                            if user_config.skip_photos or user_config.skip_videos:
                                photo_video_phrase = (
                                    "photos" if user_config.skip_videos else "videos"
                                )
                            else:
                                photo_video_phrase = "photos and videos"
                            message = f"All {photo_video_phrase} have been downloaded"
                            logger.info(message)
                            status_exchange.get_progress().photos_last_message = message
                        status_exchange.get_progress().reset()

                    if user_config.auto_delete:
                        autodelete_photos(
                            logger,
                            user_config.dry_run,
                            library_object,
                            user_config.folder_structure,
                            directory,
                            user_config.sizes,
                            lp_filename_generator,
                            user_config.align_raw,
                        )
                    else:
                        pass
        except PyiCloudFailedLoginException as error:
            logger.info(error)
            dump_responses(logger.debug, captured_responses)
            if PasswordProvider.WEBUI in global_config.password_providers:
                update_auth_error_in_webui(status_exchange, str(error))
                continue
            else:
                return 1
        except PyiCloudFailedMFAException as error:
            logger.info(str(error))
            dump_responses(logger.debug, captured_responses)
            if global_config.mfa_provider == MFAProvider.WEBUI:
                update_auth_error_in_webui(status_exchange, str(error))
                continue
            else:
                return 1
        except (
            PyiCloudServiceNotActivatedException,
            PyiCloudServiceUnavailableException,
            PyiCloudAPIResponseException,
            PyiCloudConnectionErrorException,
            ChunkedEncodingError,
            ContentDecodingError,
            StreamConsumedError,
            UnrewindableBodyError,
        ) as error:
            logger.info(error)
            dump_responses(logger.debug, captured_responses)
            # webui will display error and wait for password again
            if (
                PasswordProvider.WEBUI in global_config.password_providers
                or global_config.mfa_provider == MFAProvider.WEBUI
            ):
                if update_auth_error_in_webui(status_exchange, str(error)):
                    # retry if it was during auth
                    continue
                else:
                    pass
            else:
                pass
            if is_fatal_auth_config_error(error):
                return 1
            if is_transient_error(error) and retry_count < retry_config.max_retries:
                retry_count += 1
                wait_seconds = retry_config.next_delay_seconds(
                    retry_count,
                    throttle_error=is_throttle_error(error),
                )
                logger.info(
                    "Transient error (%s). Retrying in %.1f seconds (%d/%d)...",
                    type(error).__name__,
                    wait_seconds,
                    retry_count,
                    retry_config.max_retries,
                )
                if not shutdown.sleep_or_stop(wait_seconds):
                    if state_db_path:
                        requeue_in_progress_tasks(state_db_path)
                    signal_name = shutdown.signal_name() or "cancellation signal"
                    logger.info("Run cancelled during retry backoff by %s", signal_name)
                    return EXIT_CANCELLED
                continue
            # In single run mode, return error after webui retry attempts
            return 1
        except KeyboardInterrupt:
            shutdown.request_stop("KeyboardInterrupt")
            if state_db_path:
                requeue_in_progress_tasks(state_db_path)
            logger.info("Run cancelled by KeyboardInterrupt")
            return EXIT_CANCELLED
        except Exception:
            dump_responses(logger.debug, captured_responses)
            raise

        # In single run mode, we don't handle watch intervals - that's done at higher level
        break

    return 0
