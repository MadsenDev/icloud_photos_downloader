import datetime
import inspect
import json
import os
import shutil
from argparse import ArgumentError
from typing import Sequence, Tuple
from unittest import TestCase

import pytest
from tzlocal import get_localzone

from icloudpd.cli import format_help, parse
from icloudpd.config import GlobalConfig, UserConfig
from icloudpd.log_level import LogLevel
from icloudpd.mfa_provider import MFAProvider
from icloudpd.password_provider import PasswordProvider
from pyicloud_ipd.file_match import FileMatchPolicy
from pyicloud_ipd.live_photo_mov_filename_policy import LivePhotoMovFilenamePolicy
from pyicloud_ipd.raw_policy import RawTreatmentPolicy
from pyicloud_ipd.version_size import AssetVersionSize, LivePhotoVersionSize
from tests.helpers import (
    path_from_project_root,
    run_icloudpd_test,
    run_main,
)


class CliTestCase(TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self, caplog: pytest.LogCaptureFixture) -> None:
        self._caplog = caplog
        self.root_path = path_from_project_root(__file__)
        self.fixtures_path = os.path.join(self.root_path, "fixtures")

    def test_cli_help(self) -> None:
        result = format_help()
        # Test that help output contains key sections and content rather than exact formatting
        self.assertIn(
            "usage: icloudpd [GLOBAL] [COMMON] [<USER> [COMMON] <USER> [COMMON] ...]", result
        )
        self.assertIn("GLOBAL options. Applied for all user settings.", result)
        self.assertIn(
            "COMMON options. If specified before the first username, then used as defaults for settings for all users.",
            result,
        )
        self.assertIn("USER options. Can be specified for setting user configuration only.", result)

        # Test that all major options are present
        self.assertIn("--help, -h", result)
        self.assertIn("--version", result)
        self.assertIn("--username", result)
        self.assertIn("--directory", result)
        self.assertIn("--password-provider", result)
        self.assertIn("--mfa-provider", result)
        self.assertIn("--size", result)
        self.assertIn("--live-photo-size", result)
        self.assertIn("--auth-only", result)
        self.assertIn("--dry-run", result)

        # Test that option descriptions are present
        self.assertIn("Show this information", result)
        self.assertIn("Show the version, commit hash, and timestamp", result)
        self.assertIn("Apple ID email address. Starts a new configuration", result)
        # Directory option exists with proper help text (format varies by Python version)
        self.assertTrue(
            "-d, --directory DIRECTORY" in result
            or "-d DIRECTORY, --directory DIRECTORY" in result,
            "Expected directory option format not found in help text",
        )
        self.assertIn("Local directory to use for downloads", result)

    def test_cli_parser(self) -> None:
        self.assertEqual.__self__.maxDiff = None  # type: ignore[attr-defined]
        self.assertEqual(
            parse(["--help"]),
            (
                GlobalConfig(
                    help=True,
                    version=False,
                    use_os_locale=False,
                    only_print_filenames=False,
                    log_level=LogLevel.DEBUG,
                    no_progress_bar=False,
                    threads_num=1,
                    domain="com",
                    watch_with_interval=None,
                    password_providers=[
                        PasswordProvider.PARAMETER,
                        PasswordProvider.KEYRING,
                        PasswordProvider.CONSOLE,
                    ],
                    mfa_provider=MFAProvider.CONSOLE,
                    max_retries=0,
                    backoff_base_seconds=5.0,
                    backoff_max_seconds=300.0,
                    respect_retry_after=True,
                    throttle_cooldown_seconds=60.0,
                ),
                [],
            ),
            "--help",
        )
        self.assertEqual(
            parse(["--mfa-provider", "weBui"]),
            (
                GlobalConfig(
                    help=False,
                    version=False,
                    use_os_locale=False,
                    only_print_filenames=False,
                    log_level=LogLevel.DEBUG,
                    no_progress_bar=False,
                    threads_num=1,
                    domain="com",
                    watch_with_interval=None,
                    password_providers=[
                        PasswordProvider.PARAMETER,
                        PasswordProvider.KEYRING,
                        PasswordProvider.CONSOLE,
                    ],
                    mfa_provider=MFAProvider.WEBUI,
                    max_retries=0,
                    backoff_base_seconds=5.0,
                    backoff_max_seconds=300.0,
                    respect_retry_after=True,
                    throttle_cooldown_seconds=60.0,
                ),
                [],
            ),
            "--mfa-provider weBui",
        )
        self.assertEqual(
            parse(
                [
                    "--password-provider",
                    "weBui",
                    "--password-provider",
                    "CoNSoLe",
                    "--password-provider",
                    "WeBuI",
                ]
            ),
            (
                GlobalConfig(
                    help=False,
                    version=False,
                    use_os_locale=False,
                    only_print_filenames=False,
                    log_level=LogLevel.DEBUG,
                    no_progress_bar=False,
                    threads_num=1,
                    domain="com",
                    watch_with_interval=None,
                    password_providers=[PasswordProvider.WEBUI, PasswordProvider.CONSOLE],
                    mfa_provider=MFAProvider.CONSOLE,
                    max_retries=0,
                    backoff_base_seconds=5.0,
                    backoff_max_seconds=300.0,
                    respect_retry_after=True,
                    throttle_cooldown_seconds=60.0,
                ),
                [],
            ),
            "password-providers",
        )
        self.assertEqual(
            parse(["--version", "--use-os-locale"]),
            (
                GlobalConfig(
                    help=False,
                    version=True,
                    use_os_locale=True,
                    only_print_filenames=False,
                    log_level=LogLevel.DEBUG,
                    no_progress_bar=False,
                    threads_num=1,
                    domain="com",
                    watch_with_interval=None,
                    password_providers=[
                        PasswordProvider.PARAMETER,
                        PasswordProvider.KEYRING,
                        PasswordProvider.CONSOLE,
                    ],
                    mfa_provider=MFAProvider.CONSOLE,
                    max_retries=0,
                    backoff_base_seconds=5.0,
                    backoff_max_seconds=300.0,
                    respect_retry_after=True,
                    throttle_cooldown_seconds=60.0,
                ),
                [],
            ),
            "--version --use-os-locale",
        )
        self.assertEqual(
            parse(
                [
                    "--log-format",
                    "json",
                    "--metrics-json",
                    "/tmp/metrics.json",
                    "--max-retries",
                    "7",
                    "--backoff-base-seconds",
                    "2",
                    "--backoff-max-seconds",
                    "120",
                    "--no-respect-retry-after",
                    "--throttle-cooldown-seconds",
                    "30",
                ]
            ),
            (
                GlobalConfig(
                    help=False,
                    version=False,
                    use_os_locale=False,
                    only_print_filenames=False,
                    log_level=LogLevel.DEBUG,
                    log_format="json",
                    no_progress_bar=False,
                    threads_num=1,
                    domain="com",
                    watch_with_interval=None,
                    password_providers=[
                        PasswordProvider.PARAMETER,
                        PasswordProvider.KEYRING,
                        PasswordProvider.CONSOLE,
                    ],
                    mfa_provider=MFAProvider.CONSOLE,
                    max_retries=7,
                    backoff_base_seconds=2.0,
                    backoff_max_seconds=120.0,
                    respect_retry_after=False,
                    throttle_cooldown_seconds=30.0,
                    metrics_json="/tmp/metrics.json",
                ),
                [],
            ),
            "retry options parse",
        )
        _, users_with_chunk_size = parse(
            ["--directory", "abc", "--username", "u1", "--download-chunk-bytes", "65536"]
        )
        self.assertEqual(users_with_chunk_size[0].download_chunk_bytes, 65536)
        _, users_with_download_workers = parse(
            ["--directory", "abc", "--username", "u1", "--download-workers", "6"]
        )
        self.assertEqual(users_with_download_workers[0].download_workers, 6)
        _, users_with_verification = parse(
            [
                "--directory",
                "abc",
                "--username",
                "u1",
                "--no-verify-size",
                "--verify-checksum",
            ]
        )
        self.assertFalse(users_with_verification[0].verify_size)
        self.assertTrue(users_with_verification[0].verify_checksum)
        _, users_with_paging_options = parse(
            [
                "--directory",
                "abc",
                "--username",
                "u1",
                "--album-page-size",
                "250",
                "--no-remote-count",
            ]
        )
        self.assertEqual(users_with_paging_options[0].album_page_size, 250)
        self.assertTrue(users_with_paging_options[0].no_remote_count)
        _, users_with_added_date_filters = parse(
            [
                "--directory",
                "abc",
                "--username",
                "u1",
                "--skip-added-before",
                "2025-01-01",
                "--skip-added-after",
                "2d",
            ]
        )
        self.assertIsNotNone(users_with_added_date_filters[0].skip_added_before)
        self.assertIsNotNone(users_with_added_date_filters[0].skip_added_after)
        self.assertEqual(
            parse(
                ["--directory", "abc", "--username", "u1", "--username", "u2", "--directory", "def"]
            ),
            (
                GlobalConfig(
                    help=False,
                    version=False,
                    use_os_locale=False,
                    only_print_filenames=False,
                    log_level=LogLevel.DEBUG,
                    no_progress_bar=False,
                    threads_num=1,
                    domain="com",
                    watch_with_interval=None,
                    password_providers=[
                        PasswordProvider.PARAMETER,
                        PasswordProvider.KEYRING,
                        PasswordProvider.CONSOLE,
                    ],
                    mfa_provider=MFAProvider.CONSOLE,
                    max_retries=0,
                    backoff_base_seconds=5.0,
                    backoff_max_seconds=300.0,
                    respect_retry_after=True,
                    throttle_cooldown_seconds=60.0,
                ),
                [
                    UserConfig(
                        directory="abc",
                        username="u1",
                        auth_only=False,
                        cookie_directory="~/.pyicloud",
                        password=None,
                        sizes=[AssetVersionSize.ORIGINAL],
                        live_photo_size=LivePhotoVersionSize.ORIGINAL,
                        recent=None,
                        until_found=None,
                        albums=[],
                        list_albums=False,
                        library="PrimarySync",
                        list_libraries=False,
                        skip_videos=False,
                        skip_live_photos=False,
                        xmp_sidecar=False,
                        force_size=False,
                        auto_delete=False,
                        folder_structure="{:%Y/%m/%d}",
                        set_exif_datetime=False,
                        smtp_username=None,
                        smtp_password=None,
                        smtp_host="smtp.gmail.com",
                        smtp_port=587,
                        smtp_no_tls=False,
                        notification_email=None,
                        notification_email_from=None,
                        notification_script=None,
                        delete_after_download=False,
                        keep_icloud_recent_days=None,
                        dry_run=False,
                        keep_unicode_in_filenames=False,
                        live_photo_mov_filename_policy=LivePhotoMovFilenamePolicy.SUFFIX,
                        align_raw=RawTreatmentPolicy.AS_IS,
                        file_match_policy=FileMatchPolicy.NAME_SIZE_DEDUP_WITH_SUFFIX,
                        skip_created_before=None,
                        skip_created_after=None,
                        skip_photos=False,
                    ),
                    UserConfig(
                        directory="def",
                        auth_only=False,
                        cookie_directory="~/.pyicloud",
                        username="u2",
                        password=None,
                        sizes=[AssetVersionSize.ORIGINAL],
                        live_photo_size=LivePhotoVersionSize.ORIGINAL,
                        recent=None,
                        until_found=None,
                        albums=[],
                        list_albums=False,
                        library="PrimarySync",
                        list_libraries=False,
                        skip_videos=False,
                        skip_live_photos=False,
                        xmp_sidecar=False,
                        force_size=False,
                        auto_delete=False,
                        folder_structure="{:%Y/%m/%d}",
                        set_exif_datetime=False,
                        smtp_username=None,
                        smtp_password=None,
                        smtp_host="smtp.gmail.com",
                        smtp_port=587,
                        smtp_no_tls=False,
                        notification_email=None,
                        notification_email_from=None,
                        notification_script=None,
                        delete_after_download=False,
                        keep_icloud_recent_days=None,
                        dry_run=False,
                        keep_unicode_in_filenames=False,
                        live_photo_mov_filename_policy=LivePhotoMovFilenamePolicy.SUFFIX,
                        align_raw=RawTreatmentPolicy.AS_IS,
                        file_match_policy=FileMatchPolicy.NAME_SIZE_DEDUP_WITH_SUFFIX,
                        skip_created_before=None,
                        skip_created_after=None,
                        skip_photos=False,
                    ),
                ],
            ),
            "defaults propagated and overwritten",
        )
        self.assertEqual(
            parse(
                [
                    "-d",
                    "abc",
                    "--username",
                    "u1",
                    "--skip-created-before",
                    "2025-01-02",
                    "--skip-created-after",
                    "2d",
                ]
            ),
            (
                GlobalConfig(
                    help=False,
                    version=False,
                    use_os_locale=False,
                    only_print_filenames=False,
                    log_level=LogLevel.DEBUG,
                    no_progress_bar=False,
                    threads_num=1,
                    domain="com",
                    watch_with_interval=None,
                    password_providers=[
                        PasswordProvider.PARAMETER,
                        PasswordProvider.KEYRING,
                        PasswordProvider.CONSOLE,
                    ],
                    mfa_provider=MFAProvider.CONSOLE,
                    max_retries=0,
                    backoff_base_seconds=5.0,
                    backoff_max_seconds=300.0,
                    respect_retry_after=True,
                    throttle_cooldown_seconds=60.0,
                ),
                [
                    UserConfig(
                        directory="abc",
                        username="u1",
                        auth_only=False,
                        cookie_directory="~/.pyicloud",
                        password=None,
                        sizes=[AssetVersionSize.ORIGINAL],
                        live_photo_size=LivePhotoVersionSize.ORIGINAL,
                        recent=None,
                        until_found=None,
                        albums=[],
                        list_albums=False,
                        library="PrimarySync",
                        list_libraries=False,
                        skip_videos=False,
                        skip_live_photos=False,
                        xmp_sidecar=False,
                        force_size=False,
                        auto_delete=False,
                        folder_structure="{:%Y/%m/%d}",
                        set_exif_datetime=False,
                        smtp_username=None,
                        smtp_password=None,
                        smtp_host="smtp.gmail.com",
                        smtp_port=587,
                        smtp_no_tls=False,
                        notification_email=None,
                        notification_email_from=None,
                        notification_script=None,
                        delete_after_download=False,
                        keep_icloud_recent_days=None,
                        dry_run=False,
                        keep_unicode_in_filenames=False,
                        live_photo_mov_filename_policy=LivePhotoMovFilenamePolicy.SUFFIX,
                        align_raw=RawTreatmentPolicy.AS_IS,
                        file_match_policy=FileMatchPolicy.NAME_SIZE_DEDUP_WITH_SUFFIX,
                        skip_created_before=datetime.datetime(
                            year=2025, month=1, day=2, tzinfo=get_localzone()
                        ),
                        skip_created_after=datetime.timedelta(days=2),
                        skip_photos=False,
                    ),
                ],
            ),
            "valid skip-created parsed",
        )
        with pytest.raises(
            ArgumentError,
            match="argument --skip-created-before: Not an ISO timestamp or time interval in days",
        ):
            _ = parse(
                [
                    "-d",
                    "abc",
                    "--username",
                    "u1",
                    "--skip-created-before",
                    "2025-01-33",
                    "--skip-created-after",
                    "2d",
                ]
            )
        with pytest.raises(
            ArgumentError,
            match="argument --skip-created-after: Not an ISO timestamp or time interval in days",
        ):
            _ = parse(
                [
                    "-d",
                    "abc",
                    "--username",
                    "u1",
                    "--skip-created-before",
                    "2025-01-02",
                    "--skip-created-after",
                    "2",
                ]
            )
        _, users_with_auto_state_db = parse(
            ["--directory", "abc", "--username", "u1", "--state-db"]
        )
        self.assertEqual(users_with_auto_state_db[0].state_db, "auto")

        _, users_with_custom_state_db = parse(
            ["--directory", "abc", "--username", "u1", "--state-db", "/tmp/icloudpd.sqlite"]
        )
        self.assertEqual(users_with_custom_state_db[0].state_db, "/tmp/icloudpd.sqlite")
        _, users_with_state_db_maintenance = parse(
            [
                "--directory",
                "abc",
                "--username",
                "u1",
                "--state-db",
                "--state-db-prune-completed-days",
                "30",
                "--state-db-vacuum",
            ]
        )
        self.assertEqual(users_with_state_db_maintenance[0].state_db_prune_completed_days, 30)
        self.assertTrue(users_with_state_db_maintenance[0].state_db_vacuum)

    def test_cli(self) -> None:
        result = run_main(["--help"])
        self.assertEqual(result.exit_code, 0, "exit code")

    def test_log_levels(self) -> None:
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])

        parameters: Sequence[Tuple[str, Sequence[str], Sequence[str]]] = [
            ("debug", ["DEBUG", "INFO"], []),
            ("info", ["INFO"], ["DEBUG"]),
            ("error", [], ["DEBUG", "INFO"]),
        ]
        for log_level, expected, not_expected in parameters:
            self._caplog.clear()
            _, result = run_icloudpd_test(
                self.assertEqual,
                self.root_path,
                base_dir,
                "listing_photos.yml",
                [],
                [],
                [
                    "--username",
                    "jdoe@gmail.com",
                    "--password",
                    "password1",
                    "--recent",
                    "0",
                    "--log-level",
                    log_level,
                ],
            )
            self.assertEqual(result.exit_code, 0, "exit code")
            for text in expected:
                self.assertIn(text, self._caplog.text)
            for text in not_expected:
                self.assertNotIn(text, self._caplog.text)

    def test_tqdm(self) -> None:
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])

        _, result = run_icloudpd_test(
            self.assertEqual,
            self.root_path,
            base_dir,
            "listing_photos.yml",
            [],
            [],
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "--recent",
                "0",
            ],
            additional_env={"FORCE_TQDM": "yes"},
        )
        self.assertEqual(result.exit_code, 0, "exit code")

    def test_unicode_directory(self) -> None:
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])

        _, result = run_icloudpd_test(
            self.assertEqual,
            self.root_path,
            base_dir,
            "listing_photos.yml",
            [],
            [],
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "--recent",
                "0",
                "--log-level",
                "info",
            ],
        )
        self.assertEqual(result.exit_code, 0, "exit code")

    def test_missing_directory(self) -> None:
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])
        # need path removed
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)

        result = run_main(
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "--recent",
                "0",
                "--log-level",
                "info",
                "-d",
                base_dir,
            ],
        )
        self.assertEqual(result.exit_code, 2, "exit code")

        self.assertFalse(os.path.exists(base_dir), f"{base_dir} exists")

    def test_missing_directory_param(self) -> None:
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])
        result = run_main(
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "--recent",
                "0",
                "--log-level",
                "info",
            ],
        )
        self.assertEqual(result.exit_code, 2, "exit code")

        self.assertFalse(os.path.exists(base_dir), f"{base_dir} exists")

    def test_conflict_options_delete_after_download_and_auto_delete(self) -> None:
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])
        result = run_main(
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "-d",
                "/tmp",
                "--delete-after-download",
                "--auto-delete",
            ],
        )
        self.assertEqual(result.exit_code, 2, "exit code")

        self.assertFalse(os.path.exists(base_dir), f"{base_dir} exists")

    def test_conflict_options_delete_after_download_and_keep_icloud_recent_days(self) -> None:
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])
        result = run_main(
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "-d",
                "/tmp",
                "--delete-after-download",
                "--keep-icloud-recent-days",
                "1",
            ],
        )
        self.assertEqual(result.exit_code, 2, "exit code")

        self.assertFalse(os.path.exists(base_dir), f"{base_dir} exists")

    def test_invalid_album_page_size(self) -> None:
        result = run_main(
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "-d",
                "/tmp",
                "--album-page-size",
                "0",
            ],
        )
        self.assertEqual(result.exit_code, 2, "exit code")

    def test_invalid_download_workers(self) -> None:
        result = run_main(
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "-d",
                "/tmp",
                "--download-workers",
                "0",
            ],
        )
        self.assertEqual(result.exit_code, 2, "exit code")

    def test_invalid_state_db_prune_completed_days(self) -> None:
        result = run_main(
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "-d",
                "/tmp",
                "--state-db-prune-completed-days",
                "0",
            ],
        )
        self.assertEqual(result.exit_code, 2, "exit code")

    def test_metrics_json_export(self) -> None:
        base_dir = os.path.join(self.fixtures_path, inspect.stack()[0][3])
        metrics_path = os.path.join(base_dir, "metrics.json")
        if os.path.exists(base_dir):
            shutil.rmtree(base_dir)
        os.makedirs(base_dir, exist_ok=True)

        _data_dir, result = run_icloudpd_test(
            self.assertEqual,
            self.root_path,
            base_dir,
            "listing_photos.yml",
            [],
            [],
            [
                "--username",
                "jdoe@gmail.com",
                "--password",
                "password1",
                "--auth-only",
                "--metrics-json",
                metrics_path,
            ],
        )
        self.assertEqual(result.exit_code, 0, "exit code")
        self.assertTrue(os.path.exists(metrics_path))
        with open(metrics_path, encoding="utf-8") as f:
            payload = json.load(f)
        self.assertIn("exit_code", payload)
        self.assertIn("status", payload)
        self.assertEqual(payload["exit_code"], 0)
        self.assertIn(payload["status"], ["success", "partial_success"])
        self.assertIn("users", payload)
        self.assertTrue(len(payload["users"]) >= 1)
        self.assertIn("username", payload["users"][0])
