import glob
import inspect
import os
import sqlite3
from unittest import TestCase

import pytest

from tests.helpers import path_from_project_root, run_icloudpd_test


class ModeContractTestCase(TestCase):
    @pytest.fixture(autouse=True)
    def inject_fixtures(self) -> None:
        self.root_path = path_from_project_root(__file__)
        self.fixtures_path = os.path.join(self.root_path, "fixtures")

    def _run_copy_mode(self, *, use_state_db: bool, base_dir_suffix: str) -> tuple[str, str]:
        base_dir = os.path.join(
            self.fixtures_path,
            inspect.stack()[0][3],
            base_dir_suffix,
        )
        files_to_create = [
            ("2018/07/30", "IMG_7408.JPG", 1151066),
            ("2018/07/30", "IMG_7407.JPG", 656257),
        ]
        files_to_download = [("2018/07/31", "IMG_7409.JPG")]
        params = [
            "--username",
            "jdoe@gmail.com",
            "--password",
            "password1",
            "--recent",
            "5",
            "--skip-videos",
            "--skip-live-photos",
            "--set-exif-datetime",
            "--no-progress-bar",
            "--threads-num",
            "1",
        ]
        if use_state_db:
            params.append("--state-db")
        data_dir, result = run_icloudpd_test(
            self.assertEqual,
            self.root_path,
            base_dir,
            "listing_photos.yml",
            files_to_create,
            files_to_download,
            params,
        )
        self.assertEqual(result.exit_code, 0)
        cookie_dir = os.path.join(base_dir, "cookie")
        return data_dir, cookie_dir

    def test_legacy_stateless_mode_contract(self) -> None:
        _data_dir, cookie_dir = self._run_copy_mode(use_state_db=False, base_dir_suffix="legacy")
        self.assertFalse(os.path.exists(os.path.join(cookie_dir, "icloudpd.sqlite")))

    def test_stateful_engine_mode_contract(self) -> None:
        _data_dir, cookie_dir = self._run_copy_mode(use_state_db=True, base_dir_suffix="stateful")
        db_path = os.path.join(cookie_dir, "icloudpd.sqlite")
        self.assertTrue(os.path.exists(db_path))
        with sqlite3.connect(db_path) as conn:
            task_count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            checkpoint_count = conn.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0]
        self.assertGreater(task_count, 0)
        self.assertGreater(checkpoint_count, 0)

    def test_mode_parity_for_downloaded_files(self) -> None:
        legacy_data_dir, _legacy_cookie_dir = self._run_copy_mode(
            use_state_db=False, base_dir_suffix="parity-legacy"
        )
        stateful_data_dir, _stateful_cookie_dir = self._run_copy_mode(
            use_state_db=True, base_dir_suffix="parity-stateful"
        )

        def relative_tree(root: str) -> list[str]:
            files = glob.glob(os.path.join(root, "**/*.*"), recursive=True)
            return sorted(os.path.relpath(path, root) for path in files)

        self.assertEqual(relative_tree(legacy_data_dir), relative_tree(stateful_data_dir))
