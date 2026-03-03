import datetime
import os
import tempfile
import tracemalloc
from base64 import b32encode, b64encode
from unittest import TestCase, mock
from unittest.mock import MagicMock

from icloudpd import download
from icloudpd.limiter import AdaptiveDownloadLimiter
from pyicloud_ipd.asset_version import AssetVersion
from pyicloud_ipd.version_size import AssetVersionSize


class _FakeResponse:
    def __init__(self, *, ok: bool, status_code: int, body_chunks: list[bytes] | None = None) -> None:
        self.ok = ok
        self.status_code = status_code
        self.headers: dict[str, str] = {}
        self._body_chunks = body_chunks or []

    def iter_content(self, chunk_size: int) -> list[bytes]:  # noqa: ARG002
        return self._body_chunks


class _FakePhoto:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.created = datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc)
        self._responses = list(responses)
        self.download_calls: list[tuple[str, int]] = []

    def download(self, _session: object, url: str, current_size: int) -> _FakeResponse:
        self.download_calls.append((url, current_size))
        return self._responses.pop(0)


class _LargeStreamingResponse:
    def __init__(self, total_bytes: int) -> None:
        self._total_bytes = total_bytes

    def iter_content(self, chunk_size: int):
        sent = 0
        while sent < self._total_bytes:
            take = min(chunk_size, self._total_bytes - sent)
            sent += take
            yield b"x" * take


class DownloadConfigTestCase(TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.mkdtemp(prefix="icloudpd-download-config-")
        download.set_download_chunk_bytes(262144)
        download.set_download_verification(verify_size=True, verify_checksum=False)
        download.set_download_limiter(None)

    def tearDown(self) -> None:
        for entry in os.listdir(self._tmpdir):
            os.remove(os.path.join(self._tmpdir, entry))
        os.rmdir(self._tmpdir)

    def test_download_response_uses_configured_chunk_size(self) -> None:
        download.set_download_chunk_bytes(65536)

        response = MagicMock()
        response.iter_content.return_value = [b"abc", b"def"]

        temp_path = "/tmp/icloudpd-download-chunk-test.part"
        final_path = "/tmp/icloudpd-download-chunk-test.bin"
        for path in [temp_path, final_path]:
            if os.path.exists(path):
                os.remove(path)

        ok = download.download_response_to_path(
            response,
            temp_path,
            append_mode=False,
            download_path=final_path,
            created_date=datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc),
        )
        self.assertTrue(ok)
        response.iter_content.assert_called_once_with(chunk_size=65536)
        self.assertTrue(os.path.exists(final_path))

    def test_download_response_streaming_memory_is_bounded(self) -> None:
        download.set_download_chunk_bytes(65536)
        response = _LargeStreamingResponse(total_bytes=64 * 1024 * 1024)

        temp_path = os.path.join(self._tmpdir, "bounded-stream.part")
        final_path = os.path.join(self._tmpdir, "bounded-stream.bin")

        tracemalloc.start()
        ok = download.download_response_to_path(
            response,
            temp_path,
            append_mode=False,
            download_path=final_path,
            created_date=datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc),
        )
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.assertTrue(ok)
        self.assertEqual(os.path.getsize(final_path), 64 * 1024 * 1024)
        self.assertLess(peak, 8 * 1024 * 1024)

    def test_download_media_verifies_size(self) -> None:
        download.set_download_verification(verify_size=True, verify_checksum=False)

        photo = MagicMock()
        photo.created = datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc)
        response = MagicMock()
        response.ok = True
        response.status_code = 200
        response.iter_content.return_value = [b"abc"]
        photo.download.return_value = response

        icloud = MagicMock()
        download_path = os.path.join(self._tmpdir, "test-size.jpg")
        checksum = b64encode(b"0123456789abcdef").decode()

        ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=download_path,
            version=AssetVersion(size=3, url="https://example.test/file", type="public.jpeg", checksum=checksum),
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "test-size.jpg",
        )
        self.assertTrue(ok)
        self.assertTrue(os.path.exists(download_path))

        bad_path = os.path.join(self._tmpdir, "test-size-mismatch.jpg")
        bad_ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=bad_path,
            version=AssetVersion(size=4, url="https://example.test/file", type="public.jpeg", checksum=checksum),
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "test-size-mismatch.jpg",
        )
        self.assertFalse(bad_ok)
        self.assertFalse(os.path.exists(bad_path))

    def test_download_media_verifies_checksum(self) -> None:
        download.set_download_verification(verify_size=False, verify_checksum=True)

        photo = MagicMock()
        photo.created = datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc)
        response = MagicMock()
        response.ok = True
        response.status_code = 200
        response.iter_content.return_value = [b"abc"]
        photo.download.return_value = response

        icloud = MagicMock()
        download_path = os.path.join(self._tmpdir, "test-checksum.jpg")
        matching_checksum = b64encode(b"\x90\x01P\x98<\xd2O\xb0\xd6\x96?}(\xe1\x7fr").decode()

        ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=download_path,
            version=AssetVersion(
                size=3,
                url="https://example.test/file",
                type="public.jpeg",
                checksum=matching_checksum,
            ),
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "test-checksum.jpg",
        )
        self.assertTrue(ok)
        self.assertTrue(os.path.exists(download_path))

        bad_path = os.path.join(self._tmpdir, "test-checksum-mismatch.jpg")
        bad_checksum = b64encode(b"0000000000000000").decode()
        bad_ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=bad_path,
            version=AssetVersion(
                size=3,
                url="https://example.test/file",
                type="public.jpeg",
                checksum=bad_checksum,
            ),
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "test-checksum-mismatch.jpg",
        )
        self.assertFalse(bad_ok)
        self.assertFalse(os.path.exists(bad_path))

    def test_download_media_restarts_partial_when_range_ignored(self) -> None:
        download.set_download_verification(verify_size=True, verify_checksum=False)

        photo = MagicMock()
        photo.created = datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc)
        response = MagicMock()
        response.ok = True
        response.status_code = 200
        response.iter_content.return_value = [b"new"]
        photo.download.return_value = response

        icloud = MagicMock()
        checksum = b64encode(b"0123456789abcdef").decode()
        version = AssetVersion(size=3, url="https://example.test/file", type="public.jpeg", checksum=checksum)
        checksum_raw = b"0123456789abcdef"
        temp_download_path = os.path.join(
            self._tmpdir, b32encode(checksum_raw).decode() + ".part"
        )
        with open(temp_download_path, "wb") as part_file:
            part_file.write(b"old-bytes")

        download_path = os.path.join(self._tmpdir, "range-restart.jpg")
        ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=download_path,
            version=version,
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "range-restart.jpg",
        )
        self.assertTrue(ok)
        photo.download.assert_called_once_with(icloud.photos.session, version.url, 9)
        with open(download_path, "rb") as downloaded:
            self.assertEqual(downloaded.read(), b"new")

    def test_download_media_resumes_when_server_returns_partial_content(self) -> None:
        download.set_download_verification(verify_size=True, verify_checksum=False)

        checksum_raw = b"0123456789abcdef"
        checksum = b64encode(checksum_raw).decode()
        version = AssetVersion(size=6, url="https://example.test/file", type="public.jpeg", checksum=checksum)
        temp_download_path = os.path.join(
            self._tmpdir, b32encode(checksum_raw).decode() + ".part"
        )
        with open(temp_download_path, "wb") as part_file:
            part_file.write(b"old")

        photo = _FakePhoto(
            responses=[_FakeResponse(ok=True, status_code=206, body_chunks=[b"new"])]
        )
        icloud = MagicMock()
        download_path = os.path.join(self._tmpdir, "range-resume.jpg")
        ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=download_path,
            version=version,
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "range-resume.jpg",
        )
        self.assertTrue(ok)
        self.assertEqual(photo.download_calls, [(version.url, 3)])
        with open(download_path, "rb") as downloaded:
            self.assertEqual(downloaded.read(), b"oldnew")

    def test_download_media_restarts_partial_when_server_returns_416(self) -> None:
        download.set_download_verification(verify_size=True, verify_checksum=False)

        checksum_raw = b"0123456789abcdef"
        checksum = b64encode(checksum_raw).decode()
        version = AssetVersion(size=3, url="https://example.test/file", type="public.jpeg", checksum=checksum)
        temp_download_path = os.path.join(
            self._tmpdir, b32encode(checksum_raw).decode() + ".part"
        )
        with open(temp_download_path, "wb") as part_file:
            part_file.write(b"stale-partial")

        photo = _FakePhoto(
            responses=[
                _FakeResponse(ok=False, status_code=416),
                _FakeResponse(ok=True, status_code=200, body_chunks=[b"new"]),
            ]
        )
        icloud = MagicMock()
        download_path = os.path.join(self._tmpdir, "range-416-restart.jpg")
        ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=download_path,
            version=version,
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "range-416-restart.jpg",
        )
        self.assertTrue(ok)
        self.assertEqual(photo.download_calls, [(version.url, 13), (version.url, 0)])
        with open(download_path, "rb") as downloaded:
            self.assertEqual(downloaded.read(), b"new")

    def test_download_media_throttling_updates_limiter(self) -> None:
        limiter = AdaptiveDownloadLimiter(max_workers=4, cooldown_seconds=0.0)
        download.set_download_limiter(limiter)
        download.set_retry_config(
            retry_config=download.RetryConfig(
                max_retries=0,
                backoff_base_seconds=1.0,
                backoff_max_seconds=1.0,
                respect_retry_after=True,
                throttle_cooldown_seconds=0.0,
            )
        )

        photo = MagicMock()
        photo.created = datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc)
        response = MagicMock()
        response.ok = False
        response.status_code = 429
        response.headers = {}
        photo.download.return_value = response

        icloud = MagicMock()
        checksum = b64encode(b"0123456789abcdef").decode()

        ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=os.path.join(self._tmpdir, "throttle.jpg"),
            version=AssetVersion(size=3, url="https://example.test/file", type="public.jpeg", checksum=checksum),
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "throttle.jpg",
        )
        self.assertFalse(ok)
        self.assertEqual(limiter.current_limit, 1)

    def test_download_media_low_disk_space_classification(self) -> None:
        download.set_download_verification(verify_size=False, verify_checksum=False)
        photo = MagicMock()
        photo.created = datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc)
        icloud = MagicMock()
        checksum = b64encode(b"0123456789abcdef").decode()
        with mock.patch("icloudpd.download.has_disk_space_for_download", return_value=False):
            ok = download.download_media(
                logger=MagicMock(),
                dry_run=False,
                icloud=icloud,
                photo=photo,
                download_path=os.path.join(self._tmpdir, "low-disk.jpg"),
                version=AssetVersion(
                    size=10_000_000,
                    url="https://example.test/file",
                    type="public.jpeg",
                    checksum=checksum,
                ),
                size=AssetVersionSize.ORIGINAL,
                filename_builder=lambda _p: "low-disk.jpg",
            )
        self.assertFalse(ok)
        photo.download.assert_not_called()

    def test_download_media_refreshes_expired_url_once(self) -> None:
        download.set_download_verification(verify_size=False, verify_checksum=False)
        photo = MagicMock()
        photo.created = datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc)
        first = MagicMock()
        first.ok = False
        first.status_code = 403
        first.headers = {}
        second = MagicMock()
        second.ok = True
        second.status_code = 200
        second.iter_content.return_value = [b"abc"]
        photo.download.side_effect = [first, second]

        icloud = MagicMock()
        checksum = b64encode(b"0123456789abcdef").decode()
        original = AssetVersion(size=3, url="https://example.test/stale", type="public.jpeg", checksum=checksum)
        refreshed = AssetVersion(
            size=3, url="https://example.test/fresh", type="public.jpeg", checksum=checksum
        )
        refresh_cb = MagicMock(return_value=refreshed)

        ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=os.path.join(self._tmpdir, "refresh-url.jpg"),
            version=original,
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "refresh-url.jpg",
            refresh_version=refresh_cb,
        )
        self.assertTrue(ok)
        self.assertEqual(photo.download.call_args_list[0].args[1], "https://example.test/stale")
        self.assertEqual(photo.download.call_args_list[1].args[1], "https://example.test/fresh")
        refresh_cb.assert_called_once()
        self.assertFalse(download.consume_url_refresh_needed_signal())

    def test_download_media_sets_url_refresh_signal_when_unrecoverable(self) -> None:
        download.set_download_verification(verify_size=False, verify_checksum=False)
        photo = MagicMock()
        photo.created = datetime.datetime(2026, 3, 2, tzinfo=datetime.timezone.utc)
        response = MagicMock()
        response.ok = False
        response.status_code = 410
        response.headers = {}
        photo.download.return_value = response

        icloud = MagicMock()
        checksum = b64encode(b"0123456789abcdef").decode()
        version = AssetVersion(size=3, url="https://example.test/stale", type="public.jpeg", checksum=checksum)
        refresh_cb = MagicMock(return_value=None)

        ok = download.download_media(
            logger=MagicMock(),
            dry_run=False,
            icloud=icloud,
            photo=photo,
            download_path=os.path.join(self._tmpdir, "refresh-url-fail.jpg"),
            version=version,
            size=AssetVersionSize.ORIGINAL,
            filename_builder=lambda _p: "refresh-url-fail.jpg",
            refresh_version=refresh_cb,
        )
        self.assertFalse(ok)
        self.assertTrue(download.consume_url_refresh_needed_signal())
