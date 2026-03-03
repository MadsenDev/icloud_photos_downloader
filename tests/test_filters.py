import datetime
from unittest import TestCase
from unittest.mock import MagicMock

from icloudpd.base import where_builder
from pyicloud_ipd.item_type import AssetItemType


class FilterTestCase(TestCase):
    def test_skip_added_before_filters_older_added_date(self) -> None:
        photo = MagicMock()
        photo.item_type = AssetItemType.IMAGE
        photo.created = datetime.datetime(2026, 1, 10, tzinfo=datetime.timezone.utc)
        photo.added_date = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)

        result = where_builder(
            logger=MagicMock(),
            skip_videos=False,
            skip_created_before=None,
            skip_created_after=None,
            skip_added_before=datetime.datetime(2026, 1, 5, tzinfo=datetime.timezone.utc),
            skip_added_after=None,
            skip_photos=False,
            filename_builder=lambda _photo: "file.jpg",
            photo=photo,
        )
        self.assertFalse(result)

    def test_skip_added_after_filters_newer_added_date(self) -> None:
        photo = MagicMock()
        photo.item_type = AssetItemType.IMAGE
        photo.created = datetime.datetime(2026, 1, 10, tzinfo=datetime.timezone.utc)
        photo.added_date = datetime.datetime(2026, 1, 10, tzinfo=datetime.timezone.utc)

        result = where_builder(
            logger=MagicMock(),
            skip_videos=False,
            skip_created_before=None,
            skip_created_after=None,
            skip_added_before=None,
            skip_added_after=datetime.datetime(2026, 1, 5, tzinfo=datetime.timezone.utc),
            skip_photos=False,
            filename_builder=lambda _photo: "file.jpg",
            photo=photo,
        )
        self.assertFalse(result)

    def test_skip_added_filters_are_ignored_when_added_date_missing(self) -> None:
        class _PhotoWithoutAddedDate:
            item_type = AssetItemType.IMAGE
            created = datetime.datetime(2026, 1, 10, tzinfo=datetime.timezone.utc)

            @property
            def added_date(self) -> datetime.datetime:
                raise KeyError("addedDate")

        photo = _PhotoWithoutAddedDate()

        result = where_builder(
            logger=MagicMock(),
            skip_videos=False,
            skip_created_before=None,
            skip_created_after=None,
            skip_added_before=datetime.datetime(2026, 1, 5, tzinfo=datetime.timezone.utc),
            skip_added_after=datetime.datetime(2026, 1, 15, tzinfo=datetime.timezone.utc),
            skip_photos=False,
            filename_builder=lambda _photo: "file.jpg",
            photo=photo,
        )
        self.assertTrue(result)
