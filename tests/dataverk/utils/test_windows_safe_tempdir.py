from unittest import TestCase
from pathlib import Path

from dataverk.utils.windows_safe_tempdir import WindowsSafeTempDirectory


class TestWindowsSafeTempDirectory(TestCase):

    def test_cleanup(self):
        """
        Tests that the Class cleans up the temp directory created in the filesystem
        :return: None
        """
        t = WindowsSafeTempDirectory()
        path_to_dir = Path(t.name)
        t.cleanup()

        self.assertFalse(path_to_dir.exists(),
                         f"TemporaryDictionary should have been cleaned up!, please remove folder at: f{path_to_dir}")