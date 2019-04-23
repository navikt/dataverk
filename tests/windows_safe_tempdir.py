import os
import shutil
import stat
from tempfile import TemporaryDirectory


class WindowsSafeTempDirectory(TemporaryDirectory):
    """
    TemporaryDirectory subclass that ensures that the temporary directory in the file system is removed when cleanup()
    is called. The regular TemporaryDirectory class can fail on Windows in certain cases.
    """

    def cleanup(self):
        try:
            self._win_readonly_directories_force_remove(self.name)
        except FileNotFoundError:
            pass # Files are removed

    @classmethod
    def _cleanup(cls, name, warn_message):
        try:
            return super()._cleanup(name, warn_message)
        except FileNotFoundError:
            pass # Removed

    def _win_readonly_directories_force_remove(self, directory):
        """ Removes directories with read-only files"""
        shutil.rmtree(directory, onerror=self._remove_readonly)

    @staticmethod
    def _remove_readonly(function, path,_ ):
        "Clear the readonly bit and reattempt the removal"
        os.chmod(path, stat.S_IWRITE)
        function(path)
