# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import json
from dataverk.context.replacer import Replacer
from dataverk.utils.windows_safe_tempdir import WindowsSafeTempDirectory
from dataverk.context.secrets_importer import get_secrets_importer
from pathlib import Path
# Common input parameters
# =======================
TEST_SETTINGS_CONTENT = {
    "value1": "${REPLACE_ME1}",
    "value2": {
        "value3": "${REPLACE_ME2}"
    },
    "auth_method": "auth",
    "secret_path": "",
    "remote_secrets_url": "https://vault.no:443/path/to/secrets"
}

REPLACE_ME1 = "replaced_value_1"
REPLACE_ME2 = "replaced_value_2"


# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.tmp_secrets_dir = self._create_tmp_secrets_dir()
        self.settings = TEST_SETTINGS_CONTENT
        self.settings["secret_path"] = self.tmp_secrets_dir.name

    def tearDown(self):
        self.tmp_secrets_dir.cleanup()

    def _create_tmp_secrets_dir(self):
        tmp_secrets_dir = WindowsSafeTempDirectory()
        secret_file_1_path = Path(tmp_secrets_dir.name).joinpath("REPLACE_ME1").absolute()
        secret_file_2_path = Path(tmp_secrets_dir.name).joinpath("REPLACE_ME2").absolute()

        with Path(secret_file_1_path).open('w') as secret_file_1:
            secret_file_1.write(REPLACE_ME1)

        with Path(secret_file_2_path).open('w') as secret_file_2:
            secret_file_2.write(REPLACE_ME2)

        return tmp_secrets_dir


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_get_filled_mapping(self):
        env_store = {"SECRETS_FROM_FILES": "True"}
        secrets_importer = get_secrets_importer(self.settings, env_store)

        replacer = Replacer(secrets_importer)
        settings_with_secrets = replacer.get_filled_mapping(json.dumps(self.settings))

        self.assertEqual(settings_with_secrets["value1"], REPLACE_ME1)
        self.assertEqual(settings_with_secrets["value2"]["value3"], REPLACE_ME2)

