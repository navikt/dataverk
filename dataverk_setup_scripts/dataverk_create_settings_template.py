import os
import json

SETTINGS_TEMPLATE={
  "index_connections": {
    "elastic_local": "http://localhost:1010"
  },

  "file_storage_connections": {
    "local": {
        "path": ".",
        "bucket": ""
    }
  },

  "bucket_storage_connections": {
    "AWS_S3": {
        "host": "https://some.test.url.no",
        "bucket": "default-bucket-test"
    },
    "google_cloud": {
        "client": "tester-client",
        "bucket": "default-bucket-test",
        "credentials": {
            "type": "test_type",
            "project_id": "test_id",
            "private_key_id": "testtestkeyid1010",
            "client_email": "test@test.tester.com",
            "client_id": "test1010test",
            "auth_uri": "https://test/test/oauth2/auth",
            "token_uri": "https://test.test.com/token",

            "auth_provider_x509_cert_url": "https://www.test.test/oauth2/v10/certs"
        }
    }
  },
    "template_repository":  "https://github.com/path/to/templates",

  "vault": {
    "auth_uri": "https://test.test.no:1010/v12/test/test/test/",
    "secrets_uri": "https://test.test.no:1010/v12/test/test/test/test/test"
  },

  "jenkins": {
    "url": "http://jenkins.host.url:port"
  }
}


class CreateSettingsTemplate:
    ''' Klasse for å generere template fil for settings.json

    '''

    def __init__(self, destination: str=None):
        if destination is not None:
            self._verify_destination(destination)
            try:
                with open(os.path.join(destination, "settings.json"), 'w') as settings_file:
                    json.dump(SETTINGS_TEMPLATE, settings_file, indent=2)
            except OSError:
                raise OSError(f'Klarte ikke generere ny settings.json fil')
        else:
            try:
                with open("settings.json", 'w') as settings_file:
                    json.dump(SETTINGS_TEMPLATE, settings_file, indent=2)
            except OSError:
                raise OSError(f'Klarte ikke generere ny settings.json fil')

    def _verify_destination(self, path):
        if not isinstance(path, str):
            raise TypeError(f'Sti for lagring av settingsfil må være av type string')
        elif not os.path.exists(path=path):
            raise ValueError(f'Ønsket sti for lagring av settings fil eksisterer ikke')
        elif not os.path.isdir(path=path):
            raise ValueError(f'Ønsket sti er ikke en mappe')

def run(destination: str=None):
    CreateSettingsTemplate(destination=destination)
