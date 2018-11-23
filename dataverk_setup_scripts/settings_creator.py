import json
import os
from . import settings_template
from dataverk.utils.validators import validate_datapackage_name, validate_cronjob_schedule


class SettingsCreator:
    ''' Baseklasse for å generere dataverk settings
    '''

    def __init__(self, args):
        self.args = args
        self.settings = {}

    def _handle_missing_argument(self, arg: str):
        raise NotImplementedError("Dette er en abstrakt metode")

    def _prompt_for_user_input(self, arg):
        return input(f'Skriv inn ønsket {arg}: ')

    def _populate_settings(self):
        if self.args.nais_namespace is None:
            self.settings["nais_namespace"] = self._handle_missing_argument("nais_namespace")
        else:
            self.settings["nais_namespace"] = self.args.nais_namespace

        if self.args.elastic_endpoint is None:
            self.settings["index_connections"]["elastic_local"] = self._handle_missing_argument("elastic_endpoint")
        else:
            self.settings["index_connections"]["elastic_local"] = self.args.elastic_endpoint

        if self.args.aws_endpoint is None:
            self.settings["bucket_storage_connections"]["AWS_S3"]["host"] = self._handle_missing_argument("aws_endpoint")
        else:
            self.settings["bucket_storage_connections"]["AWS_S3"]["host"] = self.args.aws_endpoint

        if self.args.jenkins_endpoint is None:
            self.settings["jenkins"]["url"] = self._handle_missing_argument("jenkins_endpoint")
        else:
            self.settings["jenkins"]["url"] = self.args.jenkins_endpoint

        if self.args.vault_auth_uri is None:
            self.settings["vault"]["auth_uri"] = self._handle_missing_argument("vault_auth_uri")
        else:
            self.settings["vault"]["auth_uri"] = self.args.vault_auth_uri

        if self.args.vault_secrets_uri is None:
            self.settings["vault"]["secrets_uri"] = self._handle_missing_argument("vault_secrets_uri")
        else:
            self.settings["vault"]["secrets_uri"] = self.args.vault_secrets_uri

        if self.args.vault_auth_path is None:
            self.settings["vault"]["vks_auth_path"] = self._handle_missing_argument("vault_auth_path")
        else:
            self.settings["vault"]["vks_auth_path"] = self.args.vault_auth_path

        if self.args.vault_kv_path is None:
            self.settings["vault"]["vks_kv_path"] = self._handle_missing_argument("vault_kv_path")
        else:
            self.settings["vault"]["vks_kv_path"] = self.args.vault_kv_path

        if self.args.vault_role is None:
            self.settings["vault"]["vks_vault_role"] = self._handle_missing_argument("vault_role")
        else:
            self.settings["vault"]["vks_vault_role"] = self.args.vault_role

        if self.args.vault_service_account is None:
            self.settings["vault"]["service_account"] = self._handle_missing_argument("vault_service_account")
        else:
            self.settings["vault"]["service_account"] = self.args.vault_service_account

    def create_settings(self):
        if self.args.package_name is None:
            self.settings["package_name"] = self._prompt_for_user_input(arg="pakkenavn")
        else:
            self.settings["package_name"] = self.args.package_name

        validate_datapackage_name(self.settings["package_name"])

        if self.args.update_schedule is None:
            self.settings["update_schedule"] = self._prompt_for_user_input(arg="update schedule")
        else:
            self.settings["update_schedule"] = self.args.update_schedule

        validate_cronjob_schedule(self.settings["update_schedule"])

        self._populate_settings()

        return self.settings


class SettingsCreatorNoDefaults(SettingsCreator):
    ''' Klasse for å generere dataverk settings uten default verdier
    '''

    def __init__(self, args):
        super().__init__(args)

        self.settings = settings_template.SETTINGS_TEMPLATE

    def _handle_missing_argument(self, arg: str):
        return self._prompt_for_user_input(arg=arg)


class SettingsCreatorUseDefaults(SettingsCreator):
    ''' Klasse for å generere dataverk settings med default verdier hvis ikke annet spesifiseres
    '''

    def __init__(self, args, default_settings_path):
        super().__init__(args)

        with open(os.path.join(default_settings_path, 'settings.json'), 'r') as settings_file:
            self.default_settings = json.load(settings_file)
        self.settings = self.default_settings

    def _get_default(self, key: str, default_settings):
        if key not in default_settings:
            raise Exception(f'{key} finnes ikke i default settings.json')
        else:
            return default_settings[key]

    def _handle_missing_argument(self, arg: str):
        if arg is "nais_namespace":
            return self._get_default(key="nais_namespace", default_settings=self.default_settings)
        elif arg is "elastic_endpoint":
            return self._get_default(key="elastic_local", default_settings=self.default_settings["index_connections"])
        elif arg is "aws_endpoint":
            return self._get_default(key="host", default_settings=self.default_settings["bucket_storage_connections"]["AWS_S3"])
        elif arg is "jenkins_endpoint":
            return self._get_default(key="url", default_settings=self.default_settings["jenkins"])
        elif arg is "vault_auth_uri":
            return self._get_default(key="auth_uri", default_settings=self.default_settings["vault"])
        elif arg is "vault_secrets_uri":
            return self._get_default(key="secrets_uri", default_settings=self.default_settings["vault"])
        elif arg is "vault_auth_path":
            return self._get_default(key="vks_auth_path", default_settings=self.default_settings["vault"])
        elif arg is "vault_kv_path":
            return self._get_default(key="vks_kv_path", default_settings=self.default_settings["vault"])
        elif arg is "vault_role":
            return self._get_default(key="vks_vault_role", default_settings=self.default_settings["vault"])
        elif arg is "vault_service_account":
            return self._get_default(key="service_account", default_settings=self.default_settings["vault"])
        else:
            raise ValueError(f'{arg} is not a valid argument')


def get_settings_creator(args, default_settings_path: str=None) -> type(SettingsCreator):
    if args.use_defaults and default_settings_path is not None:
        return SettingsCreatorUseDefaults(args=args, default_settings_path=default_settings_path)
    else:
        return SettingsCreatorNoDefaults(args=args)
