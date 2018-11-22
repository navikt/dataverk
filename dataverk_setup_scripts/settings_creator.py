import re
import json
import os
from . import settings_template


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

    def _validate_datapackage_name(self, name):
        ''' Kontrollerer at pakkenavnet valgt består av kun små bokstaver og tall, ord separert med '-', og at det ikke
            starter eller slutter med '-'

        :param name: String med pakkenavn som skal valideres
        '''

        valid_name_pattern = '(^[a-z0-9])([a-z0-9\-])+([a-z0-9])$'
        if not re.match(pattern=valid_name_pattern, string=name):
            raise NameError(f'Ulovlig datapakkenavn ({name}): '
                            'Må være små bokstaver eller tall, ord separert med "-", og kan ikke starte eller ende med "-"')

    def _validate_cronjob_schedule(self, schedule):
        ''' Kontrollerer brukerinput for cronjob schedule
                Format på cronjob schedule string: "* * * * *"
                "minutt" "time" "dag i måned" "måned i år" "ukedag"
                 (0-59)  (0-23)    (1-31)        (1-12)     (0-6)
        '''

        schedule_list = schedule.split(' ')

        if not len(schedule_list) is 5:
            raise ValueError(
                f'Schedule {schedule} har ikke riktig format. Må ha formatet: "<minutt> <time> <dag i måned> <måned> <ukedag>". '
                f'F.eks. "0 12 * * 2,4" vil gi: Hver tirsdag og torsdag kl 12.00 UTC')

        if not schedule_list[0] is '*':
            for minute in schedule_list[0].split(','):
                if not int(minute) in range(0, 60):
                    raise ValueError(f'I schedule {schedule} er ikke {minute}'
                                     f' en gyldig verdi for minutt innenfor time. Gyldige verdier er 0-59, eller *')

        if not schedule_list[1] is '*':
            for hour in schedule_list[1].split(','):
                if not int(hour) in range(0, 24):
                    raise ValueError(f'I schedule {schedule} er ikke {hour}'
                                     f' en gyldig verdi for time. Gyldige verdier er 0-23, eller *')

        if not schedule_list[2] is '*':
            for day in schedule_list[2].split(','):
                if not int(day) in range(1, 32):
                    raise ValueError(f'I schedule {schedule} er ikke {day}'
                                     f' en gyldig verdi for dag i måned. Gyldige verdier er 1-31, eller *')

        if not schedule_list[3] is '*':
            for month in schedule_list[3].split(','):
                if not int(month) in range(1, 13):
                    raise ValueError(f'I schedule {schedule} er ikke {month}'
                                     f' en gyldig verdi for måned. Gyldige verdier er 1-12, eller *')

        if not schedule_list[4] is '*':
            for weekday in schedule_list[4].split(','):
                if not int(weekday) in range(0, 7):
                    raise ValueError(f'I schedule {schedule} er ikke {weekday}'
                                     f' en gyldig verdi for ukedag. Gyldige verdier er 0-6 (søn-lør), eller *')

    def create_settings(self):
        if self.args.package_name is None:
            self.settings["package_name"] = self._prompt_for_user_input(arg="pakkenavn")
        else:
            self.settings["package_name"] = self.args.package_name

        self._validate_datapackage_name(self.settings["package_name"])

        if self.args.update_schedule is None:
            self.settings["update_schedule"] = self._prompt_for_user_input(arg="update schedule")
        else:
            self.settings["update_schedule"] = self.args.update_schedule

        self._validate_cronjob_schedule(self.settings["update_schedule"])

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
