import json
import os
import functools
from .settings_template import SETTINGS_TEMPLATE, optional_parameters
from dataverk.utils.validators import validate_datapackage_name, validate_cronjob_schedule
from abc import ABC


class SettingsCreator(ABC):
    ''' Abstrakt baseklasse for å generere dataverk settings
    '''

    def __init__(self, args):
        self.args = args
        self.settings = {}

    def _handle_missing_argument(self, arg: str):
        raise NotImplementedError("Abstrakt metode")

    def _prompt_for_user_input(self, arg):
        return input(f'Skriv inn ønsket {arg}: ')

    def _set_settings_param(self, keys_tuple: tuple, value: str):
        functools.reduce(lambda settings_dict, key: settings_dict.setdefault(key, {}),
                         keys_tuple[:-1], self.settings)[keys_tuple[-1]] = value

    def _populate_settings(self):
        for param in optional_parameters:
            if getattr(self.args, param) is None:
                self._set_settings_param(optional_parameters[param], self._handle_missing_argument(param))
            else:
                self._set_settings_param(optional_parameters[param], getattr(self.args, param))

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

        self.settings = SETTINGS_TEMPLATE

    def _handle_missing_argument(self, arg: str):
        return self._prompt_for_user_input(arg=arg)


class SettingsCreatorUseDefaults(SettingsCreator):
    ''' Klasse for å generere dataverk settings med default verdier hvis ikke annet spesifiseres
    '''

    def __init__(self, args, default_settings_path):
        super().__init__(args)

        self.default_settings_path = default_settings_path
        with open(os.path.join(self.default_settings_path, 'settings.json'), 'r') as settings_file:
            self.default_settings = json.load(settings_file)
        self.settings = self.default_settings

    def _get_default(self, keys_list: tuple):
        for key in keys_list:
            try:
                default_value = self.settings[key]
            except KeyError:
                print(f'Key {key} is not found in {os.path.join(self.default_settings_path, "settings.json")}')
                return ""
        return default_value

    def _handle_missing_argument(self, arg: str):
        return self._get_default(optional_parameters[arg])


def get_settings_creator(args, default_settings_path: str=None) -> type(SettingsCreator):
    if args.prompt_missing_args:
        return SettingsCreatorNoDefaults(args=args)
    elif default_settings_path is not None:
        return SettingsCreatorUseDefaults(args=args, default_settings_path=default_settings_path)
    else:
        raise Exception(f'Klarte ikke generere settings.json filen. '
                        f'-p flagg (--prompt-missing-args) er ikke satt og det finnes ingen default settings.json å '
                        f'hente verdier fra.')
