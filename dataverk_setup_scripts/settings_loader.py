import os
from urllib3.util.url import parse_url
from pathlib import Path
from shutil import copyfile
import requests


class SettingsLoader:
    """ Håndterer innlasting av settings for dataverk fra eksterne ressurser
    """

    def __init__(self, settings_file_location: str):

        if self._is_valid_url_string(settings_file_location):
            self.setting_file_location = settings_file_location

    def download_to(self, save_location: str) -> Path:
        raise NotImplementedError("This is a Abstract Method")

    def _is_valid_url_string(self, url: str):
        """ Sjekker om Url Objektet den mottar er av gylding URL format og type
        """

        if not isinstance(url, str):
            raise ValueError("Url is not a string")

        result = parse_url(url)
        if result.hostname is "" or result.hostname is None:
            raise ValueError("Url is not formated correctly")

        return True

    def _copy_file(self, file_location: Path, to_dir: Path):
        filename = file_location.parts[-1]
        copyfile(str(file_location), str(to_dir.absolute()) + filename)

    def _is_location_git_repo(self, settings_file_location: str):
        """Returner True om URLen går til en .git fil
        """

        return self._get_url_suffix(settings_file_location) == "git"

    def _is_location_a_json_file(self, settings_file_location: str):
        """ Returner True om URLen går til en JSON fil
        """

        return self._get_url_suffix(settings_file_location) == "json"

    def _get_url_suffix(self, url:str):
        return url.split(".")[-1]


class FileSettingsLoader(SettingsLoader):

    def __init__(self, url: str):
        super().__init__(url)

    def download_to(self, save_location: str) -> Path:
        save_path = Path(save_location)
        if save_path.exists() and save_path.is_dir():
            if self._is_location_git_repo(self.setting_file_location):
                return self._clone_git_repo_return_path_to_settings(self.setting_file_location, save_path)
            if self._is_location_a_json_file(self.setting_file_location):
                self._copy_file(Path(self.setting_file_location), save_path)
        else:
            raise NotADirectoryError("Path is not a Directory or does not exist")

    def _is_online_resource(self, url: str):
        result = parse_url(url)
        if result.scheme == "http" or result.scheme == "https":
            return True
        else:
            return False

    def _download_file_from_online_resource(self, url: str, to_dir: Path):
        """ Laster ned en fil fra en online ressurs"""

        if to_dir.is_dir() and to_dir.exists():
            r = requests.get(url, allow_redirects=False)
            open(str(to_dir.joinpath("settings.json")), 'wb+').write(r.content)
        else:
            raise NotADirectoryError("Save location is not a Directory or does not exists")


class GitSettingsLoader(SettingsLoader):

    def __init__(self, url: str):
        super().__init__(url)

    def download_to(self, save_location: str) -> Path:
        save_path = Path(save_location)
        if save_path.exists() and save_path.is_dir():
            if self._is_location_git_repo(self.setting_file_location):
                return self._clone_git_repo_return_path_to_settings_file(self.setting_file_location, save_path)
            else:
                raise ValueError("Url does not resolve to a git repository")
        else:
            raise NotADirectoryError("Path is not a Directory or does not exist")

    def _get_git_project_name(self, repo_url: str) -> str:
        """ Returnerer git prosjekt navnet

        :param repo_url: Url to git project
        :return str: name of the git project
        """
        return parse_url(repo_url).path.split("/")[-1].split(".")[0]

    def _clone_git_repo_return_path_to_settings_file(self, repo_location: str, clone_location: Path):
        """ Kloner git repoet til en git lokasjon og returnerer en path til mappen
        """

        if not clone_location.exists():
            raise NotADirectoryError("The path to be cloned to is not a directory or does not exist")

        os.system("git clone {} {}".format(repo_location, clone_location.joinpath(self._get_git_project_name(repo_location))))
        new_folder = self._get_git_project_name(repo_location) + "/"
        return clone_location.joinpath(new_folder)


def get_settings_loader(resource="git") -> type(SettingsLoader):
    """

    :param resource: Type Ressurs settings filen skal lastes ned eller kopieres fra
    :return: Subtype av SettingsLoader
    """
    if resource == "git":
        return GitSettingsLoader
    elif resource == "file":
        return FileSettingsLoader
    else:
        raise AttributeError("resource passed is not a supported resource")