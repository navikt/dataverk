import os
import getpass

USER_IDENT_STRING = "USER_IDENT="
PASSWORD_STRING = "PASSWORD="
SETTINGS_REPO_STRING = "SETTINGS_REPO="
TEMPLATE_REPO_STRING = "TEMPLATES_REPO="
GITHUB_TOKEN_STRING = "GH_TOKEN="

class CreateEnvFile:
    ''' Klasse for å generere .env fil for dataverk

    '''

    def __init__(self, user_ident: str, password: str, github_token: str, settings_repo: str, template_repo: str, destination: str=None):
        self._verify_input_types(user_ident=user_ident, password=password, github_token=github_token, template_repo=template_repo, settings_repo=settings_repo)
        if destination is not None:
            self._verify_destination(destination)
            try:
                with open(os.path.join(destination, ".env"), 'w') as env_file:
                    print(USER_IDENT_STRING + user_ident, file=env_file)
                    print(PASSWORD_STRING + password, file=env_file)
                    print(GITHUB_TOKEN_STRING + github_token, file=env_file)
                    print(SETTINGS_REPO_STRING + settings_repo, file=env_file)
                    print(TEMPLATE_REPO_STRING + template_repo, file=env_file)
            except OSError:
                raise OSError(f'Klarte ikke generere ny .env fil')
        else:
            try:
                with open(".env", 'w') as env_file:
                    print(USER_IDENT_STRING + user_ident, file=env_file)
                    print(PASSWORD_STRING + password, file=env_file)
                    print(GITHUB_TOKEN_STRING + github_token, file=env_file)
                    print(SETTINGS_REPO_STRING + settings_repo, file=env_file)
                    print(TEMPLATE_REPO_STRING + template_repo, file=env_file)
            except OSError:
                raise OSError(f'Klarte ikke generere ny .env fil')

    def _verify_input_types(self, user_ident, password, github_token, settings_repo, template_repo):
        if not isinstance(user_ident, str):
            raise TypeError(f'user_ident må være av type string')
        if not isinstance(password, str):
            raise TypeError(f'password må være av type string')
        if not isinstance(github_token, str):
            raise TypeError(f'github_token må være av type string')
        if not isinstance(settings_repo, str):
            raise TypeError(f'settings_repo må være av type string')
        if not isinstance(template_repo, str):
            raise TypeError(f'settings_repo må være av type string')

    def _verify_destination(self, path):
        if not isinstance(path, str):
            raise TypeError(f'Sti for lagring av .env må være av type string')
        elif not os.path.exists(path=path):
            raise ValueError(f'Ønsket sti for lagring av .env fil eksisterer ikke')
        elif not os.path.isdir(path=path):
            raise ValueError(f'Ønsket sti er ikke en mappe')


def run(destination: str=None):
    default_settings_repo = "https://github.com/navikt/dataverk_settings.git"
    default_template_repo = "https://github.com/navikt/dataverk_settings.git"

    user_ident = input("Skriv inn brukerident: ")
    password = getpass.getpass("Skriv inn passord: ")
    github_token = getpass.getpass("Skriv inn github token: ")
    settings_repo = input(f'Lim inn url til settings repository [{default_settings_repo}]: ')
    template_repo = input(f'Lim inn url til templates repository [{default_template_repo}]: ')

    if not settings_repo:
        settings_repo = default_settings_repo

    if not template_repo:
        template_repo = default_template_repo

    CreateEnvFile(user_ident=user_ident, password=password, github_token=github_token,
                  settings_repo=settings_repo, template_repo=template_repo, destination=destination)
