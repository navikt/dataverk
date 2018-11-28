import os
import getpass

user_ident_string = "USER_IDENT="
password_string = "PASSWORD="
settings_repo_string = "SETTINGS_REPO="
template_repo_string = "TEMPLATES_REPO="


class CreateEnvFile:
    ''' Klasse for å generere .env fil for dataverk

    '''

    def __init__(self, user_ident: str, password: str, settings_repo: str, template_repo: str, destination: str=None):
        self._verify_input_types(user_ident=user_ident, password=password, template_repo=template_repo, settings_repo=settings_repo)
        if destination is not None:
            self._verify_destination(destination)
            try:
                with open(os.path.join(destination, ".env"), 'w') as env_file:
                    print(user_ident_string + user_ident, file=env_file)
                    print(password_string + password, file=env_file)
                    print(settings_repo_string + settings_repo, file=env_file)
                    print(template_repo_string + template_repo, file=env_file)
            except OSError:
                raise OSError(f'Klarte ikke generere ny .env fil')
        else:
            try:
                with open(".env", 'w') as env_file:
                    print(user_ident_string + user_ident, file=env_file)
                    print(password_string + password, file=env_file)
                    print(settings_repo_string + settings_repo, file=env_file)
                    print(template_repo_string + template_repo, file=env_file)
            except OSError:
                raise OSError(f'Klarte ikke generere ny .env fil')

    def _verify_input_types(self, user_ident, password, settings_repo, template_repo):
        if not isinstance(user_ident, str):
            raise TypeError(f'user_ident må være av type string')
        if not isinstance(password, str):
            raise TypeError(f'password må være av type string')
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
    settings_repo = input(f'Lim inn url til settings repository [{default_settings_repo}]: ')
    template_repo = input(f'Lim inn url til templates repository [{default_template_repo}]: ')

    if not settings_repo:
        settings_repo = default_settings_repo

    if not template_repo:
        template_repo = default_template_repo

    CreateEnvFile(user_ident=user_ident, password=password, settings_repo=settings_repo,
                  template_repo=template_repo, destination=destination)
