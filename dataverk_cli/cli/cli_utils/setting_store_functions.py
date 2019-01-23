from dataverk.context import EnvStore
import json
from dataverk_cli.cli.cli_utils import settings_loader


def create_settings_dict(args, envs: EnvStore):
    if args.nav_internal:
        default_settings_path = ""

    else:
        settings = json.loads(read_text(package='dataverk_cli.templates',
                                        resource='settings.json'))

    if args.package_name is None:
        settings["package_name"] = user_input.prompt_for_user_input(arg="pakkenavn")
    else:
        settings["package_name"] = args.package_name

    validate_datapackage_name(settings["package_name"])

    return settings


def is_intenal_project(args):
    """
    :param args: commandline arguments
    :return: is the project flagged as an internal project
    """

    if args.nav_internal:
        return True
    else:
        return False


def load_external_settings(path):
    """
    Loads settings from external settings repository.

    Git repository and File stores are supported

    :param env_store:
    :return:
    """

    try:
        loader = settings_loader.get_settings_loader(settngs_repo_url)

        result = loader.download_to()
        default_settings_path = default_settings_loader.download_to('.')

        settings_creator_object = settings_creator.get_settings_creator(args=args,
                                                                        default_settings_path=str(
                                                                            default_settings_path))
        settings = settings_creator_object.create_settings()
        settings["nav_internal"] = "true"
    finally:
        if os.path.exists(str(default_settings_path)):
            remove_folder_structure(str(default_settings_path))