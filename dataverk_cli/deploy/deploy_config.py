from collections import Mapping
from pathlib import Path
from dataverk.context.replacer import Replacer
from dataverk_cli.cli.cli_utils import repo_info
from dataverk.utils import file_functions


def edit_cronjob_config(settings_store: Mapping, yaml_path: Path = Path('cronjob.yaml')) -> None:
    """
    :return: None
    """

    cronjob_config = file_functions.read_file(yaml_path)

    tag_value_map = {"package_name": settings_store["package_name"],
                     "vks_auth_path": settings_store["vault"]["auth_uri"],
                     "vks_kv_path": settings_store["vault"]["secrets_uri"],
                     "namespace": settings_store["nais_namespace"],
                     "schedule": f"\"{settings_store['update_schedule']}\""}

    replacer = Replacer(tag_value_map)
    complete_cronjob_config = replacer.get_filled_mapping(cronjob_config, str)

    file_functions.write_file(path=yaml_path, content=complete_cronjob_config)


def edit_service_user_config(settings_store: Mapping, service_account_file: Path = Path('service_account_file.yaml')):
    service_account_data = file_functions.read_file(service_account_file)

    tag_value_map = {"service_account": settings_store["package_name"]}

    replacer = Replacer(tag_value_map)
    complete_service_account = replacer.get_filled_mapping(service_account_data, str)

    file_functions.write_file(path=service_account_file, content=complete_service_account)


def edit_jenkins_job_config(remote_repo_url: str, credential_id: str, config_file_path: Path=Path("jenkins_config.xml")):
    value_map = {"scriptPath": 'Jenkinsfile',
                 "projectUrl": remote_repo_url,
                 "url": repo_info.convert_to_ssh_url(remote_repo_url),
                 "credentialsId": credential_id}

    jenkins_config = file_functions.read_file(config_file_path)

    replacer = Replacer(value_map)
    complete_jenkins_config = replacer.get_filled_mapping(jenkins_config, str)

    file_functions.write_file(path=config_file_path, content=complete_jenkins_config)
