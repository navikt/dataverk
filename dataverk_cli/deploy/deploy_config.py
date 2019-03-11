from collections import Mapping
from pathlib import Path
from dataverk.context.replacer import Replacer
from dataverk_cli.cli.cli_utils import repo_info


def edit_cronjob_config(settings_store: Mapping, yaml_path: Path = Path('cronjob.yaml')) -> None:
    """
    :return: None
    """

    cronjob_config = _read_file(yaml_path)

    tag_value_map = {"package_name": settings_store["package_name"],
                     "vks_auth_path": settings_store["vault"]["auth_uri"],
                     "vks_kv_path": settings_store["vault"]["secrets_uri"],
                     "namespace": settings_store["nais_namespace"],
                     "schedule": settings_store["update_schedule"]}

    replacer = Replacer(tag_value_map)
    complete_cronjob_config = replacer.get_filled_mapping(cronjob_config, str)

    _write_file(data=complete_cronjob_config, file_path=yaml_path)


def edit_service_user_config(settings_store: Mapping, service_account_file: Path = Path('service_account_file.yaml')):
    service_account_data = _read_file(service_account_file)

    tag_value_map = {"service_account": settings_store["package_name"]}

    replacer = Replacer(tag_value_map)
    complete_service_account = replacer.get_filled_mapping(service_account_data, str)

    _write_file(data=complete_service_account, file_path=service_account_file)


def edit_jenkins_job_config(remote_repo_url: str, credential_id: str, config_file_path: Path=Path("jenkins_config.xml")):
    value_map = {"scriptPath": 'Jenkinsfile',
                 "projectUrl": remote_repo_url,
                 "url": repo_info.convert_to_ssh_url(remote_repo_url),
                 "credentialsId": credential_id}

    jenkins_config = _read_file(config_file_path)

    replacer = Replacer(value_map)
    complete_jenkins_config = replacer.get_filled_mapping(jenkins_config, str)

    _write_file(data=complete_jenkins_config, file_path=config_file_path)


def _read_file(file_path):
    try:
        with file_path.open('r') as file:
            return file.read()
    except OSError:
        raise OSError(f'Can not find a file on Path({file_path})')


def _write_file(data, file_path):
    try:
        with file_path.open('w') as file:
            file.write(data)
    except OSError:
        raise OSError(f'Finner ikke cronjob.yaml fil på Path({file_path})')