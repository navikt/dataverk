from enum import Enum
from pathlib import Path

import yaml


def edit_cronjob_schedule(file_path: Path, schedule) -> None:
    """ Oppdaterer cronjob YAML filen
    :return: None
    """

    try:
        with file_path.open('r') as yamlfile:
            cronjob_config = yaml.load(yamlfile)
    except OSError:
        raise OSError(f'Finner ikke cronjob.yaml fil på Path({cronjob_file_path})')

    cronjob_config['spec']['schedule'] = schedule

    try:
        with file_path.open('w') as yamlfile:
            yamlfile.write(yaml.dump(cronjob_config, default_flow_style=False))
    except OSError:
        raise OSError(f'Finner ikke cronjob.yaml fil på Path({cronjob_file_path})')