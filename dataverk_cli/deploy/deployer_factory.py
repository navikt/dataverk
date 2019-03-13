import jenkins
from collections import Mapping
from enum import Enum
from dataverk_cli.deploy.deploy_connector import DeployConnector


class BuildServer(Enum):
    JENKINS = 1


def create_deploy_connector(settings_store: Mapping, env_store: Mapping):

    if env_store is None:
        raise TypeError("env_store cannot be None")

    if settings_store is None:
        raise TypeError("settings_store cannot be None")

    build_server_type = read_build_server_from_settings(settings_store)

    # Factory
    if build_server_type is BuildServer.JENKINS:
        jenkins_connector = create_jenkins_connector(settings_store, env_store)
        return DeployConnector(settings_store["package_name"], jenkins_connector)
    else:
        raise KeyError(f"Build server matching BuildServer({build_server_type}) could not be found")


def read_build_server_from_settings(settings_store: Mapping):
    if settings_store.get("jenkins") is not None:
        return BuildServer.JENKINS
    raise LookupError(f"settings_store({settings_store} does not contain valid scheduler configurations)")


def create_jenkins_connector(settings_store, env_store):
    return jenkins.Jenkins(url=settings_store["jenkins"]["url"],
                           username=env_store['USER_IDENT'],
                           password=env_store['PASSWORD'])
