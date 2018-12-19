import argparse

from . import dataverk_create_env_file, __version__
from .datapackage_base import Action
from .datapackage_factory import get_datapackage_object
from .datapackage_publish import publish_datapackage


def main():
    # Top level parser
    arg_parser = argparse.ArgumentParser(add_help=False)
    arg_parser.add_argument('-v', '--version', action='version', version=__version__,
                            help="Viser programversjon")
    arg_parser.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    sub_arg_parser = arg_parser.add_subparsers(title='commands', dest='command')
    sub_arg_parser.required = True

    # Init command
    parser_init = sub_arg_parser.add_parser('init', add_help=False)
    parser_init.add_argument('-v', '--version', action='version', version=__version__,
                             help="Viser programversjon")
    parser_init.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")

    # Create env file command
    parser_create_env_file = sub_arg_parser.add_parser('create-env-file', add_help=False)
    parser_create_env_file.add_argument('-v', '--version', action='version', version=__version__,
                                        help="Viser programversjon")
    parser_create_env_file.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    parser_create_env_file.add_argument('-d', '--destination', dest="destination", action='store', metavar='<path>',
                                        default=None, help="Sti til ønsket lagringslokasjon for .env fil. "
                                                           "Dersom denne ikke spesifiseres vil .env filen "
                                                           "legges i stien som skriptet kjøres fra.")

    # Create command
    parse_create = sub_arg_parser.add_parser('create', add_help=False)
    parse_create.add_argument('-v', '--version', action='version', version=__version__,
                              help="Viser programversjon")
    parse_create.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    parse_create.add_argument('-p', '--prompt-missing-args', dest="prompt_missing_args", action='store_true',
                              help="Prompter bruker om å skrive inn alle settings parametere som ikke "
                                   "angis som input til skriptet (default settings fil brukes ikke)")
    parse_create.add_argument('--package-name', dest="package_name", action='store', metavar='<pakkenavn>',
                              default=None, help="Ønsket navn på ny datapakke")
    parse_create.add_argument('--update-schedule', dest="update_schedule", action='store', metavar='<schedule>',
                              default=None, help="Ønsket oppdateringsschedule for datapakke")

    parse_create.add_argument('--nais-namespace', dest="nais_namespace", action='store', metavar='<namespace>',
                              default=None, help="Namespace på NAIS plattform")
    parse_create.add_argument('--elastic-private', dest="elastic_private", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for private elastic index")
    parse_create.add_argument('--aws-endpoint', dest="aws_endpoint", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for AWS S3")
    parse_create.add_argument('--jenkins-endpoint', dest="jenkins_endpoint", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for jenkins server")
    parse_create.add_argument('--vault-secrets-uri', dest="vault_secrets_uri", action='store', metavar='<uri>',
                              default=None, help="URI for vault secrets")
    parse_create.add_argument('--vault-auth-path', dest="vault_auth_path", action='store', metavar='<path>',
                              default=None, help="Vault sti for vks auth path")
    parse_create.add_argument('--vault-kv-path', dest="vault_kv_path", action='store', metavar='<path>',
                              default=None, help="Vault sti til kv secrets")
    parse_create.add_argument('--vault-role', dest="vault_role", action='store', metavar='<role>',
                              default=None, help="Vault role")
    parse_create.add_argument('--vault-service-account', dest="vault_service_account", action='store', metavar='<service account>',
                              default=None, help="Vault service account")

    # Update command
    parse_update = sub_arg_parser.add_parser('update', add_help=False)
    parse_update.add_argument('-v', '--version', action='version', version=__version__,
                              help="Viser programversjon")
    parse_update.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    parse_update.add_argument('--package-name', dest="package_name", action='store', metavar='<pakkenavn>',
                              default=None, help="Navn på datapakke sommmm ønskes oppdatert")

    # Delete command
    parse_delete = sub_arg_parser.add_parser('delete', add_help=False)
    parse_delete.add_argument('-v', '--version', action='version', version=__version__,
                              help="Viser programversjon")
    parse_delete.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    parse_delete.add_argument('--package-name', dest="package_name", action='store', metavar='<pakkenavn>',
                              default=None, help="Navn på datapakke som ønskes fjernet")

    # Publish command
    parse_publish = sub_arg_parser.add_parser('publish', add_help=False)
    parse_publish.add_argument('-v', '--version', action='version', version=__version__,
                              help="Viser programversjon")
    parse_publish.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")

    args = arg_parser.parse_args()

    if args.command == 'init':
        pass
    elif args.command == 'create-env-file':
        dataverk_create_env_file.run(destination=args.destination)
    elif args.command == 'create':
        dp = get_datapackage_object(action=Action.CREATE, args=args)
        dp.run()
    elif args.command == 'update':
        dp = get_datapackage_object(action=Action.UPDATE, args=args)
        dp.run()
    elif args.command == 'delete':
        dp = get_datapackage_object(action=Action.DELETE, args=args)
        dp.run()
    elif args.command == "publish":
        publish_datapackage()


if __name__ == "__main__":
    main()
