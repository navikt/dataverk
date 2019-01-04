""" Kommando parsers for dataverk-cli
"""

from dataverk_cli import __version__


def add_create_env_file_command(sub_arg_parser):
    parser_create_env_file = sub_arg_parser.add_parser('create-env-file', add_help=False)
    parser_create_env_file.add_argument('-v', '--version', action='version', version=__version__,
                                        help="Viser programversjon")
    parser_create_env_file.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    parser_create_env_file.add_argument('-d', '--destination', dest="destination", action='store', metavar='<path>',
                                        default=None, help="Sti til ønsket lagringslokasjon for .env fil. "
                                                           "Dersom denne ikke spesifiseres vil .env filen "
                                                           "legges i stien som skriptet kjøres fra.")


def add_init_command(sub_arg_parser):
    parser_init = sub_arg_parser.add_parser('init', add_help=False)
    parser_init.add_argument('-v', '--version', action='version', version=__version__,
                              help="Viser programversjon")
    parser_init.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    parser_init.add_argument('-p', '--prompt-missing-args', dest="prompt_missing_args", action='store_true',
                              help="Prompter bruker om å skrive inn alle settings parametere som ikke "
                                   "angis som input til skriptet (default settings fil brukes ikke)")
    parser_init.add_argument('--package-name', dest="package_name", action='store', metavar='<pakkenavn>',
                              default=None, help="Ønsket navn på ny datapakke")
    parser_init.add_argument('--update-schedule', dest="update_schedule", action='store', metavar='<schedule>',
                              default=None, help="Ønsket oppdateringsschedule for datapakke")

    parser_init.add_argument('--nais-namespace', dest="nais_namespace", action='store', metavar='<namespace>',
                              default=None, help="Namespace på NAIS plattform")
    parser_init.add_argument('--elastic-private', dest="elastic_private", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for private elastic index")
    parser_init.add_argument('--aws-endpoint', dest="aws_endpoint", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for AWS S3")
    parser_init.add_argument('--jenkins-endpoint', dest="jenkins_endpoint", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for jenkins server")
    parser_init.add_argument('--vault-secrets-uri', dest="vault_secrets_uri", action='store', metavar='<uri>',
                              default=None, help="URI for vault secrets")
    parser_init.add_argument('--vault-auth-path', dest="vault_auth_path", action='store', metavar='<path>',
                              default=None, help="Vault sti for vks auth path")
    parser_init.add_argument('--vault-kv-path', dest="vault_kv_path", action='store', metavar='<path>',
                              default=None, help="Vault sti til kv secrets")
    parser_init.add_argument('--vault-role', dest="vault_role", action='store', metavar='<role>',
                              default=None, help="Vault role")
    parser_init.add_argument('--vault-service-account', dest="vault_service_account", action='store', metavar='<service account>',
                              default=None, help="Vault service account")


def add_update_schedule_command(sub_arg_parser):
    parser_schedule = sub_arg_parser.add_parser('schedule', add_help=False)
    parser_schedule.add_argument('-v', '--version', action='version', version=__version__,
                                 help="Viser programversjon")
    parser_schedule.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")

    parser_schedule.add_argument('--package-name', dest="package_name", action='store', metavar='<pakkenavn>',
                                 default=None, help="Navn på datapakke som ønskes schedulert")
    parser_schedule.add_argument('--update-schedule', dest="update_schedule", action='store', metavar='<update schedule>',
                                 default=None, help="Oppdateringsfrekvens for datapakke")


def add_delete_command(sub_arg_parser):
    parse_delete = sub_arg_parser.add_parser('delete', add_help=False)
    parse_delete.add_argument('-v', '--version', action='version', version=__version__,
                              help="Viser programversjon")
    parse_delete.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    parse_delete.add_argument('--package-name', dest="package_name", action='store', metavar='<pakkenavn>',
                              default=None, help="Navn på datapakke som ønskes fjernet")


def add_notebook2script_command(sub_arg_parser):
    parse_notebook2script = sub_arg_parser.add_parser('notebook2script', add_help=False)
    parse_notebook2script.add_argument('-v', '--version', action='version', version=__version__,
                              help="Viser programversjon")
    parse_notebook2script.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")


def add_publish_command(sub_arg_parser):
    parse_publish = sub_arg_parser.add_parser('publish', add_help=False)
    parse_publish.add_argument('-v', '--version', action='version', version=__version__,
                               help="Viser programversjon")
    parse_publish.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")