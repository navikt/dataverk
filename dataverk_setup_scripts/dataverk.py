import argparse
from . import dataverk_create, dataverk_create_env_file, __version__


def main():
    # Top level parser
    arg_parser = argparse.ArgumentParser(add_help=False)
    arg_parser.add_argument('-v, --version', action='version', version=__version__,
                            help="Viser programversjon")
    arg_parser.add_argument('-h, --help', action='help', help="Viser denne hjelpemeldingen")
    sub_arg_parser = arg_parser.add_subparsers(title='commands', dest='command')
    sub_arg_parser.required = True

    # Init command
    parser_init = sub_arg_parser.add_parser('init', add_help=False)
    parser_init.add_argument('-v, --version', action='version', version=__version__,
                             help="Viser programversjon")
    parser_init.add_argument('-h, --help', action='help', help="Viser denne hjelpemeldingen")

    # Create env file command
    parser_create_env_file = sub_arg_parser.add_parser('create-env-file', add_help=False)
    parser_create_env_file.add_argument('-v, --version', action='version', version=__version__,
                                        help="Viser programversjon")
    parser_create_env_file.add_argument('-h, --help', action='help', help="Viser denne hjelpemeldingen")
    parser_create_env_file.add_argument('--destination', dest="destination", action='store', metavar='<path>',
                                        default=None, help="Sti til ønsket lagringslokasjon for .env fil. "
                                                           "Dersom denne ikke spesifiseres vil .env filen "
                                                           "legges i stien som skriptet kjøres fra.")

    # Create command
    parse_create = sub_arg_parser.add_parser('create', add_help=False)
    parse_create.add_argument('-v, --version', action='version', version=__version__,
                              help="Viser programversjon")
    parse_create.add_argument('-h, --help', action='help', help="Viser denne hjelpemeldingen")
    parse_create.add_argument('-ud, --use-defaults', dest="use_defaults", action='store_true',
                              help="Setter default verdier for alle optional parametre "
                                   "som ikke spesifiseres når skriptet kjøres")
    parse_create.add_argument('-pn, --package-name', dest="package_name", action='store', metavar='<pakkenavn>',
                              default=None, help="Ønsket navn på ny datapakke")
    parse_create.add_argument('-us, --update-schedule', dest="update_schedule", action='store', metavar='<schedule>',
                              default=None, help="Ønsket oppdateringsschedule for datapakke")

    parse_create.add_argument('--nais-namespace', dest="nais_namespace", action='store', metavar='<namespace>',
                              default=None, help="Namespace på NAIS plattform")
    parse_create.add_argument('--elastic-endpoint', dest="elastic_endpoint", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for elastic index")
    parse_create.add_argument('--aws-endpoint', dest="aws_endpoint", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for AWS S3")
    parse_create.add_argument('--jenkins-endpoint', dest="jenkins_endpoint", action='store', metavar='<endpoint>',
                              default=None, help="Endepunkt for jenkins server")
    parse_create.add_argument('--vault-auth-uri', dest="vault_auth_uri", action='store', metavar='<uri>',
                              default=None, help="URI for vault autentisering")
    parse_create.add_argument('--vault-auth-uri', dest="vault_secrets_uri", action='store', metavar='<uri>',
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

    # Delete command

    args = arg_parser.parse_args()

    if args.command == 'init':
        pass
    elif args.command == 'create_env_file':
        dataverk_create_env_file.run(destination=args.destination)
    elif args.command == 'create':
        dataverk_create.run(args)


if __name__ == "__main__":
    main()
