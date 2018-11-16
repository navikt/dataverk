import argparse
from . import dataverk_init, dataverk_create, dataverk_create_settings_template, dataverk_create_env_file, __version__


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

    # Create command
    parse_create = sub_arg_parser.add_parser('create', add_help=False)
    parse_create.add_argument('-v, --version', action='version', version=__version__,
                              help="Viser programversjon")
    parse_create.add_argument('-h, --help', action='help', help="Viser denne hjelpemeldingen")
    parse_create.add_argument('--package-name', dest="package_name", action='store', metavar='<pakkenavn>',
                              default=None, help="Ønsket navn på ny datapakke")
    parse_create.add_argument('--update-schedule', dest="update_schedule", action='store', metavar='<schedule>',
                              default=None, help="Ønsket oppdateringsschedule for datapakke")
    parse_create.add_argument('--nais-namespace', dest="nais_namespace", action='store', metavar='<namespace>',
                              default=None, help="Ønsket NAIS namespace hvor cronjob skal deployes")

    # Create_settings command
    parser_create_settings = sub_arg_parser.add_parser('create_settings', add_help=False)
    parser_create_settings.add_argument('-v, --version', action='version', version=__version__, help="Viser programversjon")
    parser_create_settings.add_argument('-h, --help', action='help', help="Viser denne hjelpemeldingen")
    parser_create_settings.add_argument('--destination', dest="destination", action='store', metavar='<path>',
                                        default=None, help="Sti til ønsket lagringslokasjon for settingsfil. "
                                                           "Dersom denne ikke spesifiseres vil settings.json filen "
                                                           "legges i stien som skriptet kjøres fra.")

    # Create env file command
    parser_create_env_file = sub_arg_parser.add_parser('create_env_file', add_help=False)
    parser_create_env_file.add_argument('-v, --version', action='version', version=__version__,
                                        help="Viser programversjon")
    parser_create_env_file.add_argument('-h, --help', action='help', help="Viser denne hjelpemeldingen")
    parser_create_env_file.add_argument('--destination', dest="destination", action='store', metavar='<path>',
                                        default=None, help="Sti til ønsket lagringslokasjon for .env fil. "
                                                           "Dersom denne ikke spesifiseres vil .env filen "
                                                           "legges i stien som skriptet kjøres fra.")

    args = arg_parser.parse_args()

    if args.command == 'init':
        pass
    elif args.command == 'create':
        dataverk_create.run(package_name_in=args.package_name,
                            update_schedule_in=args.update_schedule,
                            nais_namespace_in=args.nais_namespace)
    elif args.command == 'create_settings':
        dataverk_create_settings_template.run(destination=args.destination)
    elif args.command == 'create_env_file':
        dataverk_create_env_file.run(destination=args.destination)


if __name__ == "__main__":
    main()
