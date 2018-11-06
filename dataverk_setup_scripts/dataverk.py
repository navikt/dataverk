import argparse
from . import dataverk_init, dataverk_create, __version__


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
    parse_create.add_argument('--settings-repo', dest="settings_repo", action='store', metavar='<settings_url>',
                              default=None, help="Github url for settings repo")

    args = arg_parser.parse_args()

    if args.command == 'init':
        pass
    elif args.command == 'create':
        dataverk_create.run()


if __name__ == "__main__":
    main()

