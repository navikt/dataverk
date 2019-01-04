import argparse

from . import dataverk_create_env_file, __version__
from .dataverk_base import Action
from .dataverk_factory import get_datapackage_object
from .dataverk_notebook2script import notebook2script
from .dataverk_publish import publish_datapackage
from dataverk_cli.cli_utils import commands


def main():
    # Top level parser
    arg_parser = argparse.ArgumentParser(add_help=False)
    arg_parser.add_argument('-v', '--version', action='version', version=__version__,
                            help="Viser programversjon")
    arg_parser.add_argument('-h', '--help', action='help', help="Viser denne hjelpemeldingen")
    sub_arg_parser = arg_parser.add_subparsers(title='commands', dest='command')
    sub_arg_parser.required = True

    # command parsers
    commands.add_create_env_file_command(sub_arg_parser)
    commands.add_delete_command(sub_arg_parser)
    commands.add_init_command(sub_arg_parser)
    commands.add_notebook2script_command(sub_arg_parser)
    commands.add_publish_command(sub_arg_parser)
    commands.add_update_schedule_command(sub_arg_parser)

    args = arg_parser.parse_args()

    if args.command == 'create-env-file':
        dataverk_create_env_file.run(destination=args.destination)
    elif args.command == 'init':
        dp = get_datapackage_object(action=Action.INIT, args=args)
        dp.run()
    elif args.command == 'schedule':
        dp = get_datapackage_object(action=Action.SCHEDULE, args=args)
        dp.run()
    elif args.command == 'delete':
        dp = get_datapackage_object(action=Action.DELETE, args=args)
        dp.run()
    elif args.command == "notebook2script":
        notebook2script()
    elif args.command == "publish":
        publish_datapackage()


if __name__ == "__main__":
    main()
