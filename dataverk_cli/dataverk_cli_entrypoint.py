import argparse

from git import GitError

from dataverk_cli import dataverk_create_env_file, __version__
from dataverk_cli.dataverk_notebook2script import notebook2script
from dataverk_cli.dataverk_publish import publish_datapackage
from dataverk_cli.cli.cli_utils import commands
from dataverk_cli.cli.cli_command_handlers import init_handler, schedule_handler, delete_handler
from dataverk_cli.dataverk_factory import get_datapackage_object, Action
from dataverk_cli.cli.cli_utils import setting_store_functions
from dataverk_cli.cli.cli_utils import env_store_functions

ERROR_TEMPLATE = "[ERROR] {}"

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

    try:
        if args.command == 'create-env-file':
            dataverk_create_env_file.run(destination=args.destination)
        elif args.command == 'init':
            # Create setting and env stores for init command handling
            env_store = env_store_functions.safe_create_env_store(args)
            settings_dict = setting_store_functions.create_settings_dict(args=args, env_store=env_store)

            # call the init command handler to handle user interaction and settings configuration
            settings_dict, env_store = init_handler.handle(args, settings_dict, env_store)

            # create Datapackage object with the configured settings and env
            dp = get_datapackage_object(Action.INIT, settings_dict, env_store)
            dp.run()

        elif args.command == 'schedule':
            settings_dict, env_store = schedule_handler.handle(args)
            dp = get_datapackage_object(Action.SCHEDULE, settings_dict, env_store)
            dp.run()
        elif args.command == 'delete':
            settings_dict, env_store = delete_handler.handle(args)
            dp = get_datapackage_object(Action.DELETE, settings_dict, env_store)
            dp.run()
        elif args.command == "notebook2script":
            notebook2script()
        elif args.command == "publish":
            publish_datapackage()

    except KeyboardInterrupt as user_cancel:
        print(user_cancel)

    except FileNotFoundError as bad_project_state_error:
        print(ERROR_TEMPLATE.format(bad_project_state_error))

    except GitError as git_related_error:
        print(ERROR_TEMPLATE.format(git_related_error))

    finally:
        print(f"dataverk-cli {args.command} completed")






if __name__ == "__main__":
    main()
