#!/usr/bin/env python2

from __future__ import print_function
from . import *
from . import preserve
import argparse
import os.path
import sys

DEFAULT_FRAMEWORK_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "frameworks")

class InvalidSettingError(Exception): pass

def parse_arguments():
    parser = argparse.ArgumentParser(description="Install and deploy Big Data frameworks", prog="big_data_deployer")
    subparsers = parser.add_subparsers(title="Big Data framework deployment commands")

    add_list_frameworks_subparser(subparsers)
    add_install_subparser(subparsers)
    add_deploy_subparser(subparsers)
    preserve.add_preserve_subparser(subparsers)

    return parser.parse_args()

def add_list_frameworks_subparser(parser):
    list_frameworks_parser = parser.add_parser("list-frameworks", help="list supported Big Data frameworks")
    list_frameworks_parser.add_argument("--versions", help="list all supported versions", action="store_true")
    list_frameworks_parser.set_defaults(func=list_frameworks)

def add_install_subparser(parser):
    install_parser = parser.add_parser("install", help="install a Big Data framework")
    install_parser.add_argument("-f", "--framework-dir", help="installation directory for Big Data frameworks", action="store", default=DEFAULT_FRAMEWORK_DIR)
    install_parser.add_argument("--reinstall", help="force a clean reinstallation of the framework", action="store_true")
    install_parser.add_argument("FRAMEWORK", help="name of the framework to install", action="store")
    install_parser.add_argument("VERSION", help="version of the framework to install", action="store")
    install_parser.set_defaults(func=install_framework)

def add_deploy_subparser(parser):
    deploy_parser = parser.add_parser("deploy", help="deploy a Big Data framework")
    deploy_parser.add_argument("-f", "--framework-dir", help="installation directory for Big Data frameworks", action="store", default=DEFAULT_FRAMEWORK_DIR)
    deploy_parser.add_argument("-s", "--settings", metavar="SETTINGS_FILE", help="read settings from a file, imported in order of appearance on the command line", action="append", dest="settings_files", default=[])
    deploy_parser.add_argument("--list-settings", help="list settings supported by specified framework and version", action="store_true")
    deploy_parser.add_argument("--preserve-id", help="preserve reservation id to use for deployment, or 'LAST' for the last reservation made by the user", action="store", default="LAST")
    deploy_parser.add_argument("FRAMEWORK", help="name of the framework to deploy", action="store")
    deploy_parser.add_argument("VERSION", help="version of the framework to deploy", action="store")
    deploy_parser.add_argument("SETTINGS", help="settings as 'key=value' pairs specific to the framework, overrides values for same key from all settings files", nargs='*')
    deploy_parser.set_defaults(func=deploy_framework)

def list_frameworks(args):
    print("Supported frameworks:")
    if args.versions:
        for framework_ident, framework in sorted(get_framework_registry().frameworks.iteritems()):
            for version in sorted(framework.versions):
                print("%s %s" % (framework_ident, version))
    else:
        for framework_ident in sorted(get_framework_registry().frameworks):
            print(framework_ident)

def install_framework(args):
    fm = FrameworkManager(get_framework_registry(), args.framework_dir)
    fm.install(args.FRAMEWORK, args.VERSION, force_reinstall=args.reinstall)

def deploy_framework(args):
    fm = FrameworkManager(get_framework_registry(), args.framework_dir)
    if args.list_settings:
        supported_settings = fm.get_supported_deployment_settings(args.FRAMEWORK, args.VERSION)
        if supported_settings:
            max_len = max([len(key) for key, value in supported_settings])
            for setting_name, setting_desc in supported_settings:
                print("%s  %s" % (setting_name.ljust(max_len), setting_desc))
        else:
            print("No settings available")
    else:
        # Retrieve the set of machines assigned to the preserve reservation
        reservation = preserve.get_PreserveManager().fetch_reservation(args.preserve_id)
        machines = reservation.assigned_machines

        # Parse settings
        settings = {}
        for settings_file in args.settings_files:
            with open(settings_file, "r") as settings_file_content:
                for line in settings_file_content:
                    stripped_line = line.strip()
                    if stripped_line and not stripped_line.startswith("#"):
                        if "=" not in stripped_line:
                            raise InvalidSettingError('Setting "%s" in file "%s" is not a "key=value" pair.' % (line, settings_file))
                        key_value = stripped_line.split("=", 1)
                        settings[key_value[0].strip()] = key_value[1].strip()
        for setting in args.SETTINGS:
            if "=" not in setting:
                raise InvalidSettingError('Setting "%s" on command line is not a "key=value" pair.' % setting)
            key_value = setting.split("=", 1)
            settings[key_value[0].strip()] = key_value[1].strip()

        # Deploy the framework
        fm.deploy(args.FRAMEWORK, args.VERSION, machines, settings)

def main():
    args = parse_arguments()
    args.func(args)

if __name__ == "__main__":
    main()

