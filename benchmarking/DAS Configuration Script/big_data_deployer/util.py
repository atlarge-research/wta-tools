#!/usr/bin/env python2

from __future__ import print_function
import os
import subprocess

class InvalidSetupError(Exception): pass

def log(indentation, message):
    indent_str = ""
    while indentation > 1:
        indent_str += "|  "
        indentation -= 1
    if indentation == 1:
        indent_str += "|- "
    print(indent_str + message)

def create_log_fn(base_indentation, base_log=log):
    return lambda indentation, message: base_log(base_indentation + indentation, message)

def execute_command_quietly(command_line_list):
    """Executes a command, given as a list, while supressing any output."""
    with open(os.devnull, "wb") as devnull:
        subprocess.check_call(command_line_list, stdout=devnull, stderr=subprocess.STDOUT)

def execute_command_for_output(command_line_list):
    return subprocess.Popen(command_line_list, stdout=subprocess.PIPE).communicate()[0].decode("utf-8")

