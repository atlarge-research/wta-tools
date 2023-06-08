"""
Created on Tue Oct  2 19:26:18 2018

@author: Roland
"""

import os
import re
import hashlib
import argparse


mail_dest = r'([\w]+@[\w]+(.[\w]+)*)'
paths = r'((\w:|~)?([/\\]\w[\w-]*)+)[/\\]'
ipv4 = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
exec_file_names = r'(\w[\w-]*\.(py|sh|exe|jar))'
all_file_names = r'(\w+\.([a-zA-Z]{2,4}))'

re_mail_dest = re.compile(mail_dest)
re_paths = re.compile(paths)
re_ipv4 = re.compile(ipv4)
re_exec_file_names = re.compile(exec_file_names)
re_all_file_names = re.compile(all_file_names)
re_all = re.compile(r'('+ipv4+'|'+mail_dest+'|'+paths+'|'+all_file_names+')')

# Salt for the keygen function
salt_length=16
salt = os.urandom(salt_length)

def keygen(m):
    if args.v:
        print(m.group(1))
    return hashlib.sha256(salt+m.group(1)).hexdigest()

def sub_path(s):
    return re_paths.sub(lambda m: keygen(m)+"/", s)

def sub_exec_file_names(s):
    return re_exec_file_names.sub(keygen, s)

def sub_all_file_names(s):
    return re_all_file_names.sub(keygen, s)

def sub_keywords(s,keyword_list):
    for k in keyword_list:
        s = re.sub(r'('+k+')',keygen,s)
    return s

def sub_mail_and_destination(s):
    return re_mail_dest.sub(keygen, s)
    
def sub_ipv4(s):
    return re_ipv4.sub(keygen, s)
    
def sub_do_all(s):
    return re_all.sub(keygen, s)

# anonymize, according to the flags
def anonymize():
    with open(args.source, 'r') as f, open(args.dest, 'w') as df:
        for line in f:
            # check for patterns
            if args.custom_key_list:
                line = sub_keywords(line,args.custom_key_list)
            if args.all:
                line = sub_do_all(line)
            else:
                if args.s:
                    line = sub_mail_and_destination(line)
                if args.i:    
                    line = sub_ipv4(line)
                if args.p:
                    line = sub_path(line)
                if args.e:
                    line = sub_exec_file_names(line)
                if args.a:
                    line = sub_all_file_names(line)
            # write to file
            df.write(line)

parser = argparse.ArgumentParser(description='WTA Anonymizer')
    
# Required arguments
parser.add_argument('source', type=str, help='The source file path')
parser.add_argument('dest', type=str, help='The destination file path')

# Optional arguments
parser.add_argument('--all', action='store_true', help='Apply all anonymisations')
parser.add_argument('--s', action='store_true', help='Anonymize <name>@<mail>.<server>, <username>@<server>') 
parser.add_argument('--e', action='store_true', help='Anonymize executable file names')  
parser.add_argument('--a', action='store_true', help='Anonymize all file names')
parser.add_argument('--k', action='append', dest='custom_key_list', default=[], help='Anonymize list of custom keywords')
parser.add_argument('--i', action='store_true', help='Anonymize ipv4 addresses')
parser.add_argument('--p', action='store_true', help='Anonymize file paths')
parser.add_argument('--v', action='store_true', help='Output all matched strings')
args = parser.parse_args()

anonymize()
