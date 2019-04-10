import os
import subprocess
import sys

USAGE = 'Usage: python(3) ./traverse_workflowhub_collection <path_to_workflowhub_root_directory>'


def parse(root_folder):
    for subdir, dirs, files in os.walk(root_folder):
        for file in files:
            if file.endswith(".json"):
                path = os.path.join(subdir, file)
                path_to_trace_from_root = path.replace(root_folder, "")
                path_to_trace_from_root = str(path_to_trace_from_root).lstrip(os.path.sep)
                cmd = "/home/lfdversluis/miniconda3/envs/wtaformat2/bin/python workflowhub_to_parquet.py \"{0}\" \"{1}\"".format(root_folder, path_to_trace_from_root)
                res = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

                output, error = res.communicate()
                if output:
                    print("ret> ", res.returncode)
                    print("OK> output ", output)
                if error:
                    print("ret> ", res.returncode)
                    print("Error> error ", error.strip())
                    break


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(USAGE)
        sys.exit(1)

    parse(sys.argv[1])
