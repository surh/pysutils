#!/usr/bin/env python
# Copyright (C) 2019 Sur Herrera Paredes

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


import argparse
import os
import glob
# import numpy as np
# import pandas


def process_arguments():
    # Read arguments
    parser_format = argparse.ArgumentDefaultsHelpFormatter
    parser = argparse.ArgumentParser(formatter_class=parser_format)
    required = parser.add_argument_group("Required arguments")

    # Define description
    parser.description = ("Analyze nextflow run output")

    # Define required arguments
    required.add_argument("--workdir", help=("Nextflow work directory."),
                          required=True, type=str)

    # Define other arguments
    parser.add_argument("--use_trace", help=("Look for a trace.txt file"),
                        default=False,
                        action="store_true")
    parser.add_argument("--trace_file", help=("Nextflow trace file."),
                        type=str,
                        default='trace.txt')
    parser.add_argument("--list_files", help=("Nextflow process file to list"),
                        type=str,
                        default='',
                        choices=['', 'exitcode', 'log', 'out', 'err',
                                 'begin', 'run', 'sh'])
    parser.add_argument("--exitcode", help=("Select only processes with "
                                            "this exitcode. If -1, keep "
                                            "all processes. Status is "
                                            "always obtained from the "
                                            ".exitcode file in the "
                                            "workdirs."),
                        default=-1,
                        type=int)
    parser.add_argument("--status", help=("Select only processes with "
                                          "this status from the trace file. "
                                          "If 'any', ignore this value"),
                        default='any',
                        type=str)
    parser.add_argument("--invert", help=("Select processes that don't "
                                          "match the selected exitcode "
                                          "or status."),
                        default=False,
                        action="store_true")

    # Read arguments
    print("Reading arguments")
    args = parser.parse_args()

    # Processing goes here if needed

    return args


def list_work_dirs(workdir):
    """Find the path of all work directories"""

    work_level1 = os.listdir(workdir)
    workdirs = []
    for lv1 in work_level1:
        if len(lv1) != 2:
            raise ValueError("level 1 directory not of length 2")
        lv1_dir = os.path.join(workdir, lv1)

        work_level2 = os.listdir(lv1_dir)
        for lv2 in work_level2:
            if len(lv2) != 30:
                raise ValueError("level 2 directory not of length 30")
            lv2_dir = os.path.join(lv1_dir, lv2)
            workdirs.append(lv2_dir)

    return workdirs


def get_process_exitcodes(workdirs):
    """For a list of directories, get the value of the .get_process_exitcode
    file."""

    Exitcodes = dict()
    for wd in workdirs:
        exitcode_file = os.path.join(wd, ".exitcode")
        if not os.path.isfile(exitcode_file):
            raise FileNotFoundError(".exitcode file missing at {}".format(wd))

        with open(exitcode_file, 'r') as eh:
            exitcode = int(eh.readline())
            Exitcodes[wd] = exitcode
        eh.close()

    return Exitcodes


def read_nf_trace(trace_file):
    """Read trace file from nextflow --with-trace option"""

    if not os.path.isfile(trace_file):
        raise FileNotFoundError("{} file not found".format(trace_file))

    with open(trace_file, 'r') as ih:
        header = ih.readline().split("\t")
        # Create dictionary
        Trace = dict()
        for h in header:
            Trace[h] = []

        # Readlines
        for line in ih:
            fields = line.split("\t")
            [Trace[header[i]].append(fields[i]) for i in range(len(fields))]
    ih.close()

    return Trace


def get_trace_workdirs(Trace, status='any', invert=False):
    """Read workdirs listed in nextflow trace"""

    workdirs = []
    # for hash in Trace['hash']:
    for i in range(len(Trace['hash'])):
        pattern = 'work/' + Trace['hash'][i] + '*'
        new_dir = glob.glob(pattern)
        if len(new_dir) > 1:
            raise ValueError("More than one file "
                             "returned from hash {}".format(hash))
        elif len(new_dir) == 1:
            new_dir = new_dir[0]
        else:
            raise ValueError("Mising directory from hasg {}".format(hash))

        if status != 'any':
            curr_status = Trace['status'][i]
            # Use xor to determine match and invert
            if (curr_status == status) ^ invert:
                workdirs.append(new_dir)
        else:
            workdirs.append(new_dir)

    return workdirs


if __name__ == "__main__":
    args = process_arguments()

    # Get list of work directories
    if args.use_trace:
        Trace = read_nf_trace(args.trace_file)
        print("==Trace")
        print(Trace)
        workdirs = get_trace_workdirs(Trace=Trace,
                                      status=args.status,
                                      invert=args.invert)
    else:
        workdirs = list_work_dirs(args.workdir)
    print("==Workdirs")
    print(workdirs)

    # Get exitcodes
    Exitcodes = get_process_exitcodes(workdirs)
    print("==Exitcodes")
    print(Exitcodes)
