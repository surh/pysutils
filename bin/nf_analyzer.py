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
    parser.add_argument("--use-trace", help=("Look for a trace.txt file"),
                        default=False,
                        action="store_true")
    parser.add_argument("--list-files", help=("Nextflow proces file to list"),
                        type=str,
                        default='',
                        choices=['', 'exitcode', 'log', 'out', 'err'])
    parser.add_argument("--exitcode", help=("Select only processes with "
                                            "this exitcode. If -1, keep "
                                            "all processes."),
                        default=-1,
                        type=int)

    # Read arguments
    print("Reading arguments")
    args = parser.parse_args()

    # Processing goes here if needed

    return args


def list_work_dirs(workdir, trace=None):
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


if __name__ == "__main__":
    args = process_arguments()

    workdirs = list_work_dirs(args.workdir)
    print(workdirs)
    Exitcodes = get_process_exitcodes(workdirs)
    print("==Exitcodes")
    print(Exitcodes)
