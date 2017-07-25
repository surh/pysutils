#!/usr/bin/env python
# Copyright (C) 2017 Sur Herrera Paredes

import subprocess
import csv
import os
from subprocess import CalledProcessError

def concatenate_files(infiles, outfile):
    """Concatenates a list of files.
    
    Takes a list of file names and concatenates those
    files using the UNIX cat utility
    
    Args:
        infiles: List of file names
        outfile: File name to store the concatenated result.
            It will overwrite any existing file with that
            name.
    
    Returns:
        Returns the result of calling :py:func:`subprocess.run`
        
    Raises:
        CalledProcessError: If the cat command returns a non-zero
            status.
    """
    
    command = " ".join(infiles) 
    command = "cat " + command + " > " + outfile
    check = run_command(command)
    
    if check.returncode != 0:
        raise CalledProcessError("Could not concatenate files")
    
    return(check)

def process_run_list(file,sample_col,run_col,header = True):
    """Takes a map of runs and samples and returns runs per sample.
    
    Takes a file name of a tab-delimited file containing columns
    corresponding to samples and runs (or any other mapping instance
    where multiple elements on the runs column correspond to one and
    only one element in the samples column). It returns a dictionary
    that has a list of runs per sample, indexed by the sample.
    
    Args:
        file: File name containing the table.
        sample_col: Column number corresponding to the sample ID. Must
            be a 0-indexed integer.
        run_col: Column number corresponding to the run ID. Must be
            a 0-indexed integer.
        header: Logical indicating whether the mapping file contains
            a header row. If so, the first line will be skipped.
    
    Returns:
        A dictionary indexed by the sample IDs, and where every
        entry contains a list of runs per sample
    """
    
    print("\n=============================================")
    with open(file,'r') as fh:
        print("> Processing map of runs")
        if header is True:
            colnames = fh.readline()
        reader = csv.reader(fh, delimiter = "\t")
        
        RUNS = dict()
        nruns = 0
        nsamples = 0
        for line in reader:
            sample = line[sample_col]
            run = line[run_col]
            #print([sample,run])
            if sample in RUNS:
                RUNS[sample].append(run)
            else:
                RUNS[sample] = [run]
                nsamples += 1
            nruns += 1
    fh.close()
    print("\tProcessed {} runs in {} samples".format(nruns,nsamples))
    print("=============================================")

    return(RUNS)

def qsub_submissions(submissions,logdir):
    """Submits to PBS via qsub
    
    Takes a list of file names of PBS submission files, and sends
    them to qsub.
    
    Args:
        submissions: List of file paths to submit to qsub.
        logdir: A directory path that is checked for existence before
            submitting the jobs. If it does not exists it is created.
            
    Returns:
        Nothing
    """
    
    if os.path.exists(logdir):
        print("Logdir exists")
    else:
        os.mkdir(logdir)
    
    for file in submissions:
        run_command("qsub " + file)
        
    print("==========SUBMISSIONS DONE==========\n\n")
    
def return_column(infile,col = 1, separator = '\t', header = True):
    """Returns the values in the column of a file
    
    Takes a file name and returns a list with the values in the
    specified column of that file.
    
    Args:
        infile: File name with the table.
        col: Column number to return. Must be a 1-indexed integer.
            [Default=1]
        separator: String that separates columns in the file.
            [Default="\t"]
        header: Logical indicating whether the table has a first row
            of headers. If true, the first row is skipped and the
            value in that row is not included in the returned lis.
    
    Returns:
        A list with the elements in the specified column.
    """
    
    print("\n=============================================")
    col -= 1
    with open(infile,'r') as fh:
        print("\tReading table file {}".format(infile))
        reader = csv.reader(fh,delimiter = separator)
        if header:
            header = next(reader)
        
        res = []
        i = 0
        for row in reader:
            res.append(row[col])
            i += 1
    fh.close()
    print("\tProcessed {} lines".format(str(i)))
    print("=============================================")
    return(res)
    
def run_command(command):
    """Makes a system call
    
    Takes a command and runs it via subprocess. It also
    prints the command, and its output  status to STDOUT.
    Useful wrapper for scripts that generate many system calls.
    
    Args:
        command: A string with the command to be executed
        
    Returns:
        The result from :py:func:`subprocess.run`
    """
    status = 0;
    print("Executing:\n>{}".format(command))
    status = subprocess.run(command, shell = True)
    print("Status={}\n\n".format(status));

    return(status)

def write_download(download,outfile):
    """Writes the result of :py:func:`requests.get` to a file
    
    Takes an object with a *text* attribute, such as the one
    produced by :py:func:`requests.get`, and writes it to a
    file.
    
    Args:
        download (str): Object with *text* attribute
        outfile (str): Name of the file to create and write. It
            will overwrite any existing file with that name
    
    Returns:
        Nothing
    """
    
    with open(outfile,'w') as out_fh:
        out_fh.write(download.text)
    out_fh.close()
    
def write_qsub_submission(fh, commands, dir = os.getcwd(),
                          name = "Job", memory = "1000mb",
                          logfile = "log", errorfile = "error",
                          loptions = [], queue = None, mail = "n",
                          email = None, nodes = "nodes=1:ppn=1"):
    """
    Writes a PBS submission bash file.
    
    Takes a filehandle and a list of commands, and writes to it
    a series of PBS submission instructions, followed by the commands
    passed. A breif decription of the supportd qsub options is below.
    See the qsub documentation for more details.
    
    Args:
        fh: A writable file handle from the stardad io library, i.e. the result
            of a call to :py:func:`open`.
        commands (list): A list of strings where each string is a command to be executed by
            the cluster. Commands will be executed in the order defined by this list.
        dir (str): The working directory to use for the jobs. Defaults to the current working
            directory.
        name (str): The name to give the job.
        memory (str): The memory requested for the job
        logfile (str): The file name to use for the STDOUT of the submission script
        errorfile (str): The file name to use for the STDERR of the submission script
        loptions (list): A list of strings where each one is a long option (-l) as defined
            by PBS.
        queue (str): The name of the queue to use
        mail (str): Whether to send an email or not. 
        email (str): Email address to receive job information
        nodes (str): A string of the form 'nodes=XX:ppn=YY', where XX is the number of nodes
            to reserve, and YY the number of cpus per node.
    
    Returns:
        Nothing
    
    """
    
    
    # Writing options
    fh.write("#!/bin/bash\n")
    fh.write("#PBS -N  " + name + "\n")
    fh.write("#PBS -d " + dir + "\n")
    fh.write("#PBS -o " + logfile + "\n")
    fh.write("#PBS -e " + errorfile + "\n")
    fh.write("#PBS -l mem=" + memory + "\n")
    fh.write("#PBS -m " + mail + "\n")
    fh.write("#PBS -l " + nodes + "\n")
    
    if queue is not None:
        fh.write("#PBS -q " + queue + "\n")
    if email is not None:
        fh.write("#PBS -M " + email + "\n")
    
    # Adding l options like cput, walltime,
    for opt in loptions:
        fh.write("#PBS -l " + opt + "\n")
        
    # Writing some useful information. Based
    # on suggestions at
    # http://qcd.phys.cmu.edu/QCDcluster/pbs/run_serial.html
    fh.write("echo ------------------------------------------------------\n")
    fh.write("echo -n 'Job is running on node '; cat $PBS_NODEFILE\n")
    fh.write("echo ------------------------------------------------------\n")
    fh.write("echo PBS: qsub is running on $PBS_O_HOST\n")
    fh.write("echo PBS: originating queue is $PBS_O_QUEUE\n")
    fh.write("echo PBS: executing queue is $PBS_QUEUE\n")
    fh.write("echo PBS: working directory is $PBS_O_WORKDIR\n")
    fh.write("echo PBS: execution mode is $PBS_ENVIRONMENT\n")
    fh.write("echo PBS: job identifier is $PBS_JOBID\n")
    fh.write("echo PBS: job name is $PBS_JOBNAME\n")
    fh.write("echo PBS: node file is $PBS_NODEFILE\n")
    fh.write("echo PBS: current home directory is $PBS_O_HOME\n")
    fh.write("echo PBS: PATH = $PBS_O_PATH\n")
    fh.write("echo ------------------------------------------------------\n")
    fh.write("date\n")
    
    for cmd in commands:
        fh.write(cmd + "\n")
    
    fh.write("echo ------------------------------------------------------\n")
    fh.write("date\n")
       
    

def write_table(outfile,rows, header = None, delimiter = "\t", verbose = False):
    """Writes a table
    
    Takes a file name and a list of rows, and writes a file in table format.
    It will overwrite any existing file with that  name.
    
    Args:
        outfile (str): Name of file to be created, and overwritten if necessary,
            where the table will be printed
        rows (list): List where each element is a list corresponding to the fields
            on each row
        header (list): A list with header names. If it is not None it will be printed
            before *row*.
        delimiter (str): A string indicating the delimiter to use to separate fields
            in the table.
        verbose (bool): Boolean indicating whether to print informational messages
            to STDOUT.
    
    Returns:
        The number of lines printed
    """
    
    with open(outfile,'w') as out_fh:
        writer = csv.writer(out_fh,delimiter = '\t')
        if verbose:
            print("\tWriting {}".format(outfile))
            
        nlines = 0
        if header is not None:
            writer.writerow(header)
            nlines += 1
        for row in rows:
            writer.writerow(row)
            nlines += 1
    out_fh.close()

    if verbose:
        print("\t\tWrote {} lines".format(nlines))
    
    return(nlines)
                