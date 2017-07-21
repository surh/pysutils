#!/usr/bin/env python
# Copyright (C) 2017 Sur Herrera Paredes

import subprocess
import csv
import os

def concatenate_files(infiles, outfile):
    command = " ".join(infiles) 
    command = "cat " + command + " > " + outfile
    check = download_runs.run_command(command)
    
    if check.returncode != 0:
        raise ProcessError("Could not concatenate files")
    
    return(check)

def process_run_list(file,sample_col,run_col,header = True):
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
    
    if os.path.exists(logdir):
        print("Logdir exists")
    else:
        os.mkdir(logdir)
        
    for file in submissions:
        run_command("qsub " + file)
        
    print("==========SUBMISSIONS DONE==========\n\n")
    
def return_column(infile,col = 1, separator = '\t', header = True):
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
    status = 0;
    print("Executing:\n>{}".format(command))
    status = subprocess.run(command, shell = True)
    print("Status={}\n\n".format(status));

    return(status)

def write_download(download,outfile):
    with open(outfile,'w') as out_fh:
        out_fh.write(download.text)
    out_fh.close()
    
def write_qsub_submission(fh, commands, dir = os.getcwd(),
                          name = "Job", memory = "1000mb",
                          logfile = "log", errrorfile = "error",
                          loptions = [], queue = None, mail = "n",
                          email = None, nodes = "nodes=1:ppn=1"):
    # Writing options
    fh.write("#!/bin/bash\n")
    fh.write("#PBS -N  " + name + "\n")
    fh.write("#PBS -d " + dir + "\n")
    fh.write("#PBS -o " + logfile + "\n")
    fh.write("#PBS -e " + errorfile + "\n")
    fh.write("#PBS -l mem=" + memory + "\n")
    fh.write("#PBS -m " + mail + "\n")
    fh.write("#PBS -l mem=" + nodes + "\n")
    
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
    fh.write("echo ------------------------------------------------------")
    fh.write("echo -n 'Job is running on node '; cat $PBS_NODEFILE")
    fh.write("echo ------------------------------------------------------")
    fh.write("echo PBS: qsub is running on $PBS_O_HOST")
    fh.write("echo PBS: originating queue is $PBS_O_QUEUE")
    fh.write("echo PBS: executing queue is $PBS_QUEUE")
    fh.write("echo PBS: working directory is $PBS_O_WORKDIR")
    fh.write("echo PBS: execution mode is $PBS_ENVIRONMENT")
    fh.write("echo PBS: job identifier is $PBS_JOBID")
    fh.write("echo PBS: job name is $PBS_JOBNAME")
    fh.write("echo PBS: node file is $PBS_NODEFILE")
    fh.write("echo PBS: current home directory is $PBS_O_HOME")
    fh.write("echo PBS: PATH = $PBS_O_PATH")
    fh.write("echo ------------------------------------------------------")
    
    for cmd in commands:
        fh.write(cmd + "\n")   
    

def write_table(outfile,rows, header = None, delimiter = "\t", verbose = False):
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
                