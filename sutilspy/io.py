#!/usr/bin/env python
# Copyright (C) 2017 Sur Herrera Paredes

import subprocess
import csv

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
                
def concatenate_files(infiles, outfile):
    command = " ".join(infiles) 
    command = "cat " + command + " > " + outfile
    check = download_runs.run_command(command)
    
    if check.returncode != 0:
        raise ProcessError("Could not concatenate files")
    
    return(check)

def write_download(download,outfile):
    with open(outfile,'w') as out_fh:
        out_fh.write(download.text)
    out_fh.close()

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

def run_command(command):
    status = 0;
    print("Executing:\n>{}".format(command))
    status = subprocess.run(command, shell = True)
    print("Status={}\n\n".format(status));

    return(status)

def qsub_submissions(submissions,logdir):
    
    if os.path.exists(logdir):
        print("Logdir exists")
    else:
        os.mkdir(logdir)
        
    for file in submissions:
        run_command("qsub " + file)
        
    print("==========SUBMISSIONS DONE==========\n\n")

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