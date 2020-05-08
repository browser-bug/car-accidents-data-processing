import sys
import getopt
import os
import glob
import subprocess
from subprocess import DEVNULL, STDOUT, check_call
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import textwrap
import time
import datetime
import re

# this wraps output messages such that each line is at most width characters long.
wrapper = textwrap.TextWrapper(width=80, replace_whitespace=False)


def main(argv):

    if len(argv) == 1:
        usage()
        sys.exit(0)

    # Default parameters
    binaryFile = ''
    hostFile = ''
    maxNumProcess = 4
    maxNumThreads = 4
    dimension = '1M'
    numiter = 10

    # Remember that "unlike GNU getopt(), after a non-option argument, all further arguments are considered also non-options".
    try:
        opts, args = getopt.getopt(argv[1:], "p:t:d:i:f:Th", [
            "numprocess=", "numthreads=", "dimension=", "numiter=",
            "hostfile=", "test", "help"
        ])
    except getopt.GetoptError:
        usageShort()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h' or opt == '--help':
            usage()
            sys.exit()
        elif opt in ("-p", "--numprocess"):
            maxNumProcess = int(arg)
        elif opt in ("-t", "--numthreads"):
            maxNumThreads = int(arg)
        elif opt in ("-d", "--dimension"):
            dimension = arg
        elif opt in ("-i", "--numiter"):
            numiter = int(arg)
        elif opt in ("-f", "--hostfile"):
            hostFile = arg
        elif opt in ("-T", "--test"):
            dimension = "test"

    binaryFile = argv[-1]  # last arg is the binary file
    datasetFile = f"dataset/collisions_{dimension}.csv"

    # Paths sanity checks
    binaryFilePath = Path(binaryFile)
    hostFilePath = Path(hostFile)
    datasetFilePath = Path(datasetFile)
    if not binaryFilePath.is_file():
        print(f"Binary file ({binaryFile}) doesn't exist...")
        sys.exit(1)
    elif not hostFilePath.is_file() and hostFile:
        print(f"Host file ({hostFile}) doesn't exist...")
        sys.exit(1)
    elif not datasetFilePath.is_file():
        print(f"Dataset file ({datasetFile}) doesn't exists...")
        sys.exit(1)

    binaryFileName = os.path.basename(binaryFile)
    binaryFileMode = getBinaryFileMode(binaryFile)
    # if serial mode set num. of process and threads
    if (binaryFileMode == 'serial'):
        maxNumProcess = 1
        maxNumThreads = 1

    # Cleaning old stats
    for filename in glob.glob(f"stats/*_{binaryFileMode}_{dimension}*"):
        os.remove(filename)

    startingTime = time.perf_counter()
    # Starts running specified binary {numiter} times with increasing num. of process and num. of threads
    for numprocess in range(1, maxNumProcess + 1):
        for numthreads in range(1, maxNumThreads + 1):
            runningMessage = f"Starting {binaryFile} for {numiter} iterations with {numprocess} MPI processes, {numthreads} OpenMP threads, {dimension} dataset size"
            print(runningMessage)

            if binaryFileMode != "serial":
                runCommand = f"mpirun -n {numprocess} " + \
                    (f"-f {hostFilePath.absolute()} " if hostFile else "") + \
                    f"./{binaryFileName} {numthreads} {dimension}"
            else:
                runCommand = f"./{binaryFileName} {dimension}"

            for _ in tqdm(range(numiter)):
                subprocess.run(runCommand,
                               cwd=(binaryFilePath.absolute().parent),
                               stdout=DEVNULL,
                               shell=True)

            # Collecting statistics
            statsFile = f"stats/stats_{binaryFileMode}_{dimension}_{numprocess}p_{numthreads}t.csv"
            statsFilePath = Path(statsFile)
            if not statsFilePath.is_file():
                print(f"Stats file ({statsFile}) doesn't exist...")
                sys.exit(1)
            statsDataFrame = pd.read_csv(statsFilePath)
            avgOverall = statsDataFrame["Overall"].mean()
            avgLoading = statsDataFrame["Loading"].mean()
            avgScattering = statsDataFrame["Scattering"].mean()
            avgProcessing = statsDataFrame["Processing"].mean()
            avgWriting = statsDataFrame["Writing"].mean()

            # Writing results
            resultsFile = f"stats/final_{binaryFileMode}_{dimension}.csv"
            resultsFilePath = Path(resultsFile)
            if not resultsFilePath.is_file():
                header = [
                    "DIMENSION", "NUM_PROCESS", "NUM_THREADS", "OVERALL",
                    "LOADING", "SCATTERING", "PROCESSING", "WRITING"
                ]
                outputDataFrame = pd.DataFrame(columns=header)
                outputDataFrame.to_csv(resultsFilePath.absolute(), index=False)

            newRow = {
                "DIMENSION": dimension,
                "NUM_PROCESS": numprocess,
                "NUM_THREADS": numthreads,
                "OVERALL": avgOverall,
                "LOADING": avgLoading,
                "SCATTERING": avgScattering,
                "PROCESSING": avgProcessing,
                "WRITING": avgWriting
            }
            outputDataFrame = pd.DataFrame(newRow, index=[0])
            outputDataFrame.to_csv(resultsFilePath.absolute(),
                                   mode='a',
                                   header=False,
                                   index=False)

    # Printing time duration
    durationTime = datetime.timedelta(seconds=(time.perf_counter() -
                                               startingTime))
    hours = durationTime.seconds // 3600
    minutes = (durationTime.seconds // 60) % 60
    seconds = durationTime.seconds % 60
    print(f"Done. Time elapsed: {hours}:{minutes}:{seconds} (h:m:s)")

    # Cleaning garbage files
    for filename in glob.glob(f"stats/stats_{binaryFileMode}*"):
        os.remove(filename)


def getBinaryFileMode(binaryFile):
    if "serial" in binaryFile:
        return "serial"
    if "mono" in binaryFile:
        return "monoread"
    if "multi" in binaryFile:
        return "multiread"


def usageShort():
    """Print short summary on script usage"""

    shortUsageMessage = "run.py -p <max_num_mpi_process> -t <max_num_omp_threads> -d <size> -i <numiter> -f <hosts_file> -T binary_file"
    print(shortUsageMessage)


def usage():
    """Print script description, usage and options with high verbosity"""

    descriptionMessage = """Description:
    This script allows the user to make a scalability analysis by evaluating the performance when increasing the number of OpenMP threads and/or the number of MPI processes. A precompiled (and working) binary file must be present in order to allow a correct execution environment, as well as an existing dataset to test the binary with.
    """
    word_list = wrapper.wrap(text=descriptionMessage)
    for line in word_list:
        print(line)
    usageMessage = "\nUsage:\trun.py [global-opts] <global-args> binary_file"
    print(usageMessage)
    optionsMessage = """Options:
-h, --help\t\tshow this help message
-p, --numprocess\tselect max. number of MPI process you want to run (default 4)
-t, --numthreads\tselect max. number of OpenMP threads to spawn for each process (default 4)
-d, --dimension\t\tset the dataset size to work on (default: 1M)
-i, --numiter\t\tset the number of iterations to execute for each testcase (default: 10)   
-f, --hostfile\t\thost file used by mpirun
-T, --test\t\trun with the test (small sized) dataset"""
    print(optionsMessage)


if __name__ == "__main__":
    main(sys.argv)
