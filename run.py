import sys, getopt, argparse, subprocess
from pathlib import Path


def main(argv):

    # Sanity check of arguments
    binary = ''
    hostFile = ''
    numprocess = 4
    numthreads = 4
    dimension = '1M'
    numiter = 10
    test = False

    # Remember that "unlike GNU getopt(), after a non-option argument, all further arguments are considered also non-options".
    try:
        opts, args = getopt.getopt(argv, "p:t:d:i:f:Th", [
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
            numprocess = int(arg)
        elif opt in ("-t", "--numthreads"):
            numthreads = int(arg)
        elif opt in ("-d", "--dimension"):
            dimension = arg
        elif opt in ("-i", "--numiter"):
            numiter = int(arg)
        elif opt in ("-f", "--hostfile"):
            hostFile = arg
        elif opt in ("-T", "--test"):
            test = True

        binary = argv[-1]  # last arg is the binary file
        datasetName = f"collisions_{dimension}.csv"

    # Paths sanity checks
    binaryPath = Path(binary)
    hostFilePath = Path(hostFile)
    if not binaryPath.is_file():
        print("Binary file doesn't exist...")
        sys.exit(1)
    elif not hostFilePath.is_file() and hostFile:
        print("Host file doesn't exist...")
        sys.exit(1)

    # Starts running specified binary N times
    runningMessage = f"Starting {binary} for {numiter} times with {numprocess} MPI processes, {numthreads} OpenMP threads, {dimension} dataset size"
    print(runningMessage)
    for _ in range(numiter):
        subprocess.run(["mpirun", "-n", f"{numprocess}", f"./{binary}"],
                       cwd=(binaryPath.absolute().parent))

    # Collect statistics

    # Averaging and outputting graph


def usageShort():
    """Print short summary on script usage"""

    shortUsageMessage = "run.py -p <num_mpi_process> -t <num_omp_threads> -d <size> -i <numiter> -f <hosts_file> -T"
    print(shortUsageMessage)


def usage():
    """Print script usage and options with high verbosity"""

    usageMessage = "Usage: run.py binary_name [global-opts] <global-args>"
    optionsMessage = """Options:
    -h, --help\t\tshow this help message
    -p, --numprocess\tselect number of MPI process you want to run (default 4, min 1, max 8)
    -t, --numthreads\tselect number of OpenMP threads to spawn for eachprocess (default 4, min 1, max 8)
    -d, --dimension\tset the dataset size to work on (default: 1M)
    -i, --numiter\tset the number of iteration to execute for each testcase (default: 10)   
    -f, --hostfile\thost file used by mpirun
    -t,--test\t\trun with the test (small sized) dataset
    """
    print(usageMessage + optionsMessage)


if __name__ == "__main__":
    main(sys.argv[1:])