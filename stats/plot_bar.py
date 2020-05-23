import sys
import pandas as pd

import matplotlib.pyplot as plt
import re


def main(argv):
    if (len(argv) != 3):
        print(
            f"""Generates a bar plot for <final_results_file> based on <num_threads> showing each stage of the pipeline in sequence.
Usage: {argv[0]} <final_results_file> <num_threads>""")
        sys.exit(2)

    resultFile = argv[1]
    numThreads = int(argv[2])

    mode = getMode(resultFile)
    dimension = getDatasetSize(resultFile)
    df = pd.read_csv(resultFile)
    grouped = df.groupby('NUM_PROCESS')

    processGroups = [grouped.get_group(x) for x in grouped.groups]
    numTotProc = len(processGroups)

    # Every i-th element here represent the stage time for i processes
    loadTimes = [0] * numTotProc
    scatteringTimes = [0] * numTotProc
    processingTimes = [0] * numTotProc
    outputTimes = [0] * numTotProc

    processNumbers = []
    # Collecting stage times for every num. processes
    for i, gp in enumerate(processGroups):
        num_proc = gp.iloc[0]['NUM_PROCESS']
        processNumbers.append(num_proc)

        num_threads_list = gp['NUM_THREADS'].tolist()

        if (len(num_threads_list) < numThreads):
            print(f"Number of threads requested exceeded: {numThreads}")
            sys.exit(1)

        loadTimes[i] = gp['LOADING'].tolist()[numThreads - 1]
        if (mode not in "serial"):
            scatteringTimes[i] = gp['SCATTERING'].tolist()[numThreads - 1]
        processingTimes[i] = gp['PROCESSING'].tolist()[numThreads - 1]
        outputTimes[i] = gp['WRITING'].tolist()[numThreads - 1]

    # Creating plot figure
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.set_title(
        f"Mode: {mode} | Dataset: {dimension} rows | Threads per process: {numThreads}"
    )
    ax.set_yticks(processNumbers)

    # Creating stage bars
    loadBar = ax.barh(processNumbers,
                      loadTimes,
                      color="green",
                      label="Loading")
    processingBar = ax.barh(processNumbers,
                            processingTimes,
                            left=loadTimes,
                            color="brown",
                            label="Processing")
    load_processing = [x + y for x, y in zip(loadTimes, processingTimes)]
    outputBar = ax.barh(processNumbers,
                        outputTimes,
                        left=load_processing,
                        color="b",
                        label="Writing")
    load_processing_writing = [
        x + y for x, y in zip(load_processing, outputTimes)
    ]
    scatteringBar = ax.barh(processNumbers,
                            scatteringTimes,
                            left=load_processing_writing,
                            color="y",
                            label="Scattering")

    # Add time values on top of relevant bars
    threshold = 0.1  # filters all stage times below this value
    for sub_bar in loadBar + scatteringBar + processingBar + outputBar:
        value = sub_bar.get_width()
        if (value < threshold):
            continue
        bl = sub_bar.get_xy()
        x = 0.5 * sub_bar.get_width() + bl[0]
        y = 0.5 * sub_bar.get_height() + bl[1]
        ax.text(x,
                y,
                '%.1f' % float(value),
                ha='center',
                va='center',
                color='w')

    plt.xlabel('Time(s)')
    plt.ylabel('Num. process')
    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))

    plt.show()
    # Save charts
    # fig.set_size_inches(25, 11)
    # plt.savefig(f"bar_{mode}_{dimension}_{numThreads}threads.pdf")


def getMode(resultFile):
    """Retrieve the processing mode used (eg. monoread, multiread, etc.)"""

    result = re.search(r"final_([a-zA-Z]+)_", resultFile)
    if not result:
        print(f"Result file name isn't correct ({resultFile})")
        sys.exit(1)
    return result.group(1)


def getDatasetSize(resultFile):
    """Retrieve the dataset dimension from input file name"""

    # this is not completely correct (es. "_123.M" gets pass it) but does the work
    result = re.search(r"_((\d+\.)?\d*M).csv", resultFile)
    if not result:
        print(f"Result file name isn't correct ({resultFile})")
        sys.exit(1)
    return result.group(1)


if __name__ == "__main__":
    main(sys.argv)