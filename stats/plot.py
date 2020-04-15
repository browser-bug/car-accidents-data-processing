import sys, getopt, argparse
import os, glob
import subprocess
from subprocess import DEVNULL, STDOUT, check_call
from pathlib import Path
import pandas as pd
from tqdm import tqdm

import numpy as np
import matplotlib.pyplot as plt
import csv
import re


def main(argv):
    if (len(argv) != 2):
        print(f"Usage: {argv[0]} <final_results_file>")
        sys.exit(2)

    resultFile = argv[1]
    mode = getMode(resultFile)
    dimension = getDatasetSize(resultFile)
    df = pd.read_csv(resultFile)
    grouped = df.groupby('NUM_THREADS')

    threadGroups = [grouped.get_group(x) for x in grouped.groups]

    fig, axes = plt.subplots(1, 4)
    ax1 = axes[0]  # Overall times
    ax2 = axes[1]  # Loading times
    ax3 = axes[3]  # Scattering times
    ax4 = axes[2]  # Processing times
    fig.suptitle(f"Mode: {mode} | Dataset: {dimension} rows")
    for i, gp in enumerate(threadGroups):
        num_threads = gp.iloc[0]['NUM_THREADS']
        num_proc = gp['NUM_PROCESS'].tolist()

        # Overall
        ax1.title.set_text("Overall times")
        overallTimes = gp['OVERALL'].tolist()
        ax1.plot(num_proc,
                 overallTimes,
                 label=f'#threads {num_threads}',
                 marker='*')
        ax1.set_xticks(num_proc)

        # Loading
        if i == 0:  # we need just one since num.threads doesn't affect loading times
            ax2.title.set_text("Loading times")
            loadTimes = gp['LOADING'].tolist()
            ax2.plot(num_proc, loadTimes, marker='*')
            ax2.set_xticks(num_proc)

        # Scattering
        ax3.title.set_text("Scattering times")
        if mode == 'monoread' and i == 0:  # we need just one since num.threads doesn't affect scattering times
            scatteringTimes = gp['SCATTERING'].tolist()
            ax3.plot(num_proc, scatteringTimes, marker='*')
        ax3.set_xticks(num_proc)

        # Processing
        ax4.title.set_text("Processing times")
        procTimes = gp['PROCESSING'].tolist()
        ax4.plot(num_proc,
                 procTimes,
                 label=f'#threads {num_threads}',
                 marker='*')
        ax4.set_xticks(num_proc)

    totalOverallTimes = df['OVERALL'].tolist()
    totalLoadingTimes = df['LOADING'].tolist()
    totalScatteringTimes = df['SCATTERING'].tolist()
    totalProcessingTimes = df['PROCESSING'].tolist()
    ax1.set_yticks(roundAndRange(totalOverallTimes, 0.5))
    ax2.set_yticks(roundAndRange(totalLoadingTimes, 0.5))
    if mode == 'monoread':
        ax3.set_yticks(roundAndRange(totalScatteringTimes, 0.5))
    ax4.set_yticks(roundAndRange(totalProcessingTimes, 0.5))

    for ax in axes.flat:
        ax.set_xlabel('Num. process')
        ax.set_ylabel('Time (s)')
        ax.grid('on')
        ax.legend(loc='upper right')

    # Output charts
    plt.show()

    # Save charts
    # fig.set_size_inches(25, 11)
    # plt.savefig(f"chart_{mode}_{dimension}.pdf")


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


def roundAndRange(l, S):
    """Round all element of a list <l> and return a range with step <S>"""

    roundedList = list(map(round, l))
    return np.arange(min(roundedList), max(roundedList) + 1, S)


if __name__ == "__main__":
    main(sys.argv)