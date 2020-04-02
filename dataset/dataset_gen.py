import pandas as pd
from math import modf
import sys

#
# ASSUMPTION: dataset input file number of rows order is 1M
#
if len(sys.argv) < 3:
    print 'Express num. of rows in M: ', sys.argv[0], '<input_file_path> [<num_of_rows>]+'    
    exit(-1)

dataset_path = sys.argv[1]
print "Reading input file..."
dataframe_input = pd.read_csv(dataset_path)
print "Done."

print "Shuffling dataframe..."
dataframe_input = dataframe_input.sample(frac=1).reset_index(drop=True) # shuffle rows
print "Done."

for k in range(2, len(sys.argv)):
    dataframe_output = pd.DataFrame()

    num_rows = float(sys.argv[k]) # number of rows to generate
    decimal_part, integer_part = modf(num_rows)

    print "\nGenerating new dataset with %sM rows... " % (sys.argv[k])
    dataframe_output = dataframe_input

    dfs = []
    if decimal_part != 0:
        take_num = int(decimal_part * 10**6);
        df_last_rows = dataframe_input.tail(take_num) # take last n rows
        dfs.append(df_last_rows)
    if integer_part != 1:
        for _ in range(int(integer_part)):
            dfs.append(dataframe_input)

    if len(dfs) > 0:
        dataframe_output = pd.concat(dfs, ignore_index=True)
    print "Done."

    print "Writing output file..."
    dataframe_output.to_csv('collisions_' + sys.argv[k] + 'M.csv', index=False)
    print "Done."