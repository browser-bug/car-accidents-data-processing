#! /bin/sh
binary_file=$1
python3 run.py -d 0.2M -p 8 -t 6 -f mpich2.hosts $binary_file  \
&& python3 run.py -d 0.5M -p 8 -t 6 -f mpich2.hosts $binary_file \
&& python3 run.py -d 1M -p 8 -t 6 -f mpich2.hosts $binary_file \
&& python3 run.py -d 2M -p 8 -t 6 -f mpich2.hosts $binary_file \
&& python3 run.py -d 5M -p 8 -t 6 -f mpich2.hosts $binary_file \
&& python3 run.py -d 10M -p 8 -t 6 -f mpich2.hosts $binary_file