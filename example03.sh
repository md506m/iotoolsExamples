#!/usr/bin/env sh
#
# Name: example03.sh
# Date: 2014-07-16
# Author: Taylor Arnold <taylor@research.att.com>
# Purpose: Illustrate re-doing example02.R using a shell script
#   and manually calling the hadoop streaming job. This forgoes
#   the hmr function in iotools, but takes advantage of lower
#   level functions in the package. This construct is useful
#   as (i) it illustrates the inner workings of the iotools package,
#   and (ii) it can be useful to call streaming directly when
#   using mappers and reducers in different languages (i.e., parsing
#   text data in python using nltk from the mappers and then running
#   statistically summaries with R in the reducers).

# Unlike example01 and example02, there is a lot less commentary
# in this example. It simple mimics the functionality of example02
# using a different calling construct.

# Note, this is a shell script and will not run if called by R. It also
# requires the Rscript be present and on the Hadoop nodes and accesible
# via "/usr/bin/env"; otherwise you will need to edit the hashbang line
# of the mappers and reducers

# Change these if needed as in the set-up from example01:
HADOOP_HOME=/usr/lib/hadoop
HADOOP_STREAMING_JAR=/usr/lib/hadoop-mapreduce/hadoop-streaming-2*.jar

# Need to create state_map as a file and pass explicitly using
# the -files option to the streaming call
R -e "saveRDS(cbind(state.abb, state.name), 'state_map.Rds')"

# Remove the output directory if it already exists
$HADOOP_HOME/bin/hadoop fs -rm -r iotools_examples/output03

/usr/lib/hadoop/bin/hadoop \
    jar $HADOOP_STREAMING_JAR \
    -D mapreduce.job.name="iotools_example03" \
    -D mapred.reduce.tasks=1 \
    -files example03_map.R,example03_reduce.R,state_map.Rds \
    -input iotools_examples/input/input02_hh_income_2013.csv \
    -input iotools_examples/input/input03_hh_income_2014.csv \
    -output iotools_examples/output03 \
    -mapper example03_map.R \
    -reducer example03_reduce.R

# As before, see the head of the output:
/usr/lib/hadoop/bin/hadoop fs -cat iotools_examples/output03/part-* | head -n 30




