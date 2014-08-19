#!/usr/bin/env Rscript
#
# Name: example01.R
# Date: 2014-07-16
# Author: Taylor Arnold <taylor@research.att.com>
# Purpose: Set up iotools for an existing hadoop installation
#   and demo simple example using iotools. You must first
#   run the create_data.R script and install iotools

# Load in the iotools package (if this fails, you will need
# to make sure the package has been installed correctly)

library("iotools")

# The first step is to configure iotools to work with your
# hadoop set-up. This script assumes that you already have
# a working hadoop client.

# The script needs to know the base directory of your
# hadoop installation. This is done by setting the HADOOP_HOME
# or HADOOP_PREFIX environment variable. By default, iotools
# will try to find the installation in /usr/lib/hadoop; this
# flag may also have already been set up on your system.

# If you are having trouble figuring out what to set HADOOP_HOME
# to, note that it should be set such that the "hadoop" command
# lives at $HADOOP_HOME/bin/hadoop.

if(Sys.getenv("HADOOP_HOME") == "") {
  Sys.setenv(HADOOP_HOME="/usr/lib/hadoop")
}

# Secondly, the iotools package needs to be able to figure out
# where the hadoop streaming jar is located. However, for hadoop
# versions 2.0+, the iotools package will likely be able to find
# the streaming jar automatically. If this fails, edit the code
# below to manually specify the location using the envirnoment
# variable HADOOP_STREAMING_JAR.

# You should only edit this if the hmr command below fails!

if(Sys.getenv("HADOOP_STREAMING_JAR") != "") {
  Sys.setenv(HADOOP_STREAMING_JAR="your_location_here")
}

# A handy shortcut for the hadoop fs command:
hfs = paste0(Sys.getenv("HADOOP_HOME"), "/bin/hadoop fs")

# Now, we need to create space on the Hadoop file system to store
# the input datasets. We'll using ~/iotools_examples/input. These
# commands create the directory (if not already extant) and push
# the three datasets created by "create_data.R" onto hdfs:

system(paste0(hfs, " -rmr iotools_examples/"))
system(paste0(hfs, " -mkdir -p iotools_examples/input"))
system(paste0(hfs, " -put data/* iotools_examples/input"))

# Now, we are ready to run an hadoop streaming job using iotools.
# Our goal is to take the file "input_data01.dat", which contains
# a large 20 column matrix of independent samples from a random
# standard normal distribution, calculate the maximum of each row,
# and then calculate the mean of this maximum over all rows.

# The basic call to "hmr" (hadoop mapreduce) needs to know where
# the input data is, where the output data should go (this must be
# an non-existing directory on hdfs) and what functions to call as
# the mappers and and reducers. No other options are needed to make
# this example work.

# Remove the output directory, in case it already exists

system(paste0(hfs, " -rm -r iotools_examples/output01"))

# Run the streaming job (note, there will be a lot of output from
# this job). The output variable r will be a character vector
# indicating the output path on hdfs.

r = hmr(input = hinput("iotools_examples/input/input01_rnorm_matrix.dat"),
        output = hpath("iotools_examples/output01"),
        wait = TRUE,
        map = function(m) {
            m_numeric = apply(m, 2, as.numeric)
            m_max = apply(m_numeric, 1, max)
            names(m_max) = rep("0", length(m_max))
            return(m_max)
        },
        reduce = function(m) {
            m_mean = mean(as.numeric(m))
            return(m_mean)
        }
  )

# As the finaly, result should just be a single number, we can display
# the outout using a call to hadoop fs -cat:

system(paste0(hfs, " -cat iotools_examples/output01/part-*"))

# The only difficult part about calling the hmr function is understanding
# the way data is input and output from the mappers and reducers. By default
# the input to both uses a call to the mstrsplit function; locally we can
# replicate this on the first three rows of the input dataset:

x = scan("data/input01_rnorm_matrix.dat", what="raw", sep="\n", nmax=3)
m = mstrsplit(x, "|", "\t")
print(m)

# Printing m, one can see that the mstrsplit function creates a character
# matrix. This is why we need to use "apply(m,2,as.numeric)" on m in the
# map function. In our case, we constructed the input to use "|" as the
# seperating character; if you have a different seperating character, or
# want an entirely different formatter altogether, the hmr function accepts
# a formatter argument. There is good documentation on both hmr and mstrsplit
# detailing how to do this, and we will demonstrate this in the next example.

# The formatting of the output of a call to the mapper (or reducer) depends
# on the class of the output. We can simulate this using the as.output function
# in iotools (note: this is not an exported object, should not be considered
# part of the formal package API, and is being used here only for illustration):

# For a matrix, the as.output function creates a character vector with the
# rowname seperated by a tab character, with the remainder of the vector
# being collapsed by the "|" seperator:

iotools:::as.output.matrix(m)

# For a vector (such as m_max in our example) the output is instead
# the names of the vector seperated by a tab character, pasted to the
# values of the vector:

m_max = apply(apply(m, 2, as.numeric),1, max)
names(m_max) = rep("1", length(m_max))
iotools:::as.output.default(m_max)

# There are additional methods for lists and tables that work similarly.

# WHY DO WE USE THESE COMPLICATED OUTPUT FUNCTION, and WHY ARE WE USING
# *BOTH* TABS AND VERTICAL BARS!? - The reason for two types of seperators
# is to facilitate seperating the output from the mappers into <key,value>
# pairs in the hadoop streaming job. The iotools package is set up to
# send key value pairs seperated by a tab character. So the tab seperates
# the <key> and the verticle bar seperates all of the columns in the single
# <value>. The mstrsplit function was written to handle input using two types
# of seperators; the "key" is placed into the rownames/names argument of the
# matrix/vector, and the value is split by the "|" seperator (in the case of
# a matrix).

# In our example, we set all of the keys equal to "1", so that hadoop would
# know to send all of the values to the same reducer in order to calculate a
# grand mean. In the next example we'll show a more involved use of keys to
# join two tables together.

# NOTE: The default formatter function for hmr converts a one column matrix
# into a vector. This is why the reduce function uses as.numeric(m) rather
# than as.numeric(m[,1]). If you manually specify the formatter in the call
# to mstrsplit(m, "|", "\t"), you would need to use the latter form.

