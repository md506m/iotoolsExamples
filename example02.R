#!/usr/bin/env Rscript
#
# Name: example02.R
# Date: 2014-07-16
# Author: Taylor Arnold <taylor@research.att.com>
# Purpose: Show a more involved example using the hmr function
#   in the iotools package to merge two datasets on a common key.
#   Assumes that create_data.R as been run to create the data, and
#   example01.R has been run to push the data to hdfs.


# Load in the iotools package and configure the environment
# variables and set conveniance function hfs; refer to the
# comments in example01.R for help with this step.

library("iotools")

if(Sys.getenv("HADOOP_PREFIX") == "") {
  Sys.setenv(HADOOP_PREFIX="/usr/lib/hadoop")
}

if(Sys.getenv("HADOOP_STREAMING_JAR") != "") {
  Sys.setenv(HADOOP_STREAMING_JAR="your_location_here")
}

hfs = paste0(Sys.getenv("HADOOP_PREFIX"), "/bin/hadoop fs")

# In this example, we have two (simulated) datasets. The first,
# input02_hh_income_2013, is a two column csv file giving a
# household alphanumeric key, followed by the household's 2013
# income. The second file, input02_hh_income_2014, is a three
# column matrix in csv format giving a household alphanumeric
# key, followed by the household's 2014 income, and a two digit
# code giving the household's state abbreviation. Approximately
# 50% of the keys in one file matches the keys in the other file;
# also, many of the state codes are missing in the 2014 file.

# Our goal is to do a left join of the 2014 data on the 2013 data,
# joining on the alphanumeric household key. We also want to convert
# the state abbreviations to the full state names.

# Consider the additional difficulties in this example over the first:
#
#   (1) Must the specify that the data is seperated by commas
#   (2) Need matching keys to go to the same reducer, but can have multiple
#       reducers for the job
#   (3) Need to input multiple files
#   (4) The different files have a different number of columns
#   (5) Need to pass information mapping state abbreviations to state names
#   (6) We need to be able to identify if a record came from the 2013 dataset
#         or the 2014 data. Since the state abbreviations are missing sometimes,
#         we cannot rely on this.

# Because the input files have similar names, it is possible to work
# around issue (3) by specifying the hinput as "data/input0[23]_hh_income_201[34].csv"
# This works well is some situations, but we will give a solution that does
# not depend on inputs with similar filenames. As a state names mapping is present
# in the base environment of R, we can also work around issue (5), but will again
# present a more generic solution.

# So, for generic solutions to these issues we will do the following:
#
#   (1) Specify the map formatter as function(m) {mstrsplit(m, ",")}
#   (2) Set the row names of the output appropriately
#   (3) Declare an additional input file using the "hadoop.opt" option to hmr
#   (4) This is not actually a problem as an input to the mappers, but will need
#         to address this in the output of the mappers by making sure all of the
#         output from the mappers have the same number of columns
#   (5) Will send additional data to the mappers and reducers using the "aux"
#         option to hmr
#   (6) We will add an additional column to the output of the mappers giving an
#         id the data records to indicate which input the record came from.

# The reason that (4) is not a problem for the mappers is that a single mapper
# will only ever get inputs from a single input file.

# Now, we show the code to implement these changes:

# We need to remove the output directory, if it exists:

system(paste0(hfs, " -rm -r iotools_examples/output02"))

# We also construct the map between state abbreviation and name:

state_map = cbind(state.abb, state.name)

# Run the streaming job (note, there will be a lot of output to the console
# from this call).

r = hmr(input = hinput("iotools_examples/input/input02_hh_income_2013.csv"),
        output = hpath("iotools_examples/output02"),
        formatter = list(map = function(m) return(mstrsplit(m, ",")), # for csv file
                         reduce = function(m) mstrsplit(m, "|", "\t")),
        wait = TRUE,
        aux = list(state_map = state_map),
        reducers = 10, # probably overkill, but helps to illustrate the example
        map = function(m) {
            if(ncol(m) == 2) { # Test if this mapper has 2013 data
              output = cbind("2013", m[,2], "") # note: extra column for missing state
              rownames(output) = m[,1]
            }
            if(ncol(m) == 3) { #  Test if this mapper has 2014 data
              output = cbind("2014", m[,2:3])
              rownames(output) = m[,1]

              # Convert state abbreviations to state names
              index = match(output[,3], state_map[,1])
              output[!is.na(index),3] = state_map[index[!is.na(index)],2]
            }
            return(output)
        },
        reduce = function(m) {
            mat_2013 = m[m[,1] == "2013",-1]
            mat_2014 = m[m[,1] == "2014",-1]

            # Left join mat_2014 on mat_2013
            index = match(rownames(mat_2014), rownames(mat_2013))
            mat_2014 = cbind(mat_2014, "") # make an empty column for 2013 data
            if(any(!is.na(index))) {
              mat_2014[!is.na(index),3] = mat_2013[index[!is.na(index)],1]
            }

            # Reformat the output data
            output = cbind(rownames(mat_2014), mat_2014[,c(3,1,2)])
            rownames(output) = NULL
            return(output) # format: key|income_2013|income_2014|state_name
        },
        hadoop.opt="-input iotools_examples/input/input03_hh_income_2014.csv"
  )

# You can see how hadoop distributed the job by seeing the file sizes of the
# ten reducers:

system(paste0(hfs, " -du iotools_examples/output02"))

# And we can view a few rows of the join by calling hadoop fs -cat (also
# can use -tail on a particular file, but this requires making sure you
# know which reducers actually received input data, which may not be all
# of them in this small example)

system(paste0(hfs, " -cat iotools_examples/output02/part-* | head -n 30"))
