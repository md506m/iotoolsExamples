#!/usr/bin/env Rscript
#
# Name: example03_reduce.sh
# Date: 2014-07-16
# Author: Taylor Arnold <taylor@research.att.com>
# Purpose: To be used in conjunction with example03.sh. See the
#   shell script for more information.

library(iotools)

# Set up a reader to stdin and writer to stdout
input = file("stdin", "rb")
output = stdout()
reader = iotools::chunk.reader(input)

state_map = readRDS("state_map.Rds")

# While scanning through lines of the mapper:
while(TRUE) {
  # Input boilerplate
  chunk = iotools::read.chunk(reader)
  if (!length(chunk)) break
  m = mstrsplit(chunk, "|", "\t")

  # Now, the stdoutame as the reducer in the hmr call:
  mat_2013 = m[m[,1] == "2013",-1,drop=FALSE]
  mat_2014 = m[m[,1] == "2014",-1,drop=FALSE]

  # Left join mat_2014 on mat_2013
  index = match(rownames(mat_2014), rownames(mat_2013))
  mat_2014 = cbind(mat_2014, "") # make an empty column for 2013 data
  if(any(!is.na(index))) {
    mat_2014[!is.na(index),3] = mat_2013[index[!is.na(index)],1]
  }
  # Reformat the output data
  out = cbind(rownames(mat_2014), mat_2014[,c(3,1,2)])

  # Manually format the output and write to stdout
  out = iotools:::as.output.matrix(out)
  writeLines(out, output)
}