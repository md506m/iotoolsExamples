#!/usr/bin/env Rscript
#
# Name: example03_map.sh
# Date: 2014-07-16
# Author: Taylor Arnold <taylor@research.att.com>
# Purpose: To be used in conjunction with example03.sh. See the
#   shell script for more information.

library(iotools)

# Set up a reader to stdin and writer to stdout
input = file("stdin", "rb")
output = stdout()
reader = iotools::chunk.reader(input)

# this is present because the call to files in the streaming call
state_map = readRDS("state_map.Rds")

# While scanning through lines of the mapper:
while(TRUE) {
  # Input boilerplate
  chunk = iotools::read.chunk(reader)
  if (!length(chunk)) break
  m = mstrsplit(chunk, ",")

  # Now, the same as the mapper in the hmr call:
  if(ncol(m) == 2) { # 2013
    out = cbind("2013", m[,2], "") # note: extra column for missing state
    rownames(out) = m[,1]
  }
  if(ncol(m) == 3) { # 2014
    out = cbind("2014", m[,2:3])
    rownames(out) = m[,1]

    # Convert state abbreviations to state names
    index = match(out[,3], state_map[,1])
    out[!is.na(index),3] = state_map[index[!is.na(index)],2]
  }

  # Manually format the output and write to stdout
  out = iotools:::as.output.matrix(out)
  writeLines(out, output)
}

