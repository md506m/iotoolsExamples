#!/usr/bin/env Rscript
#
# Name: create_data.R
# Date: 2014-07-16
# Author: Taylor Arnold <taylor@research.att.com>
# Purpose: Construct three datasets (locally) needed for
#   running example01.R, example02.R, and examploe03.sh

# This script should be executed from within the examples
# directory. It can be run on any standard installation
# of R, and does not itself call any Hadoop functions or
# make calls to the iotools package.

system("mkdir -p data")

# input_data01.dat:
N = 1e6
input = matrix(round(rnorm(N*20),3), nrow=N)
write.table(input, "data/input01_rnorm_matrix.dat",
            sep="|", col.names=FALSE, row.names=FALSE)

# input02_hh_income_2013.dat:
N = 1e6
keys = apply(matrix(sample(c(letters,LETTERS), 14*N, replace=TRUE),
                    ncol=14),1,paste0,collapse="")
input = data.frame(keys = keys,
                   income = round(rgamma(N,3,3/60000)))
write.table(input[sample(1:nrow(input),N/2),], "data/input02_hh_income_2013.csv",
            sep=",", col.names=FALSE, row.names=FALSE, quote=FALSE)

# input03_hh_income_2014.date
input$income = round(input$income + runif(N, min=0, max=10000))
input$state = sample(c(rep("",10), state.abb), nrow(input), replace=TRUE)
write.table(input[sample(1:nrow(input),N/2),], "data/input03_hh_income_2014.csv",
            sep=",", col.names=FALSE, row.names=FALSE, quote=FALSE)

