#!/usr/bin/env Rscript
#
# Name: example04.R
# Date: 2014-08-19
# Author: Taylor Arnold <taylor@research.att.com>
# Purpose: Show a simple example of using hadoop to run a small
#   logistic regression using coordinate descent. NOTE: This is
#   not a particularly good way in practice to do large logistic
#   regressions, and is only meant as a simple illustration.
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

# The data we generated in "create_data.R" has the y values in the first
# column, and the data matrix X in the remaning columns. We have already
# included a column of 1's, so there is no need to include an intercept
# into the model. As our example is relatively small, we can read the data
# locally into R:
z = read.table("data/input04_logistic_regression.dat", sep="|", header=FALSE)
x = as.matrix(z[,-1])
y = z[,1]

# Again, as we have choosen a small example, the true maximum likelihood
# logistic regression vector can be easily calculated using the glm function:
beta_hat_actual = coef(glm(y ~ x - 1, family=binomial))

# Unfortunately, you cannot simply run the glm function on blocks of the data
# and just combine the results (actually, you can do this and in many cases it
# can be a good idea, but it won't give the maximum likelihood estimator we
# are after here). Instead, we need to estimate the beta_hat estimator using
# a seperable algorithm such as gradient descent.

# As a starting point, this code runs gradient descent locally on the entire
# data matrix, using 6 interations and printing out the L2 distance between
# the estimated beta_hat and the beta_hat_actual from the glm function:

beta_hat = rep(0, ncol(x))
lambda = 0.01

for(rep in 1:6) {
  p_hat = 1 / (1 + exp(-1 * x %*% beta_hat))
  grad = t(y - p_hat) %*% x
  beta_hat = as.numeric(beta_hat + lambda * grad)
  print(sum((beta_hat - beta_hat_actual)^2))
}

# This should show the estimated value converging to the value from the glm
# function.

# In order to apply gradient descent over hadoop mappers, we calculate the
# p_hat (predicted probabilites) and partial gradients over row blocks of
# the data. The partial gradients can then be added together component-wise
# within the reducer. The update is then done external the hadoop job.

# Notice that this requires hadoop to execute an entire map-reduce job for
# each iteration.

# NOTE: Change the "-rmr" flag below to "-rm -r" if using a newer version of
#   hadoop (you will know because the "-rmr" produces an error in newer versions)

beta_hat = rep(0, ncol(x))
lambda = 0.01

error = rep(NA, 6)
for(rep in 1:6) {
  r = hmr(hinput("iotools_examples/input/input04_logistic_regression.dat"),
          hpath("iotools_examples/output02"),
          reducers=1L,
          aux=list(beta_hat = beta_hat),
          formatter=list(mapper=function(m) mstrsplit(m, "|"),
                         reducer=function(m) mstrsplit(m, "|", "\t")),
          map = function(x) {
            x = apply(x, 2, as.numeric)
            y = x[,1]
            x = x[,-1]
            p_hat = 1 / (1 + exp(-1 * x %*% beta_hat))
            grad = t(y - p_hat) %*% x
            return(paste0("1\t",paste0(grad,sep="",collapse="|")))
          },
          reduce = function(grad) {
            grad = apply(grad, 2, as.numeric)
            grad = apply(grad, 2, sum)
            return(paste0(grad,sep="",collapse="|"))
          })

  grad = system(paste0(hfs, " -cat ", r, "/part-00000"),intern=TRUE)
  grad = as.numeric(mstrsplit(gsub("\t", "", grad), "|"))
  beta_hat = as.numeric(beta_hat + lambda * grad)

  system(paste0(hfs, " -rmr ", r),intern=TRUE)

  error[rep] = sum((beta_hat - beta_hat_actual)^2)
  print(error)
}

# Print the errors at the end, as the get lost in all of the hadoop log output:
print(error)



