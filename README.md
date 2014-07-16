iotoolsExamples
===============

This project contains three examples of using the [iotools](https://github.com/s-u/iotools)
R package for running jobs using hadoop streaming. It assumes that a working
Hadoop installation has already been set-up, and the iotools package
has been installed (tested using version 0.1-4). It has a copious
number of comments, offering a gentle introduction along with
several motivating examples.

The "create_data.R" script must be executed first to construct the
various datasets used in the examples. Then, the three example
scripts can be executed:

1. example01: Given a numeric matrix, calculate the maximum of each row, and then the mean of these maximums across all rows using a call to iotools::hmr.
2. example02: Execute a left join between two datasets on a common key and do some data manipulation with a smaller table along the way, again using a call to iotools::hmr.
3. example03: Replicate the previous example using a shell script and hand coding the data formating using the lower-level functions of the iotools package.

These show a fairly wide range of potential uses for hadoop streaming
with the iotools package. Other examples and documentation can be
found within the iotools package manual pages.
