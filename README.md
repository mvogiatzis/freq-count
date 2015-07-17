# Frequency Count Algorithms for Data Streams
The project provides a Scala implementation of the Lossy Counting and Sticky Sampling algorithms for efficient counting on data streams. You can find a [detailed description of the algorithms at this post](http://blank.com).

We want to know which items exceed a certain frequency and identify events and patterns. Answers to such questions in real-time over a continuous data stream is not an easy task when serving millions of hits due to the following challenges:

* Single Pass
* Limited memory
* Volume of data in real-time

The above impose a smart counting algorithm. Data stream mining to identify events & patterns can be performed by applying the following algorithms: Lossy Counting and Sticky Sampling.

## How to run
Using sbt to build and run:

Lossy Counting:  
`sbt "run-main frequencycount.lossycounting.LossyCountingModel"`

Sticky Sampling:  
`sbt "run-main frequencycount.stickysampling.StickySamplingModel"`

Run the tests using
`sbt test`

## Contributing
Have you found any issues?
Can you implement this in a distributed fashion?

Please contact me at michael@micvog.com or create a new Issue. Pull requests are always welcome.
