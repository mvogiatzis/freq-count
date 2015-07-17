# Frequency Count Algorithms for Data Streams
The project provides a Scala implementation of the Lossy Counting and Sticky Sampling algorithms for counting 
on data streams. You can find a detailed description of the algorithms at [this post](http://blank.com).

## How to run
Using sbt to build and run:

Lossy Counting:  
`sbt "run-main frequencycount.lossycounting.LossyCountingModel"`

Sticky Sampling:  
`sbt "run-main frequencycount.stickysampling.StickySamplingModel"`

## Contributing
Have you found any issues?
Can you implement this in a distributed fashion?

Please contact me at michael@micvog.com or create a new Issue. Pull requests are always welcome.
