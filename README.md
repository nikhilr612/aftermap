# aftermap
It's not everyday that one needs to apply an _expensive_ pure function on an immutable collection and persist the results.
But it happens often enough, that one finds writing code for this rather *choresome* to say the least.
This is particularly so, when you're running a bunch of experiments on a fragile local environment.
Sometimes, you want logs for them: track what happened and when.
`aftermap` aims to make incorporating this feature an after thought when you've implemented the underlying pure function. 

The design is simple: consider an n-partition of the collection, and send each partition to a worker process.
The worker process applies the pure function to each of element in its parition serially. At each point, logs and results are persisted.
Having completed all this, the parent process after joining these workers kills them and exits.
This is the approach to go for if lock contention is the main problem.
Often, this is not the case.

Some other approaches are to use an LRU scheduler or Work-Stealing to handle maximize utilization when job are uneven.
For the sake of simplicity, currently `aftermap` uses a simple `round-robin` approach, which  will prove to be sufficient if the expensive pure function does a bounded amount of work.
Perhaps, in the future the other variants will be supported..

ZeroMQ is used as a high-performance asynchronous message queue for database write requests (since sqlite does not allow multiple writers)
Multiprocessing is used over multi-threading to sidestep python GIL shenanigans.
