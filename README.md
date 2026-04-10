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
For the sake of simplicity, currently `aftermap` uses a simple "fair-queue" approach, which  will prove to be sufficient if the expensive pure function does a bounded amount of work.
Perhaps, in the future the other variants will be supported..

ZeroMQ is used as a high-performance asynchronous message queue for database write requests (since sqlite does not allow multiple writers)
Multiprocessing is used over multi-threading to sidestep python GIL shenanigans.

## Usage

Let's say your python code looks something like this:
```python
class Value(BaseModel):
    x: int

class Result(BaseModel):
    value: int

def transform(data: Value) -> Result:
    """
    The really really expensive function which takes a lot of time and resources. 
    """
    return Result(value=data.x * 2)

# plain-old map - no persistence of results; can't resume in case of a sudden crash, can't retry failed function evaluations, etc.
results = list(map(transform, [Value(x=i) for i in range(1000)]))
```

Delegating a bunch of tasks to some workers and having them save their work periodically is trivial. Or so you would desire.
But I've since learnt that everything is trivial _in principle_, yet often a bit of thought and effort is needed to see it through.
Notwithstanding, `aftermap` provides a simple fair-queue-mpsc system which can replace `map`.

```python
class Value(BaseModel):
    x: int

class Result(BaseModel):
    value: int

def transform(data: Value) -> Result:
    return Result(value=data.x * 2)

mapper = AfterMap.builder().build().maplike(Value, Result, "results.db") # one line
results = list(mapper(transform, [Value(x=i) for i in range(1000)]))
```
