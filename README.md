streamline
============

The goal of this project is to make data acessible and actionable on the CLI. Streamline does this by implementing the utility functions necessary for taking an input stream and parallelizing work done on it.

First, some *Vocabulary*:

* Entry: A small wrapper around a value being passed along through the stream. Commonly a single line of input.
* Generator: An asynchonous generator function that takes no input and yields Entry objects.
* Executor:  An asyncronous function that takes a single value and returns a new value. Usually some unit of work is done and the result of that work is returned as the new value.
* Streamer: An asynchronous generator function that takes as an argument an asynchronous source iterable that yields Entry objects. Commonly streamers do some manipulation of each Entry it gets from the source iterable and then sets a new value on the entry.
* Consumer: An asynchronous function that reads all entries of an asynchronous source iterable. Usually this function writes to some output (such as stdout).
