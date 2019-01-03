streamline
============

The goal of this project is to make data acessible and actionable on the CLI. Streamline does this by implementing the utility functions necessary for taking an input stream and parallelizing work done on it.

The sequence of operations in an invocation:

1. The "Generator" object loads entries from a source data stream (usually stdin).
2. The "Streamers" selected are sequentially given each input value and are exepcted to filter and make modifications to those values and yield them for the next streamer.
3. The "Consumer" object takes the entries yielded from the last streamer and usually outputs them for storage or viewing (usually stdout).

## Installation

* Requires Python 3.6+ and pip
* `pip install streamline`

## Guide

The simplest call specifies no streaming operations it just reads from stdin and writes to stdout exactly what was written:

```bash
  $ printf "foo\nbar" | streamline
  foo
  bar
```

By default streamline takes input from stdin and writes to stdout. This is very flexible as it makes the tool composible with other CLI tools. However you can also use the `--input` and `--output` flags to control output. Lets assume that you have a file with the same data you just sent in with printf:

```bash
  $ streamline --input my_source_file.txt --output my_target_file.txt
  $ cat my_target_file.txt
  foo
  bar
```

Now lets do something a little less useless. Lets use the "shell" streamer to execute a shell command for each entry and check if they're listening for https traffic:

```bash
  $ printf "www.google.com\nslashdot.org" | streamline -s shell -- "nc -zv {value} 443"
  {"stdout": "", "stderr": "Connection to www.google.com 443 port [tcp/https] succeeded!\n", "exit_code": 0}
  {"stdout": "", "stderr": "Connection to slashdot.org 443 port [tcp/https] succeeded!\n", "exit_code": 0}
```

Streamline modules aim to provide all the useful information in object form as output can then be customized with other streaming modules. For exampe, to take the above output and get just the exit code that tells us whether the port is open we can just add the `--headers` option to prefix each output with the original input and the `--extract exit_code` option to set the value to the `exit_code` property of each result:

```bash
  $ printf "www.google.com\nslashdot.org" | streamline --extract exit_code --headers -s shell  -- "nc -zv {value} 443"
  www.google.com: 0
  slashdot.org: 0
```

Everything before the `--` is an option for the streamline command and options after that are for the particular modules we're using (`shell` in this case).


## Built-in Modules

There are many modules available that do asynchronous jobs and transformations to input.  To see all available modules use the main help option to list them with examples:

```bash
$ streamline --help
```

To get available options for a particular module run (substituting "http" for the module you're interested in):

```bash
streamline -s http --help
```


## Technical Vocabulary

* Entry: A small wrapper around a value being passed along through the stream. Commonly a single line of input.
* Generator: An asynchonous generator function that takes no input and yields Entry objects.
* Executor:  An asyncronous function that takes a single value and returns a new value. Usually some unit of work is done and the result of that work is returned as the new value.
* Streamer: An asynchronous generator function that takes as an argument an asynchronous source iterable that yields Entry objects. Commonly streamers do some manipulation of each Entry it gets from the source iterable and then sets a new value on the entry.
* Consumer: An asynchronous function that reads all entries of an asynchronous source iterable. Usually this function writes to some output (such as stdout).

