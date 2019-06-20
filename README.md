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

Streamline modules aim to provide all the useful information in object form as output can then be customized with other streaming modules. For exampe, to take the above output and get just the exit code that tells us whether the port is open we can just add the `headers` streamer to prefix each output with the original input and the `extract` streamer (with its `--selector` option) option to set the value to the `exit_code` property of each result:

```bash
  $ printf "www.google.com\nslashdot.org" | streamline -s shell extract headers -- "nc -zv {value} 443" --selector exit_code
  www.google.com: 0
  slashdot.org: 0
```

## Built-in Modules

There are many modules available that do asynchronous jobs and transformations to input.  To see all available modules use the main help option to list them with examples:

```bash
$ streamline --help

=============== Streamline ===============


usage: streamline [--generator GENERATOR] [--consumer CONSUMER]
                  [-s [STREAMERS [STREAMERS ...]]] [-h]

optional arguments:
  --generator GENERATOR
                        Entry Generator Module
  --consumer CONSUMER   Entry Consumer/Writer Module
  -s [STREAMERS [STREAMERS ...]], --streamers [STREAMERS [STREAMERS ...]]
                        Additional streamers to apply (-s is optional)
  -h, --help            Print help
  -p {buffer,stream-output,streaming}, --progress {buffer,stream-output,streaming}
                        Print progress to stdout. ("buffer": buffers input and
                        output, "stream-output" buffers only input, "stream"
                        for no buffering at all)
  -w WORKERS, --workers WORKERS
                        Number of concurrent workers for any one async
                        execution module to have



=============== Streamers ===============

::extract::
	Description: Change the value to an attribute of the current value
	Example: streamline -s extract -- --selector exit_code

::py::
	Description: Translate each value by assigning it to the result of a python expression
	Example: streamline -s py -- "value.upper()"

::pyfilter::
	Description: Filter out values that dont have a truthy result to a particular python expression
	Example: streamline -s pyfilter -- "'foobar' in value"

::truthy::
	Description: Filter out values that are not truthy
	Example: streamline -s truthy -- 

::noop::
	Description: No operation. Just for testing.
	Example: streamline -s noop -- 

::split_list::
	Description: Take any values that are an array and treat each value of an array as a separate input 
	Example: streamline -s split_list -- 

::split::
	Description: Take any values that are an array and treat each value of an array as a separate input 
	Example: streamline -s split -- 

::breakdown::
	Description: Show a report of how many input values ended up with a particular result value
	Example: streamline -s breakdown -- 

::headers::
	Description: Force each value to a string and prefix each with the original input value
	Example: streamline -s headers -- 

::filter_out_errors::
	Description: Filter out any entries that have produced an error
	Example: streamline -s filter_out_errors -- 

::errors::
	Description: Use the latest error on the entry as the value
	Example: streamline -s errors -- 

::buffer::
	Description: Hold entries in memory until a certain number is reached (give no args to buffer all)
	Example: streamline -s buffer -- --buffer 20

::json::
	Description: Take json strings and parse them into objects so other streamers can inspect attributes
	Example: streamline -s json -- 

::strip::
	Description: Strip surrounding whitespace from each string entry, removing entries that are only whitespace
	Example: streamline -s strip -- --buffer 20

::head::
	Description: Only take the first X entries (Default 1)
	Example: streamline -s head -- --count 20

::readfile::
	Description: Read the file indicated by the file
	Example: streamline -s readfile -- --path ~/dir/{value}.json

::combine::
	Description: Combine two previous historical values by setting an attribute
	Example: streamline -s combine -- --source "-1" --target "-2"

::http::
	Description: Use a template to execute an HTTP request for each value
	Example: streamline -s http -- "https://{value}/"

::ssh::
	Description: Treat each value as a host to connect to. SSH in and run a command returning the output
	Example: streamline -s ssh -- "uptime"

::ssh_exec::
	Description: Copy a script to target machine and execute
	Example: streamline -s ssh_exec -- ~/dostuff.sh

::shell::
	Description: Run a shell command for each value
	Example: streamline -s shell -- "nc -zv {value} 22"

::scp::
	Description: Treat each value as a host to connect to. Copy a file to or from this host
	Example: streamline -s scp -- "/tmp/file.txt" "{value}:/tmp/file.txt"

::sleep::
	Description: Sleep for a second (or for {value} seconds) for each entry making no change to its value
	Example: streamline -s sleep -- 

::history:push::
	Description: Start a new history tree
	Example: streamline -s history:push -- 

::history:pop::
	Description: Walk back up one level in the history tree
	Example: streamline -s history:pop -- 

::history:collapse::
	Description: Treat the latest value as the original
	Example: streamline -s history:collapse -- 

::history:reset::
	Description: Clear all levels of history
	Example: streamline -s history:reset -- 

::history:values::
	Description: Set the current value to a list of all previous values
	Example: streamline -s history:values -- 

```

To get available options for a particular module run (substituting "http" for the module you're interested in):

```bash
streamline -s http --help
```


## YAML support

YAML files defining the streamers and their options is supported. 

Given the following yaml file:

```yaml
streamers:
 -
    name: split
 -
    name: http
    output: code
    input: value
    target: status_code
    options:
      url: 'https://{value}'`
```

It could be used to split a string of domains and get the http code for each one:

```bash
$ echo "www.google.com www.slashdot.org" | streamline -y http_codes.yaml
{"base": "www.google.com", "status_code": 200}
{"base": "www.slashdot.org", "status_code": 200}
```



You can also specify the generator and consumer in the same way. Given the below yaml and csv file:
```yaml
generator:
  name: csv
  options:
    input: domains.csv
consumer:
  name: csv
streamers:
  -
     name: http
     output: code
     input: domain
     target: status_code
     options:
       url: 'https://{value}'
```

```
domain,label
www.google.com,The famous big B
www.slashdot.org,Mosh pit of opinions
```

You can use it the following way:
```bash
$ streamline -y http_codes.yaml 
domain,label,status_code
www.google.com,The famous big B,200
www.slashdot.org,Mosh pit of opinions,200
```



## Technical Vocabulary

* Entry: A small wrapper around a value being passed along through the stream. Commonly a single line of input.
* Generator: An asynchonous generator function that takes no input and yields Entry objects.
* Executor:  An asyncronous function that takes a single value and returns a new value. Usually some unit of work is done and the result of that work is returned as the new value.
* Streamer: An asynchronous generator function that takes as an argument an asynchronous source iterable that yields Entry objects. Commonly streamers do some manipulation of each Entry it gets from the source iterable and then sets a new value on the entry.
* Consumer: An asynchronous function that reads all entries of an asynchronous source iterable. Usually this function writes to some output (such as stdout).

