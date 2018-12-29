import argparse
import asyncio
import logging
import sys
import os

from . import executors
from . import generators
from . import consumers
from . import streamers
from .core import pipe

logger = logging.getLogger(__file__)

def streamline_command():
    cmd_parser = argparse.ArgumentParser(prog='streamline')
    cmd_parser.add_argument('--input', help='Set source (Default stdin)', default='-')
    cmd_parser.add_argument('--output', help='Set target of output (Default stdout)', default='-')
    cmd_parser.add_argument(
        '--show-input',
        help='Only show input values with truthy non-exception results (useful with --filter)',
        action='store_true',
        default=False,
    )
    cmd_parser.add_argument(
        '-m', '--executor',
        help='Execution Module',
        default=None,
    )
    cmd_parser.add_argument(
        '--generator',
        help='Entry Generator Module',
        default='file',
    )
    cmd_parser.add_argument(
        '--consumer',
        help='Entry Consumer/Writer Module',
        default='file',
    )
    cmd_parser.add_argument(
        '-f', '--filter',
        help='Python filter expression (e.g. "\'foo\' in value")',
    )
    cmd_parser.add_argument(
        '-x', '--python',
        help='Python transformation expression (e.g. "value = value.replace")',
    )
    cmd_parser.add_argument(
        '-w', '--workers',
        type=int,
        help='Number of concurrent workers for execution modules',
        default=10,
    )
    cmd_parser.add_argument(
        '-d', '--headers',
        action='store_true',
        help='Prefix output with entry name',
        default=False,
    )
    cmd_parser.add_argument(
        '-p', '--progress',
        action='store_true',
        help='Output progress bar',
        default=False,
    )
    cmd_parser.add_argument(
        '-e', '--extract',
        help='Extract a part of the result by dot path (e.g. result.foobar)',
        default=None,
    )
    cmd_parser.add_argument(
        '--stacktraces',
        action='store_true',
        default=False,
        help='Extract a part of the result by dot path (e.g. result.foobar)',
    )
    args, extra_args = cmd_parser.parse_known_args()

    # Setup input/output modules
    Generator = generators.load_generator(args.generator)
    generator = Generator(args.input)
    Consumer = consumers.load_consumer(args.consumer)
    consumer = Consumer(args.output, headers=args.headers)

    command_streamers = []

    # Configure the any async executor
    loop = asyncio.get_event_loop()
    if args.executor:
        executor = executors.load_executor(args.executor, extra_args)
        if executor is None:
            print('Invalid executor specified:', args.executor)
            sys.exit(1)
        ae = streamers.AsyncExecutor(
            executor,
            show_progress=args.progress,
            stacktraces=args.stacktraces,
            workers=args.workers,
            loop=loop,
        )
        command_streamers.append(ae.stream)

    # Python Exec
    if args.python:
        command_streamers.append(streamers.PyExecTransform(code=args.python).stream)

    # Pyton Filters
    if args.filter:
        command_streamers.append(streamers.PyExecFilter(code=args.filter).stream)

    # Extractor
    if args.extract:
        command_streamers.append(streamers.ExtractionStreamer(selector=args.extract).stream)

    future = pipe(generator.stream(), command_streamers, consumer=consumer.stream)

    # Loop until complete
    task = asyncio.ensure_future(future, loop=loop)
    loop.run_until_complete(task)
