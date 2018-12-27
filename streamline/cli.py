import argparse
import asyncio
import logging
import os

from .executors import EXECUTORS, load_executor
from .streamers import STREAMERS, load_streamer
from .core import streamline

logger = logging.getLogger(__file__)

def streamline_command():
    cmd_parser = argparse.ArgumentParser(prog='streamline')
    cmd_parser.add_argument('--input', help='Set source (Default stdin)', default='-')
    cmd_parser.add_argument('--output', help='Set target of output (Default stdout)', default='-')
    cmd_parser.add_argument(
        '-n', '--headers-only',
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
        '-s', '--streamer',
        help='Execution Module',
        default='line',
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
        help='Output progress bar and dont display results until the end',
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

    Streamer = load_streamer(args.streamer)
    executor = load_executor(args.executor, extra_args)
    streamer = Streamer(args.input, args.output)
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(streamline(
        streamer,
        executor=executor,
        headers=args.headers,
        progress=args.progress,
        extract=args.extract,
        stacktraces=args.stacktraces,
        worker_count=args.workers,
    ))
    loop.run_until_complete(task)
