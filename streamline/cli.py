import argparse
import asyncio
import logging
import sys
import os
import re

from . import utils
from . import executors
from . import generators
from . import consumers
from . import streamers
from .core import pipe

logger = logging.getLogger(__file__)

SHORTHAND_PATTERN = r'^(?P<target_attr>[-\[\]_\w]+=)?(?P<streamer>[-\[\]_.\w]+)\((?P<input_extract>[-\[\]._\w]+), ?(?P<output_extract>[-._\w\[\]]+)\)$'

def streamline_command(args):
    cmd_parser = argparse.ArgumentParser(prog='streamline', add_help=False)
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
        '-s', '--streamers',
        help='Additional streamers to apply (-s is optional)',
        nargs='*',
    )
    cmd_parser.add_argument(
        '-h', '--help',
        action='store_true',
        default=False,
        help='Print help',
    )
    cmd_parser.add_argument(
        '-p', '--progress',
        default=None,
        choices=['buffer', 'stream-output', 'streaming'],
        help='Print progress to stdout. ("buffer": buffers input and output, "stream-output" buffers only input, "stream" for no buffering at all)',
    )
    cmd_parser.add_argument(
        '-w', '--workers',
        type=int,
        default=streamers.AsyncExecutor.DEFAULT_WORKERS,
        help='Number of concurrent workers for any one async execution module to have',
    )
    if args and '-s' not in args:
        args.insert(0, '-s'),
    args, extra_args = cmd_parser.parse_known_args(args)

    # Load Generator
    Generator = generators.load_generator(args.generator)
    generator_options = {}
    if hasattr(Generator, 'args'):
        generator_parser = argparse.ArgumentParser(
            prog=args.generator,
            add_help=False,
            usage='streamline [streamers...] --generator %(prog)s [generator options]',
        )
        Generator.args(generator_parser)
        generator_args, extra_args = generator_parser.parse_known_args(extra_args)
        generator_options.update(generator_args.__dict__)
    generator = Generator(**generator_options)

    Consumer = consumers.load_consumer(args.consumer)
    consumer_options = {}
    if hasattr(Consumer, 'args'):
        consumer_parser = argparse.ArgumentParser(
            prog=args.consumer,
            add_help=False,
            usage='streamline [streamers...] --consumer %(prog)s [generator options]',
        )
        Consumer.args(consumer_parser)
        consumer_args, extra_args = consumer_parser.parse_known_args(extra_args)
        consumer_options.update(consumer_args.__dict__)
    consumer = Consumer(**consumer_options)

    command_streamers = []

    if args.help:
        print('=' * 15 + ' Streamline ' + '=' * 15)
        print('\n')
        cmd_parser.print_help()
        if args.streamers:
            for streamer_name in args.streamers:
                print('\n\n')
                print('=' * 15 + ' Streamer::{} '.format(streamer_name) + '=' * 15)
                print('\n')
                load_streamer(streamer_name, extra_args, print_help=True, ae_args={'workers': args.workers})
        else:
            print('\n')
            print('=' * 15 + ' Streamers ' + '=' * 15)
            for streamer_name, streamer in streamers.STREAMERS.items():
                description = getattr(streamer, '_arg_description', 'An undocumented module')
                example = getattr(streamer, '_arg_example', '')
                invocation = 'streamline -s {} --'.format(streamer_name)
                print('\n::{}::\n\tDescription: {}\n\tExample: {} {}'.format(
                    streamer_name,
                    description,
                    invocation,
                    example,
                ))
                
        return

    # Streamers
    if args.streamers:
        for streamer_name in args.streamers:
            # Support shorthand syntax "attr=streamer(input.path, output.path)"
            shorthand_match = re.match(SHORTHAND_PATTERN, streamer_name)
            if shorthand_match:
                shorthand_options = shorthand_match.groupdict()
                # Load the main streamer
                main_streamer, extra_args = load_streamer(shorthand_options['streamer'], extra_args, ae_args={'workers': args.workers})

                # Compose the sub-command
                command_streamers.append(streamers.history_push)
                if shorthand_options['input_extract'] != 'value':
                    input_extract = streamers.ExtractionStreamer(
                        selector=shorthand_options['input_extract']
                    )
                    command_streamers.append(input_extract.stream)
                command_streamers.append(main_streamer)
                if shorthand_options['output_extract'] != 'value':
                    output_extract = streamers.ExtractionStreamer(
                        selector=shorthand_options['output_extract']
                    )
                    command_streamers.append(output_extract.stream)
                command_streamers.append(streamers.history_pop)
                if shorthand_options['target_attr']:
                    attr = shorthand_options['target_attr'][:-1]
                    combiner = streamers.Combiner(path=attr, target=-2, source=-1)
                    command_streamers.append(combiner.stream)

            else:
                streamer, extra_args = load_streamer(streamer_name, extra_args, ae_args={'workers': args.workers})
                command_streamers.append(streamer)

    # Ensure we don't have any extra arguments
    if extra_args:
        sys.stderr.write('Extra arguments found: {}\n'.format(' '.join(extra_args)))
        sys.exit(2)

    # Override streamers
    if args.progress:
        buffer_start = args.progress in ('buffer', 'stream-output')
        buffer_end = args.progress in ('buffer')
        progress = streamers.ProgressStreamer(buffer_start=buffer_start, buffer_end=buffer_end)
        command_streamers = [progress.streamer_start, *command_streamers, progress.streamer_end]

    future = pipe(generator.stream(), command_streamers, consumer=consumer.stream)

    # Loop until complete
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(future, loop=loop)
    loop.run_until_complete(task)

def load_streamer(path, arg_list, options=None, print_help=False, ae_args=None):
    if arg_list and arg_list[0] == '--':
        arg_list = arg_list[1:]
    kwargs = {}
    if options:
        kwargs.update(options)
    if path is None:
        return None
    elif '.' in path:
        Streamer = utils.import_obj(path)
    else:
        Streamer = streamers.STREAMERS.get(path)
    if Streamer is None:
        raise ValueError('Invalid streamer: {}'.format(path))

    if print_help:
        streamer_parser = argparse.ArgumentParser(
            prog=path,
            add_help=False,
            usage='streamline -s %(prog)s -- [options]',
        )
        if hasattr(Streamer, 'args'):
            Streamer.args(streamer_parser)
        streamer_parser.print_help()
        return None

    if hasattr(Streamer, 'async_handler'):
        # This is really a handler that needs wrapped with AsyncExecutor
        Executor = Streamer
        if Executor and hasattr(Executor, 'handle'):
            if hasattr(Executor, 'args'):
                streamer_parser = argparse.ArgumentParser(add_help=False)
                Executor.args(streamer_parser)
                executor_args, arg_list = streamer_parser.parse_known_args(arg_list)
                kwargs.update(executor_args.__dict__)
            executor = Executor(**kwargs).handle
        else:
            executor = Executor

        # Now build the wrapper
        ae = streamers.AsyncExecutor(executor, **ae_args)
        return ae.stream, arg_list
    else:
        if type(Streamer) == type:
            if hasattr(Streamer, 'args'):
                streamer_parser = argparse.ArgumentParser(add_help=False)
                Streamer.args(streamer_parser)
                streamer_args, arg_list = streamer_parser.parse_known_args(arg_list)
                kwargs.update(streamer_args.__dict__)
            return Streamer(**kwargs).stream, arg_list
        return Streamer, arg_list
