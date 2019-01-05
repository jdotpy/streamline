import argparse
import asyncio
import logging
import sys
import os

from . import utils
from . import executors
from . import generators
from . import consumers
from . import streamers
from .core import pipe

logger = logging.getLogger(__file__)

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
                load_streamer(streamer_name, extra_args, print_help=True)
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
            streamer, extra_args = load_streamer(streamer_name, extra_args)
            command_streamers.append(streamer)

    # Ensure we don't have any extra arguments
    if extra_args:
        sys.stderr.write('Extra arguments found: {}\n'.format(' '.join(extra_args)))
        sys.exit(2)

    future = pipe(generator.stream(), command_streamers, consumer=consumer.stream)

    # Loop until complete
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(future, loop=loop)
    loop.run_until_complete(task)

def load_streamer(path, arg_list, options=None, print_help=False):
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
        if hasattr(Streamer, 'async_handler'):
            streamers.AsyncExecutor.args(streamer_parser)
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
        ae_parser = argparse.ArgumentParser(add_help=False)
        streamers.AsyncExecutor.args(ae_parser)
        ae_args, arg_list = ae_parser.parse_known_args(arg_list)
        ae = streamers.AsyncExecutor(executor, **ae_args.__dict__)
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
