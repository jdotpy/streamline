import argparse
import asyncio
import logging
import yaml
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

SHORTHAND_PATTERN = r'^(?P<target_attr>[-\[\]_\w]+=)?(?P<streamer>[-\[\]_.\w]+)\((?P<input_extract>[-\[\]._*\w]+), ?(?P<output_extract>[-._*\w\[\]]+)\)$'


class OptionsProcessor():
    def __init__(self, args):
        self.args = args

    def parse(self, parser):
        # Support '--' separator between commands
        if self.args and self.args[0] == '--':
            self.args = self.args[1:]

        parsed_args, remaining = parser.parse_known_args(self.args)
        self.args = remaining
        return parsed_args

    def has_remaining_args(self):
        return len(self.args) > 0

    def remaining_args(self):
        return self.args.copy()


def _parser_main():
    cmd_parser = argparse.ArgumentParser(prog='streamline', add_help=False)
    cmd_parser.add_argument(
        '--generator',
        help='Entry Generator Module',
    )
    cmd_parser.add_argument(
        '--consumer',
        help='Entry Consumer/Writer Module',
    )
    cmd_parser.add_argument(
        '-s', '--streamers',
        help='Additional streamers to apply (-s is optional)',
        nargs='*',
    )
    cmd_parser.add_argument(
        '-h', '--help',
        action='store_true',
        help='Print help',
    )
    cmd_parser.add_argument(
        '-p', '--progress',
        choices=['buffer', 'stream-output', 'streaming'],
        help='Print progress to stdout. ("buffer": buffers input and output, "stream-output" buffers only input, "stream" for no buffering at all)',
    )
    cmd_parser.add_argument(
        '-w', '--workers',
        type=int,
        help='Number of concurrent workers for any one async execution module to have',
    )
    cmd_parser.add_argument(
        '-y', '--yaml',
        help='Take options from a yaml or json config file',
    )
    return cmd_parser

def _generator_parser(generator):
    if not hasattr(generator, 'args'):
        return None
    
    generator_parser = argparse.ArgumentParser(
        prog=generator.__name__,
        add_help=False,
        usage='streamline [streamers...] --generator %(prog)s [generator options]',
    )
    generator.args(generator_parser)
    return generator_parser

def _consumer_parser(consumer):
    if not hasattr(consumer, 'args'):
        return None
    
    consumer_parser = argparse.ArgumentParser(
        prog=consumer.__name__,
        add_help=False,
        usage='streamline [streamers...] --consumer %(prog)s [consumer options]',
    )
    consumer.args(consumer_parser)
    return consumer_parser

def _streamer_parser(Streamer):
    streamer_parser = argparse.ArgumentParser(
        prog=Streamer.__name__,
        add_help=False,
        usage='streamline -s %(prog)s -- [options]',
    )
    if hasattr(Streamer, 'args'):
        Streamer.args(streamer_parser)
    return streamer_parser


def load_config(*sources, ignore_nulls=True):
    config = {}
    for source in sources:
        if source is None:
            continue
        if not isinstance(source, dict):
            source = source.__dict__
        if ignore_nulls:
            source = utils.strip_nulls(source)
        config.update(source)
    return config

def wrap_streamer(streamer, input_path=None, output_path=None, target=None):
    if not (input_path or output_path or target):
        return [streamer]

    if target and not (input_path or output_path):
        combiner = streamers.Combiner(path=target, target=-2, source=-1)
        return [streamer, combiner]

    subpipe = []
    # Compose the sub-command
    subpipe.append(streamers.history_push)
    if input_path != 'value':
        input_extract = streamers.ExtractionStreamer(
            selector=input_path,
        )
        subpipe.append(input_extract.stream)
    subpipe.append(streamer)
    if output_path != 'value':
        output_extract = streamers.ExtractionStreamer(
            selector=output_path
        )
        subpipe.append(output_extract.stream)
    subpipe.append(streamers.history_pop)
    if target:
        combiner = streamers.Combiner(path=target, target=-2, source=-1)
        subpipe.append(combiner.stream)
    return subpipe

def streamline_command(args):
    if args and '-s' not in args:
        args.insert(0, '-s'),
    options_processor = OptionsProcessor(args)
    main_parser = _parser_main()
    main_args = options_processor.parse(main_parser)

    yaml_config = {}
    use_yaml = main_args.yaml is not None
    if main_args.yaml:
        with open(main_args.yaml, 'r') as f:
            yaml_config = yaml.safe_load(f.read())

    yaml_generator_config = yaml_config.get('generator', {})
    yaml_consumer_config = yaml_config.get('consumer', {})
    yaml_streamers = yaml_config.get('streamers', [])
    command_config = load_config(
        {
            'generator': 'file',
            'consumer': 'file',
            'workers': streamers.AsyncExecutor.DEFAULT_WORKERS,
            'streamers': [],
        },
        {
            'generator': yaml_generator_config.get('name', None),
            'consumer': yaml_consumer_config.get('name', None),
        },
        main_args,
        ignore_nulls=True,
    )
    ae_args = {'workers': command_config.get('workers')}
        
    # Load Generator & Consumer
    Generator = generators.load_generator(command_config['generator'])
    if 'options' in yaml_generator_config:
        generator_options = yaml_generator_config.get('options')
    else:
        generator_options = options_processor.parse(_generator_parser(Generator)).__dict__
    generator = Generator(**generator_options)

    Consumer = consumers.load_consumer(command_config['consumer'])
    if 'options' in yaml_consumer_config:
        consumer_options = yaml_consumer_config.get('options')
    else:
        consumer_options = options_processor.parse(_consumer_parser(Consumer)).__dict__
    consumer = Consumer(**consumer_options)

    if main_args.help:
        print('=' * 15 + ' Streamline ' + '=' * 15)
        print('\n')
        main_parser.print_help()
        streamer_list = command_config.get('streamers', [])
        if streamer_list:
            for streamer_name in streamer_list:
                print('\n\n')
                print('=' * 15 + ' Streamer::{} '.format(streamer_name) + '=' * 15)
                print('\n')
                load_streamer(streamer_name, print_help=True, ae_args=ae_args)
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

    # Parse streamers
    command_streamers = []
    if use_yaml:
        for streamer_conf in yaml_streamers:
            streamer = load_streamer(
                streamer_conf['name'],
                options=streamer_conf.get('options', {}),
                ae_args=ae_args,
            )
            sub_pipeline = wrap_streamer(
                streamer,
                input_path=streamer_conf.get('input'),
                output_path=streamer_conf.get('output'),
                target=streamer_conf.get('target'),
            )
            command_streamers.extend(sub_pipeline)
    else:
        for streamer_name in command_config.get('streamers',[]):
            # Support shorthand syntax "attr=streamer(input.path, output.path)"
            shorthand_match = re.match(SHORTHAND_PATTERN, streamer_name)
            if shorthand_match:
                shorthand_options = shorthand_match.groupdict()

                # Load the main streamer
                streamer_name = shorthand_options.get('streamer')
                main_streamer = load_streamer(shorthand_options['streamer'], options_processor, ae_args=ae_args)

                target_attr = shorthand_options.get('target_attr', '')
                if target_attr:
                    target_attr = target_attr[:-1]
                    
                command_streamers.extend(wrap_streamer(
                    main_streamer,
                    input_path=shorthand_options.get('input_extract', None),
                    output_path=shorthand_options.get('output_extract', None),
                    target=target_attr,
                ))
            else:
                streamer = load_streamer(streamer_name, options_processor, ae_args=ae_args)
                command_streamers.append(streamer)

    # Ensure we don't have any extra arguments
    if options_processor.has_remaining_args():
        sys.stderr.write('Extra arguments found: {}\n'.format(' '.join(options_processor.remaining_args())))
        sys.exit(2)

    # Override streamers
    progress_option = command_config.get('progress', None)
    if command_config.get('progress', None):
        buffer_start = progress_option in ('buffer', 'stream-output')
        buffer_end = progress_option in ('buffer')
        progress = streamers.ProgressStreamer(buffer_start=buffer_start, buffer_end=buffer_end)
        command_streamers = [progress.streamer_start, *command_streamers, progress.streamer_end]

    future = pipe(generator.stream(), command_streamers, consumer=consumer.stream)

    # Loop until complete
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(future, loop=loop)
    loop.run_until_complete(task)

def load_streamer(path, options_processor=None, options=None, print_help=False, ae_args=None):
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
        _streamer_parser(Streamer).print_help()
        return None

    if options_processor:
        streamer_parser = _streamer_parser(Streamer)
        streamer_args = options_processor.parse(streamer_parser)
        kwargs.update(load_config(streamer_args.__dict__))


    if hasattr(Streamer, 'async_handler'):
        # This is really a handler that needs wrapped with AsyncExecutor
        Executor = Streamer
        if Executor and hasattr(Executor, 'handle'):
            executor = Executor(**kwargs).handle
        else:
            executor = Executor

        # Now build the wrapper
        ae = streamers.AsyncExecutor(executor, **ae_args)
        return ae.stream
    else:
        if type(Streamer) == type:
            return Streamer(**kwargs).stream
        return Streamer
