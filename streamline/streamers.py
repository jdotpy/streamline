from collections import OrderedDict
import traceback
import argparse
import asyncio
import json
import re
import sys

from .entries import entry_wrap, Entry
from .extractor import Extractor
from . import executors
from . import utils

arg_help = utils.arg_help

class BaseStreamer():
    def __init__(self, **options):
        self.options = options
        self.initialize()

    def __aiter__(self):
        return self.stream()

    async def handle(self, value):
        raise NotImplemented('Implement either the stream or handle method of a BaseStreamer subclass')

    async def stream(self, source):
        async for entry in source:
            result = await self.handle(entry.value)
            entry.value = result
            yield entry

    def initialize(self):
        pass

@arg_help('Translate each value by assigning it to the result of a python expression', example='"value.upper()"')
class PyExecTransform(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            'code',
            nargs='?',
            help='Python code to evaluate',
        )
        parser.add_argument(
            '--statement',
            action='store_true',
            help='Indicates that the python code is not an expression but a statement',
            default=False,
        )

    def __init__(self, code=None, statement=False, show_exceptions=False):
        self.show_exceptions = show_exceptions
        self.expression = not statement
        self.runner = exec if statement else eval
        try:
            self.code = compile(code, '<string::transform>', self.runner.__name__)
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    async def stream(self, source):
        async for entry in source:
            scope = {'value': entry.value, 'input': entry.original_value, 'i': entry.index, 'index': entry.index}
            try:
                if self.expression:
                    entry.value = self.runner(self.code, globals(), scope)
                else:
                    self.runner(self.code, globals(), scope)
                    if 'result' in scope:
                        entry.value = scope.get('result', None) 
                    else:
                        entry.value = scope.get('result')
            except Exception as e:
                print(e)
                if self.show_exceptions:
                    entry.exception(e)
                    traceback.print_exc()
                else:
                    continue
            yield entry

@arg_help('Filter out values that dont have a truthy result to a particular python expression', example='"\'foobar\' in value"')
class PyExecFilter(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            'code',
            nargs='?',
            help='Python code to evaluate',
        )

    def __init__(self, code=None, show_exceptions=False):
        self.show_exceptions = show_exceptions
        try:
            self.code = compile(code, '<string::filter>', 'eval')
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    async def stream(self, source):
        async for entry in source:
            scope = {'value': entry.value, 'input': entry.original_value, 'i': entry.index, 'index': entry.index}
            try:
                keep = eval(self.code, globals(), scope)
            except Exception as e:
                keep = False
                if self.show_exceptions:
                    traceback.print_exc()
            if keep:
                yield entry

@arg_help('Filter out values that dont have a truthy result to a particular python expression', example='--selector exit_code')
class ExtractionStreamer(BaseStreamer):
    def initialize(self):
        self.extractor = Extractor(self.options['selector'])

    async def handle(self, value):
        return self.extractor.extract(value)

class AsyncExecutor():
    """
        :: Worker-oriented event loop processor

        Workers are no longer a necessary concept in an asynchronous world. However, the concept can still be very
        helpful for controlling resource usage on the host machine or remote systems used by the job. For this reason
        I'm re-implementing a worker-style executor pool which could be used with jobs that are threaded or async.
    """
    DEFAULT_WORKERS = 5

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '-w', '--workers',
            type=int,
            help='Number of concurrent workers for execution modules',
            default=10,
        )
        parser.add_argument(
            '-p', '--show-progress',
            action='store_true',
            help='Output progress bar',
            default=False,
        )

    def __init__(self, executor=None, show_progress=False, stacktraces=False, workers=DEFAULT_WORKERS, loop=None):
        self.executor = executor
        self.input_queue = asyncio.Queue()
        self.output_queue = asyncio.Queue()
        self.show_progress = show_progress
        self.worker_count = workers or self.DEFAULT_WORKERS
        self.stacktraces = stacktraces

        # State data
        self.entry_count = 0
        self.complete_count = 0
        self.active_count = 0
        self.all_enqueued = False
        self.loop = loop or asyncio.get_event_loop()

    def _show_progress(self, newline=False):
         if not self.show_progress:
             return

         sys.stdout.write('\r {done} out of {total} tasks complete - {percent_done}% (running={running}; pending={pending})'.format(
             done=self.complete_count,
             total=self.entry_count,
             percent_done=int(self.complete_count / self.entry_count * 100),
             running=self.active_count,
             pending=self.input_queue.qsize(),
         ))
         if newline:
             # Finish with a newline
             print('')

    def _save_result(self, entry):
        self.complete_count += 1
        self.output_queue.put_nowait(entry)

    def _is_complete(self):
        return self.complete_count == self.entry_count

    async def stream(self, source):
        # Enqueue all work tasks
        async for entry in source:
            self.entry_count += 1
            self.input_queue.put_nowait(entry)
        self.all_enqueued = True

        # Start workers
        workers = []
        for i in range(self.worker_count):
            worker = self.loop.create_task(self._worker())
            workers.append(worker)
            asyncio.ensure_future(worker)

        while not self._is_complete() or self.output_queue.qsize():
            try:
                entry = await asyncio.wait_for(self.output_queue.get(), timeout=.1)
            except asyncio.TimeoutError:
                continue
            self.output_queue.task_done()
            yield entry
            self._show_progress()
        self._show_progress(newline=True)

    async def _worker(self):
        while True:
            try:
                entry = await asyncio.wait_for(self.input_queue.get(), timeout=.1)
            except asyncio.TimeoutError:
                if self.input_queue.qsize() == 0 and self.all_enqueued:
                    return
                else:
                    continue
            self.active_count += 1
            try:
                if asyncio.iscoroutinefunction(self.executor):
                    entry.value = await self.executor(entry.value)
                else:
                    def executor_wrapper():
                        return self.executor(entry.value)
                    entry.value = await self.loop.run_in_executor(None, executor_wrapper)
            except Exception as e:
                entry.error(e)
                if self.stacktraces:
                    result = '{}'.format(traceback.format_exc())
                else:
                    result = str(e)
            self._save_result(entry)
            self.input_queue.task_done()
            self.active_count -= 1

@arg_help('No operation. Just for testing.')
async def noop(source):
    for entry in source:
        yield entry

@arg_help('Filter out values that are not truthy')
async def truthy(source):
    async for entry in source:
        if entry.value:
            yield entry

@arg_help('Take json strings and parse them into objects so other streamers can inspect attributes')
async def json_parser(source):
    async for entry in source:
        try:
            entry.value = json.loads(entry.value)
        except Exception as e:
            entry.error(e)
        yield entry

@arg_help('Take any values that are an array and treat each value of an array as a separate input ')
async def split_lists(source):
    """ Splits arrays into multiple entries """
    async for entry in source:
        # No-op on non-list values
        if not isinstance(entry.value, list):
            yield entry


        for wrapped_value in entry_wrap(entry.value):
            yield wrapped_value

@arg_help('Show a report of how many input values ended up with a particular result value')
class ValueBreakdown(BaseStreamer):
    """ Gives summary stats either instead of the values or as an extra event at the end"""

    def __init__(self, inputs=False, append=False):
        self.inputs = inputs
        self.append = append

    async def stream(self, source):
        stats = OrderedDict()
        async for entry in source:
            if entry.value in stats:
                value_stats = stats[entry.value]
                value_stats['count'] += 1
                if self.inputs:
                    value_stats['inputs'].append(entry.original_value)
            else:
                metadata = {
                    'value': entry.value,
                    'count': 1,
                }
                if self.inputs:
                    metadata['inputs'] = [entry.original_value]
                stats[entry.value] = metadata

            if self.append:
                yield entry

        if self.append:
            yield Entry(list(stats.values()))
        else:
            for wrapped_value in entry_wrap(stats.values()):
                yield wrapped_value


STREAMERS = {
    'extract': ExtractionStreamer,
    'py': PyExecTransform,
    'pyfilter': PyExecFilter,
    'truthy': truthy,
    'noop': noop,
    'split': split_lists,
    'breakdown': ValueBreakdown,

    # We don't directly expose the async executor because it requires the module loading
    #'exec': AsyncExecutor,
    
}
# Add executors that need to be wrapped with AsyncExecutor
STREAMERS.update(executors.EXECUTORS)

def load_streamer(path, arg_list, options=None, print_help=False):
    kwargs = {}
    if options:
        kwargs.update(options)
    if path is None:
        return None
    elif '.' in path:
        Streamer = utils.import_obj(path)
    else:
        Streamer = STREAMERS.get(path)
    if Streamer is None:
        raise ValueError('Invalid streamer: {}'.format(path))

    if print_help:
        streamer_parser = argparse.ArgumentParser(
            prog=path,
            add_help=False,
            usage='streamline -s %(prog)s -- [options]',
        )
        if hasattr(Streamer, 'async_handler'):
            AsyncExecutor.args(streamer_parser)
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
            else:
                executor = Executor(**kwargs).handle
        else:
            executor = Executor

        # Now build the wrapper
        ae_parser = argparse.ArgumentParser(add_help=False)
        AsyncExecutor.args(ae_parser)
        ae_args, arg_list = ae_parser.parse_known_args(arg_list)
        ae = AsyncExecutor(executor, **ae_args.__dict__)
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
