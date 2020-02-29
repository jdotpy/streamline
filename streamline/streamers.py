from collections import OrderedDict
from operator import itemgetter
import traceback
import argparse
import asyncio
import math
import json
import copy
import re
import os
import sys

from .entries import entry_wrap, Entry
from .extractor import Extractor
from . import executors
from . import utils

arg_help = utils.arg_help

def get_eval_scope(entry):
    entry_scope = {'value': entry.value, 'input': entry.original_value, 'i': entry.index, 'index': entry.index}
    scope = {**globals(), **entry_scope}
    # Right now we're just exposing a single scope so that its treated like writing a python file at the global scope
    return scope, scope

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

    def __init__(self, code=None, statement=False):
        self.expression = not statement
        self.runner = exec if statement else eval
        try:
            self.code = compile(code, '<string::transform>', self.runner.__name__)
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    async def stream(self, source):
        async for entry in source:
            global_scope, local_scope = get_eval_scope(entry)
            try:
                if self.expression:
                    entry.value = self.runner(self.code, global_scope, local_scope)
                else:
                    self.runner(self.code, global_scope, local_scope)
                    if 'result' in local_scope:
                        entry.value = local_scope.get('result')
                    else:
                        entry.value = local_scope.get('result')
            except Exception as e:
                entry.error(e)
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

    def __init__(self, code=None):
        try:
            self.code = compile(code, '<string::filter>', 'eval')
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    async def stream(self, source):
        async for entry in source:
            global_scope, local_scope = get_eval_scope(entry)
            try:
                keep = eval(self.code, global_scope, local_scope)
            except Exception as e:
                keep = False
            if keep:
                yield entry

@arg_help('Change the value to an attribute of the current value', example='--selector exit_code')
class ExtractionStreamer(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--selector',
            required=True,
            help='dot-separated path to desired attribute',
        )

    def initialize(self):
        self.extractor = Extractor(self.options.get('selector', None), value_symbol=True)

    async def handle(self, value):
        return self.extractor.extract(value)

@arg_help('Combine two previous historical values by setting an attribute', example='--source "-1" --target "-2"')
class Combiner(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--source',
            type=int,
            default=-1,
            help='index of history item to get insertion value from',
        )
        parser.add_argument(
            '--target',
            type=int,
            default=-2,
            help='index of history item to insert into',
        )
        parser.add_argument(
            '--disallow-wrapping',
            action='store_true',
            default=False,
            help='Dont automatically wrap non-object results in an object',
        )
        parser.add_argument(
            '--path',
            default=None,
            help='Path of attribute on "target" to set as the value of "source"',
        )

    async def stream(self, source):
        path = self.options.get('path', None)

        async for entry in source:
            history = entry.get_history()
            try:
                source = history[self.options['source']]
                target = history[self.options['target']]
            except IndexError as e:
                entry.error(e)
                yield entry
                continue

            if path is None:
                path = 'value'
            if not isinstance(target, dict) :
                if self.options.get('disallow_wrapping', False):
                    # We can't set any attributes here, move on
                    entry.error('Cannot combine attributes as target is not an object')
                else:
                    entry.value = {'base': target, path: source}
            else:
                new_value = copy.deepcopy(target)
                new_value[path] = source
                entry.value = new_value
            yield entry

class AsyncExecutor():
    """
        :: Worker-oriented event loop processor

        Workers are no longer a necessary concept in an asynchronous world. However, the concept can still be very
        helpful for controlling resource usage on the host machine or remote systems used by the job. For this reason
        I'm re-implementing a worker-style executor pool which could be used with jobs that are threaded or async.
    """
    DEFAULT_WORKERS = utils.get_env_as('STREAMLINE_WORKER_COUNT', int, default=20)

    def __init__(self, executor=None, workers=DEFAULT_WORKERS, loop=None):
        self.executor = executor
        self.output_queue = asyncio.Queue()
        self.worker_count = workers or self.DEFAULT_WORKERS

        # State data
        self.entry_count = 0
        self.complete_count = 0
        self.active_count = 0
        self.loop = loop or asyncio.get_event_loop()

    def _save_result(self, entry):
        self.complete_count += 1
        self.output_queue.put_nowait(entry)

    async def stream(self, source):
        self.source = source
        all_read = False
        pending = 0
        next_input = None
        next_output = None
        
        while True:
            # Break condition
            if all_read and pending == 0:
                break

            if not next_input and not all_read and pending < self.worker_count:
                next_input = asyncio.create_task(self.source.__anext__())

            if not next_output:
                next_output = asyncio.create_task(self.output_queue.get())

            awaitables = [aw for aw in (next_input, next_output) if aw]
            tasks_done, tasks_pending = await asyncio.wait(awaitables, return_when=asyncio.FIRST_COMPLETED)

            if next_input in tasks_done:
                try:
                    entry = next_input.result()
                    next_input = None
                    entry_future = asyncio.create_task(self.handle(entry))
                    pending += 1
                except StopAsyncIteration:
                    all_read = True

            if next_output in tasks_done:
                pending -= 1
                yield next_output.result()
                self.output_queue.task_done()
                next_output = None

    async def handle(self, entry):
        try:
            if asyncio.iscoroutinefunction(self.executor):
                entry.value = await self.executor(entry.value)
            else:
                def executor_wrapper():
                    return self.executor(entry.value)
                entry.value = await self.loop.run_in_executor(None, executor_wrapper)
        except Exception as e:
            entry.error(e)
        self._save_result(entry)
        self.active_count -= 1

@arg_help('No operation. Just for testing.')
async def noop(source):
    async for entry in source:
        yield entry

@arg_help('Filter out values that are not truthy')
async def truthy(source):
    async for entry in source:
        if entry.value:
            yield entry

@arg_help('Filter out values that are truthy')
async def falsey(source):
    async for entry in source:
        if not entry.value:
            yield entry

@arg_help('Take json strings and parse them into objects so other streamers can inspect attributes')
async def json_parser(source):
    async for entry in source:
        if not isinstance(entry.value, str):
            yield entry
            continue
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
            continue

        sub_entries = []
        for sub in entry.value:
            new_entry = entry.clone()
            new_entry.value = sub
            yield new_entry

@arg_help('Replace the value with the original input')
async def input_values(source):
    """ Splits arrays into multiple entries """
    async for entry in source:
        entry.value = entry.original_value
        yield entry

@arg_help('Take any values that are an array and treat each value of an array as a separate input ')
class Split(BaseStreamer):
    """ Splits arrays into multiple entries """
    DEFAULT_DELIMITER = r'\s+'

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--delimiter',
            default=cls.DEFAULT_DELIMITER,
            help='Regular expression pattern on which to split',
        )

    def initialize(self):
        delim = self.options.get('delimiter', self.DEFAULT_DELIMITER)
        self.pattern = re.compile(delim)

    async def stream(self, source):
        async for entry in source:
            # No-op on non-str values
            if not isinstance(entry.value, str):
                yield entry
                continue

            for wrapped_value in entry_wrap(re.split(self.pattern, entry.value)):
                yield wrapped_value

@arg_help('Show a report of how many input values ended up with a particular result value')
class ValueBreakdown(BaseStreamer):
    """ Gives summary stats either instead of the values or as an extra event at the end"""

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--inputs',
            action='store_true',
            default=False,
            help='Report the array of input values that had a particular result value',
        )
        parser.add_argument(
            '--append-summary',
            action='store_true',
            default=False,
            help='Instead of replacing values with the result breakdown, append a single entry at the end with the data',
        )
        parser.add_argument(
            '--group-by',
            default='value',
            help='Which value to do the breakdown by',
        )

    def __init__(self, inputs=False, append_summary=False, group_by=None):
        self.inputs = inputs
        self.append = append_summary
        if isinstance(group_by, list):
            self.group_by = group_by
        elif group_by:
            self.group_by = group_by.split(',')
        else:
            self.group_by = ['value']
        self.group_by_extractors = [Extractor(gb, value_symbol=True) for gb in self.group_by]

    async def stream(self, source):
        stats = OrderedDict()
        async for entry in source:
            group_by_value = [e.extract(entry.value) for e in self.group_by_extractors]
            if len(group_by_value) == 1:
                group_by_value = group_by_value[0]
            else:
                group_by_value = tuple(group_by_value)
            if group_by_value in stats:
                value_stats = stats[group_by_value]
                value_stats['count'] += 1
                if self.inputs:
                    value_stats['inputs'].append(entry.original_value)
            else:
                metadata = {
                    'value': group_by_value,
                    'count': 1,
                }
                if self.inputs:
                    metadata['inputs'] = [entry.original_value]
                stats[group_by_value] = metadata

            if self.append:
                yield entry

        if self.append:
            yield Entry(list(stats.values()))
        else:
            for wrapped_value in entry_wrap(stats.values()):
                yield wrapped_value

@arg_help('Force each value to a string and prefix each with the original input value')
class InputHeaders(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--indexes',
            action='store_true',
            default=False,
            help='Also prepend index',
        )

    async def stream(self, source):
        async for entry in source:
            value = utils.force_string(entry.value)
            header = utils.force_string(entry.original_value)
            if self.options.get('indexes', False):
                entry.value = '[{}] {}: {}'.format(entry.index, header, value)
            else:
                entry.value = '{}: {}'.format(header, value)
            yield entry

@arg_help('Filter out any entries that have produced an error')
async def filter_out_errors(source):
    async for entry in source:
        if not entry.errors:
            yield entry

@arg_help('Use the latest error on the entry as the value')
async def error_values(source):
    async for entry in source:
        if not entry.errors:
            yield entry
            continue

        error = entry.errors[-1]
        if isinstance(error, Exception) and getattr(error, '__traceback__', None):
            error = '{}: {}\n{}'.format(
                str(type(error).__name__),
                str(error),
                '\n'.join(traceback.format_tb(error.__traceback__))       
            )
        entry.value = error
        yield entry

@arg_help('Hold entries in memory until a certain number is reached (give no args to buffer all)', example='--buffer 20')
class StreamingBuffer(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--buffer',
            default='all',
            help='Number of entries to buffer (blank for all)',
        )

    def initialize(self):
        try:
            self.buffer_size = int(self.options['buffer'])
        except Exception as e:
            self.buffer_size = None

    async def stream(self, source):
        buffer_list = []
        async for entry in source:
            buffer_list.append(entry)
            if self.buffer_size is None:
                # We want to accrue all
                continue
            elif buffer_list >= self.buffer_size:
                # Empty the buffer
                for entry in buffer_list:
                    yield entry
                buffer_list = []

        # Drain any remaining
        for entry in buffer_list:
            yield entry

@arg_help('Strip surrounding whitespace from each string entry, removing entries that are only whitespace', example='--buffer 20')
class StripWhitespace(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--keep-blank',
            default=False,
            help='Dont remove blank entries',
        )

    async def stream(self, source):
        keep_blank = bool(self.options.get('keep_blank', False))
        async for entry in source:
            value = entry.value
            if not isinstance(value, str):
                yield entry
                continue

            value = value.strip()
            if value == '' and not keep_blank:
                continue
            if value != entry.value:
                entry.value = value
            yield entry

@arg_help('Only take the first X entries (Default 1)', example='--count 20')
class HeadStreamer(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--count',
            default=1,
            type=int,
            help='How many entries to keep',
        )

    async def stream(self, source):
        max_count = self.options.get('count', 1)
        count = 0
        async for entry in source:
            yield entry
            count += 1
            if count >= max_count:
                break

@arg_help('Read the file indicated by the file', example='--path ~/dir/{value}.json')
class ReadFileStreamer(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--path',
            default='{value}',
            help='Where to read the files from',
        )
        parser.add_argument(
            '--file-metadata',
            default=False,
            action='store_true',
            help='Whether to include the file metadata or just the content',
        )

    async def stream(self, source):
        file_metadata = self.options.get('file_metadata', False)
        file_path = os.path.expanduser(self.options.get('path'))
        if os.path.isdir(file_path):
            file_path = os.path.join(file_path,'{value}')
        async for entry in source:
            entry_file_path = file_path.format(value=entry.value)
            try:
                with open(entry_file_path, 'r') as f:
                    contents = f.read()
            except Exception as e:
                entry.error(e)
                yield entry
                continue
            if file_metadata:
                entry.value = {
                    'content': contents,
                    'path': entry_file_path,
                    'size': os.path.getsize(entry_file_path),
                }
            else:
                entry.value = contents
            yield entry

@arg_help('Perform mathmatical statistics on a given numeric value', example='--path value')
class StatsStreamer(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--path',
            default='value',
            help='Where to read the numeric value from',
        )

    def initialize(self):
        self.extractor = Extractor(self.options.get('path', None), value_symbol=True)

    async def stream(self, source):
        stats = {
            'count': 0,
            'sum': 0,
            'min': None,
            'max': None,
        }
        async for entry in source:
            value = self.extractor.extract(entry.value)
            try:
                num = float(value)
            except Exception as e:
                continue
            stats['count'] += 1
            stats['sum'] += num
            if stats['min'] is None or num < stats['min']:
                stats['min'] = num
            if stats['max'] is None or num > stats['max']:
                stats['max'] = num
        if stats['count'] == 0:
            stats['average'] = 0
        else:
            stats['average'] = stats['sum'] / stats['count']
        yield Entry(stats)

@arg_help('Sort entries alphanumerically or numerically given a value or subvalue', example='--path value --numeric')
class SortStreamer(BaseStreamer):
    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--path',
            default='value',
            help='Where to read the sort value from',
        )
        parser.add_argument(
            '--numeric',
            default=False,
            action='store_true',
            help='Force the sort to compare numerically and fail otherwise',
        )
        parser.add_argument(
            '--descending',
            default=False,
            action='store_true',
            help='Sort in descending fasion sort order',
        )

    def initialize(self):
        self.extractor = Extractor(self.options.get('path', None), value_symbol=True)
        self.numeric = self.options.get('numeric', False)
        self.descending = self.options.get('descending', False)

    def _extract_sort_value(self, entry):
        sort_value = self.extractor.extract(entry.value)
        if self.numeric:
            try:
                sort_value = float(sort_value)
            except Exception as e:
                sort_value = None
        elif sort_value is not None:
            sort_value = str(sort_value)
        return sort_value

    async def stream(self, source):
        # Buffer all entries and then sort after all values have been calculated
        items_with_value = []
        items_without_value = []
        async for entry in source:
            sort_value = self._extract_sort_value(entry)
            if sort_value is None:
                items_without_value.append(entry)
            else:
                items_with_value.append((entry, sort_value))

        result = []
        sorted_items = [e[0] for e in sorted(items_with_value, key=itemgetter(1), reverse=self.descending)]
        if self.descending:
            result.extend(sorted_items)
            result.extend(items_without_value)
        else:
            result.extend(items_without_value)
            result.extend(sorted_items)

        for entry in result:
            yield entry

STREAMERS = {
    'extract': ExtractionStreamer,
    'py': PyExecTransform,
    'pyfilter': PyExecFilter,
    'truthy': truthy,
    'noop': noop,
    'split_list': split_lists,
    'split': Split,
    'breakdown': ValueBreakdown,
    'headers': InputHeaders,
    'inputs': input_values,
    'filter_out_errors': filter_out_errors,
    'errors': error_values,
    'buffer': StreamingBuffer,
    'json': json_parser,
    'strip': StripWhitespace,
    'head': HeadStreamer,
    'readfile': ReadFileStreamer,
    'combine': Combiner,
    'stats': StatsStreamer,
    'sort': SortStreamer,
}
# Add executors that need to be wrapped with AsyncExecutor
STREAMERS.update(executors.EXECUTORS)

@arg_help('Start a new history tree')
async def history_push(source):
    async for entry in source:
        entry.push()
        yield entry
        
@arg_help('Walk back up one level in the history tree')
async def history_pop(source):
    async for entry in source:
        entry.pop()
        yield entry

@arg_help('Treat the latest value as the original')
async def history_collapse(source):
    async for entry in source:
        entry.collapse()
        yield entry

@arg_help('Clear all levels of history')
async def history_reset(source):
    async for entry in source:
        entry.reset()
        yield entry

@arg_help('Set the current value to a list of all previous values')
async def history_all(source):
    async for entry in source:
        entry.value = entry.get_history()
        yield entry

# entry-transformations
STREAMERS.update({
    'history:push': history_push,
    'history:pop': history_pop,
    'history:collapse': history_collapse,
    'history:reset': history_reset,
    'history:values': history_all,
})

# Stateful hidden streamers
class ProgressStreamer():
    def __init__(self, buffer_start=True, buffer_end=True):
        self.started_count = 0
        self.complete_count = 0
        self.in_progress_count = 0
        self.buffer_start = buffer_start
        self.buffer_end = buffer_end
        self.all_loaded = False

    def update_progress(self, complete=False, started=False):
        if complete:
            self.complete_count += 1
        elif started:
            self.started_count += 1
        complete_perc = min(100, math.floor((self.complete_count / self.started_count) * 100))

        loading_bar = ('#' * (complete_perc)) + ((100 - complete_perc) * ' ')
        message = '|{}| ({}/{} - {}%)'.format(
            loading_bar,
            self.complete_count,
            self.started_count,
            complete_perc,
        )
        message = '\r' + message
        sys.stdout.write(message)
        if self.all_loaded and self.complete_count == self.started_count:
            sys.stdout.write('\n')

    async def streamer_start(self, source):
        if self.buffer_start:
            started = []
            async for entry in source:
                started.append(entry)
                self.update_progress(started=True)
            for entry in started:
                yield entry
        else:
            async for entry in source:
                self.update_progress(started=True)
                yield entry
        self.all_loaded = True

    async def streamer_end(self, source):
        if self.buffer_end:
            done = []
            async for entry in source:
                done.append(entry)
                self.update_progress(complete=True)
            for entry in done:
                yield entry
        else:
            async for entry in source:
                self.update_progress(complete=True)
                yield entry
