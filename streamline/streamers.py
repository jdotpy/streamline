import traceback
import asyncio
import json
import re
import sys

from .extractor import Extractor
from . import utils


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
            await self.handle(entry.value)
            entry.value = result
            yield entry

    def initialize(self):
        pass

class PyExecTransform(BaseStreamer):
    def __init__(self, text=None, expression=True, show_exceptions=False):
        self.show_exceptions = show_exceptions
        self.runner = eval if expression else exec
        try:
            self.code = compile(transform_py, '<string::transform>', self.runner.__name__)
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    async def stream(self, source):
        async for i, entry in enumerate(source):
            scope = {'value': entry.value, 'input': entry.original_value, 'i': entry.index, 'index': entry.index}
            try:
                if expression:
                    entry.value = self.runner(self.code, globals(), scope)
                else:
                    self.runner(self.code, globals(), scope)
                    entry.value = scope.get('result', None) or entry.value
            except Exception as e:
                if self.show_exceptions:
                    entry.exception(e)
                    traceback.print_exc()
                else:
                    continue
            yield entry

class PyExecFilter(BaseStreamer):
    def __init__(self, text=None, show_exceptions=False):
        self.show_exceptions = show_exceptions
        try:
            self.code = compile(text, '<string::filter>', 'eval')
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    async def stream(self, source):
        async for i, entry in enumerate(source):
            scope = {'value': entry.value, 'input': entry.original_value, 'i': entry.index, 'index': entry.index}
            try:
                keep = eval(self.code, globals(), scope)
            except Exception as e:
                keep = False
                if self.show_exceptions:
                    traceback.print_exc()
            if keep:
                yield entry

class ExtractionStreamer(BaseStreamer):
    def initialize(self):
        self.extractor = Extractor(self.options['path'])

    async def handle(self, value):
        yield self.extractor.extract(result)

class AsyncExecutor():
    """
        :: Worker-oriented event loop processor

        Workers are no longer a necessary concept in an asynchronous world. However, the concept can still be very
        helpful for controlling resource usage on the host machine or remote systems used by the job. For this reason
        I'm re-implementing a worker-style executor pool which could be used with jobs that are threaded or async.
    """
    DEFAULT_WORKERS = 5

    def __init__(self, executor=None, show_progress=True, stacktraces=False, extract=None, workers=DEFAULT_WORKERS):
        self.extract = extract
        self.executor = executor
        self.input_queue = asyncio.Queue()
        self.output_queue = asyncio.Queue()
        self.show_progress = show_progress
        self.worker_count = workers or self.DEFAULT_WORKERS
        self.stacktraces = stacktraces
        self.loop = asyncio.get_event_loop()

        # State data
        self.entry_count = 0
        self.complete_count = 0
        self.active_count = 0
        self.all_enqueued = False

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
        for entry in source:
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
                result_pair = await asyncio.wait_for(self.output_queue.get(), timeout=.1)
            except asyncio.TimeoutError:
                continue
            yield result_pair
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
                entry.errors(e)
                if self.stacktraces:
                    result = '{}'.format(traceback.format_exc())
                else:
                    result = str(e)
            self._save_result(entry)
            self.input_queue.task_done()
            self.active_count -= 1

async def noop(source, **kwargs):
    for entry in source:
        yield entry


STREAMERS = {
    'extractor': ExtractionStreamer,
    'py': PyExecTransform,
    'pyfilter': PyExecFilter,
    'noop': noop,
}
