
from importlib import import_module
import asyncio
import logging
import traceback
import json
import sys

from .extractor import extract_path, parse_selectors

logger = logging.getLogger(__file__)

class AsyncExecutor():
    """
        :: Worker-oriented event loop processor

        Workers are no longer a necessary concept in an asynchronous world. However, the concept can still be very
        helpful for controlling resource usage on the host machine or remote systems used by the job. For this reason
        I'm re-implementing a worker-style executor pool which could be used with jobs that are threaded or async.
    """
    DEFAULT_WORKERS = 5

    def __init__(self, source, target, executor, show_progress=True, stacktraces=False, extract=None, workers=DEFAULT_WORKERS):
        self.source = source
        self.target = target
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

    def _save_result(self, entry, result):
        self.complete_count += 1
        self.output_queue.put_nowait((entry, result))

    def start(self):
        # Enqueue all work tasks
        for entry in self.source:
            self.entry_count += 1
            self.input_queue.put_nowait(entry.rstrip('\n'))
        self.all_enqueued = True

        # Start workers
        workers = []
        for i in range(self.worker_count):
            worker = self.loop.create_task(self._worker())
            workers.append(worker)
            asyncio.ensure_future(worker)

        #self.loop.run_until_complete(self._execute_jobs())

    def _is_complete(self):
        return self.complete_count == self.entry_count

    async def _execute_jobs(self):
        # Wait and show progress
        while True:
            self._show_progress()
            await asyncio.wait(workers, timeout=0.1)
            if self._is_complete():
                break
            else:
                self._show_progress()
        self._show_progress(newline=True)

    async def stream_results(self):
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
                    result = await self.executor(entry)
                else:
                    def executor_wrapper():
                        return self.executor(entry)
                    result = await self.loop.run_in_executor(None, executor_wrapper)
            except Exception as e:
                if self.stacktraces:
                    result = '{}'.format(traceback.format_exc())
                else:
                    result = str(e)
            self._save_result(entry, result)
            self.input_queue.task_done()
            self.active_count -= 1

async def noop_executor(source):
    for entry in source:
        yield entry, entry

async def streamline(Streamer, executor=None, headers=None, progress=False, extract=None, worker_count=None, stacktraces=True):
    if extract is None:
        extractor = lambda x: x
    else:
        selectors = parse_selectors(extract)
        extractor = lambda x: extract_path(x, selectors)

    with Streamer as streamer:
        if executor:
            ae = AsyncExecutor(
                streamer.read(),
                streamer.write,
                executor,
                show_progress=progress,
                stacktraces=stacktraces,
                workers=worker_count,
            )
            ae.start()
            result_stream = ae.stream_results()
        else:
            result_stream = noop_executor(streamer.read())
            
        async for entry, result in result_stream:
            extracted_result = extractor(result)

            if headers:
                if not isinstance(extracted_result, str):
                    extracted_result = json.dumps(entry)
                extracted_result = '{}: {}'.format(entry, extracted_result)
            streamer.write(entry, extracted_result)
