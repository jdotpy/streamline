import traceback
import json
import sys
import re


class BaseStreamer():
    def __init__(self, source=None, **options):
        self.source = source
        self.options = options

    def __aiter__(self):
        return self.stream()

class PyExecTransform(BaseStreamer):
    def __init__(self, source, text=None, expression=True, show_exceptions=False):
        self.source = source
        self.show_exceptions = show_exceptions
        self.runner = eval if expression else exec
        try:
            self.code = compile(transform_py, '<string::transform>', self.runner.__name__)
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    async def stream(self):
        async for i, result_pair in enumerate(self.source):
            entry, result = result_pair
            scope = {'entry': entry, 'result': result, 'i': i, 'index': i}
            try:
                if expression:
                    result = self.runner(self.code, globals(), scope)
                else:
                    self.runner(self.code, globals(), scope)
                    result = scope.get('result', None) or result
            except Exception as e:
                if self.show_exceptions:
                    traceback.print_exc()
                if force_blank:
                    line = ''
                else:
                    continue
            yield entry, result

class PyFilter(BaseStreamer):
    def __init__(self, source, text=None, show_exceptions=False):
        self.source = source
        self.show_exceptions = show_exceptions
        try:
            self.code = compile(text, '<string::filter>', 'eval')
        except Exception as e:
            traceback.print_exc()
            sys.exit(1)

    async def stream(self):
        async for i, result_pair in enumerate(self.source):
            entry, result = result_pair
            scope = {'entry': entry, 'result': result, 'i': i, 'index': i}
            try:
                keep = eval(self.code, globals(), scope)
            except Exception as e:
                keep = False
                if self.show_exceptions:
                    traceback.print_exc()
            if keep:
                yield result_pair

