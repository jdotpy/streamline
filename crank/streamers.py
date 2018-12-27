import json
import sys

from .utils import import_obj

def _get_file_io(name, write=False):
    if name == '-' and write:
        return sys.stdout
    elif name == '-' and not write:
        return sys.stdin
    else:
        return open(name, 'w' if write else 'r', 1)


class UninitializedStreamerError(Exception):
     default_message = 'This is a default message!'

     def __init__(self, message=None, **kwargs):
         super().__init__(message or default_message, **kwargs)


class LineStreamer():
    DELIMITER = '\n'

    def __init__(self, source_name, target_name, force_flush=False):
        self.source_name = source_name
        self.target_name = target_name
        self.force_flush = force_flush

        self.first_written = False
        self.source = None
        self.target = None
        self.target_template = None

    def __iter__(self):
        return self.read()

    def __enter__(self):
        self.source = _get_file_io(self.source_name)
        if '{entry}' in self.target_name:
            self.target_template = self.target_name
        else:
            self.target = _get_file_io(self.target_name, write=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self.source, 'close'):
            self.source.close()
        if self.target and hasattr(self.target, 'close'):
            self.target.close()

    def read(self):
        if self.source is None:
            raise UninitializedStreamerError()

        for line in self.source:
            yield line.rstrip('\n')

    def write(self, entry, result):
        if not isinstance(result, str):
            result = json.dumps(result)

        if self.first_written:
            result = self.DELIMITER + result
        else:
            self.first_written = True

        if self.target_template:
            with open(self.target_template.format(entry=entry), 'w') as target_file:
                target_file.write(result)
        elif self.target is None:
            raise UninitializedStreamerError()
        else:
            self.target.write(result)
            if self.force_flush and hasattr(self.target, 'flush'):
                self.target.flush()
        
STREAMERS = {
    'line': LineStreamer,
}

def load_streamer(path):
    if path is None:
        return None
    elif '.' in path:
        Streamer = import_obj(path)
    else:
        Streamer = STREAMERS.get(path)
    return Streamer
