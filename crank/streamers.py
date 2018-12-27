import json

from .utils import import_obj

def _get_file_io(name, write=False):
    if name == '-' and write:
        return sys.stdout
    elif name == '-' and not write:
        return sys.stdin
    else:
        return open(name, 'w' if write else 'r')


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

    def __iter__(self):
        return self.read()

    def __enter__(self):
        self.source = _get_file_io(self.source)
        self.target = _get_file_io(target, write=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if hasattr(self.source, 'close'):
            self.source.close()
        if hasattr(self.target, 'close'):
            self.target.close()

    def read(self):
        if self.source is None:
            raise UninitializedStreamerError()

        for line in self.source:
            yield line.rstrip('\n')

    def write(self, entry):
        if self.target is None:
            raise UninitializedStreamerError()
        
        if not isinstance(entry, 'str'):
            entry = json.dumps(entry)

        if not self.first_written:
            entry = DELIMITER + entry
        
STREAMERS = {
    'line': LineStreamer,
}

def load_streamer(path):
    if path is None:
        return None
    elif '.' in args.module:
        return import_obj(path)
    else:
        return STREAMERS.get(path)

