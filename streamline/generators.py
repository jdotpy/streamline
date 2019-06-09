from . import utils 
from . import entries

import glob
import json
import csv
import os

class FileReader():
    DELIMITER = '\n'
    DEFAULT_SOURCE = '-'

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--input',
            default=cls.DEFAULT_SOURCE,
            help='Set source (Default stdin)',
        )
        parser.add_argument(
            '-k', '--keep-trailing-newline',
            help='Dont automatically trim the ending newline character',
            action='store_true',
            default=False,
        )

    def __init__(self, input=DEFAULT_SOURCE, keep_trailing_newline=False):
        self.source_name = input 
        self.keep_trailing_newline = keep_trailing_newline
        self.source = None

    async def stream(self):
        factory = entries.EntryFactory()
        source = utils.get_file_io(self.source_name)

        ending_delim = False
        try:
            for index, line in enumerate(source):
                ending_delim = line.endswith(self.DELIMITER)
                if ending_delim:
                    line = line.rstrip(self.DELIMITER)
                yield factory(line.rstrip(self.DELIMITER))
        except KeyboardInterrupt as e:
            pass

        if ending_delim and self.keep_trailing_newline:
            yield factory('')

        if hasattr(source, 'close'):
            source.close()

class CSVReader():
    DEFAULT_SOURCE = '-'

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--input',
            default=cls.DEFAULT_SOURCE,
            help='Set source (Default stdin)',
        )

    def __init__(self, input=None, **kwargs):
        self.source_name = input
        self.source = None

    async def stream(self):
        factory = entries.EntryFactory()
        source = utils.get_file_io(self.source_name)
        reader = csv.DictReader(source)

        for row in reader:
            yield factory(row)

        if hasattr(source, 'close'):
            source.close()

class JsonReader():
    DEFAULT_SOURCE = '-'

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--input',
            default=cls.DEFAULT_SOURCE,
            dest='source_name',
            help='Set source (Default stdin)',
        )

    def __init__(self, source_name, **kwargs):
        self.source_name = source_name
        self.source = None

    async def stream(self):
        factory = entries.EntryFactory()
        source = utils.get_file_io(self.source_name)
        content = source.read()
        if hasattr(source, 'close'):
            source.close()

        data = json.loads(content)
        if isinstance(data, list):
            for row in data:
                yield factory(row)
        else:
            yield factory(data)

class MultifileReader():
    DEFAULT_PATTERN= '.'

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--pattern',
            default=cls.DEFAULT_PATTERN,
            help='Set directory to list files from',
        )
        parser.add_argument(
            '--metadata',
            default=False,
            action='store_true',
            help='Include file metadata',
        )

    def __init__(self, pattern=None, metadata=False):
        self.include_metadata = metadata
        self.pattern = os.path.expanduser(pattern)
        self.pattern = os.path.abspath(self.pattern)
        if os.path.isdir(self.pattern):
            self.pattern += '/*'

    async def stream(self):
        factory = entries.EntryFactory()
        for path in glob.glob(self.pattern):
            if os.path.isfile(path):
                with open(path, 'r') as f:
                    content = f.read()
                if self.include_metadata:
                    yield factory({
                        'path': path,
                        'content': content,
                    })
                else:
                    yield factory(content)


GENERATORS = {
    'file': FileReader,
    'csv': CSVReader,
    'json': JsonReader,
    'files': MultifileReader,
}

def load_generator(path):
    if path is None:
        return None
    elif '.' in path:
        Generator = utils.import_obj(path)
    else:
        Generator = GENERATORS.get(path)
    if Generator is None:
        raise ValueError('Invalid generator: {}'.format(path))
    return Generator

