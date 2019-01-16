from . import utils 
from . import entries

import json
import csv

class FileReader():
    DELIMITER = '\n'
    DEFAULT_SOURCE = '-'

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--input',
            default=cls.DEFAULT_SOURCE,
            dest='source_name',
            help='Set source (Default stdin)',
        )
        parser.add_argument(
            '-k', '--keep-trailing-newline',
            help='Dont automatically trim the ending newline character',
            action='store_true',
            default=False,
        )

    def __init__(self, source_name=DEFAULT_SOURCE, keep_trailing_newline=False):
        self.source_name = source_name
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
            dest='source_name',
            help='Set source (Default stdin)',
        )

    def __init__(self, source_name, **kwargs):
        self.source_name = source_name
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


GENERATORS = {
    'file': FileReader,
    'csv': CSVReader,
    'json': JsonReader,
}

def load_generator(path):
    if path is None:
        return None
    elif '.' in path:
        Generator = utils.import_obj(path)
    else:
        Generator = GENERATORS.get(path)
    return Generator

