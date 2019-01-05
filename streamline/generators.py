from . import utils 
from . import entries

import csv

class FileReader():
    DELIMITER = '\n'

    def __init__(self, source_name, keep_trailing_newline=False):
        self.source_name = source_name
        self.keep_trailing_newline = keep_trailing_newline
        self.source = None

    async def stream(self):
        factory = entries.EntryFactory()
        source = utils.get_file_io(self.source_name)

        for index, line in enumerate(source):
            ending_delim = line.endswith(self.DELIMITER)
            if ending_delim:
                line = line.rstrip(self.DELIMITER)
            yield factory(line.rstrip(self.DELIMITER))

        if ending_delim and self.keep_trailing_newline:
            yield factory('')

        if hasattr(source, 'close'):
            source.close()

class CSVReader():
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

GENERATORS = {
    'file': FileReader,
    'csv': CSVReader,
}

def load_generator(path):
    if path is None:
        return None
    elif '.' in path:
        Generator = utils.import_obj(path)
    else:
        Generator = GENERATORS.get(path)
    return Generator

