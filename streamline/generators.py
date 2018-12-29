from . import utils 
from . import entries

class FileReader():
    DELIMITER = '\n'

    def __init__(self, source_name, force_flush=False):
        self.source_name = source_name
        self.force_flush = force_flush

        self.first_written = False
        self.source = None

    async def stream(self):
        source = utils.get_file_io(self.source_name)
        for index, line in enumerate(source):
            yield entries.Entry(line.rstrip(self.DELIMITER), index=index)

        if hasattr(source, 'close'):
            source.close()

GENERATORS = {
    'file': FileReader,
}

def load_generator(path):
    if path is None:
        return None
    elif '.' in path:
        Generator = utils.import_obj(path)
    else:
        Generator = GENERATORS.get(path)
    return Generator

