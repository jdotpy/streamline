import json

from . import utils 

class FileWriter():
    DELIMITER = '\n'

    def __init__(self, target_name):
        self.target_name = target_name
        self.target = None
        self.target_template = None
        self.first_written = False

    def _output_value(self, entry):
        return utils.force_string(entry.value)

    async def stream(self, source):
        async for entry in source:
            if '{input}' in self.target_name:
                self.target_template = self.target_name
            else:
                self.target = utils.get_file_io(self.target_name, write=True)

            output = self._output_value(entry)
            if self.first_written:
                output = self.DELIMITER + output
            else:
                self.first_written = True

            if self.target_template:
                with open(self.target_template.format(input=entry.original_value), 'w') as target_file:
                    target_file.write(output)
            else:
                self.target.write(output)
            if hasattr(self.target, 'flush'):
                self.target.flush()

        if self.target and hasattr(self.target, 'close'):
            self.target.close()

CONSUMERS = {
    'file': FileWriter,
}

def load_consumer(path):
    if path is None:
        return None
    elif '.' in path:
        Consumer = utils.import_obj(path)
    else:
        Consumer = CONSUMERS.get(path)
    return Consumer

