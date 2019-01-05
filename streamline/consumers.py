import json
import csv
import os

from . import utils 

def stringify_all(source):
    return [utils.force_string(v) for v in source]

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
        output = ''
        if '{' in self.target_name:
            self.target_template = self.target_name
        else:
            self.target = utils.get_file_io(self.target_name, write=True)
        async for entry in source:
            output = self._output_value(entry)
            if self.target_template:

                file_name = self.target_template.format(input=entry.original_value, index=entry.index)
                with open(file_name, 'w') as target_file:
                    target_file.write(output)
            else:
                if self.first_written:
                    output = self.DELIMITER + output
                else:
                    self.first_written = True
                self.target.write(output)
            if hasattr(self.target, 'flush'):
                self.target.flush()

        if not output.endswith('\n') and os.environ.get('STREAMLINE_CLOSING_NEWLINE'):
            self.target.write('\n')

        if self.target and hasattr(self.target, 'close'):
            self.target.close()

class CSVWriter():
    def __init__(self, target_name):
        self.target_name = target_name

    def _parse_fields(self, entry):
        if not isinstance(entry.value, dict):
            return None
        else:
            return entry.value.keys()
    
    def _get_values(self, entry, fields):
        if fields is None:
            return stringify_all((entry.original_value, entry.value))

        # Return blanks for all invalid rows
        if not isinstance(entry.value, dict):
            return [''] * len(fields)

        values = [entry.value.get(field, '') for field in fields]
        return values

    async def stream(self, source):
        self.target = utils.get_file_io(self.target_name, write=True)
        writer = csv.writer(self.target, 'unix', quoting=csv.QUOTE_MINIMAL)

        fields = None
        header_written = False
        async for entry in source:
            # Write csv column names
            if not header_written:
                fields = self._parse_fields(entry)
                if fields is None:
                    writer.writerow(['input', 'value'])
                else:
                    writer.writerow(fields)
                header_written = True

            # Write values
            entry_values = self._get_values(entry, fields)
            writer.writerow(entry_values)

        if hasattr(self.target, 'close'):
            self.target.close()


CONSUMERS = {
    'file': FileWriter,
    'csv': CSVWriter,
}

def load_consumer(path):
    if path is None:
        return None
    elif '.' in path:
        Consumer = utils.import_obj(path)
    else:
        Consumer = CONSUMERS.get(path)
    return Consumer

