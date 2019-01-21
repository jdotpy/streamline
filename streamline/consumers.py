import json
import csv
import os

from . import utils 

def stringify_all(source):
    return [utils.force_string(v) for v in source]

class FileWriter():
    DEFAULT_OUTPUT = '-'
    DELIMITER = '\n'

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--output',
            default=cls.DEFAULT_OUTPUT,
            help='Set target of output (Default stdout)',
        )

    def __init__(self, output=None):
        self.target_name = output
        self.target = None
        self.target_template = None
        self.first_written = False

        self.force_closing_newline = os.environ.get('STREAMLINE_CLOSING_NEWLINE')

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
                if self.force_closing_newline:
                    output = output + self.DELIMITER
                elif self.first_written:
                    output = self.DELIMITER + output
                else:
                    self.first_written = True
                self.target.write(output)
            if hasattr(self.target, 'flush'):
                self.target.flush()

        if self.target and hasattr(self.target, 'close'):
            self.target.close()

class CSVWriter():
    DEFAULT_OUTPUT = '-'

    @classmethod
    def args(cls, parser):
        parser.add_argument('--output', help='Set target of output (Default stdout)', default=cls.DEFAULT_OUTPUT)
        parser.add_argument(
            '--input-column',
            action='store_true',
            default=False,
            help='Automatically add a column for input value',
        )

    def __init__(self, output=DEFAULT_OUTPUT, input_column=False):
        self.target_name = output
        self.input_column = input_column

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
        if self.input_column:
            values.insert(0, entry.original_value)
        return stringify_all(values)

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
                    header = list(fields)
                    if self.input_column:
                        header.insert(0, 'input')
                    writer.writerow(header)
                header_written = True

            # Write values
            entry_values = self._get_values(entry, fields)
            writer.writerow(entry_values)

        if hasattr(self.target, 'close'):
            self.target.close()

class JsonWriter():
    DEFAULT_OUTPUT = '-'

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            '--output',
            default=cls.DEFAULT_OUTPUT,
            help='Set target of output (Default stdout)',
        )

    def __init__(self, output=DEFAULT_OUTPUT):
        self.target_name = output

    async def stream(self, source):
        self.target = utils.get_file_io(self.target_name, write=True)
        self.target.write('[\n')
        first = True
        async for entry in source:
            if not first:
                self.target.write(',\n    ')
            else:
                self.target.write('    ')
            self.target.write(json.dumps(entry.value))
            first = False
        self.target.write('\n]')

        if hasattr(self.target, 'close'):
            self.target.close()


CONSUMERS = {
    'file': FileWriter,
    'csv': CSVWriter,
    'json': JsonWriter,
}

def load_consumer(path):
    if path is None:
        return None
    elif '.' in path:
        Consumer = utils.import_obj(path)
    else:
        Consumer = CONSUMERS.get(path)
    if Consumer is None:
        raise ValueError('Invalid consumer: {}'.format(path))
    return Consumer

