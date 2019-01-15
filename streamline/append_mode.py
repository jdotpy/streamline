from . import entries
from . import utils

arg_help = utils.arg_help

class AppendingEntry(entries.Entry):
    def get_value(self):
        return self.values[0]

    def set_value(self, new_value):
        self.values.append(new_value)

    value = property(get_value, set_value)

@arg_help('Start appending values instead of replacing them.')
async def append_start(source):
    async for entry in source:
        if not isinstance(entry, AppendingEntry):
            yield AppendingEntry(entry.value)
        else:
            yield entry

@arg_help('Stop appending values and go back to replacing them.')
async def append_stop(source):
    async for entry in source:
        if isinstance(entry, AppendingEntry):
            yield entries.Entry(entry.values)
        else:
            yield entry
