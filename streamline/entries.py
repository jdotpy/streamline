class EntryFactory():
    def __init__(self, error_value=None):
        self.error_value = error_value
        self.index = 0

    def __call__(self, value):
        entry = Entry(
            value,
            error_value=self.error_value,
            index=self.index
        )
        self.index += 1
        return entry

class Entry():
    def __init__(self, value, index=None, error_value=None):
        self.index = index
        self.values = [value]
        self.errors = []
        self.error_value = error_value

    @property
    def original_value(self):
        return self.values[0]

    def get_value(self):
        return self.values[-1]

    def set_value(self, new_value):
        self.values.append(new_value)

    def error(self, e):
        self.errors.append(e)
        self.value = self.error_value

    value = property(get_value, set_value)

def entry_wrap(items):
    return [Entry(item) for item in items]

def entry_unwrap(entries):
    return [entry.value for entry in entries]
