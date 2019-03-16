import copy

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
    def __init__(self, value=None, index=None, error_value=None):
        self.index = index
        self.history = [[value]]
        self.errors = []
        self.error_value = error_value

    def push(self, value=None):
        if value is None:
            value = self.value
        self.history.append([value])

    def pop(self):
        if len(self.history) == 1:
            raise ValueError('Attempted to pop an entry history that is only 1 level deep!')
        value = self.value
        self.history = self.history[:-1]
        self.value = value

    def reset(self):
        self.history = [[self.history[0][0]]]

    def collapse(self):
        self.history[-1] = [self.history[-1][-1]]

    def get_history(self):
        return self.history[-1]

    @property
    def original_value(self):
        return self.history[-1][0]

    def get_value(self):
        return self.history[-1][-1]

    def set_value(self, new_value):
        self.history[-1].append(new_value)

    def error(self, e):
        self.errors.append(e)
        self.value = self.error_value

    def clone(self):
        new_clone = Entry(index=self.index, error_value=self.error_value)
        new_clone.errors = self.errors.copy()
        new_clone.history = [h.copy() for h in self.history]
        return new_clone

    value = property(get_value, set_value)

def entry_wrap(items):
    return [Entry(item) for item in items]

def entry_unwrap(entries):
    return [entry.value for entry in entries]
