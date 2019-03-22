import re

from . import utils

INDEX_ATTR_PATTERN = r'(\[\*\])|(\[\d\])'
class IndexSelector:
    def __init__(self, index):
        self.index = index

    def __str__(self):
        return 'IndexSelector({})'.format(self.index)

def parse_selectors(path):
    if path is None:
        return []
    selectors = []
    dotted_chunks = path.split('.')
    for section in dotted_chunks:
        chunks = utils.truthy(re.split(INDEX_ATTR_PATTERN, section))
        for chunk in chunks:
            if chunk.startswith('[') and chunk.endswith(']'):
                selectors.append(IndexSelector(chunk[1:-1]))
            else:
                selectors.append(chunk)
    return selectors

def extract_path(data, path=None):
    if isinstance(path, list):
        selectors = path
    else:
        selectors = parse_selectors(path)

    result = data
    for i, selector in enumerate(selectors):
        if isinstance(selector, IndexSelector):
            if selector.index == '*':
                if result is None:
                    return []
                return [extract_path(entry, selectors[i + 1:]) for entry in result]
            else:
                try:
                    result = result[int(selector.index)]
                except (TypeError, KeyError, IndexError) as e:
                    result = None
        elif selector == '*':
            try:
                values = result.values()
            except Exception as e:
                return []
            return [extract_path(value, selectors[i + 1:]) for value in values]
        else:
            if isinstance(result, dict):
                result = result.get(selector, None)
            else:
                result = getattr(result, selector, None)
    return result

class Extractor():
    """ Expose the extraction logic in class form """
    def __init__(self, path, value_symbol=False):
        self.selectors = parse_selectors(path)

        # Support the optional "value..." prefix syntax
        if value_symbol and self.selectors and self.selectors[0] == 'value':
            self.selectors = self.selectors[1:]

    def extract(self, data):
        return extract_path(data, self.selectors)

    def __call__(self, data):
        return self.extract(self, data)
