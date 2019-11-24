from importlib import import_module
import json
import sys
import os

def strip_nulls(source):
    return {key: value for key, value in source.items() if value is not None}

def get_file_io(name, write=False):
    # Test for file-like objects we can use first
    if hasattr(name, 'write') and write:
        return name
    elif hasattr(name, 'read') and not write:
        return name

    # Test for stdin/stdout special case
    if name == '-' and write:
        return sys.stdout
    elif name == '-' and not write:
        return sys.stdin

    # Assume this is a file
    return open(name, 'w' if write else 'r', 1)

def get_env_as(var, constructor, default=0):
    if var not in os.environ:
        return default
    try:
        return constructor(os.environ[var])
    except Exception as e:
        return default

def import_obj(path):
    # Ensure the current directory is on the path
    current_dir = os.path.abspath('.')
    if current_dir not in sys.path:
        sys.path.append(current_dir)

    # Split the object name and the module path
    module_path, handler_name = path.rsplit('.', 1)
    module = import_module(module_path)
    return getattr(module, handler_name)
    for attr in module_path.split('.')[1:]:
        module = getattr(module, attr)
    handler_obj = getattr(module, handler_name)
    if handler_obj is None:
        raise ValueError('Unable to import object: {}'.format(path))
    return handler_obj

def inject_module(module_name, namespace):
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as e:
        print('This plugin requires module: "{}"\nTry "pip install {}"'.format(module_name, module_name))
        sys.exit(6)
    sys.modules[module_name] = module
    namespace[module_name] = module

def truthy(entries):
    return [entry for entry in entries if entry]

def arg_help(description, example=None):
    def decorator(streamer):
        if description is not None:
            streamer._arg_description = description
        if example is not None:
            streamer._arg_example = example
        return streamer
    return decorator

def force_string(value):
    if isinstance(value, str):
        return value
    try:
        value = json.dumps(value)
    except Exception as e:
        value = str(value)
    return value
