from importlib import import_module
import sys
import os

def import_obj(path):
    current_dir = os.path.abspath('.')
    if current_dir not in sys.path:
        sys.path.append(current_dir)
    module_path, handler_name = path.rsplit('.', 1)
    module = import_module(module_path)
    for attr in module_path.split('.')[1:]:
        module = getattr(module, attr)
    handler_obj = getattr(module, handler_name)
    return handler_obj

def inject_module(module_name, namespace):
    try:
        module = import_module(module_name)
    except ModuleNotFoundError as e:
        print('This plugin requires module: "{}"\nTry "pip install {}"'.format(module_name, module_name))
        sys.exit(6)
    sys.modules[module_name] = module
    namespace[module_name] = module
