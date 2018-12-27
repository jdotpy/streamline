from importlib import import_module
import sys

def _import_obj(path):
    current_dir = os.path.abspath('.')
    if current_dir not in os.path:
        sys.path.append(current_dir)
    module_path, handler_name = path.rsplit('.', 1)
    module = import_module(module_path)
    for attr in module_path.split('.')[1:]:
        module = getattr(module, attr)
    handler_obj = getattr(module, handler_name)
    return handler_obj

