from pathlib import Path, os, sys

def filepath(*args, **kwargs):
    name, file_type, data_path = args[0], 'data', 'sqlite3'
    if 'type' in list(kwargs.keys()): file_type = kwargs['type']
    if 'subpath' in list(kwargs.keys()): data_path = kwargs['subpath']
    if sys.platform == 'win32':
        if sys.version_info.major > 2 and sys.version_info.minor > 3:
            return os.sep.join((str(Path.home()), file_type, data_path, name))
        else:
            file_path = os.sep.join((str(Path.home()), file_type, data_path))
    # if sys.platform == 'linux-armv7l': file_drive, file_path = '', sep.join(('mnt', 'sdcard', file_type, data_path))
    if sys.platform in ('linux', 'linux2'):
        if sys.version_info.major > 2 and sys.version_info.minor > 3:
            if 'EXTERNAL_STORAGE' in os.environ.keys(): return os.sep.join((str(Path.home()), 'storage', 'external-1', file_type, data_path, name))
            return os.sep.join((str(Path.home()), file_type, data_path, name))
        else:
            place = 'shared'
            if 'ACTUAL_HOME' in os.environ.keys(): file_path = os.sep.join((str(Path.home()), file_type, data_path))
            elif ('EXTERNAL_STORAGE' in os.environ.keys()) and ('/' in os.environ['EXTERNAL_STORAGE']):
                place = 'external-1'
                file_path = os.sep.join((str(Path.home()), 'storage', place, file_type, data_path))
    return os.sep.join((file_path, name))
