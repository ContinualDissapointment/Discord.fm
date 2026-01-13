import os

# Find and include DBus-1.0.typelib for pystray's notify_dbus support
datas = []
hiddenimports = []
binaries = []

# Common locations for GI typelibs
typelib_paths = [
    '/usr/lib/x86_64-linux-gnu/girepository-1.0',
    '/usr/lib64/girepository-1.0',
    '/usr/lib/girepository-1.0',
]

for path in typelib_paths:
    dbus_typelib = os.path.join(path, 'DBus-1.0.typelib')
    if os.path.exists(dbus_typelib):
        datas.append((dbus_typelib, 'gi_typelibs'))
        break
