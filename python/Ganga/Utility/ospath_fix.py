# on some python 2.3 distributions the os.path.realpath() is broken and the symlinks are
# resolved wrt to the current working directory

import os.path

def realpath(filename):
    """Return the canonical path of the specified filename, eliminating any
symbolic links encountered in the path."""
    if os.path.isabs(filename):
        bits = ['/'] + filename.split('/')[1:]
    else:
        bits = [''] + filename.split('/')

    for i in range(2, len(bits)+1):
        component = os.path.join(*bits[0:i])
        # Resolve symbolic links.
        if os.path.islink(component):
            resolved = _resolve_link(component)
            if resolved is None:
                # Infinite loop -- return original component + rest of the path
                return os.path.abspath(os.path.join(*([component] + bits[i:])))
            else:
                newpath = os.path.join(*([resolved] + bits[i:]))
                return realpath(newpath)

    return os.path.abspath(filename)


def _resolve_link(path):
    """Internal helper function.  Takes a path and follows symlinks
    until we either arrive at something that isn't a symlink, or
    encounter a path we've seen before (meaning that there's a loop).
    """
    paths_seen = []
    while os.path.islink(path):
        if path in paths_seen:
            # Already seen this path, so we must have a symlink loop
            return None
        paths_seen.append(path)
        # Resolve where the link points to
        resolved = os.readlink(path)
        if not os.path.isabs(resolved):
            dir = os.path.dirname(path)
            path = os.path.normpath(os.path.join(dir, resolved))
        else:
            path = os.path.normpath(resolved)
    return path

os.path.realpath = realpath
os.path._resolve_link = _resolve_link

