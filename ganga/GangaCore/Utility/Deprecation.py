import warnings
import ast
import inspect
import re
import jedi
import os.path
import json

from typing import Any, Callable
from datetime import date
from pathlib import Path
from typing_extensions import deprecated
from collections import defaultdict, namedtuple


DEPRECATION_MESSAGE_TEMPLATE = "Deprecated: {reason} | Removal scheduled: {expires_on} (v{version})"

_deprecation_filter_enable, _deprecation_filter_disable = None, None


def _update_closed_over_var(f: Callable, var_name: str, var_val: Any):
    """Update a closed over variable"""

    closure = getattr(f, "__closure__", None)
    code = getattr(f, "__code__", None)
    if closure and code:
        free_var_names = code.co_freevars

        var_index = free_var_names.index(var_name)
        closure[var_index].cell_contents = var_val
        return True


def deprecation_deadline(*, expires_on: date, version: str):
    """
    Decorator to indicate the date and version of removal for a deprecated object.

    This decorator should be applied on top of the `deprecated` decorator
    (as defined in PEP 702) to set deadlines for compulsory deprecation.
    The information added is included in error messages and is also extracted
    by deprecation management tooling in the codebase. The deprecation management
    tooling depends on the patterns described below to track the deprecations.

    Usage:
        from warnings import deprecated
        from GangaCore.Utility.Deprecation import deprecation_deadline

        @deprecation_deadline(expires_on=date(2025, 11, 12), version="1.29")
        @deprecated("use class B instead")
        class A:
            pass

        @deprecation_deadline(expires_on=date(2026, 11, 12), version="1.30")
        @deprecated("Use function f instead")
        def g(): pass
    """
    def decorator(obj):
        # ensure the object is marked as deprecated
        deprecated_msg = getattr(obj, "__deprecated__", None)
        if not deprecated_msg:
            raise RuntimeError("This function or class is not marked as deprecated. Use @deprecation_deadline like:\n "
                               + """
        from warnings import deprecated
        from GangaCore.Utilitty.Deprecation import deprecation_deadline

        @deprecation_deadline(expires_on=date(2026, 11, 12), version="1.30")
        @deprecated("Use function f instead")
        def g(): pass""")

        err = DEPRECATION_MESSAGE_TEMPLATE.format(
            reason=deprecated_msg,
            expires_on=expires_on,
            version=version
        )
        obj.__deprecated__ = err

        if isinstance(obj, type):
            _update_closed_over_var(obj.__new__, "msg", err)
            _update_closed_over_var(obj.__init_subclass__, "msg", err)
        elif callable(obj):
            _update_closed_over_var(obj, "msg", err)
        return obj

    return decorator


def _get_filter_for_deprecation_warning():
    for f in warnings.filters:
        if f[2] == DeprecationWarning:
            return f
    return ()


def enable_deprecation_warnings():
    global _deprecation_filter_enable, _deprecation_filter_disable

    f = _get_filter_for_deprecation_warning()

    if f == _deprecation_filter_enable:
        return

    if f == _deprecation_filter_disable:
        warnings.filters.remove(_deprecation_filter_disable)
        _deprecation_filter_disable = None

    warnings.simplefilter('always', DeprecationWarning)
    _deprecation_filter_enable = warnings.filters[0]


def disable_deprecation_warnings():
    global _deprecation_filter_enable, _deprecation_filter_disable

    f = _get_filter_for_deprecation_warning()

    if f == _deprecation_filter_disable:
        return

    if f == _deprecation_filter_enable:
        warnings.filters.remove(_deprecation_filter_enable)
        _deprecation_filter_enable = None

    warnings.simplefilter('ignore', DeprecationWarning)
    _deprecation_filter_disable = warnings.filters[0]


def _locate_obj_position(o):
    """
    Locate the file path, line number and column number of an imported object
    Used later to locate all uses of the `deprecated` decorator
    """
    o_name = o.__name__
    o_path = inspect.getsourcefile(o)
    if o_path is None:
        raise ValueError(f"Cannot find source file of {o}")
    o_lineno = inspect.getsourcelines(o)[1]

    with open(o_path, "r") as f:
        f_lines = f.readlines()

    o_line = f_lines[o_lineno - 1]
    o_col = None

    if inspect.isfunction(o):
        mtch = re.search(r'\bdef\s+(' + re.escape(o_name) + r')\b', o_line)
        if mtch:
            o_col = mtch.start(1)
    elif inspect.isclass(o):
        mtch = re.search(r'\bclass\s+(' + re.escape(o_name) + r')\b', o_line)
        if mtch:
            o_col = mtch.start(1)
    else:
        raise ValueError(f"{o} must be a function or class")

    if o_col is None:
        raise LookupError(f"cannot get column of object {o} definition")
    return [o_path, o_lineno, o_col]


def _is_path_under_dir(child_path, parent_path):
    """
    Checks if child_path is under the directory parent_path
    Used to later filter out references that are not in our project
    """
    child = Path(child_path).resolve()
    parent = Path(parent_path).resolve()

    return parent in child.parents

# Files where the utilities are defined and tested. To make sure
# We don't index them


def find_deprecated_refs(root: Path | str ='.') -> set [str]:
    """
    Locate all the files that use the @deprecated decorator throughout the codebase.
    `root` is the root directory of the project.
    """

    root = str(Path(root).expanduser().resolve())

    import jedi

    [fpath, line, col] = _locate_obj_position(deprecated)

    proj = jedi.Project(path=root)
    script = jedi.Script(path=fpath, project=proj)

    refs = script.get_references(line=line, column=col, scope='project')

    # Extract from the references the relative path of all the files that have
    # a references to @deprecated and that are inside our codebase
    pths = set(
        map(lambda r: os.path.relpath(r.module_path, root),
            # .. and are inside our codebase.
            filter(lambda r: _is_path_under_dir(r.module_path, root), refs))
    )

    def_and_test_files = set(map(lambda p: os.path.relpath(p, root), [
        # The current file ...
        __file__,
        # ... and the test file
        str((Path(__file__) / '../../test/Unit/Utility/TestDeprecationDecorators.py').resolve())
        # must not be indexed ...
    ]))

    # ... so we remove them
    pths = pths - def_and_test_files
    return pths


class DeprecationNotice(namedtuple('DeprecationNotice',
                                   ['name', 'type', 'file_path', 'lineno', 'reason', 'deadline', 'last_version']
                                   )):
    """A class for deprecation notice found in code.

    Attributes:
        name (str): name of the object being deprecated
        type (str): Type of object, either function or class
        file_path (str): Path to the file defining the object, relative to project root
        lineno (int): Line number where deprecation was found
        reason (str): Explanation of why it's deprecated
        deadline (str): When the deprecation takes effect
        last_version (str): Last version that will support this feature
    """
    __slots__ = ()


class DeprecationNodeFinder (ast.NodeVisitor):
    """A NodeVisitor to locate all the deprecation notices and parse
    them into DeprecationNotice objects
    """

    def __init__(self, file_path):
        self.deprecation_instances = []
        self.file_path = file_path

    def visit_FunctionDef(self, node):
        self._check_decorators(node, "function")

    def visit_AsyncFunctionDef(self, node):
        self._check_decorators(node, "function")

    def visit_ClassDef(self, node):
        self._check_decorators(node, "class")

    def _check_decorators(self, node, o_type):
        deprecation_notice_dec, deprecation_deadline_dec = None, None

        for d in node.decorator_list:
            if isinstance(d, ast.Call) and isinstance(d.func, ast.Name):
                name = d.func.id
                if name == "deprecated":
                    deprecation_notice_dec = d
                elif name == "deprecation_deadline":
                    deprecation_deadline_dec = d

        if deprecation_notice_dec is not None:
            self.generic_visit(deprecation_notice_dec)
            reason = deprecation_notice_dec.args[0].value
            assert reason is not None, "reason should not be None"

            self.generic_visit(node)
            new_deprecation = {
                "reason": reason,
                "name": node.name,
                "type": o_type,
                "file_path": self.file_path,
                "lineno": node.lineno,
            }

            if deprecation_deadline_dec is not None:
                self.generic_visit(deprecation_deadline_dec)
                try:
                    kw = {k: v for k, v in (
                        # Here we use `eval` to extract the arguments because the arguments are either
                        # string literals or use the `date` function imported in this module. If the user
                        # respect the format defined in the documentation of the deprecation decorators,
                        # this should extract the correct information.
                        map(lambda x: (x.arg, eval(ast.unparse(x.value))),
                            deprecation_deadline_dec.keywords))
                          }
                except Exception as e:
                    raise ValueError(f"Could not parse decorator args in {self.file_path}:{node.lineno}: {e}")

                new_deprecation["deadline"] = kw["expires_on"]
                new_deprecation["last_version"] = kw["version"]
            else:
                new_deprecation["deadline"] = None
                new_deprecation["last_version"] = None

            self.deprecation_instances.append(DeprecationNotice(**new_deprecation))


def extract_deprecation_info(deprecation_refs: set [str], root: Path | str ='.') -> list [DeprecationNotice]:
    """Extract the deprecation info from the locations provided by find_deprecated_refs.
    Returns a list of DeprecationNotice objects.
    """

    root = str(Path(root).expanduser().resolve())
    deprecations = []

    for rpath in deprecation_refs:
        pth = os.path.join(root, rpath)
        with open(pth) as f:
            t = ast.parse(f.read())

        f = DeprecationNodeFinder(rpath)
        f.visit(t)
        deprecations.extend(f.deprecation_instances)

    return deprecations


def _convert_notice(notice):
    data = notice._asdict()
    if isinstance(data['deadline'], date):
        data['deadline'] = data['deadline'].isoformat()
    return data


def serialize_deprecations(deprecations: list [DeprecationNotice]) -> str:
    return json.dumps([_convert_notice(notice) for notice in deprecations])


def deserialize_deprecations(json_str: str) -> list [DeprecationNotice]:
    data = json.loads(json_str)
    notices = []
    for item in data:
        if item.get('deadline') and isinstance(item['deadline'], str):
            try:
                item['deadline'] = date.fromisoformat(item['deadline'])
            except BaseException:
                pass

        notices.append(DeprecationNotice(**item))
    return notices


def generate_deprecation_json_report(root : Path | str ='./') -> str:
    """Generate a JSON string of all deprecations notices in the codebase
    """
    rr = find_deprecated_refs(root)
    ii = extract_deprecation_info(rr, root)
    return serialize_deprecations(ii)
