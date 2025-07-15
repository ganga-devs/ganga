from typing import Any, Callable
import warnings
from datetime import date

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
    by deprecation management tooling in the codebase.

    Usage:

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
            raise RuntimeError("This function or class is not marked as deprecated. Use @deprecation_dealine like:\n "
                               + """
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
