from typing_extensions import deprecated as deprecated
import warnings
from datetime import date
import sys
from typing import Optional, List

DEPRECATION_MESSAGE_TEMPLATE = "Deprecated: {reason} | Removal scheduled: {expires_on} (v{version})"

# Global state to track deprecation warning configuration
_deprecation_warnings_enabled = False
_original_warning_filters = None

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
        def g():
            pass
    """
    def decorator(obj):
        # ensure the object is marked as deprecated
        deprecated_msg = getattr(obj, "__deprecated__", None)
        if not deprecated_msg:
            raise RuntimeError("This function or class is not marked as deprecated. "
                               "Apply @deprecated before @deprecation_deadline.")

        err = DEPRECATION_MESSAGE_TEMPLATE.format(
            reason=deprecated_msg,
            expires_on=expires_on,
            version=version
        )
        obj.__deprecated__ = err

        # update the closure cell for 'msg'
        closure = getattr(obj, "__closure__", None)
        code = getattr(obj, "__code__", None)
        if closure and code:
            free_var_names = code.co_freevars
            try:
                msg_index = free_var_names.index('msg')
                closure[msg_index].cell_contents = err
            except (ValueError, IndexError, AttributeError):
                # `msg` not found in closure or cannot update cell; ignore
                pass

        return obj
    return decorator

def enable_deprecation_warnings(force: bool = False, show_source: bool = True, 
                               filter_level: str = 'default') -> bool:
    """
    Enable deprecation warnings with robust configuration.
    
    Args:
        force: If True, force re-enable even if already enabled
        show_source: If True, include source location in warning messages
        filter_level: Warning filter level ('default', 'always', 'module', 'once', 'ignore')
    
    Returns:
        bool: True if warnings were successfully enabled, False otherwise
    
    Raises:
        ValueError: If filter_level is invalid
    """
    global _deprecation_warnings_enabled, _original_warning_filters
    
    try:
        # Validate filter level
        valid_levels = ['default', 'always', 'module', 'once', 'ignore']
        if filter_level not in valid_levels:
            raise ValueError(f"Invalid filter_level '{filter_level}'. Must be one of: {valid_levels}")
        
        # Check if already enabled
        if _deprecation_warnings_enabled and not force:
            return True
        
        # Store original warning filters if not already stored
        if _original_warning_filters is None:
            _original_warning_filters = warnings.filters[:]
        
        # Configure deprecation warnings
        warnings.simplefilter(filter_level, DeprecationWarning)
        
        # Enable source location in warnings if requested
        if show_source:
            warnings.formatwarning = lambda message, category, filename, lineno, line=None: \
                f"{filename}:{lineno}: {category.__name__}: {message}\n"
        
        _deprecation_warnings_enabled = True
        
        # Verify the configuration was applied
        current_filters = warnings.filters
        deprecation_filters = [f for f in current_filters if f[2] == DeprecationWarning]
        
        if not deprecation_filters:
            warnings.warn("Failed to properly configure deprecation warnings", RuntimeWarning)
            return False
            
        return True
        
    except Exception as e:
        # Log the error but don't raise to maintain backward compatibility
        print(f"Warning: Failed to enable deprecation warnings: {e}", file=sys.stderr)
        return False

def disable_deprecation_warnings(restore_original: bool = True) -> bool:
    """
    Disable deprecation warnings with option to restore original configuration.
    
    Args:
        restore_original: If True, restore the original warning filters that were active
                         before enable_deprecation_warnings was called
    
    Returns:
        bool: True if warnings were successfully disabled, False otherwise
    """
    global _deprecation_warnings_enabled, _original_warning_filters
    
    try:
        if restore_original and _original_warning_filters is not None:
            # Restore original warning filters
            warnings.filters[:] = _original_warning_filters
            _original_warning_filters = None
        else:
            # Simply ignore deprecation warnings
            warnings.simplefilter('ignore', DeprecationWarning)
        
        _deprecation_warnings_enabled = False
        return True
        
    except Exception as e:
        print(f"Warning: Failed to disable deprecation warnings: {e}", file=sys.stderr)
        return False

def is_deprecation_warnings_enabled() -> bool:
    """
    Check if deprecation warnings are currently enabled.
    
    Returns:
        bool: True if deprecation warnings are enabled, False otherwise
    """
    global _deprecation_warnings_enabled
    return _deprecation_warnings_enabled

def get_deprecation_warning_filters() -> List[tuple]:
    """
    Get current deprecation warning filters.
    
    Returns:
        List of tuples representing current deprecation warning filters
    """
    return [f for f in warnings.filters if f[2] == DeprecationWarning]
