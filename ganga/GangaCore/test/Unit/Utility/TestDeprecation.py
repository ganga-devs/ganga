import unittest
import pytest
from datetime import date
from pathlib import Path
from packaging.version import Version

from ganga.GangaCore import _gangaVersion

from ganga.GangaCore.Utility.Deprecation import (
    extract_deprecation_info,
    find_deprecated_refs
)

today = date.today ()
last_ganga_version = Version (_gangaVersion)
root = str ((Path (__file__) / '../../../../../').resolve ())

def deprecation_test_cases ():
    rr = find_deprecated_refs(root)
    dd = extract_deprecation_info(rr, root)
    return dd

@pytest.mark.parametrize ("dep_notice", deprecation_test_cases ())
def test_is_deprecated_object_used (dep_notice):
    pth = (Path (root) / dep_notice.file_path).resolve ()
    if dep_notice.deadline is not None:
        removal_date = date.isoformat (dep_notice.deadline)
        assert dep_notice.deadline > today, (
            f"use of {dep_notice.type} <{dep_notice.name}> at {pth}:{dep_notice.lineno} " +
            f"is deprecated, should have been removed since {removal_date}"
        )

    if dep_notice.last_version is not None:
        last_version = Version (dep_notice.last_version)
        assert last_version > last_ganga_version, (
            f"use of {dep_notice.type} <{dep_notice.name}> at {pth}:{dep_notice.lineno} " + 
            f"is deprecated in Ganga {last_ganga_version} and should have been removed " +
            f"in Ganga {last_version}"
        )
