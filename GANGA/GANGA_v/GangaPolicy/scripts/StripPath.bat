@echo off

set tmpfile="%TEMP%\StripPath_tmpsetup.bat"

python %GANGAPOLICYROOT%\scripts\PathStripper.py --shell=bat --output=%tmpfile% -e PATH -e PYTHONPATH -e JOBOPTSEARCHPATH -e HPATH --root --dirac --ganga

call %tmpfile%


if exist %tmpfile% del %tmpfile%
set tmpfile=

