#!/bin/bash

# Lint and autopep8 locally. Lifted from the remote workflows files for
# testing local modifications before submission

set -e
if ! command -v flake8 &> /dev/null; then
   echo "Error: flake8 is not installed. Install with: pip install flake8"
   exit 1
fi

if ! command -v autopep8 &> /dev/null; then
   echo "Error: autopep8 is not installed. Install with: pip install autopep8"
   exit 1
fi

# Get changed Python files
changed_files=$(git diff --name-only HEAD~1...HEAD | grep '\.py$' || true)

if [ -z "$changed_files" ]; then
   echo "No Python files to lint"
   exit 0
fi

file_count=$(echo $changed_files | wc -w)
echo "Checking $file_count files"

# Run linting
files_to_fix=""
for file in $changed_files; do
   if [[ -f "$file" ]]; then
       linting_output=$(flake8 --count --exit-zero "$file")
       count=$(echo "$linting_output" | tail -1)
       if [[ $count -gt 0 ]]; then
           files_to_fix="$files_to_fix $file"
           echo "$linting_output"
       fi
   fi
done

# If linting failed, ask user about autofix
if [ -n "$files_to_fix" ]; then
   echo -n "Linting failed. Run autopep8 fixes? (y/n): "
   read -r response
   if [[ "$response" =~ ^[Yy]$ ]]; then
       autopep8 --in-place --aggressive --aggressive $files_to_fix
       echo "Fixes applied"
   fi
else
   echo "Linting passed!"
fi
