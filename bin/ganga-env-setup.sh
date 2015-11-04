#!/bin/bash

# TODO: check if everything's there already

# setup the PYTHONPATH from the helper script
BINDIR=$( dirname "${BASH_SOURCE[0]}" )
BASEDIR=$( dirname "${BINDIR}" )
export PYTHONPATH=$PYTHONPATH:$BASEDIR/python
export PYTHONPATH=$PYTHONPATH:`$BINDIR/ganga-external-env.py`
