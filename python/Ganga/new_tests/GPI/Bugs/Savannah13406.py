import os
jobs.remove()
j = Job()
assert(os.path.exists(j.inputdir + '/../..'))
jobs.remove()
# Check is repository/Local or repository/Remote still exists
assert(os.path.exists(os.path.abspath(j.inputdir + '/../..')))
