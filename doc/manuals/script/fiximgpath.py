#!/usr/bin/env python

import sys
import os.path

doc = sys.argv[1]
filename = 'html/%s/%s.html'%(doc,doc)

path = os.path.abspath('.')+'/ganga-%s/'%(doc)

lines =  file(filename).readlines()
of = file(filename,'w')
for l in lines:
    l = l.replace(path,'')
    print >>of, l,

