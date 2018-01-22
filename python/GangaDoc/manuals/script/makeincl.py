#! /usr/bin/env python
import sys

f = file('Makefile.incl','w')

tmpl = """Makefile-%(doc)s: $(wildcard src/%(doc)s/*)
	@echo Creating Makefile-%(doc)s
	script/make-doc.py %(doc)s
include Makefile-%(doc)s
"""

for i in sys.argv[1:]:
	print >>f, tmpl % {'doc':i}
## 	print >>f,'Makefile-%s: $(wildcard ganga-%s/*)'%(i,i)
## 	print >>f,'	@echo Creating Makefile-%s'%(i)			
## 	print >>f,'	script/make-doc.py %s'%i 			
## 	print >>f,'include Makefile-%s'%i
## 	print >>f
