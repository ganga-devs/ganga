PAPER=a4

TOOLSDIR=tools
PYTHON=python
DVIPS=dvips -N0 -t $(PAPER)
PWD=$(shell pwd)
GANGA_HOME=/afs/cern.ch/sw/ganga/external
DOCUTIL_HOME=$(GANGA_HOME)/docutils/LATEST/slc3_gcc323
DOCUTIL_BIN=$(DOCUTIL_HOME)/bin
DOCUTIL_LIB=$(DOCUTIL_HOME)/lib/python2.3/site-packages
PYTHONLIB=$(GANGA_HOME)/Python/2.3.4/slc3_gcc323/lib
TEXINPUTS= .:$(PWD)/commontex:

PYTHONDOCS="See <i><a href=\"about.html\">About this document...</a></i> for information on suggesting changes."

MKHOWTO=   TEXINPUTS=$(TEXINPUTS) $(PWD)/tools/mkhowto
MKDVI=	   $(MKHOWTO) --paper=$(PAPER) --dvi
MKHTML=	   $(MKHOWTO) -D --html --about html/stdabout.dat \
		--iconserver ../icons --favicon ../icons/pyfav.gif \
		--address $(PYTHONDOCS) --up-link 'http://cern.ch/ganga/' \
		--up-title "G<small>ANGA</small> Home" \
		--global-module-index "../modindex.html" --dvips-safe
MKHTMLOPT= --numeric --split=4
MKPDF=	   $(MKHOWTO) --paper=$(PAPER) --pdf
MKPS=	   $(MKHOWTO) --paper=$(PAPER) --ps
MKRSTHTML= LD_LIBRARY_PATH=$(PYTHONLIB):$(LD_LIBRARY_PATH) \
           PYTHONPATH=$(DOCUTIL_LIB):$(PYTHONPATH) \
           $(DOCUTIL_BIN)/rst2html.py \
           --no-xml-declaration -g -d -t -s --cloak-email-addresses

GangaWorkbook:
MKHTMLOPT= --split=4

HTMLBASE=  file:`pwd`

FIGS= gif jpg png eps

#%.ps: %.dvi
#	$(DVIPS) -o $@ $<

all: Makefile.incl html

# generic

GANGADOC := $(subst src/,,$(wildcard src/*))
GANGADOC := $(subst CVS,,$(GANGADOC))

Makefile.incl: $(wildcard src/*)
	script/makeincl.py $(GANGADOC)


include Makefile.incl

$(ALLCSSFILES): html/style.css
	cp $< $@

.PHONY html: $(ALLHTML)
.PHONY pdf: $(ALLPDF)
.PHONY ps: $(ALLPS)

.PHONY clean: 
clean:
	@echo Clear all:
	rm -rf $(GANGADOC:%=html/%)
	rm -f paper-*/*
	rm -f Makefile-*
	rm -f Makefile.incl
	rm -f *.l2h
	rm -f *.l2h~

testtest:= aaa

.PHONY test: 
test: 
	echo test
ifdef testtest
	echo testtest
endif
