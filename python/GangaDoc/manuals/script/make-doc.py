#! /usr/bin/env python
import sys

doc = sys.argv[1]

f = file('Makefile-%s'%doc,'w')

tmpl = """
%(doc)sSRC := $(subst CVS,,$(wildcard src/%(doc)s/*))
%(doc)sTEX := $(filter %(TEXfilter)s,$(%(doc)sSRC))
%(doc)sRST := $(filter %(RSTfilter)s,$(%(doc)sSRC))
%(doc)sFIG := $(foreach fig,$(FIGS),$(wildcard src/%(doc)s/*.$(fig)))
.PHONY %(doc)s: %(doc)s.html %(doc)s.pdf %(doc)s.ps
.PHONY %(doc)s.html: html/%(doc)s/%(doc)s.html html/%(doc)s/%(doc)s.css
.PHONY %(doc)s.pdf: paper-$(PAPER)/%(doc)s.pdf
.PHONY %(doc)s.ps: paper-$(PAPER)/%(doc)s.ps
ALLCSSFILES += html/%(doc)s/%(doc)s.css
ALLHTML += %(doc)s.html
ALLPDF += %(doc)s.pdf
ALLPS += %(doc)s.ps
f_rst2html = $(foreach a,$(1),$(MKRSTHTML) $(a) $(subst src,html,$(subst .rst,.html,$(a)));)

html/%(doc)s/%(doc)s.html: $(%(doc)sSRC)
ifneq ($(%(doc)sTEX),)
	@echo $(%(doc)sTEX)
	mkdir -p html/%(doc)s
	$(MKHTML) --dir html/%(doc)s $(MKHTMLOPT) $(%(doc)sTEX)
endif
ifneq ($(%(doc)sRST),)
	mkdir -p html/%(doc)s
	$(call f_rst2html,$(%(doc)sRST))
endif
ifneq ($(strip $(%(doc)sFIG)),)
	cp  $(%(doc)sFIG) html/%(doc)s
	script/fiximgpath.py %(doc)s
endif
	rm -f html/%(doc)s/*.pl

paper-$(PAPER)/%(doc)s.pdf: $(%(doc)sSRC)
	mkdir -p paper-$(PAPER)
ifneq ($(strip $(%(doc)sFIG)),)
	cp $(%(doc)sFIG) paper-$(PAPER)
endif
	cd paper-$(PAPER) && $(MKPDF) ../src/%(doc)s/%(doc)s.tex

paper-$(PAPER)/%(doc)s.dvi: $(%(doc)sSRC)
	mkdir -p paper-$(PAPER)
ifneq ($(strip $(%(doc)sFIG)),)
	cp $(%(doc)sFIG) paper-$(PAPER)
endif
	cd paper-$(PAPER) && $(MKDVI) ../src/%(doc)s/%(doc)s.tex

paper-$(PAPER)/%(doc)s.ps: $(%(doc)sSRC)
	mkdir -p paper-$(PAPER)
ifneq ($(strip $(%(doc)sFIG)),)
	cp $(%(doc)sFIG) paper-$(PAPER)
endif
	cd paper-$(PAPER) && $(MKPS) ../src/%(doc)s/%(doc)s.tex
"""

print >> f, tmpl%{'doc':doc, 'TEXfilter':'%.tex', 'RSTfilter':'%.rst'}
