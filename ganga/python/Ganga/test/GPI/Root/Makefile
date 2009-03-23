
# Simple "GNU Make" Makefile
# for Linux. Uses implicit rules

#compiler settings
CXX           = g++
CXXFLAGS      = -O -Wall -fPIC
LD            = g++
LDFLAGS       = -O
SOFLAGS       = -shared

#find out about the installed ROOT
ROOTCINT     := $(ROOTSYS)/bin/rootcint
ROOTCONFIG   := $(ROOTSYS)/bin/root-config
ROOTCFLAGS   := $(shell $(ROOTCONFIG) --cflags)
ROOTLDFLAGS  := $(shell $(ROOTCONFIG) --ldflags)

#append to options
CXXFLAGS     += $(ROOTCFLAGS)
LDFLAGS      += $(ROOTLDFLAGS)

#source files
HEADERS		= Main.h MainDict.h

SOURCE          = Main.cpp MainDict.cpp

OBJS         	= Main.o MainDict.o

SHAREDLIB       = libMain.so

all:            $(SHAREDLIB)

$(SHAREDLIB):   MainDict.cpp $(OBJS)
		$(LD) $(LDFLAGS) $(SOFLAGS) $(OBJS) -o $(SHAREDLIB)

.PHONY clean:
clean:		
		@rm -f $(OBJS) MainDict.cpp MainDict.h $(SHAREDLIB)

###
MainDict.cpp: Main.h
		@rm -f MainDict.cpp MainDict.h
		$(ROOTCINT) MainDict.cpp -c Main.h


