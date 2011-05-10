#!/usr/bin/env python

import xml.dom.minidom, sys
from xml.dom.minidom import Node

#from optparse import OptionParser

#parser = OptionParser()
#parser.add_option("-s", "--sframe", dest="s_xml", \
#                  help="SFrame XML optionfile", metavar="FILE")
#parser.add_option("-p", "--pool", dest="p_xml", \
#                  help="Pool file catalog", metavar="FILE")

#(options, args) = parser.parse_args()

#if not options.s_xml:
#  print "ERROR! no SFrame xml given"
#  sys.exit(666)
#if not options.p_xml:
#  print "ERROR! no Pool file given"
#  sys.exit(666)


if len(sys.argv) == 3:
  #s_doc = xml.dom.minidom.parse(options.s_xml)
  #p_doc = xml.dom.minidom.parse(options.p_xml) 

  s_doc = xml.dom.minidom.parse(sys.argv[1])
  p_doc = xml.dom.minidom.parse(sys.argv[2])   

  #  fname = options.s_xml.split('/')[-1]
  fname = sys.argv[1].split('/')[-1]
   
  ofile =open("ganga_"+fname,'w')

  lumi = ""

  #remove inputfiles from the SFrame xml
  for node in s_doc.getElementsByTagName("In"):
    lumi = node.getAttribute("Lumi")
    print "Getting Luminosity: %s" % lumi
    print "Removing %s" % node.getAttribute("FileName")
    node.parentNode.removeChild(node)
    node.unlink()
  
  #get the InputData node 
  c_node_list = s_doc.getElementsByTagName("InputData")

  toadd_list = []

  # now add new input files
  for node in p_doc.getElementsByTagName("pfn"):
          toadd_list.append(node)

  #some modifications...
  for parent_node in c_node_list:

    first_node = parent_node.firstChild
    
    for node in toadd_list:
      new_node = s_doc.createElement("In")
      new_node.setAttribute("FileName", node.getAttribute("name"))
      new_node.setAttribute("Lumi",lumi)
      parent_node.insertBefore(new_node, first_node)

  s_doc.writexml(ofile)
  ofile.close()
else:
  print "Wrong number of arguments: %s" % `sys.argv`
  sys.exit(666)

  
sys.exit(0)
