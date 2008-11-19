#!/usr/bin/env python

import xml.dom.minidom, sys
from xml.dom.minidom import Node

if len(sys.argv) == 3:


  s_doc = xml.dom.minidom.parse(sys.argv[1])
  i_fil = open(sys.argv[2],'r')   

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
  for line in i_fil:
          toadd_list.append(line.strip())

  #some modifications...
  for parent_node in c_node_list:

    first_node = parent_node.firstChild
    
    for node in toadd_list:
      new_node = s_doc.createElement("In")
      new_node.setAttribute("FileName", node)
      new_node.setAttribute("Lumi",lumi)
      parent_node.insertBefore(new_node, first_node)

  s_doc.writexml(ofile)
  ofile.close()
else:
  print "Wrong number of arguments: %s" % `sys.argv`
  sys.exit(666)

  
sys.exit(0)
