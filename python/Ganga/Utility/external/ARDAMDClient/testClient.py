#!/usr/bin/env python
from __future__ import absolute_import
#!/usr/bin/env python
from . import mdstandalone
from . import mdinterface
from . import mdclient
import time
import math

# client=mdstandalone.MDStandalone('/tmp/')
client = mdclient.MDClient(host='gangamd.cern.ch', port=9922, login='asaroka')
client.requireSSL("/tmp/x509up_u8032", "/tmp/x509up_u8032")

try:
    print "Creating directory /pytest ..."
    client.createDir("/pytest")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Adding entries in bulk..."
    client.addEntries(["/pytest/a", "/pytest/b", "/pytest/c"])
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Adding attribute..."
    client.addAttr("/pytest", "events", "int")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Adding attribute..."
    client.addAttr("/pytest", "eventGen", "varchar(20)")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Adding attribute..."
    client.addAttr("/pytest", "sssin", "float")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Adding attribute..."
    client.addAttr("/pytest", "l1", "int")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    attributes, types = client.listAttr("/pytest/t0")
    print attributes
    print types
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    for i in range(0, 10):
        client.addEntry("/pytest/t" + str(i),
                        ['events', 'eventGen', 'sssin', 'l1'],
                        [str(i * 100), 'myGen', str(math.sin(float(i))), str(i % 2)])
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Getting all attributes..."
    client.getattr('/pytest', ['eventGen', 'sssin', 'events'])
    while not client.eot():
        file, values = client.getEntry()
        print "->", file, values
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Creating directory /pytest/testdir ..."
    client.createDir("/pytest/testdir")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Listing entries..."
    client.listEntries('/pytest')
    while not client.eot():
        file, type = client.getEntry()
        print "->", file, type[0]
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Creating directory /pylock ..."
    client.createDir("/pylock")
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Adding attribute id..."
    client.addAttr("/pylock", "id", "int")
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Creating LOCK table..."
    client.addEntry("/pylock/lock", ['id'], [4711])
    client.addEntry("/pylock/lock2", ['id'], [100])
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Selecting attributes"
    client.selectAttr(
        ['/pytest:eventGen', '/pytest:sssin', '/pytest:events'], '/pytest:FILE="t1"')
#    client.selectAttr(['/pytest:eventGen', '/pytest:sssin', '/pytest:events'], '/pytest:events = 100')
    while not client.eot():
        values = client.getSelectAttrEntry()
        print "selcted ->", values
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Updating attributes"
    client.updateAttr(
        '/pytest', ["sssin 'sssin + 1'"], '/pytest:events = /pylock:id')
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Setting env to 42..."
    client.setAttr('/pytest/?', ['events'], [42])
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Getting all attributes..."
    client.getattr('/pytest', ['eventGen', 'sssin', 'events'])
    while not client.eot():
        file, values = client.getEntry()
        print "->", file, values
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Removing directory /pylock/testdir ..."
    client.removeDir("/pytest/testdir")
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Removing entries"
    client.rm("/pytest/*")
except mdinterface.CommandException as ex:
    print "Error:", ex


try:
    print "Removing entries"
    client.rm("/pylock/*")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Removing attribute..."
    client.removeAttr("/pytest", "events")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Removing attribute..."
    client.removeAttr("/pytest", "sssin")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Removing attribute..."
    client.removeAttr("/pytest", "eventGen")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Removing attribute..."
    client.removeAttr("/pytest", "l1")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Removing attribute..."
    client.removeAttr("/pylock", "id")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Creating sequence..."
    client.sequenceCreate("seq", "/pytest")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Getting next from sequence..."
    print client.sequenceNext("/pytest/seq")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Getting next from sequence..."
    print client.sequenceNext("/pytest/seq")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Removing sequence..."
    client.sequenceRemove("/pytest/seq")
except mdinterface.CommandException as ex:
    print "Error:", ex

# try:
#    print "Removing directory /pylock/testdir ..."
#    client.removeDir("/pytest/testdir")
# except mdinterface.CommandException as ex:
#    print "Error:", ex


try:
    print "Removing directory /pylock..."
    client.removeDir("/pytest")
except mdinterface.CommandException as ex:
    print "Error:", ex

try:
    print "Removing directory /pylock..."
    client.removeDir("/pylock")
except mdinterface.CommandException as ex:
    print "Error:", ex
