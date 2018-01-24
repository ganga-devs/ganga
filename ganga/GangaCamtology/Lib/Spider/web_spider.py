#!/usr/bin/python

# A thread class for killing threads
# nicked from http://www.velocityreviews.com/forums/t330554-kill-a-thread-in-python.html
import sys, trace, threading
import urllib2, robotparser, urlparse, os, time

class GetHTMLThread(threading.Thread):
    """A subclass of threading.Thread, with a kill() method."""
    
    def __init__(self, link):
        threading.Thread.__init__(self)
        self.killed = False
        self._link = link
        self._html = ""
        self._complete = False

    def start(self):
        """Start the thread."""
        self.__run_backup = self.run
        self.run = self.__run # Force the Thread to install our trace.
        threading.Thread.start(self)

    def __run(self):
        """Hacked run function, which installs the
        trace."""
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

    def run(self):

        import urllib2, urlparse, os

        # retrieve the html
        try:
            urllib_file = urllib2.urlopen(self._link)
            self._html = urllib_file.read()
            urllib_file.close()
            
        except:
            print "Error opening link " + repr(self._link)
            self._html = ""

        self._complete = True
        
    def globaltrace(self, frame, why, arg):
        if why == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, why, arg):
        if self.killed:
            if why == 'line':
                raise SystemExit()
            return self.localtrace
        
    def kill(self):
        self.killed = True

# ------------------------------------------------                
def pullURLData( link ):

    # set up a thread to pull the link data
    thread = GetHTMLThread(link)
    thread.start()
    tout = 10 * 10

    while not thread._complete and tout > 0:
        time.sleep(0.1)
        tout -= 1

    if not thread.isAlive():
        # kill the thread
        thread.kill()

    data = thread._html
    return data


# ------------------------------------------------                
class ImageParser:

    _domain = ""
    _viewed = []
    _queued = []
    _images = []
    _newdomains = []
    _safedomains = []
    _newlinks = []
    _addfiles = []
    _currlink = ""
    _imgext = []
    _fileext = []
    
    def __init__( self, links, ext ):

        import os
        import urlparse
        
        self._domain = ''
        self._viewed = []
        self._queued = []
        self._images = []
        self._newimages = []
        self._newdomains = []
        self._safedomains = []
        self._addfiles = []
        self._imgext = []
        self._fileext = []
        
        # links list
        file = open(links)
        for url in file.readlines():
            self._queued.append( url.strip() )
        file.close()

        self._currlink = self._queued.pop(0)

        # setup the extensions
        ext_file = open(ext, "r")
        img = True
        for ln in ext_file.readlines():
            
            if ln.strip() == "--":
                img = False
                continue

            if ln.strip() == "":
                continue
            
            if img:
                self._imgext.append(ln.strip())
            else:
                self._fileext.append(ln.strip())

        # safe domains
        sd_file = open('safe_doms.txt', 'r')
        for sd in sd_file.readlines():
            self._safedomains.append(sd)
        sd_file.close()
        
        # image info
        f = open( "image_info.txt", "w")
        f.close()

        # ext info
        f = open( "ext_info.txt", "w")
        f.close()
        
    def Crawl( self ):
        "crawl the websites given the current condition"
        import urlparse, time, socket

        if os.environ['SPIDER_PAYLOAD'] != '':
            try:
                payload = __import__(os.path.basename(os.environ['SPIDER_PAYLOAD'].strip('.py')))
                payload.init()
            except:
                print "Failed to load payload " + os.environ['SPIDER_PAYLOAD']
                
        socket.setdefaulttimeout(10)

        pl_total_time = 0
        start_time = time.time()
        
        while True:

            if (self._currlink != "SLEEP"):
                # parse the current link
                print "--------------------------------------------------"
                url = urlparse.urlparse( self._currlink )
                print "Attempting to parse " + self._currlink
                
                self._domain = url[1]
                print "Found domain " + url[1]
                
                html = self.GetHTML()
                self._viewed.append( self._currlink )
                images, addfiles, alts, links = self.ParseHTML( html )

                # apply payload if necessary
                if os.environ['SPIDER_PAYLOAD'] != '':
                    
                    try:
                        pl_start = time.time()
                        payload.new_html( html )
                        pl_stop = time.time()

                        pl_total_time += pl_stop - pl_start
                    except:
                        print "Unable to deploy payload - new_html()"
                        
                # check for previous images
                if len( self._imgext ) > 0:
                    for image in images:
                        
                        alt = alts[ images.index(image) ]
                
                        if not image in self._images:

                            # check size
                            size = self.GetImageSize( image )
                            surr = self.GetExtraText( html, image )
                            self._images.append( image )
                            self.PrintImageInfo( image, alt, surr, size )

                # check for additional files
                if len( self._fileext ) > 0:
                    for addfile in addfiles:
                        if not addfile in self._addfiles:
                            surr = self.GetExtraText( html, addfile )
                            self._addfiles.append( addfile )
                            self.PrintAddFileInfo( addfile, surr )
                            
                # check for links
                for link in links:
                    self._newlinks.append(link)
            else:
                time.sleep(0.5)

            # update curr link
            if len(self._queued) == 0:
                break
                
            self._currlink = self._queued.pop(0)
            
        stop_time = time.time()
        print "------------------------------------------------"
        print "Tims statistics:"
        print "Total Crawl time = %d" % ((stop_time - start_time) / 60)
        print "Total Payload time = %d" % (pl_total_time / 60)
        
        if os.environ['SPIDER_PAYLOAD'] != '':

            try:        
                payload.end( )
            except:
                print "Unable to deploy payload - end()"
                        
        self.SaveState()
        
    def GetImageSize( self, image ):
        "Get the size of the linked image (in KB)"

        import urllib2, os
        
        size = -1
        
        print "Getting image size of " + image
        
        data = pullURLData(image)
        size = len(data) / 1024.
        print "Image size %.2fkB" % size
        return size

    def GetHTML( self ):
        "Get the HTML from the current link"

        import urllib2, robotparser, urlparse, os, time
        
        print "Getting HTML from " + self._currlink
        
        # check robots.txt
        rp = robotparser.RobotFileParser()
        url = urlparse.urlparse( self._currlink )
        
        newurl = []
        
        i = 0
        while i < 2:
            newurl.append(url[i])
            i = i + 1

        newurl.append("robots.txt")
        i = 0
        while i < 3:
            newurl.append("")
            i = i + 1

        robots_url = urlparse.urlunparse(newurl)
        print "Checking robots file " + robots_url
        
        try:
            rp.parse( pullURLData(robots_url) )
            if not rp.can_fetch("urllib2", self._currlink):
                print self._currlink + " disallowed by robots.txt"
                return ""
        except:
            print "Error reading " + robots_url

        # do a quick dos2unix
        data = pullURLData(self._currlink)
        if data == "":
            return data
        f = open("temp.html", "wb").write(data)
        os.system("dos2unix temp.html")
        data = open("temp.html").read()
        
        # remove scripts - badly formed comments can mess things up
        data2 = ""
        for ln in data.split('\n'):

            if ln.find("//--><!]]>") == -1:
                data2 += ln + '\n'
            
        return data2
    
    def ParseHTML( self, page ):
        "Parse the HTML and return the images and links found"

        import re
        import os
        import urlparse
        
        images = []
        addfiles = []
        alts = []
        links = []

        # import and use beautful soup
        from BeautifulSoup import BeautifulSoup
        try:
            soup = BeautifulSoup(page)
        except:
            return images, addfiles, alts, links

        # find the links
        slinks = soup.findAll('a')
        for ln in slinks:

            try:
                ref = ln['href'].strip('\"')

            except:
                continue

            ref = ln['href'].strip('\"')
            if ref.find("mailto") == -1 and ref.find("javascript") == -1:
                
                url = urlparse.urlparse( ref )        
                link = self.MakeFullPath( ref )
                ext = os.path.basename(link)
                        
                if len( ext.split(".") ) > 1:
                    ext = ext.split(".")[1]
                else:
                    ext = ""
                    
                ext_acc = [ "", "html", "htm", "php", "shtml", "cfm", "asp", "aspx"]

                # pull other extensions
                if ext != "" and ext in self._fileext:
                    try:
                        print "Found Document " + link
                        addfiles.append( link )
                    except:
                        print "More unicode trouble"

                try:
                    
                    if ext in ext_acc and link.find("#") == -1:

                        safe_domain = False
                        if len(self._safedomains) == 1 and self._safedomains[0] == '*':
                            safe_domain = True

                        for sd in self._safedomains:
                            if url[1].find(sd.strip('\n')) != -1:
                                safe_domain = True
                                    
                        if (url[1] == self._domain or url[1] == ""):
                            # add a trailing / if it's a directory
                            if os.path.basename(link).find(".") == -1:
                                link = link.rstrip("/") + "/"

                            print "Found link " + link
                            links.append( link )
                        elif not url[1] in self._newdomains and safe_domain:
                            print "Found domain " + url[1]
                            self._newdomains.append(url[1])
                except:
                    print "Error extracting link"

        if len(slinks) == 0:
            # check for redirect
            slinks = soup.findAll('meta')
            for ln in slinks:

                try:
                    if ln["http-equiv"] == "refresh":
                        start = ln['content'].find("URL=")
                        if start == -1:
                            start = ln['content'].find("url=")
                        start = start + 4
                        link =  self.MakeFullPath( ln['content'][ start : ] )
                        
                        # check if this redirects to another domain
                        url = urlparse.urlparse( link )
                        if (url[1] == self._domain or url[1] == ""):
                            links.append( link )
                            print "Found link " + link
                        else:
                            safe_domain = False
                            if len(self._safedomains) == 1 and self._safedomains[0] == '*':
                                safe_domain = True

                            for sd in self._safedomains:
                                if url[1].find(sd.strip('\n')) != -1:
                                    safe_domain = True

                            if not url[1] in self._newdomains and safe_domain:
                                print "Found domain " + url[1]
                            self._newdomains.append(url[1])

                except:
                    continue
                
                        
        # find the images
        slinks = soup.findAll('img')

        for ln in slinks:

            try:
                ref = ln['src'].strip('\"')

            except:
                continue
                            
            ref = ln['src']
            image = ""

            for imgext in self._imgext:
                if ref.find("." + imgext) != -1:
                    image = self.MakeFullPath(ref.strip("\""))

            try:
                alt = ln['alt']
                alt.find("test")
                
            except:
                alt = ""

            if image != "":
                try:
                    print "Found image " + image
                    images.append( image )
                    alts.append( alt )
                except:
                    print "Odd Image name. Ignoring"
                                            
        return images, addfiles, alts, links

    def MakeFullPath( self, link):

        import urlparse
        import os
        
        url = urlparse.urlparse( link )
        currlinkurl = urlparse.urlparse( self._currlink )
        
        newurl = []
        
        i = 0
        while i < 3:
            ln = url[i]
            ln = ln.replace(' ', '')
            ln = ln.replace('\n', '')
            ln = ln.replace('\r', '')
            newurl.append(ln)
            i = i + 1

        i = 0
        while i < 3:
            newurl.append("")
            i = i + 1
                            
        if newurl[1] == "":
            # check for relative/absolute paths
            if len(newurl[2]) > 0 and newurl[2][0] != "/":
                newurl[2] = os.path.join(os.path.dirname(currlinkurl[2]), newurl[2])
                newurl[2] = os.path.normpath(newurl[2])
                                
            newurl[1] = currlinkurl[1]
            newurl[0] = currlinkurl[0]
        
        return urlparse.urlunparse( newurl )
    
    def GetExtraText( self, html, image ):
        "parse for the image url and return any alternate text, etc."

        import os

        try:
            size = 200
            name = os.path.basename(image)
            start = html.find( name ) - size
            if start < 0:
                start = 0

            stop = html.find( name ) + len(image) + size
            if stop > len(html):
                stop = len(html)

            text = html[start:stop]
        except:
            # there's some problem scanning the html
            print "Error getting extra text from " + self._currlink
            text = ""
            
        return text.replace("\n", "")

    def PrintImageInfo( self, image, alt, surr, size ):
        "print the image info to a file"

        f = open( "image_info.txt", "a")
        
        f.write("------------------------------------------------------\n")
        f.write("File:     " + image + "\n")
        f.write("Page:     " + self._currlink + "\n")
        str = "Size:     %.2d" % size + "KB\n"
        f.write(str)

        try:
            f.write("Alt Text: " + alt + "\n")
        except:
            f.write("Atl Text:\n")
            
        f.write("Surround: " + repr(surr) + "\n")
        
        f.close()

    def PrintAddFileInfo( self, file, surr, ):
        "print the additional file info"

        f = open( "ext_info.txt", "a")
        
        f.write("------------------------------------------------------\n")
        f.write("File:     " + file + "\n")
        f.write("Page:     " + self._currlink + "\n")
        f.write("Size:\n");
        f.write("Atl Text:\n")
        f.write("Surround: " + repr(surr) + "\n")
        
        f.close()
        
    def SaveState( self ):
        "save the state of the crawler"

        # viewed lists
        file = open("viewed_list.txt", "w")
        for url in self._viewed:
            try:
                file.write( url + "\n" )
            except:
                continue
        file.close()

        # queue list
        file = open("links_list.txt", "w")
        for url in self._newlinks:
            try:
                file.write( url )
                file.write( "\n" )
            except:
                continue

        file.close()

        # image list
        file = open("img_list.txt", "w")
        for url in self._images:
            try:
                file.write( url + "\n" )
            except:
                continue

        file.close()
                
        # external list
        file = open("ext_list.txt", "w")
        for url in self._addfiles:
            try:
                file.write( url + "\n" )
            except:
                continue
        file.close()

import sys

#if len(sys.argv) != 2:
#    print "Not enough arguments given."
#    print "usage is ./WebCrawler.py <domain_list>"
#    print "e.g. ./WebCrawler.py domList.txt"
#    sys.exit(2)

print "+------------------------------------------------------------+"
print "|                                                            |"
print "|             HTML Image Parser                              |"
print "|                                                            |"
print "|                                      by Mark Slater        |"
print "+------------------------------------------------------------+"
#print "\n   Check img_list.txt for the images and spider_output.txt" 
#print "           for the spider's output. Enjoy!\n"

# set off a single parser
wc = ImageParser( 'input_list.txt', 'ext.txt' )
wc.Crawl()

# output a domain list
f = open("dom_list.txt", "w")
                
for dom in wc._newdomains:

    f.write(dom + "\n")

f.close()

