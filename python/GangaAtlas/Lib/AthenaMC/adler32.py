#exec python -x "$0" "$@"
import zlib,sys

#adler starting value is _not_ 0L
adler=1
filename=sys.argv[1]

try:
    openFile = open(filename, 'rb')
    
    for line in openFile:
        adler=zlib.adler32(line, adler)
        
except:
    raise Exception('Could not get checksum of %s'%filename)

openFile.close()

#backflip on 32bit
if adler < 0:
    adler = adler + 2**32
    
print str('%08x'%adler) #return as padded hexified string

