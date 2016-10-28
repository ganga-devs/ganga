
import os
## NB parseCommandLine first then import Dirac!!
import datetime
import gzip
import sys
from contextlib import closing
from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC.Interfaces.API.Dirac import Dirac
parseCommandLine()
dirac=Dirac()
errmsg=''
file_name='###UPLOAD_FILE###'
file_label=file_name

compressed = ###COMPRESSED###
if compressed:
    file_name += '.gz'
    f_in = open(file_label, 'rb')
    f_out = gzip.open(file_name, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

lfn=os.path.join('###LFN_BASE###', file_name)
wildcard='###WILDCARD###'
storage_elements=###SEs###

with closing(open('###LOCATIONSFILE_NAME###','ab')) as locationsfile:
    for se in storage_elements:
        sys.stdout.write('\nUploading file: \"%s\" as \"%s\" at \"%s\"\n' % (file_name, lfn, se))
        try:
            result = dirac.addFile(lfn, file_name, se)
        except Exception as x:
            sys.stdout.write('Exception running dirac.addFile command: %s' % str(x))
            break
        if result.get('OK',False) and lfn in result.get('Value',{'Successful':{}})['Successful']:
            guid = dirac.getMetadata(lfn)['Value']['Successful'][lfn]['GUID']
            locationsfile.write("DiracFile:::%s&&%s->%s:::['%s']:::%s\\n" % (wildcard, file_label, lfn, se, guid))
            locationsfile.flush()
            locationsfile.close()
            break
        errmsg+="(%s, %s), " % (se, result)
    else:
        locationsfile.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' could not be uploaded to any SE (%s):::NotAvailable\\n" % (wildcard, file_label, file_name, errmsg))
        sys.stdout.write('Could not upload file %s' % file_name)

