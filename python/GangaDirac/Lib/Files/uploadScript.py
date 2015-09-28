
import os
## NB parseCommandLine first then import Dirac!!
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
dirac=Dirac()
errmsg=''
file_name='###UPLOAD_FILE###'
file_label=file_name

compressed = ###COMPRESSED###
if compressed:
    import gzip
    file_name += '.gz'
    f_in = open(file_label, 'rb')
    f_out = gzip.open(file_name, 'wb')
    f_out.writelines(f_in)
    f_out.close()
    f_in.close()

lfn= os.path.join('###LFN_BASE###', file_name)
wildcard='###WILDCARD###'
storage_elements=###SEs###

from contextlib import closing
with closing(open('###LOCATIONSFILE_NAME###','ab')) as locationsfile:
    for se in storage_elements:
        try:
            result = dirac.addFile(lfn, file_name, se)
        except Exception as x:
            import sys
            sys.stdout.write('Exception running dirac.addFile command: %s' % str(x))
            break
        if result.get('OK',False) and lfn in result.get('Value',{'Successful':{}})['Successful']:
            import datetime
            guid = dirac.getMetadata(lfn)['Value']['Successful'][lfn]['GUID']
            locationsfile.write("DiracFile:::%s&&%s->%s:::['%s']:::%s\\n" % (wildcard, file_label, lfn, se, guid))
            locationsfile.flush()
            locationsfile.close()
            break
        errmsg+="(%s, %s), " % (se, result)
    else:
        locationsfile.write("DiracFile:::%s&&%s->###FAILED###:::File '%s' could not be uploaded to any SE (%s):::NotAvailable\\n" % (wildcard, file_label, file_name, errmsg))
        import sys
        sys.stdout.write('Could not upload file %s' % file_name)

