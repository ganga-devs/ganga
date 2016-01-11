# XMLSummary parsing
import os, sys

if 'XMLSUMMARYBASEROOT' not in os.environ:
    sys.stderr.write("'XMLSUMMARYBASEROOT' env var not defined so summary.xml not parsed")
else:
    schemapath  = os.path.join(os.environ['XMLSUMMARYBASEROOT'],'xml/XMLSummary.xsd')
    summarypath = os.path.join(os.environ['XMLSUMMARYBASEROOT'],'python/XMLSummaryBase')
    sys.path.append(summarypath)
    import summary

    outputxml = os.path.join(os.getcwd(), 'summary.xml')
    if not os.path.exists(outputxml):
        sys.stderr.write("XMLSummary not passed as 'summary.xml' not present in working dir")
    else:
        try:
            XMLSummarydata = summary.Summary(schemapath,construct_default=False)
            XMLSummarydata.parse(outputxml)
        except:
            sys.stderr.write("Failure when parsing XMLSummary file 'summary.xml'")

        try:
            fn = open('__parsedxmlsummary__','w')
            for name, method in activeSummaryItems().iteritems():
                try:
                    fn.write( '%s = %s\n' % ( name, str(method(XMLSummarydata)) ) )
                except Exception, e:
                    fn.write( '%s = None\n' % name )
                    sys.stderr.write('XMLSummary warning: Method "%s" not available for this job\n' % name)
        except:
            sys.stderr.write('XMLSummary error: Failed to create __parsedxmlsummary__ file')
        finally:
            fn.close()

