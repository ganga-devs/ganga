
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Common XMLPostProcessing code for AppBase, Bender and GaudiPython objects for
   LHCb. This is common to these jobs but relies on the LHCb output information.'''

# Required for post-processing script
import os
import sys
import re
from GangaLHCb.Lib.Applications.AppsBaseUtils import backend_handlers, activeSummaryItems

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


def _XMLJobFiles():
    return ['summary.xml', '__parsedxmlsummary__']

# Post-Processing script taken from AppBase to be shared among multiple
# Job types

def postprocess(self, logger):

    j = self.getJobObject()
    parsedXML = os.path.join(j.outputdir, '__parsedxmlsummary__')
    # use to avoid replacing 'lumi' etc as return value and not the method
    # pointer
    metadataItems = {}
    if os.path.exists(parsedXML):
        #Get rid of the long representation
        xml_string = open(parsedXML).read()
        xml_string = re.sub(r'(\d)L([\,\}])', r'\1\2', xml_string)
        exec(compile(xml_string, parsedXML, 'exec'), {}, metadataItems)

    # Combining subjobs XMLSummaries.
    if j.subjobs:
        env = self.getenv(self.is_prepared is None)
        if 'XMLSUMMARYBASEROOT' not in env:
            logger.warning(
                '"XMLSUMMARYBASEROOT" env var not defined so summary.xml files not merged for subjobs of job %s' % j.fqid)
            return

        summaries = []
        for sj in j.subjobs:
            outputxml = os.path.join(sj.outputdir, 'summary.xml')
            if not os.path.exists(outputxml):
                logger.warning("XMLSummary for job %s subjobs will not be merged as 'summary.xml' not present in job %s outputdir" % (j.fqid, sj.fqid))
                return
            elif os.path.getsize(outputxml) == 0 or os.stat(outputxml).st_size == 0:
                logger.warning("XMLSummary for job %s subjobs will not be merged as %s appears to be an empty file" % (j.fqid, outputxml))
                logger.warning("Please try to recreate this file by either resubmitting your job or re-downloading the data from the backend")
                return
            summaries.append(outputxml)

        # Not needed now that we dont merge if ANY of subjobs have missing summary.xml
        #if not summaries:
        #    logger.debug('None of the subjobs of job %s produced the output XML summary file "summary.xml". Merging will therefore not happen' % j.fqid)
        #    return

        schemapath = os.path.join(env['XMLSUMMARYBASEROOT'], 'xml/XMLSummary.xsd')
        from GangaLHCb.Lib.XMLSummary.summary import Merge

        try:
            XMLSummarydata = Merge(summaries, schemapath)
        except Exception as err:
            logger.error('Problem while merging the subjobs XML summaries')
            raise

        for name, method in activeSummaryItems().items():
            try:
                metadataItems[name] = method(XMLSummarydata)
            except:
                metadataItems[name] = None
                logger.debug('Problem running "%s" method on merged xml output.' % name)

    for key, value in metadataItems.items():
        if value is None:  # Has to be explicit else empty list counts
            j.metadata[key] = 'Not Available.'
        else:
            j.metadata[key] = value

def GaudiExecPostProcess(self, logger):
    from GangaGaudi.Lib.Applications import GaudiXMLSummary
    j = self.getJobObject()
    metadataItems = {}
    #If this is a subjob then get the metadata from the summary.xml file
    if os.path.exists(os.path.join(j.outputdir, 'summary.xml')):
       sjSummary =  GaudiXMLSummary(j, 'summary.xml').summary()
       sjMetadataItems = {}
       for name, method in activeSummaryItems().items():
           try:
               sjMetadataItems[name] = method(sjSummary)
           except:
               sjMetadataItems[name] = None
               logger.debug('Problem running "%s" method on merged xml output.' % name)

       for key, value in sjMetadataItems.items():
           if value is None:  # Has to be explicit else empty list counts
               j.metadata[key] = 'Not Available.'
           else:
               j.metadata[key] = value

    # Combining subjobs XMLSummaries.
    if j.subjobs:
        env = self.getenv(self.is_prepared is None)
        if 'XMLSUMMARYBASEROOT' not in env:
            logger.warning(
                '"XMLSUMMARYBASEROOT" env var not defined so summary.xml files not merged for subjobs of job %s' % j.fqid)
            return

        summaries = []
        for sj in j.subjobs:
            outputxml = os.path.join(sj.outputdir, 'summary.xml')
            if not os.path.exists(outputxml):
                logger.warning("XMLSummary for job %s subjobs will not be merged as 'summary.xml' not present in job %s outputdir" % (j.fqid, sj.fqid))
                return
            elif os.path.getsize(outputxml) == 0 or os.stat(outputxml).st_size == 0:
                logger.warning("XMLSummary fro job %s subjobs will not be merged as %s appears to be an empty file" % (j.fqid, outputxml))
                logger.warning("Please try to recreate this file by either resubmitting your job or re-downloading the data from the backend")
                return
            summaries.append(outputxml)

        # Not needed now that we dont merge if ANY of subjobs have missing summary.xml
        #if not summaries:
        #    logger.debug('None of the subjobs of job %s produced the output XML summary file "summary.xml". Merging will therefore not happen' % j.fqid)
        #    return

        schemapath = os.path.join(env['XMLSUMMARYBASEROOT'], 'xml/XMLSummary.xsd')
        from GangaLHCb.Lib.XMLSummary.summary import Merge

        try:
            XMLSummarydata = Merge(summaries, schemapath)
        except Exception as err:
            logger.error('Problem while merging the subjobs XML summaries')
            raise

        for name, method in activeSummaryItems().items():
            try:
                metadataItems[name] = method(XMLSummarydata)
            except:
                metadataItems[name] = None
                logger.debug('Problem running "%s" method on merged xml output.' % name)

    for key, value in metadataItems.items():
        if value is None:  # Has to be explicit else empty list counts
            j.metadata[key] = 'Not Available.'
        else:
            j.metadata[key] = value

