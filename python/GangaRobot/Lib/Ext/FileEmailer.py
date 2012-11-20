"""File emailer IAction implementation.

The FileEmailer class can be configured to email text and/or html files.
 
"""

from GangaRobot.Framework.Action import IAction
from GangaRobot.Framework import Utility
from Ganga.Utility.logging import getLogger
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from smtplib import SMTP

logger = getLogger()

class FileEmailer(IAction):
    
    """File emailer IAction implementation.
    
    A configurable action which emails text and/or html files to one or more
    recipients. See execute() for details.
    
    """

    def execute(self, runid):
        """Send files as an email.
        
        Keyword arguments:
        runid -- A UTC ID string which identifies the run.
        
        The following parameters are loaded from the Robot configuration:
        FileEmailer_Host e.g. localhost or localhost:25
        FileEmailer_Type e.g. text or html
        FileEmailer_From e.g. sender@domain.org
        FileEmailer_Recipients e.g. recipient1@domain.org, recipient2@domain.org
        FileEmailer_Subject e.g. Report ${runid}.
        FileEmailer_TextFile e.g. ~/gangadir/robot/report/${runid}.txt
        FileEmailer_HtmlFile e.g. ~/gangadir/robot/report/${runid}.html
        
        If Recipients are not specified then no email is sent.
        In Subject, TextFile and HtmlFile the token ${runid} is replaced by the
        runid argument.
        If Type is text, then TextFile is sent.
        If Type is html, then HtmlFile is sent, or if TextFile is also specified
        then a multipart message is sent containing TextFile and HtmlFile.
        
        """
        # get configuration properties
        host = self.getoption('FileEmailer_Host')
        type = self.getoption('FileEmailer_Type')
        from_ = self.getoption('FileEmailer_From')
        # extract recipients ignoring blank entries
        recipients = [recipient.strip() for recipient in \
                      self.getoption('FileEmailer_Recipients').split(',') \
                      if recipient.strip()]
        subject = Utility.expand(self.getoption('FileEmailer_Subject'), runid = runid)
        textfilename = Utility.expand(self.getoption('FileEmailer_TextFile'), runid = runid)
        htmlfilename = Utility.expand(self.getoption('FileEmailer_HtmlFile'), runid = runid)
        
        if not recipients:
            logger.warn('No recipients specified. Email will not be sent.')
            return
        
        logger.info('Emailing files to %s.', recipients)

        # build message
        if type == 'html':
            msg = self._gethtmlmsg(textfilename, htmlfilename)
        else:
            msg = self._gettextmsg(textfilename)
        msg['Subject'] = subject
        msg['From'] = from_
        msg['To'] = ', '.join(recipients)

        # send message
        session = SMTP()
        try:
            session.connect(host)
            session.sendmail(from_, recipients, msg.as_string())
            session.quit()
        finally:
            session.close()

        logger.info('Files emailed.')
            
    def _gettextmsg(self, textfilename):
        text = Utility.readfile(textfilename)
        textmsg = MIMEText(text)
        return textmsg
    
    def _gethtmlmsg(self, textfilename, htmlfilename):
        html = Utility.readfile(htmlfilename)
        htmlmsg = MIMEText(html, 'html')
        if textfilename:
            textmsg = self._gettextmsg(textfilename)
            multimsg = MIMEMultipart('alternative')
            multimsg.attach(textmsg)
            multimsg.attach(htmlmsg)
            return multimsg
        else:
            return htmlmsg
        
