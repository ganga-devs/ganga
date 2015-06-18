##########################################################################
# Ganga Project. http://cern.ch/ganga
#
##########################################################################

from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException, IPostProcessor
from Ganga.GPIDev.Schema import Schema, SimpleItem, Version
from Ganga.Utility.logging import getLogger
import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('Configuration')
import smtplib
import email

logger = getLogger()
# set the checkers config up


class Notifier(IPostProcessor):

    """
    Object which emails a user about jobs status are they have finished. The default behaviour is to email when a job has failed or when a master job has completed.
    Notes: 
    * Ganga must be running to send the email, so this object is only really useful if you have a ganga session running the background (e.g. screen session).
    * Will not send emails about failed subjobs if autoresubmit is on.
    """
    _schema = Schema(Version(1, 0), {
        'verbose': SimpleItem(defvalue=False, doc='Email on subjob completion'),
        'address': SimpleItem(defvalue='', doc='Email address', optional=False)
    })
    _category = 'postprocessor'
    _name = 'Notifier'
    order = 3

    def execute(self, job, newstatus):
        """
        Email user if:
        * job is a master job, or
        * job has failed but do not have auto resubmit
        * job has not failed but verbose is set to true
        """
        if len(job.subjobs) or (newstatus == 'failed' and job.do_auto_resubmit is False) or (newstatus != 'failed' and self.verbose is True):
            return self.email(job, newstatus)
        return True

    def email(self, job, newstatus):
        """
        Method to email a user about a job
        """
        sender = 'project-ganga-developers@cern.ch'
        receivers = self.address

        subject = 'Ganga Notification: Job(%s) has %s.' % (job.fqid, newstatus)
        msg_string = """
Dear User,\n
Job(%s) has gone into %s state.\n
Regards,
Ganga\n
PS: This is an automated notification from Ganga, 
if you would like these messages to stop please 
remove the notifier object from future jobs.
        """ % (job.fqid, newstatus)
        msg = email.message_from_string(msg_string)
        msg['Subject'] = subject
        msg['From'] = sender
        msg['To'] = receivers
        string_message = msg.as_string()
        try:
            smtpObj = smtplib.SMTP(config['SMTPHost'])
            smtpObj.sendmail(sender, receivers, string_message)

        except smtplib.SMTPException as e:
            raise PostProcessException(str(e))
        return True
