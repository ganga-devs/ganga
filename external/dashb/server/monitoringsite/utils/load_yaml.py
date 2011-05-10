# L. Kokoszkiewicz
# lukasz.kokoszkiewicz@cern.ch

from django.core.management.base import NoArgsCommand
from monitoringsite.gangamon.eventproc import *
#from monitoringsite.gangamon.models import *

import yaml
    
def importFromYaml():
    p = GangaEventProcessor()
    count = 0
    
    filename = raw_input('Enter YAML file path: ')
    print 'Importing data from "%s"' % filename
    try:
        f = open(filename, 'rb')
    except IOError:
        print 'Error! File %s doesn\'t exists!' % filename
        return
        
    try:
        data=yaml.safe_load_all(f)
    except:
        print 'Error! Wrong data format.'
        return
    
    for message in data:
        try:
            headers = message['headers']
            body = eval(message['body'])
            timestamp = headers['_publisher_timestamp']
            timestamp = timestamp[:timestamp.find('.')]
            
            data = [timestamp,body['event'],body]
            
            p.process_event(data)
            count = count + 1
        except:
            pass
        
    print '\nProcessed %d events.' % count

