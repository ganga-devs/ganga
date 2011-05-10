from django.conf.urls.defaults import *

urlpatterns = patterns('monitoringsite.gangausage.views',
    (r'^current', 'current'),
    (r'^day-view', 'dayView'),
    (r'^week-view', 'weekView'),        
    (r'^month-view', 'monthView'),                  
)
