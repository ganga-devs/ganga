from django.conf.urls.defaults import *

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('monitoringsite.gangaerrorreport.views',
    (r'^reports', 'default'),   
    (r'^download/(\d+)', 'download'),   
    (r'^login/(\d+)', 'login'),
    (r'^get_reports_JSON', 'get_reports_JSON'), 
)
