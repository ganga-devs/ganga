from django.conf.urls.defaults import *
import settings

#from settings import SUB_DIRECTORY

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns('',

    (r'^django_media/(?P<path>.*)$', 'django.views.static.serve', {'document_root': settings.MEDIA_ROOT}),
    (r'^gangamon/', include('monitoringsite.gangamon.urls')),   
    (r'^usage/', include('monitoringsite.gangausage.urls')), 
    (r'^errorreports/', include('monitoringsite.gangaerrorreport.urls')),
    (r'^cmserrorreports/', include('monitoringsite.cmserrorreport.urls')),                    
    (r'^usage', 'monitoringsite.gangausage.views.current'),
    (r'^errorreports', 'monitoringsite.gangaerrorreport.views.default'),
    (r'^cmserrorreports', 'monitoringsite.cmserrorreport.views.default'),                               
    

    # Uncomment the admin/doc line below and add 'django.contrib.admindocs' 
    # to INSTALLED_APPS to enable admin documentation:
    # (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    (r'^admin/', include(admin.site.urls))
)
